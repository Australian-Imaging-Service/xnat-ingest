"""Finding staged sessions and their resources on XNAT, and comparing them with
what XNAT holds"""

import datetime
import pprint
import re
import threading
import typing as ty
from collections import defaultdict
from urllib.parse import quote

import attrs
import xnat
from dateutil import parser as dateutil_parser
from fileformats.medimage import DicomCollection, DicomImage
from fileformats.vendor.xnat.medimage import Snapshots

from accessionflow.exceptions import (
    IncompleteCheckumsException,
    SessionIdentityRefused,
    UnknownStudyIdentity,
    UnsupportedModalityError,
)
from accessionflow.helpers.logging import logger
from accessionflow.helpers.remotes import (
    LocalSessionListing,
    S3SessionListing,
    S3SessionOnlyListing,
    SessionListing,
    SessionOnlyListing,
    read_grouping_uid,
)
from accessionflow.model.accession import Accession
from accessionflow.model.acquisition import Acquisition
from accessionflow.model.resource import Resource

from .scan_types import (
    SNAPSHOTS_RESOURCE_LABEL,
    group_by_modality,
    xnat_resource_label_from_sop_class,
    xnat_scan_type_from_sop_class,
)

IDENTITY_FIELD = "xnat_ingest_uid"
IDENTITY_PATH = f"fields/field[name={IDENTITY_FIELD}]/field"
IDENTITY_HEADERS = {"Cache-Control": "no-cache", "Pragma": "no-cache"}

# An upload loop keeps this first-listing result across sessions and passes.
_xnat_has_checksums: bool | None = None
_xnat_checksum_warning_lock = threading.Lock()


@attrs.define
class XnatRows:
    """One XNAT REST listing as plain rows, without xnatpy objects.

    xnatpy 0.7.2 retains listing objects in a class-level registry. In 0.8.1,
    the registry instead accumulates weak references during repeated reads.
    Code that runs on every pass of `upload --loop` reads XNAT through this
    class without adding registry entries. It sends the request that
    xnatpy's XNATListing sends and applies the same rules to the result.
    """

    rows: list[dict[str, ty.Any]]

    @classmethod
    def read(
        cls,
        connection: xnat.XNATSession,
        uri: str,
        lookup_field: str,
        xsi_type: str | None = None,
        fresh: bool = False,
    ) -> "XnatRows":
        columns = f"ID,URI,{lookup_field}"
        if xsi_type is None:
            columns += ",xsiType"
        if fresh:
            result = connection.get(
                uri, format="json", query={"columns": columns}, headers=IDENTITY_HEADERS
            ).json()
        else:
            result = connection.get_json(uri, query={"columns": columns})
        rows = []
        for row in result["ResultSet"]["Result"]:
            if "URI" not in row and "ID" not in row:
                # A resource row has no ID column (as in xnatpy)
                row["ID"] = row.get("xnat_abstractresource_id", row.get("label"))
            row_type = row.get("xsiType", row.get("element_name", xsi_type)) or ""
            if not str(row.get("ID") or "").strip() or not row_type.strip():
                continue  # xnatpy skips these as empty objects
            rows.append(row)
        return cls(rows)

    def by_id(self) -> dict[str, dict[str, ty.Any]]:
        """The rows by ID, as XNATListing.values() sees them."""
        return {row["ID"]: row for row in self.rows}

    def lookup(self, key: str, lookup_field: str) -> dict[str, ty.Any]:
        """Find a row by ID, then by `lookup_field`, as XNATListing does.

        Raises
        ------
        KeyError
            if no row matches, or more than one row has `key` in `lookup_field`
        """
        by_id = self.by_id()
        if key in by_id:
            return by_id[key]
        matches = [row for row in self.rows if row.get(lookup_field) == key]
        if len(matches) != 1:
            raise KeyError(key)
        return matches[0]


def _assert_session_identity(
    connection: xnat.XNATSession,
    uri: str,
    grouping_uid: str | None,
    project_id: str | None = None,
    subject_id: str | None = None,
    adopt: bool = False,
) -> None:
    """Check fresh subject and grouping identity before reuse or completeness.

    A legacy experiment can adopt the incoming identity only in the upload path,
    after its subject matches. This does not prove the identity of its old files,
    and the read/write/read sequence is not an atomic claim against other writers.
    Session-only uploads have no expected subject and cannot adopt an identity.
    """
    if subject_id is None and grouping_uid is None:
        return
    try:
        items = connection.get(uri, format="json", headers=IDENTITY_HEADERS).json()[
            "items"
        ]
        if len(items) != 1:
            raise ValueError("expected one experiment document")
        item = items[0]
        fields = item["data_fields"]
        if subject_id is not None:
            subjects = XnatRows.read(
                connection,
                f"/data/projects/{project_id}/subjects",
                "label",
                xsi_type="xnat:subjectData",
                fresh=True,
            )
            subject = subjects.lookup(subject_id, "label")
            if fields.get("subject_ID") != subject["ID"]:
                raise ValueError("subject mismatch")
        if grouping_uid is None:
            return  # Legacy local metadata still gets the subject check.
        values = []
        children = item.get("children", [])
        if not isinstance(children, list):
            raise ValueError("malformed custom fields")
        for child in children:
            if child["field"] == "fields/field":
                if not isinstance(child["items"], list):
                    raise ValueError("malformed custom field entries")
                for entry in child["items"]:
                    custom = entry["data_fields"]
                    if custom["name"] == IDENTITY_FIELD:
                        values.append(custom["field"])
        if len(values) > 1:
            raise ValueError("duplicate grouping identity")
        remote_uid = values[0] if values else None
        if remote_uid not in (None, ""):
            if not isinstance(remote_uid, str) or remote_uid != grouping_uid:
                raise ValueError("grouping identity mismatch")
            return
    except Exception as exc:
        # Do not disclose patient identifiers or fall back after unreadable evidence.
        raise SessionIdentityRefused("Cannot verify XNAT session identity") from exc
    if not adopt or subject_id is None:
        raise UnknownStudyIdentity(
            "Cannot verify XNAT session identity: grouping UID absent"
        )
    meta = item.get("meta")
    xsi_type = meta.get("xsi:type") if isinstance(meta, dict) else None
    if (
        not isinstance(xsi_type, str)
        or not re.fullmatch(r"[A-Za-z_][\w.-]*:[A-Za-z_][\w.-]*", xsi_type)
        or not xsi_type.endswith("SessionData")
    ):
        raise SessionIdentityRefused(
            "Cannot verify XNAT session identity: invalid experiment type"
        )
    logger.warning(
        "XNAT session has no ingest grouping identity; old identity is unverified. "
        "Adopting the incoming identity after the subject check",
        extra={"event": "xnat_identity_adoption"},
    )
    try:
        scoped_uri = (
            f"/data/projects/{quote(project_id, safe='')}"
            f"/subjects/{quote(subject_id, safe='')}"
            f"/experiments/{quote(fields['ID'], safe='')}"
        )
        response = connection.put(
            scoped_uri,
            query={f"{xsi_type}/{IDENTITY_PATH}": grouping_uid},
            accepted_status=tuple(range(200, 300)),
        )
        if not 200 <= response.status_code < 300:
            raise ValueError("identity update was not accepted")
    except Exception as exc:
        raise SessionIdentityRefused(
            "Cannot verify XNAT session identity: identity update failed"
        ) from exc
    _assert_session_identity(connection, uri, grouping_uid, project_id, subject_id)


def _find_xnat_session_by_label(
    connection: xnat.XNATSession, session_id: str
) -> ty.Any:
    matches = [
        experiment
        for experiment in connection.experiments.values()
        if experiment.label == session_id
    ]
    if len(matches) > 1:
        raise RuntimeError(
            f"Multiple XNAT sessions found with label '{session_id}'. "
            "Session labels must be globally unique for session-only uploads."
        )
    return matches[0] if matches else None


def _find_xnat_session_uri_by_label(
    connection: xnat.XNATSession, session_id: str
) -> ty.Optional[str]:
    experiments = XnatRows.read(connection, "/data/experiments", "label")
    matches = [
        experiment
        for experiment in experiments.by_id().values()
        if experiment.get("label") == session_id
    ]
    if len(matches) > 1:
        raise RuntimeError(
            f"Multiple XNAT sessions found with label '{session_id}'. "
            "Session labels must be globally unique for session-only uploads."
        )
    return f"/data/experiments/{matches[0]['ID']}" if matches else None


def _session_date_from_metadata(session: Accession, field: str) -> str:
    """Resolve a metadata field to one ISO-formatted calendar date."""

    try:
        value = session.metadata[field]
    except KeyError:
        raise ValueError(
            f"Could not set the XNAT session date for '{session.name}': metadata "
            f"field '{field}' was not found. Available metadata fields: "
            f"{sorted(session.metadata.keys())}"
        ) from None

    def iter_values(item: ty.Any) -> ty.Iterator[ty.Any]:
        if isinstance(item, (list, tuple, set)):
            for child in item:
                yield from iter_values(child)
        elif item is not None and item != "":
            yield item

    dates: set[datetime.date] = set()
    for item in iter_values(value):
        if isinstance(item, datetime.datetime):
            parsed = item.date()
        elif isinstance(item, datetime.date):
            parsed = item
        else:
            try:
                parsed = dateutil_parser.parse(str(item)).date()
            except (ValueError, OverflowError) as e:
                raise ValueError(
                    f"Could not set the XNAT session date for '{session.name}': "
                    f"value {item!r} from metadata field '{field}' is not a valid date"
                ) from e
        dates.add(parsed)

    if not dates:
        raise ValueError(
            f"Could not set the XNAT session date for '{session.name}': metadata "
            f"field '{field}' has no date value"
        )
    if len(dates) > 1:
        raise ValueError(
            f"Could not set the XNAT session date for '{session.name}': metadata "
            f"field '{field}' resolves to multiple dates: "
            f"{sorted(date.isoformat() for date in dates)}"
        )
    return next(iter(dates)).isoformat()


def get_xnat_session(
    session: Accession,
    xproject: ty.Any,
    modality: str | None = None,
    date_field: str | None = None,
    skip_uid_check: bool = False,
) -> ty.Any:
    """Get the XNAT session object for the given session

    Parameters
    ----------
    session : Accession
        the session to upload
    xproject : ty.Any
        the XNAT project to upload to
    modality : str, optional
        the DICOM modality code (e.g. 'MR', 'PT', 'CT') that determines the XNAT
        session type when the session needs to be created. Defaults to the modality
        in the session's metadata, ignoring non-image modalities such as structured
        reports and preferring MR, then PT, then CT if it has several. Sessions whose
        modalities have no session type on the server are created as
        'xnat:otherDicomSessionData' unless the modality is given explicitly
    date_field : str, optional
        Session metadata field to use as the date when creating a new XNAT
        session. Existing sessions are returned unchanged.

    Returns
    -------
    xsession : ty.Any
        the XNAT session object
    """
    xclasses = xproject.xnat_session.classes

    grouping_uid = read_grouping_uid(session.metadata)
    try:
        xsession = xproject.experiments[session.session_id]
    except KeyError:
        session_date = (
            _session_date_from_metadata(session, date_field)
            if date_field is not None
            else None
        )
        if modality is not None:
            SessionClass = _xnat_session_class(xclasses, modality)
            if SessionClass is None:
                raise UnsupportedModalityError(
                    f"XNAT server has no 'xnat:{xnat_session_type(modality)}' session "
                    f"type for the '{modality}' modality given for {session.name}"
                )
        else:
            if session.modalities is None:
                raise RuntimeError(
                    f"Cannot determine modality for session {session.name} because it "
                    "has no modalities metadata. Please provide the modality explicitly "
                    "via the 'modality' parameter."
                )
            # Use the session type of the first image modality the server has one
            # for, trying the preferred modalities first
            image_modalities = sorted(
                (m for m in session.modalities if m not in NON_IMAGE_MODALITIES),
                key=lambda m: (
                    PREFERRED_SESSION_MODALITIES.index(m)
                    if m in PREFERRED_SESSION_MODALITIES
                    else len(PREFERRED_SESSION_MODALITIES)
                ),
            )
            SessionClass = next(
                (
                    c
                    for c in (
                        _xnat_session_class(xclasses, m) for m in image_modalities
                    )
                    if c is not None
                ),
                None,
            )
            if SessionClass is None:
                # The session type XNAT itself uses for DICOMs of modalities
                # without a session type of their own
                logger.warning(
                    "No XNAT session type for the modalities of %s (%s), creating "
                    "it as 'xnat:otherDicomSessionData'",
                    session.name,
                    ", ".join(session.modalities),
                )
                try:
                    SessionClass = xclasses.OtherDicomSessionData
                except AttributeError:
                    raise UnsupportedModalityError(
                        f"XNAT server has no session type for the modalities of "
                        f"{session.name} ({', '.join(session.modalities)}), and no "
                        "'xnat:otherDicomSessionData' type to fall back to"
                    ) from None
        xsubject = xclasses.SubjectData(label=session.subject_id, parent=xproject)
        session_kwargs: dict[str, ty.Any] = {
            "label": session.session_id,
            "parent": xsubject,
        }
        if session_date is not None:
            session_kwargs["date"] = session_date
        if grouping_uid is not None:
            session_kwargs[IDENTITY_PATH] = grouping_uid
        xsession = SessionClass(**session_kwargs)
        if grouping_uid is not None:
            _assert_session_identity(
                xproject.xnat_session,
                xsession.uri,
                grouping_uid,
                xproject.id,
                session.subject_id,
            )
    else:
        _assert_session_identity(
            xproject.xnat_session,
            xsession.uri,
            None if skip_uid_check else grouping_uid,
            xproject.id,
            session.subject_id,
            adopt=True,
        )
    return xsession


# The modality used to pick the session type when a session contains more than one,
# e.g. a PET/CT session is stored as a PET session
PREFERRED_SESSION_MODALITIES = ("MR", "PT", "CT")

# DICOM modalities of objects that annotate or are derived from the images in a
# session rather than being acquisitions in their own right, so they shouldn't
# determine its session type (e.g. an MR session with a structured dose report)
NON_IMAGE_MODALITIES = frozenset(
    (
        "SR",  # Structured report
        "PR",  # Presentation state
        "KO",  # Key object selection
        "REG",  # Registration
        "SEG",  # Segmentation
        "FID",  # Fiducials
        "RWV",  # Real world value map
        "DOC",  # Encapsulated document
        "PLAN",  # Plan
        "RTSTRUCT",  # Radiotherapy structure set
        "RTPLAN",  # Radiotherapy plan
        "RTRECORD",  # Radiotherapy treatment record
        "RTDOSE",  # Radiotherapy dose
    )
)

# DICOM modalities whose XNAT session type isn't named after the lower-cased modality
# code (e.g. 'MR' -> 'mrSessionData', 'OPT' -> 'optSessionData'), see the
# 'xnat:*SessionData' types in XNAT's xnat.xsd schema
XNAT_SESSION_TYPE_EXCEPTIONS = {"PT": "petSessionData"}


def xnat_session_type(modality: str) -> str:
    """The XSI type (without the 'xnat:' prefix) of the XNAT session that DICOMs of
    the given modality are stored in, e.g. 'MR' -> 'mrSessionData'"""
    modality = modality.upper()
    return XNAT_SESSION_TYPE_EXCEPTIONS.get(modality, f"{modality.lower()}SessionData")


def _xnat_session_class(xclasses: ty.Any, modality: str) -> ty.Any:
    """The xnatpy class for the session type of the given modality, or None if the
    server has no such type"""
    xsi_type = xnat_session_type(modality)
    # xnatpy names the classes after the XSI type with the first letter capitalised
    return getattr(xclasses, xsi_type[0].upper() + xsi_type[1:], None)


def _xnat_scan_class_from_session(xsession: ty.Any) -> ty.Any:
    """The xnatpy class for the scan type matching the session's type (e.g.
    'xnat:petSessionData' -> 'xnat:petScanData'), or 'xnat:otherDicomScanData' if
    the server has no such type"""
    xclasses = xsession.xnat_session.classes
    session_type = getattr(xsession, "__xsi_type__", None)
    if isinstance(session_type, str) and session_type.endswith("SessionData"):
        scan_type = session_type[: -len("SessionData")] + "ScanData"
        ScanClass = xclasses.XNAT_CLASS_LOOKUP.get(scan_type)
        if ScanClass is not None:
            return ScanClass
    return xclasses.OtherDicomScanData


def _files_held_in(
    local_checksums: ty.Mapping[str, str],
    xnat_checksums_list: ty.Iterable[ty.Mapping[str, str]],
) -> bool:
    """Whether every staged file is held, by name and checksum, somewhere in the
    given XNAT resources. As in compare_resource_with_xnat, a file XNAT has not
    yet calculated a digest for ('') is matched by name only."""
    held: dict[str, set[str]] = defaultdict(set)
    for xnat_checksums in xnat_checksums_list:
        for name, digest in xnat_checksums.items():
            held[name].add(digest)
    return all(
        name in held and (checksum in held[name] or "" in held[name])
        for name, checksum in local_checksums.items()
    )


@attrs.define
class ResourceComparison:
    """How a staged resource compares with what XNAT actually holds.

    Shared by the upload path and api/check_upload_api.py so the two cannot
    drift apart.

    NOTE THE ORIENTATION: `missing` means files WE hold that XNAT does not, the
    ones an upload could supply. check_upload_api names its locals the other way
    round.
    """

    missing: ty.Set[str] = attrs.field(factory=set)
    extra: ty.Set[str] = attrs.field(factory=set)
    differing: ty.Set[str] = attrs.field(factory=set)
    comparable: bool = True
    without_checksums: ty.Set[str] = attrs.field(factory=set)

    @property
    def complete(self) -> bool:
        """XNAT holds everything we do, with nothing unexpected."""
        return not (self.missing or self.extra or self.differing)

    @property
    def repairable(self) -> bool:
        """Safe to fix by uploading the missing files and nothing else.

        Deliberately strict: `extra` and `differing` cannot be resolved by
        uploading, so treating them as repairable would append to a resource
        that is already wrong.
        """
        return bool(self.missing) and not self.extra and not self.differing


def compare_resource_with_xnat(
    local_checksums: ty.Mapping[str, str],
    xnat_checksums: ty.Mapping[str, str],
) -> ResourceComparison:
    """Compare a staged resource's manifest against XNAT's file listing.

    XNAT leaves `digest` empty until a catalog refresh populates it, so files
    uploaded but not yet refreshed report ''. Comparing content in that state
    would call a healthy resource corrupt, so a file with no digest is compared
    by NAME only and recorded in `without_checksums`. `comparable` means at
    least one remote digest exists, not that every file was verified by content.
    api/check_upload_api.py guards the same way.
    """
    local_names = set(local_checksums)
    xnat_names = set(xnat_checksums)
    comparable = any(xnat_checksums.values())
    differing: ty.Set[str] = set()
    if comparable:
        differing = {
            n
            for n in local_names & xnat_names
            if xnat_checksums[n] and xnat_checksums[n] != local_checksums[n]
        }
    return ResourceComparison(
        missing=local_names - xnat_names,
        extra=xnat_names - local_names,
        differing=differing,
        comparable=comparable,
        without_checksums={
            n for n in local_names & xnat_names if not xnat_checksums[n]
        },
    )


def log_name_only_comparison(
    comparison: ResourceComparison, resource: str, session: str
) -> None:
    """Keep per-resource detail at DEBUG; checksum support is reported once."""
    if comparison.without_checksums:
        logger.debug(
            "'%s' in '%s' has %d file(s) compared by file name only: XNAT lists them "
            "without checksums",
            resource,
            session,
            len(comparison.without_checksums),
        )


def _report_xnat_checksum_support(checksums: ty.Mapping[str, str]) -> None:
    """Use the first nonempty resource listing for this process's warning.

    This is an observation of returned digests, not a request for site settings.
    Empty resources do not decide support. Later partial catalogs still use the
    same per-file comparisons, but do not add more warnings.
    """
    global _xnat_has_checksums
    if not checksums:
        return
    with _xnat_checksum_warning_lock:
        if _xnat_has_checksums is None:
            _xnat_has_checksums = any(checksums.values())
            if not _xnat_has_checksums:
                logger.warning(
                    "XNAT returns no file checksums; files are compared by name only"
                )


def split_resource_by_modality(resource: Resource) -> ty.List[Resource]:
    """Split a DICOM scan resource that holds more than one modality into one
    resource per modality, each destined for its own scan.

    XNAT does this itself when it rebuilds a session's scans from the DICOM headers
    (pullDataFromHeaders): a series carrying more than one modality under the same
    SeriesNumber -- e.g. a PET series with an embedded dose-report SR object -- comes
    out as separate scans named '<scan-id>-<modality>' rather than one. Uploading
    everything under the original scan id and letting that pull split it apart
    afterwards is the same class of mistake as pre-creating a "secondary" resource
    (see `xnat_resource_label_from_sop_class`): the files end up moved out from under
    whichever scan/resource accessionflow thinks it created, and a later run that looks
    for the original scan id to decide what is already uploaded won't find it.

    So this predicts the split up front and uploads directly under the scan ids XNAT
    will use, the same way the resource label is chosen to match ahead of time rather
    than corrected after the fact.

    Parameters
    ----------
    resource : Resource
        the staged resource to check for a modality split

    Returns
    -------
    list[Resource]
        `[resource]` unchanged if it isn't a DICOM scan resource, or if every file in
        it shares the same modality; otherwise one resource per modality present,
        named the same as `resource` but scanned under '<scan-id>-<modality>'
    """
    # If there is only one modality, or this isn't a DICOM scan resource, no split is needed.
    if (
        resource.scan is None
        or not isinstance(resource.fileset, DicomCollection)
        or len(resource.metadata.get("Modality", [])) <= 1
    ):
        return [resource]
    # `resource.fileset.contents` and each image's own `.metadata` are cached on the
    # objects themselves once read, so for the common case -- a single-modality series,
    # where this returns `[resource]` unchanged -- this read is not a new cost: the
    # SOP-class-based label/type decision that `get_xnat_resource` makes on the same
    # (unmodified) resource right after reuses the same cached per-file metadata. Only
    # when a split actually happens are fresh sub-filesets built, whose own files get
    # read again for their own SOP class, but a mixed-modality series is the rare case
    # this function exists for in the first place
    groups = group_by_modality(
        (fspath, image.metadata.get("Modality"))
        for image in resource.fileset.contents
        for fspath in image.fspaths
    )
    if groups is None:
        return [resource]
    scan = resource.scan
    return [
        Resource(
            name=resource.name,
            fileset=type(resource.fileset)(fspaths),
            scan=Acquisition(
                id=f"{scan.id}-{modality}", type=scan.type, session=scan.session
            ),
        )
        for modality, fspaths in groups.items()
    ]


def get_xnat_resource(
    resource: Resource, xsession: ty.Any
) -> tuple[ty.Any, ty.Optional[ty.Set[str]]]:
    """Get the XNAT resource object for the given resource

    RETURNS A PAIR AT EVERY EXIT, so the caller cannot mistake "nothing to do"
    for "upload everything". The second element is the set of file names to
    upload, or None meaning "all of them".

    Parameters
    ----------
    resource : Resource
        the resource to upload
    xsession : ty.Any
        the XNAT session object

    Returns
    -------
    xresource : ty.Any
        the XNAT resource object, or None when there is nothing to upload
    only_files : set[str] or None
        the file names still to upload. None means the whole resource, which is
        the case for a resource being created. A set is returned only when the
        resource already exists on XNAT and is short of exactly these files.

    Raises
    ------
    IncompleteCheckumsException
        when the resource on XNAT differs in a way an upload cannot fix
    """
    xclasses = xsession.xnat_session.classes
    resource_name = resource.name

    if resource.scan is None:
        try:
            xresource = xsession.resources[resource_name]
        except KeyError:
            pass
        else:
            # Same three outcomes as the scan branch below: repair, refuse, or
            # nothing to do. The caller reads None as "already uploaded", so
            # returning it for a short resource loses the data silently.
            comparison = compare_resource_with_xnat(
                resource.checksums, get_xnat_checksums(xresource)
            )
            log_name_only_comparison(comparison, resource.path, xsession.label)
            if comparison.repairable:
                logger.warning(
                    "'%s' session resource exists on XNAT but is missing %d of "
                    "%d file(s) held in the staged session. Uploading the "
                    "missing file(s): %s%s",
                    resource_name,
                    len(comparison.missing),
                    len(resource.checksums),
                    sorted(comparison.missing)[:10],
                    "..." if len(comparison.missing) > 10 else "",
                )
                return xresource, comparison.missing
            if not comparison.complete:
                logger.error(
                    # Same literal phrase as the scan branch; see above.
                    "'%s' session resource already exists on XNAT with "
                    "different checksums.\nMissing paths: %s\nAdditional "
                    "paths: %s\nDiffering paths: %s",
                    resource_name,
                    sorted(comparison.missing),
                    sorted(comparison.extra),
                    sorted(comparison.differing),
                )
                if comparison.missing:
                    raise IncompleteCheckumsException(
                        f"'{resource_name}' session resource exists on XNAT but "
                        f"is missing {len(comparison.missing)} file(s) present "
                        "in the staged session, and cannot be repaired by "
                        "uploading because XNAT also holds "
                        f"{len(comparison.extra)} unexpected file(s) and "
                        f"{len(comparison.differing)} file(s) with different "
                        "content. Delete the resource on XNAT to have it "
                        f"uploaded afresh. Missing: {sorted(comparison.missing)[:10]}"
                        + ("..." if len(comparison.missing) > 10 else "")
                    )
            return None, None
        logger.debug(
            "Creating session resource %s in %s", resource_name, xsession.label
        )
        uri = f"{xsession.uri}/resources/{resource_name}"
        xsession.xnat_session.put(uri)
        xsession.clearcache()
        return xsession.xnat_session.create_object(uri), None

    # XNAT decides both the resource label a scan's DICOM is catalogued under and the
    # scan-data type from the SOP class alone -- ImageType isn't consulted -- and
    # regenerates both whenever the scan XML is rebuilt from the headers (e.g. by
    # pullDataFromHeaders). Uploading under a different name/type leaves the catalog
    # pointing at a directory the files aren't in, which breaks downloads, so whatever
    # XNAT would choose is used instead.
    #
    # Only resources XNAT parses as DICOM can determine this. Other formats may
    # expose a SOPClassUID in their metadata without being DICOM upload resources;
    # using that key alone would incorrectly rename their XNAT catalogs.
    if isinstance(resource.fileset, DicomCollection):
        sop_class_uids = resource.metadata.get("SOPClassUID")
    elif isinstance(resource.fileset, DicomImage):
        # Use the image being uploaded, not resource metadata that may have been
        # inherited from a larger source collection.
        sop_class_uids = resource.fileset.metadata.get("SOPClassUID")
        if not sop_class_uids:
            raise ValueError(f"DICOM image in {resource.path} has no SOPClassUID")
        expected_label = xnat_resource_label_from_sop_class(sop_class_uids)
        if resource_name != expected_label:
            raise ValueError(
                f"DICOM image in {resource.path} must be staged under XNAT "
                f"resource {expected_label!r} for its SOP class, not "
                f"{resource_name!r}."
            )
    else:
        sop_class_uids = None
    if sop_class_uids:
        resource_name = xnat_resource_label_from_sop_class(sop_class_uids)
    elif isinstance(resource.fileset, Snapshots):
        # XNAT only shows a scan's snapshots from its SNAPSHOTS resource
        resource_name = SNAPSHOTS_RESOURCE_LABEL
    # A packaged or other non-DICOM resource can retain the source SOP class.
    # It determines the scan datatype, but does not change the staged label.
    scan_sop_class_uids = sop_class_uids or resource.metadata.get("SOPClassUID")

    try:
        xscan = xsession.scans[resource.scan.id]
    except KeyError:
        if scan_sop_class_uids:
            scan_type = xnat_scan_type_from_sop_class(scan_sop_class_uids)
            ScanClass = xclasses.XNAT_CLASS_LOOKUP[f"xnat:{scan_type}"]
        else:
            # No SOP class is available (e.g. vendor raw data), or
            # DICOM whose header didn't carry one -- so the scan takes the modality
            # of its session, whose types are named in parallel with the scan ones
            # (e.g. 'xnat:petSessionData' -> 'xnat:petScanData'). Falls back to the
            # catch-all XNAT itself uses for an unrecognised SOP class.
            ScanClass = _xnat_scan_class_from_session(xsession)
        logger.debug(
            "Creating scan %s in %s", resource.scan.id, resource.scan.session.path
        )
        xscan = ScanClass(
            id=resource.scan.id,
            type=resource.scan.type,
            series_description=resource.scan.type,
            parent=xsession,
        )
    if scan_sop_class_uids:
        expected_scan_type = (
            f"xnat:{xnat_scan_type_from_sop_class(scan_sop_class_uids)}"
        )
        existing_scan_type = getattr(xscan, "__xsi_type__", None)
        if (
            isinstance(existing_scan_type, str)
            and existing_scan_type != expected_scan_type
        ):
            raise ValueError(
                f"XNAT scan {resource.scan.id!r} in {resource.scan.session.path} "
                f"has datatype {existing_scan_type!r}, but its SOPClassUID metadata "
                f"requires {expected_scan_type!r}. Recreate the scan on XNAT "
                "before retrying; XNAT cannot change a scan's datatype."
            )
    try:
        xresource = xscan.resources[resource_name]
    except KeyError:
        pass
    else:
        xnat_checksums = get_xnat_checksums(xresource)
        comparison = compare_resource_with_xnat(resource.checksums, xnat_checksums)
        log_name_only_comparison(comparison, resource.path, resource.scan.path)
        if comparison.repairable:
            # We hold files XNAT does not and nothing else is wrong, so hand
            # back the shortfall. Not the whole resource: XNAT would reject or
            # duplicate the files it already holds.
            logger.warning(
                "'%s' resource in '%s' exists on XNAT but is missing %d of %d "
                "file(s) held in the staged session. Uploading the missing "
                "file(s): %s%s",
                resource_name,
                resource.scan.path,
                len(comparison.missing),
                len(resource.checksums),
                sorted(comparison.missing)[:10],
                "..." if len(comparison.missing) > 10 else "",
            )
            return xresource, comparison.missing
        if not comparison.complete:
            logger.error(
                # Keep this phrase stable: external log alerts may match it.
                "'%s' resource in '%s' already exists on XNAT with different "
                "checksums.\nMissing paths: %s\nAdditional paths: %s\n"
                "Differing paths: %s",
                resource_name,
                resource.scan.path,
                sorted(comparison.missing),
                sorted(comparison.extra),
                sorted(comparison.differing),
            )
            if comparison.missing:
                # Missing alone is repaired above, so XNAT also holds files we
                # do not or a shared file differs. An upload can only add, so it
                # cannot fix either. Raise, so the session is not reported clean.
                raise IncompleteCheckumsException(
                    f"'{resource_name}' resource in '{resource.scan.path}' exists "
                    f"on XNAT but is missing {len(comparison.missing)} file(s) "
                    f"present in the staged session, and cannot be repaired by "
                    f"uploading because XNAT also holds "
                    f"{len(comparison.extra)} unexpected file(s) and "
                    f"{len(comparison.differing)} file(s) with different content. "
                    f"Delete the resource on XNAT to have it uploaded afresh. "
                    f"Missing: {sorted(comparison.missing)[:10]}"
                    + ("..." if len(comparison.missing) > 10 else "")
                )
            if comparison.differing:
                # The listing fetched above, NOT re-fetched per file. Building
                # this dict with a call inside the comprehension issued one full
                # GET of the resource listing per differing file, so a resource
                # re-staged after re-anonymisation, where every name differs,
                # made thousands of requests to write one log line.
                difference = {
                    k: (xnat_checksums[k], resource.checksums[k])
                    for k in sorted(comparison.differing)
                }
                logger.error(
                    "'%s' resource in '%s' already exists on XNAT with "
                    "different checksums. Please delete on XNAT to overwrite:\n%s",
                    resource_name,
                    resource.scan.path,
                    pprint.pformat(difference),
                )
        # Ensure that catalog is rebuilt if the file counts are 0
        if not xscan.files:
            xresource.xnat_session.post(
                "/data/services/refresh/catalog?options=populateStats,append,delete,checksum&"
                f"resource=/archive/experiments/{xsession.id}/scans/{xscan.id}"
            )
            if not xscan.files:
                logger.error(
                    "'%s' resource in '%s' already exists on XNAT with "
                    "and is empty. Please delete on XNAT to overwrite\n",
                    resource_name,
                    resource.scan.path,
                )
        return None, None
    logger.debug(
        "Creating resource %s in %s",
        resource_name,
        resource.scan.path,
    )
    xresource = xscan.create_resource(resource_name)
    return xresource, None


def get_xnat_checksums(xresource: ty.Any) -> dict[str, str]:
    """
    Downloads the MD5 digests associated with the files in a resource.

    Parameters
    ----------
    xresource : xnat.classes.Resource
        XNAT resource to retrieve the checksums from

    Returns
    -------
    dict[str, str]
        the checksums calculated by XNAT
    """
    return get_xnat_checksums_at(
        xresource.xnat_session, xresource.uri + "/files", xresource.id
    )


def get_xnat_checksums_at(
    connection: xnat.XNATSession, files_uri: str, resource_id: ty.Any = None
) -> dict[str, str]:
    """get_xnat_checksums() for a resource given by the URI of its file listing.

    Parameters
    ----------
    connection : xnat.XNATSession
        the XNAT connection to read from
    files_uri : str
        the URI of the resource's file listing ("<resource URI>/files")
    resource_id : Any, optional
        the resource ID to name in an error, by default the URI

    Returns
    -------
    dict[str, str]
        the checksums calculated by XNAT
    """
    result = connection.get(files_uri)
    if result.status_code != 200:
        raise RuntimeError(
            "Could not download metadata for resource {}. Files "
            "may have been uploaded but cannot check checksums".format(
                files_uri if resource_id is None else resource_id
            )
        )
    checksums: dict[str, str] = {}
    for row in result.json()["ResultSet"]["Result"]:
        # Name is only the basename. URI retains the path inside the resource,
        # and its resource identifier may be numeric even when we queried by label.
        # The observed XNAT listing and xnatpy treat this suffix as raw text.
        # URL parsing or decoding would change filenames with literal ?, # or %.
        uri = row.get("URI")
        match = (
            re.fullmatch(r".*?/resources/[^/]+/files/(.+)", uri, flags=re.DOTALL)
            if isinstance(uri, str)
            else None
        )
        if match is None:
            raise ValueError(
                f"XNAT file listing has no resource-relative path: {uri!r}"
            )
        path = match.group(1)
        if row["Name"] not in (path, path.rsplit("/", 1)[-1]):
            raise ValueError(
                f"XNAT file URI {uri!r} does not match its Name {row['Name']!r}"
            )
        if path in checksums:
            raise ValueError(
                f"XNAT file listing repeats resource-relative path {path!r}"
            )
        checksums[path] = row["digest"]
    _report_xnat_checksum_support(checksums)
    return checksums


class XnatSessionListing(SessionListing):
    """A staged session listing that can find its session on XNAT.

    Mixed into each kind of listing in accessionflow.helpers.remotes, before it in
    the bases, so that a session-only listing can override the lookup.
    """

    def find_xnat_session(self, connection: xnat.XNATSession) -> ty.Any:
        """Resolve this listing to the XNAT session it belongs to, or None.

        Separate from all_uploaded() so the completeness rule can be shared.
        Listings differ in how they find their session, by project and label
        here or by a global label search for a session-only staging directory,
        but they must not differ in what counts as fully uploaded.

        Returns
        -------
        ty.Any or None
            the XNAT session object, or None when it does not exist yet
        """
        try:
            xproject = connection.projects[self.project_id]
        except KeyError:
            raise KeyError(
                "Project '{}' does not exist on XNAT".format(self.project_id)
            ) from None
        try:
            xsession = xproject.experiments[self.session_id]
        except KeyError:
            return None
        _assert_session_identity(
            connection, xsession.uri, self.expected_uid, xproject.id, self.subject_id
        )
        return xsession

    def find_xnat_session_uri(self, connection: xnat.XNATSession) -> ty.Optional[str]:
        """Like find_xnat_session(), but returns the session's REST URI, or None.

        It reads XNAT directly and builds no xnatpy objects (see XnatRows).
        """
        projects = XnatRows.read(
            connection, "/data/archive/projects", "name", xsi_type="xnat:projectData"
        )
        try:
            project = projects.lookup(self.project_id, "name")
        except KeyError:
            raise KeyError(
                "Project '{}' does not exist on XNAT".format(self.project_id)
            ) from None
        experiments = XnatRows.read(
            connection, f"/data/projects/{project['ID']}/experiments", "label"
        )
        try:
            experiment = experiments.lookup(self.session_id, "label")
        except KeyError:
            return None
        # The session's own URI, not one under the project. XNAT does not list
        # scans under /data/projects/<P>/experiments/<E>/scans (it returns the
        # session document), and /data/experiments/<E> works for a session
        # that is shared into this project.
        uri = f"/data/experiments/{experiment['ID']}"
        _assert_session_identity(
            connection, uri, self.expected_uid, project["ID"], self.subject_id
        )
        return uri

    def all_uploaded(self, connection: xnat.XNATSession) -> bool:
        """Checks whether all the resources in this session have been uploaded to XNAT

        It reads XNAT directly on every call and builds no xnatpy objects. So
        the answer never comes from a cache, and `upload --loop` does not leave
        objects behind on each pass.

        Parameters
        ----------
        connection : xnat.XNATSession
            the XNAT connection to read from

        Returns
        -------
        bool
            whether every staged resource is complete on XNAT
        """
        try:
            session_uri = self.find_xnat_session_uri(connection)
        except UnknownStudyIdentity:
            return False  # The upload path must explicitly adopt the legacy identity.
        if session_uri is None:
            return False

        # Resource path -> URI of the resource's file listing
        xresources: dict[str, str] = {}
        xresources_by_scan_id: dict[str, list[str]] = defaultdict(list)
        scans = XnatRows.read(connection, f"{session_uri}/scans", "type")
        for scan in scans.by_id().values():
            scan_uri = f"{session_uri}/scans/{scan['ID']}"
            resources = XnatRows.read(connection, f"{scan_uri}/resources", "label")
            for resource in resources.by_id().values():
                files_uri = f"{scan_uri}/resources/{resource['ID']}/files"
                path = f"{scan['ID']}.{scan.get('type')}/{resource.get('label')}"
                xresources[path] = files_uri
                xresources_by_scan_id[str(scan["ID"])].append(files_uri)
        session_resources = XnatRows.read(
            connection, f"{session_uri}/resources", "label"
        )
        for resource in session_resources.by_id().values():
            xresources[resource.get("label")] = (
                f"{session_uri}/resources/{resource['ID']}/files"
            )

        # A resource that EXISTS is not necessarily a resource that is
        # COMPLETE: one holding 5 of 8 files carries the same label as one
        # holding all 8, so labels alone cannot decide this.
        #
        # Manifests are the source of truth for what should be there. A session
        # staged without them is checked by resource path alone, as before.
        try:
            manifests = self.resource_manifests
        except Exception:  # noqa: BLE001 - see below, any failure means "unknown"
            # Unknown is not complete. On the S3 path this is a download and a
            # JSON parse per resource, so one failure discards all of them for
            # the session. Returning False costs a session download and then
            # finds nothing to do, since get_xnat_resource compares each
            # resource itself. Returning True would cost the data.
            logger.warning(
                "Could not read the staged manifests for '%s', so whether its "
                "resources are complete on XNAT cannot be determined. Treating "
                "it as not uploaded and letting the per-resource comparison "
                "decide.",
                self.name,
                exc_info=True,
            )
            return False

        xnat_checksums_cache: dict[str, dict[str, str]] = {}

        def xnat_checksums(files_uri: str) -> dict[str, str]:
            if files_uri not in xnat_checksums_cache:
                xnat_checksums_cache[files_uri] = get_xnat_checksums_at(
                    connection, files_uri
                )
            return xnat_checksums_cache[files_uri]

        for resource_path in sorted(self.resource_paths):
            manifest = manifests.get(resource_path)
            local_checksums = manifest.get("checksums") if manifest else None
            xresource = xresources.get(resource_path)
            if xresource is not None:
                if not local_checksums:
                    continue
                comparison = compare_resource_with_xnat(
                    local_checksums, xnat_checksums(xresource)
                )
                if not comparison.complete:
                    logger.info(
                        "'%s' in '%s' exists on XNAT but is not complete: %d file(s) "
                        "missing, %d unexpected, %d differing. Not skipping the "
                        "session.",
                        resource_path,
                        self.name,
                        len(comparison.missing),
                        len(comparison.extra),
                        len(comparison.differing),
                    )
                    return False
                continue
            # Upload can store a staged resource under a different scan ID or
            # label than its staged path. So look for its files, by
            # name and checksum, across every resource in the matching scans.
            if local_checksums and "/" in resource_path:
                scan_id = resource_path.split("/", 1)[0].split(".", 1)[0]
                candidates = [
                    xr
                    for xscan_id, xrs in xresources_by_scan_id.items()
                    if xscan_id == scan_id or xscan_id.startswith(f"{scan_id}-")
                    for xr in xrs
                ]
                if candidates and _files_held_in(
                    local_checksums, [xnat_checksums(xr) for xr in candidates]
                ):
                    continue
            logger.info(
                "'%s' in '%s' was not found on XNAT. Not skipping the session.",
                resource_path,
                self.name,
            )
            return False
        return True


@attrs.define
class XnatLocalSessionListing(XnatSessionListing, LocalSessionListing):
    """A local staging directory named 'project.subject.session'"""


@attrs.define
class XnatSessionOnlyListing(XnatSessionListing, SessionOnlyListing):
    """A local staging directory named by its existing XNAT session label only"""

    def find_xnat_session(self, connection: xnat.XNATSession) -> ty.Any:
        """Look up an existing XNAT session by label across all accessible projects.

        Returns None if not found, raises RuntimeError if multiple sessions match.
        """
        xsession = _find_xnat_session_by_label(connection, self.session_id)
        if xsession is not None:
            _assert_session_identity(connection, xsession.uri, self.expected_uid)
        return xsession

    def find_xnat_session_uri(self, connection: xnat.XNATSession) -> ty.Optional[str]:
        """Like find_xnat_session(), but returns the session's REST URI, or None.

        It reads XNAT directly and builds no xnatpy objects (see XnatRows).
        """
        uri = _find_xnat_session_uri_by_label(connection, self.session_id)
        if uri is not None:
            _assert_session_identity(connection, uri, self.expected_uid)
        return uri


@attrs.define
class XnatS3SessionListing(XnatSessionListing, S3SessionListing):
    """A staging folder in an S3 bucket named 'project.subject.session'"""


@attrs.define
class XnatS3SessionOnlyListing(XnatS3SessionListing, S3SessionOnlyListing):
    """An S3 staging folder named only by its existing XNAT session label."""

    def find_xnat_session(self, connection: xnat.XNATSession) -> ty.Any:
        xsession = _find_xnat_session_by_label(connection, self.session_id)
        if xsession is not None:
            _assert_session_identity(connection, xsession.uri, self.expected_uid)
        return xsession

    def find_xnat_session_uri(self, connection: xnat.XNATSession) -> ty.Optional[str]:
        uri = _find_xnat_session_uri_by_label(connection, self.session_id)
        if uri is not None:
            _assert_session_identity(connection, uri, self.expected_uid)
        return uri
