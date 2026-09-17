"""Helper functions and classes for working with remote storage locations, such as S3 buckets, SSH servers and XNAT repositories"""

import abc
import datetime
import hashlib
import os
import pprint
import shutil
import tempfile
import threading
import typing as ty
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import attrs
import boto3.resources.base
import paramiko
import xnat
from fileformats.application import Json
from fileformats.core import FileSet
from fileformats.medimage import DicomCollection
from tqdm import tqdm

from ..exceptions import IncompleteCheckumsException
from ..model.resource import ImagingResource
from ..model.session import ImagingSession
from .arg_types import StoreCredentials
from .logging import logger
from .metadata import Metadata
from .xnat_scan_types import xnat_scan_type_from_sop_class


class SessionListing(metaclass=abc.ABCMeta):
    # Every subclass supplies this, as a field or a property, and `ids` below
    # has always relied on it. Declared so that reliance is part of the
    # interface rather than an assumption each caller has to make afresh.
    name: str

    @property
    @abc.abstractmethod
    def cache_path(self) -> Path:
        pass

    @property
    @abc.abstractmethod
    def resource_paths(self) -> set[str]:
        pass

    @property
    def resource_manifests(self) -> dict[str, dict[str, ty.Any]]:
        """The staged manifests keyed by resource path.

        A manifest holds more than checksums, so the values are deliberately
        untyped: "checksums" maps file name to digest, other keys do not.

        Read from the staging directory. A listing that stages elsewhere
        overrides this. With no manifest present this raises, which
        all_uploaded() treats as "cannot check".
        """
        manifests = {}
        for relpath in sorted(self.resource_paths):
            resource_dir = self.cache_path / relpath
            if resource_dir.is_dir():
                manifest = Json(ImagingResource.manifest_fpath(resource_dir))
                manifests[relpath] = manifest.contents
        return manifests

    @property
    def ids(self):
        if "." in self.name:
            ids = self.name.split(".")
        else:
            # For backwards compatibility
            ids = self.name.split("-")[:3]
        return ids

    @property
    def project_id(self) -> str:
        return self.ids[0]

    @property
    def subject_id(self) -> str:
        return self.ids[1]

    @property
    def session_id(self) -> str:
        return self.ids[2]

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
            return xproject.experiments[self.session_id]
        except KeyError:
            return None

    def all_uploaded(self, connection: xnat.XNATSession) -> bool:
        """Checks whether all the resources in this session have been uploaded to XNAT

        Parameters
        ----------
        session : ImagingSession
            the session to upload
        xnat_repo : Xnat
            the XNAT repository to upload to

        Returns
        -------
        xsession : xnat.classes.ExperimentData | None
            the XNAT session object
        """
        xsession = self.find_xnat_session(connection)
        if xsession is None:
            return False

        xresources = {}
        for xscan in xsession.scans.values():
            for xresource in xscan.resources.values():
                xresources[f"{xscan.id}.{xscan.type}/{xresource.label}"] = xresource
        for xresource in xsession.resources.values():
            xresources[xresource.label] = xresource
        if not set(xresources).issuperset(self.resource_paths):
            return False

        # A resource that EXISTS is not necessarily a resource that is
        # COMPLETE: one holding 5 of 8 files carries the same label as one
        # holding all 8, so labels alone cannot decide this.
        #
        # Manifests are the source of truth for what should be there. A session
        # staged without them leaves this loop empty, so it is not newly strict.
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
        for resource_path, manifest in manifests.items():
            local_checksums = manifest.get("checksums") if manifest else None
            if not local_checksums:
                continue
            xresource = xresources.get(resource_path)
            if xresource is None:
                return False
            comparison = compare_resource_with_xnat(
                local_checksums, get_xnat_checksums(xresource)
            )
            if not comparison.complete:
                logger.info(
                    "'%s' in '%s' exists on XNAT but is not complete: %d file(s) "
                    "missing, %d unexpected, %d differing. Not skipping the session.",
                    resource_path,
                    self.name,
                    len(comparison.missing),
                    len(comparison.extra),
                    len(comparison.differing),
                )
                return False
        return True


@attrs.define
class LocalSessionListing(SessionListing):
    fspath: Path

    @property
    def cache_path(self) -> Path:
        return self.fspath

    @property
    def resource_paths(self) -> set[str]:
        paths = {str(p.relative_to(self.fspath)) for p in self.fspath.glob("*/*")}
        # session resources: top-level dirs with no "." in name
        for item in self.fspath.iterdir():
            if item.is_dir() and "." not in item.name:
                paths.add(item.name)
        paths -= {p for p in paths if Path(p).name == Metadata.FNAME}
        return paths

    @property
    def name(self) -> str:  # type: ignore[override]  # base declares a plain attr
        return self.fspath.name

    @property
    def session_id(self) -> str:
        return self.ids[2]


@attrs.define
class SessionOnlyListing(SessionListing):
    """A staging directory named by session label only (no project.subject.visit structure).

    Used when uploading resources directly to an existing XNAT session identified only by
    its label. The label must be globally unique across all projects accessible to the upload
    account — an error is raised if multiple sessions match.
    """

    fspath: Path

    @property
    def cache_path(self) -> Path:
        return self.fspath

    @property
    def name(self) -> str:  # type: ignore[override]  # base declares a plain attr
        return self.fspath.name

    @property
    def session_id(self) -> str:
        return self.fspath.name

    @property
    def resource_paths(self) -> set[str]:
        # FIXME: This doesn't look right. It looks like it is picking out the
        # scan level not the session level.
        return {
            item.name
            for item in self.fspath.iterdir()
            if item.is_dir() and "." not in item.name
        }

    def find_xnat_session(self, connection: xnat.XNATSession) -> ty.Any:
        """Look up an existing XNAT session by label across all accessible projects.

        Returns None if not found, raises RuntimeError if multiple sessions match.
        """
        matches = [
            e for e in connection.experiments.values() if e.label == self.session_id
        ]
        if len(matches) > 1:
            raise RuntimeError(
                f"Multiple XNAT sessions found with label '{self.session_id}'. "
                "Session labels must be globally unique for session-only uploads."
            )
        return matches[0] if matches else None

    # all_uploaded is deliberately NOT defined here. Only find_xnat_session
    # differs between the modes; a second copy of the completeness rule is how
    # this class kept a label comparison after the base class stopped using one.


@attrs.define
class S3SessionListing(SessionListing):
    name: str
    bucket: ty.Any
    objects: ty.List[ty.Tuple[ty.List[str], ty.Any]]
    _cache_path: Path
    max_workers: ty.Optional[int] = None
    _downloaded: bool = attrs.field(default=False, init=False)
    _download_lock: threading.Lock = attrs.field(factory=threading.Lock, init=False)

    @property
    def cache_path(self) -> Path:
        """Download the session once and reuse it for the life of the listing.

        api/upload_api.py reads this four times for a single upload. Downloading
        inside a property meant each read re-fetched the whole session, so a
        1.2 GB session was pulled from S3 four times over. The download is
        guarded by a lock as well as the flag, because the property is reachable
        from more than one thread and a bare flag would let two downloads write
        the same paths concurrently, each opening them with mode "wb".
        """
        with self._download_lock:
            if self._downloaded:
                return self._cache_path
            self._download_objects()
            self._downloaded = True
        return self._cache_path

    def _download_objects(self) -> None:
        logger.info("Downloading session '%s' from S3 bucket", self.name)

        def _download(item: ty.Tuple[ty.List[str], ty.Any]) -> None:
            relpath, obj = item
            obj_path = self._cache_path.joinpath(*relpath)
            obj_path.parent.mkdir(parents=True, exist_ok=True)
            logger.debug("Downloading %s to %s", obj, obj_path)
            with open(obj_path, "wb") as f:
                self.bucket.download_fileobj(obj.key, f)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            list(
                tqdm(
                    executor.map(_download, self.objects),
                    total=len(self.objects),
                    desc=f"Downloading scans in '{self.name}' session from S3 bucket",
                )
            )

    @property
    def resource_paths(self) -> set[str]:
        paths = set()
        for path_parts, _ in self.objects:
            if not path_parts:
                continue
            first = path_parts[0]
            if "." in first:
                # scan resource: <scan_id>.<scan_type>/<resource_name>
                if len(path_parts) >= 2:
                    paths.add(f"{first}/{path_parts[1]}")
            else:
                # session resource: <resource_name> (no dot in dir name)
                paths.add(first)
        paths -= {p for p in paths if Path(p).name == Metadata.FNAME}
        return paths

    @property
    def resource_manifests(self) -> dict[str, dict[str, str]]:
        manifests = {}
        manifest_fnames_by_relpath: dict[str, str] = {}
        for path_parts, obj in self.objects:
            if not path_parts:
                # An object sitting directly at the session root. resource_paths
                # skips these; without the same guard this raised IndexError on
                # every pass, which the caller reads as "cannot check".
                continue
            fname = path_parts[-1]
            if fname not in (
                ImagingResource.MANIFEST_FNAME,
                ImagingResource.OLD_MANIFEST_FNAME,
            ):
                continue
            relpath = "/".join(path_parts[:-1])
            # Prefer the current manifest filename over the legacy one if both are
            # present in the same resource directory
            if (
                relpath in manifest_fnames_by_relpath
                and fname == ImagingResource.OLD_MANIFEST_FNAME
            ):
                continue
            manifest_fnames_by_relpath[relpath] = fname
            manifest_path = self._cache_path / relpath / fname
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            with open(manifest_path, "wb") as f:
                self.bucket.download_fileobj(obj.key, f)
            manifests[relpath] = Json(manifest_path).contents
        return manifests


def iterate_s3_sessions(
    bucket_path: str,
    store_credentials: StoreCredentials,
    temp_dir: Path | None,
    wait_period: int,
) -> ty.Iterator[SessionListing]:
    """Iterate over sessions stored in an S3 bucket

    Parameters
    ----------
    bucket_path : str
        the path to the S3 bucket
    store_credentials : StoreCredentials
        the credentials to access the S3 bucket
    temp_dir : Path, optional
        the temporary directory to download the sessions to, by default None
    wait_period : int
        the number of seconds after the last write before considering a session complete
    """
    # List sessions stored in s3 bucket
    s3: boto3.resources.base.ServiceResource = boto3.resource(
        "s3",
        aws_access_key_id=store_credentials.access_key,
        aws_secret_access_key=store_credentials.access_secret,
    )
    bucket_name, prefix = bucket_path[5:].split("/", 1)
    bucket = s3.Bucket(bucket_name)
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    all_objects = bucket.objects.filter(Prefix=prefix)
    session_objs = defaultdict(list)
    for obj in all_objects:
        if obj.key.endswith("/"):
            continue  # skip directories
        path_parts = obj.key[len(prefix) :].split("/")
        session_name = path_parts[0]
        if session_name.startswith("__") and session_name.endswith("__"):
            continue  # skip internal directories
        session_objs[session_name].append((path_parts[1:], obj))

    num_sessions = len(session_objs)
    # Bit of a hack to allow the caller to know how many sessions are in the bucket
    # we yield the number of sessions as the first item in the iterator
    yield num_sessions  # type: ignore[misc]

    if temp_dir:
        tmp_download_dir = Path(temp_dir) / "xnat-ingest-download"
        tmp_download_dir.mkdir(parents=True, exist_ok=True)
    else:
        tmp_download_dir = Path(tempfile.mkdtemp())

    for session_name, objs in session_objs.items():
        # Just in case the manifest file is not included in the list of objects
        # we recreate the project/subject/session directory structure
        session_tmp_dir = tmp_download_dir / session_name
        session_tmp_dir.mkdir(parents=True, exist_ok=True)
        # Check to see if the session is still being updated
        last_modified = None
        for _, obj in objs:
            if last_modified is None or obj.last_modified > last_modified:
                last_modified = obj.last_modified
        assert last_modified is not None
        if (
            datetime.datetime.now(datetime.timezone.utc) - last_modified
        ) >= datetime.timedelta(seconds=wait_period):
            yield S3SessionListing(
                name=session_name,
                objects=session_objs[session_name],
                bucket=bucket,
                cache_path=session_tmp_dir,
            )
        else:
            logger.info(
                "Skipping session '%s' as it was last modified less than %d seconds ago "
                "and waiting until it is complete",
                session_name,
                wait_period,
            )
        shutil.rmtree(session_tmp_dir)  # Delete the tmp session after the upload

    logger.info("Found %d sessions in S3 bucket '%s'", num_sessions, bucket_path)
    logger.debug("Created sessions iterator")


def remove_old_files_on_s3(remote_store: str, threshold: int) -> None:
    # Parse S3 bucket and prefix from remote store
    bucket_name, prefix = remote_store[5:].split("/", 1)

    # Create S3 client
    s3_client = boto3.client("s3")

    # List objects in the bucket with the specified prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    now = datetime.datetime.now()

    # Iterate over objects and delete files older than the threshold
    for obj in response.get("Contents", []):
        last_modified = obj["LastModified"]
        age = (now - last_modified).days
        if age > threshold:
            s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])


def remove_old_files_on_ssh(remote_store: str, threshold: int) -> None:
    # Parse SSH server and directory from remote store
    server, directory = remote_store.split("@", 1)

    # Create SSH client
    ssh_client = paramiko.SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.connect(server)

    # Execute find command to list files in the directory
    stdin, stdout, stderr = ssh_client.exec_command(f"find {directory} -type f")

    now = datetime.datetime.now()

    # Iterate over files and delete files older than the threshold
    for file_path in stdout.read().decode().splitlines():
        last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        age = (now - last_modified).days
        if age > threshold:
            ssh_client.exec_command(f"rm {file_path}")

    ssh_client.close()


def get_xnat_session(session: ImagingSession, xproject: ty.Any) -> ty.Any:
    """Get the XNAT session object for the given session

    Parameters
    ----------
    session : ImagingSession
        the session to upload
    xnat_repo : Xnat
        the XNAT repository to upload to

    Returns
    -------
    xsession : ty.Any
        the XNAT session object
    """
    xclasses = xproject.xnat_session.classes

    xsubject = xclasses.SubjectData(label=session.subject_id, parent=xproject)
    try:
        xsession = xproject.experiments[session.session_id]
    except KeyError:
        if "MR" in session.modalities:
            SessionClass = xclasses.MrSessionData
        elif "PT" in session.modalities:
            SessionClass = xclasses.PetSessionData
        elif "CT" in session.modalities:
            SessionClass = xclasses.CtSessionData
        else:
            raise RuntimeError(
                "Found the following unsupported modalities in "
                f"{session.name}: {session.modalities}"
            )
        xsession = SessionClass(label=session.session_id, parent=xsubject)
    return xsession


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
    by NAME only and the result is marked not `comparable`.
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
    )


def get_xnat_resource(
    resource: ImagingResource, xsession: ty.Any
) -> tuple[ty.Any, ty.Optional[ty.Set[str]]]:
    """Get the XNAT resource object for the given resource

    RETURNS A PAIR AT EVERY EXIT, so the caller cannot mistake "nothing to do"
    for "upload everything". The second element is the set of file names to
    upload, or None meaning "all of them".

    Parameters
    ----------
    resource : ImagingResource
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

    try:
        xscan = xsession.scans[resource.scan.id]
    except KeyError:
        image_type = resource.metadata.get("ImageType")
        is_secondary = image_type and image_type[:2] == ["DERIVED", "SECONDARY"]
        if is_secondary:
            resource_name = "secondary"
        if isinstance(resource.fileset, DicomCollection):
            scan_type = xnat_scan_type_from_sop_class(
                resource.metadata.get("SOPClassUID")
            )
            ScanClass = xclasses.XNAT_CLASS_LOOKUP[f"xnat:{scan_type}"]
        else:
            if isinstance(xsession, xclasses.MrSessionData):
                default_scan_modality = "MR"
            elif isinstance(xsession, xclasses.PetSessionData):
                default_scan_modality = "PT"
            else:
                default_scan_modality = "CT"
            modality = (
                "SC"
                if is_secondary
                else resource.metadata.get("Modality", default_scan_modality)
            )
            if modality == "SC":
                ScanClass = xclasses.ScScanData
            elif modality == "MR":
                ScanClass = xclasses.MrScanData
            elif modality == "PT":
                ScanClass = xclasses.PetScanData
            elif modality == "CT":
                ScanClass = xclasses.CtScanData
            else:
                ScanClass = xclasses.OtherDicomScanData
        logger.debug(
            "Creating scan %s in %s", resource.scan.id, resource.scan.session.path
        )
        xscan = ScanClass(
            id=resource.scan.id,
            type=resource.scan.type,
            series_description=resource.scan.type,
            parent=xsession,
        )
    try:
        xresource = xscan.resources[resource_name]
    except KeyError:
        pass
    else:
        xnat_checksums = get_xnat_checksums(xresource)
        comparison = compare_resource_with_xnat(resource.checksums, xnat_checksums)
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
                # THE WORDING IS LOAD-BEARING: the Loki rules shipped with the
                # AIS-Edge charts match this message on the literal phrase
                # "already exists on XNAT with different checksums". Rewording
                # it switches the operator alert off silently.
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
    result = xresource.xnat_session.get(xresource.uri + "/files")
    if result.status_code != 200:
        raise RuntimeError(
            "Could not download metadata for resource {}. Files "
            "may have been uploaded but cannot check checksums".format(xresource.id)
        )
    return dict((r["Name"], r["digest"]) for r in result.json()["ResultSet"]["Result"])


def calculate_checksums(
    scan: FileSet, max_workers: ty.Optional[int] = None
) -> ty.Dict[str, str]:
    """
    Calculates the MD5 digests associated with the files in a fileset.

    Parameters
    ----------
    scan : FileSet
        the file-set to calculate the checksums for
    max_workers : int, optional
        the number of threads to use to hash the files concurrently. If None,
        defaults to `concurrent.futures.ThreadPoolExecutor`'s default.

    Returns
    -------
    dict[str, str]
        the calculated checksums
    """

    def _hash(fspath: Path) -> ty.Tuple[str, str]:
        try:
            hsh = hashlib.md5()
            with open(fspath, "rb") as f:
                for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b""):
                    hsh.update(chunk)
            checksum = hsh.hexdigest()
        except OSError:
            raise RuntimeError(f"Could not create digest of '{fspath}' ")
        return str(fspath.relative_to(scan.parent)), checksum

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        return dict(executor.map(_hash, scan.fspaths))


HASH_CHUNK_SIZE = 2**20


def dir_older_than(path: Path, period: int) -> bool:
    """
    Get the most recent modification time of a directory and its contents.

    Parameters
    ----------
    path : Path
        the directory to get the modification time of
    period : int
        the number of seconds after the last modification time to check against

    Returns
    -------
    bool
        whether the directory is older than the specified period
    """
    mtimes = [path.stat().st_mtime]
    for root, _, files in os.walk(path):
        for file in files:
            mtimes.append((Path(root) / file).stat().st_mtime)
    last_modified = datetime.datetime.fromtimestamp(max(mtimes))
    return (datetime.datetime.now() - last_modified) >= datetime.timedelta(
        seconds=period
    )


def upload_file_to_s3(file_path: Path, bucket: str, s3_key: str) -> None:

    s3_client = boto3.client("s3")
    s3_client.upload_file(str(file_path), bucket, s3_key)


def list_session_dirs(sorted_dir: Path) -> list[Path]:
    """List the session directories in the sorted directory, excluding any directories that start with '__'.

    Includes both dotted dirs (PROJ.SUBJ.VISIT) and no-dot dirs (session label only).
    """
    return [
        p
        for p in Path(sorted_dir).iterdir()
        if p.is_dir() and not p.name.startswith("__") and not p.name.endswith("__")
    ]
