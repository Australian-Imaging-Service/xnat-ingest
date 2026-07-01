import hashlib
import inspect
import json
import logging
import os
import platform
import re
import requests
import typing as ty
from collections import Counter
from datetime import datetime
from functools import cached_property
from glob import glob
from pathlib import Path

import attrs
import yaml
from filelock import SoftFileLock
from tqdm import tqdm
from fileformats.core import FileSet, from_mime, from_paths, to_mime
from fileformats.core.utils import collate_metadata_series
from fileformats.application import Yaml
from fileformats.medimage import DicomCollection
from frametree.core.exceptions import FrameTreeDataMatchError
from frametree.core.frameset import FrameSet
from typing_extensions import Self

from ..exceptions import StagingError
from ..helpers.arg_types import AssociatedFiles, IDSpec
from ..helpers.metadata import Metadata
from .resource import ImagingResource
from .scan import ImagingScan

logger = logging.getLogger("xnat-ingest")

_DATE_FORMATS = ["%d.%m.%y", "%d.%m.%Y", "%Y-%m-%d", "%Y%m%d", "%m/%d/%y", "%m/%d/%Y"]
_TIME_FORMATS = ["%H.%M.%S", "%H:%M:%S", "%H%M%S"]


def _parse_datetime_to_str(date_str: str, time_str: str | None) -> str:
    """Parse date (and optional time) strings using common formats, return YYYYMMDDHHMMSS or YYYYMMDD."""
    parsed_date = None
    for fmt in _DATE_FORMATS:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            break
        except ValueError:
            continue
    if parsed_date is None:
        raise ValueError(
            f"Cannot parse date '{date_str}' — tried formats: {_DATE_FORMATS}"
        )

    if time_str:
        for fmt in _TIME_FORMATS:
            try:
                parsed_time = datetime.strptime(time_str, fmt)
                return parsed_date.strftime("%Y%m%d") + parsed_time.strftime("%H%M%S")
            except ValueError:
                continue
        raise ValueError(
            f"Cannot parse time '{time_str}' — tried formats: {_TIME_FORMATS}"
        )

    return parsed_date.strftime("%Y%m%d")


def scans_converter(
    scans: ty.Union[ty.Sequence[ImagingScan], ty.Dict[str, ImagingScan]],
) -> dict[str, ImagingScan]:
    if isinstance(scans, ty.Sequence):
        duplicates = [i for i, c in Counter(s.id for s in scans).items() if c > 1]
        if duplicates:
            raise ValueError(f"Found duplicate scan IDs in list of scans: {duplicates}")
        scans = {s.id: s for s in scans}
    return scans


@attrs.define(slots=False)
class ImagingSession:
    """Representation of an imaging session to be uploaded to XNAT, which is a set of scans that
    belong together under the same project/subject/visit IDs.

    Parameters
    ----------
    project_id: str, optional
        The project ID of the session
    subject_id: str, optional
        The subject ID of the session
    visit_id: str, optional
        The visit ID of the session
    scans: ty.Dict[str, ImagingScan]
        The scans in the session
    run_uid: ty.Optional[str]
        The run UID of the session, if it exists
    """

    uid: str
    project_id: str | None = None
    subject_id: str | None = None
    visit_id: str | None = None
    scans: ty.Dict[str, ImagingScan] = attrs.field(
        factory=dict,
        converter=scans_converter,
        validator=attrs.validators.instance_of(dict),
    )
    session_resources: ty.Dict[str, ImagingResource] = attrs.field(factory=dict)
    run_uid: ty.Optional[str] = attrs.field(default=None)
    explicit_session_id: ty.Optional[str] = attrs.field(default=None)
    metadata: Metadata = attrs.field(eq=False, repr=False, init=False)

    METADATA_FNAME = "__METADATA__.yaml"
    METADATA_DIR = "__metadata__"

    def __attrs_post_init__(self) -> None:
        for scan in self.scans.values():
            scan.session = self

    def __getitem__(self, fieldname: str) -> ty.Any:
        return self.metadata[fieldname]

    @metadata.default
    def _metadata_default(self):
        return Metadata({}, self)

    @property
    def name(self) -> str:
        if any(i is None for i in (self.project_id, self.subject_id, self.visit_id)):
            return None
        return f"{self.project_id}.{self.subject_id}.{self.visit_id}"

    @property
    def invalid_ids(self) -> bool:
        return (
            self.project_id.startswith("INVALID")
            or self.subject_id.startswith("INVALID")
            or self.visit_id.startswith("INVALID")
        )

    @property
    def path(self) -> str:
        return ":".join([self.project_id, self.subject_id, self.visit_id])

    @property
    def staging_relpath(self) -> list[str]:
        if self.name is None:
            return [self.uid]
        return [self.name]

    @property
    def session_id(self) -> str:
        if self.explicit_session_id is not None:
            return self.explicit_session_id
        return f"{self.subject_id}_{self.visit_id}"

    @cached_property
    def modalities(self) -> str | tuple[str, ...]:
        try:
            modalities_metadata = self.metadata["Modality"]
        except KeyError as e:
            e.add_note(f"Available metadata: {list(self.metadata)}")
            raise e
        if isinstance(modalities_metadata, str):
            return modalities_metadata
        modalities: set[str] = set()
        for modality in modalities_metadata:
            if isinstance(modality, str):
                modalities.add(modality)
            else:
                assert isinstance(modality, ty.Iterable)
                modalities.update(modality)
        return tuple(modalities)

    @property
    def primary_parents(self) -> ty.Set[Path]:
        "Return parent directories for all resources in the session"
        return set(r.fileset.parent for r in self.primary_resources)

    @property
    def resources(self) -> ty.List[ImagingResource]:
        return list(self.session_resources.values()) + [
            r for p in self.scans.values() for r in p.resources.values()
        ]

    @property
    def primary_resources(self) -> ty.List[ImagingResource]:
        return [
            r
            for s in self.scans.values()
            for r in s.resources.values()
            if not s.associated
        ]

    def new_empty(self) -> Self:
        """Return a new empty session with the same IDs as the current session"""
        return type(self)(
            project_id=self.project_id,
            subject_id=self.subject_id,
            visit_id=self.visit_id,
            session_id_override=self.explicit_session_id,
        )

    def select_resources(
        self,
        dataset: ty.Optional[FrameSet],
        always_include: ty.Sequence[str | FileSet] = (),
    ) -> ty.Iterator[ImagingResource]:
        """Returns selected resources that match the columns in the dataset definition

        Parameters
        ----------
        dataset : FrameSet
            Arcana dataset definition
        always_include : sequence[str | FileSet]
            mime-types or "mime-like" (see https://arcanaframework.github.io/fileformats/)
            of file-format to always include in the upload, regardless of whether they are
            specified in the dataset or not

        Yields
        ------
        scan_id : str
            the ID of the scan should be uploaded to
        scan_type : str
            the desc/type to assign to the scan
        resource_name : str
            the name of the resource under the scan to upload it to
        scan : FileSet
            a fileset to upload
        """
        if not dataset and not always_include:
            raise ValueError(
                "Either 'dataset' or 'always_include' must be specified to select "
                f"appropriate resources to upload from {self.name} session"
            )
        store = ImagingSessionMockStore(self)

        uploaded = set()
        for mime_like in always_include:
            if inspect.isclass(mime_like) and issubclass(mime_like, FileSet):
                fileformat = mime_like
            elif mime_like == "all":
                fileformat = FileSet
            else:
                fileformat = from_mime(mime_like)  # type: ignore[assignment]
                if not issubclass(fileformat, FileSet):
                    raise ValueError(
                        f"{mime_like!r} does not correspond to a file format ({fileformat})"
                    )
            for resource in self.session_resources.values():
                if isinstance(resource.fileset, fileformat):
                    uploaded.add((None, resource.name))
                    yield resource
            for scan in self.scans.values():
                for resource in scan.resources.values():
                    if isinstance(resource.fileset, fileformat):
                        uploaded.add((scan.id, resource.name))
                        yield resource
        if dataset is not None:
            for column in dataset.columns.values():
                try:
                    entry = column.match_entry(store.row)
                except FrameTreeDataMatchError as e:
                    raise StagingError(
                        f"Did not find matching entry for {column} column in {dataset} from "
                        f"{self.name} session"
                    ) from e
                else:
                    scan_id, resource_name = entry.uri
                    scan = self.scans[scan_id]
                    if (scan.id, resource_name) in uploaded:
                        logger.info(
                            "%s/%s resource is already uploaded as 'always_include' is set to "
                            "%s and doesn't need to be explicitly specified",
                            scan.id,
                            resource_name,
                            always_include,
                        )
                        continue
                    resource = scan.resources[resource_name]
                    if not isinstance(resource.fileset, column.datatype):
                        resource = ImagingResource(
                            name=resource_name,
                            fileset=column.datatype(resource.fileset),
                            scan=scan,
                        )
                    uploaded.add((scan.id, resource_name))
                yield resource

    @classmethod
    def from_paths(
        cls,
        files_path: str | Path | ty.Sequence[str | Path],
        datatypes: ty.Union[ty.Type[FileSet], ty.Sequence[ty.Type[FileSet]]],
        session_uid_field: list[IDSpec],
        scan_id_field: list[IDSpec],
        scan_desc_field: list[IDSpec],
        resource_field: list[IDSpec],
        recursive: bool = False,
        avoid_clashes: bool = True,
    ) -> ty.List[Self]:
        """Loads all imaging sessions from a list of DICOM files

        Parameters
        ----------
        files_path : str or Path
            Path to a directory containing the resources to load the sessions from, or a
            glob string that selects the paths
        datatypes : type or list[type]
            the fileformats to load from the paths, e.g. DicomSeries or
            [DicomSeries, NiftiGz]
        session_uid_field: list[IdField]
            the metadata field that uniquely identifies the session, used to group files
            together before project/subject/visit IDs are extracted (e.g. StudyInstanceUID)
        scan_id_field: list[IdField]
            the value of this field is used to group resources under single scans.
        scan_desc_field: list[IdField]
            the value of this field is used to label scans
        resource_field: list[IdField]
            the value of this field is to resources
        recursive : bool, optional
            recurse into directories passed as file paths (i.e. by appending '**/*' and running a glob),
            by default False
        avoid_clashes : bool, optional
            if a resource with the same name already exists in the scan, increment the
            resource name by appending _1, _2 etc. to the name until a unique name is found,
            by default False


        Returns
        -------
        list[ImagingSession]
            all imaging sessions that are present in list of dicom paths

        Raises
        ------
        ImagingSessionParseError
            if values extracted from IDs across the DICOM scans are not consistent across
            DICOM files within the session
        """
        if isinstance(files_path, (Path, str)):
            files_path = [files_path]
        elif not isinstance(files_path, ty.Sequence):
            raise TypeError(
                "Invalid type of 'files_path', must be a pathlib.Path, str or list of"
            )
        fspaths: list[Path] = []
        for fspath in files_path:
            logger.debug("Searching for file types in '%s'", str(fspath))
            if isinstance(fspath, Path) or "*" not in fspath:
                fspath = Path(fspath)
                if not fspath.exists():
                    raise ValueError(
                        f"Provided file-system path '{fspath}' does not exist"
                    )
                if fspath.is_dir():
                    if recursive:
                        logger.debug(
                            "Recursively searching for all paths '%s' directory",
                            str(fspath),
                        )
                        fspaths.extend(
                            Path(p) for p in glob(str(fspath) + "/**/*", recursive=True)
                        )
                    else:
                        logger.debug(
                            "Adding contents of '%s' directory to list", str(fspath)
                        )
                        fspaths.extend(Path(fspath).iterdir())
                else:
                    logger.debug(
                        "Directly appending '%s' to list of files", str(fspath)
                    )
                    fspaths.append(fspath)
            else:
                logger.debug("Searching for file-system paths using glob '%s'", fspath)
                fspaths.extend(Path(p) for p in glob(fspath, recursive=True))

        fspaths = [fix_long_path(p) for p in fspaths]

        if nonexistent := [str(p) for p in fspaths if not Path(p).exists()]:
            raise ValueError(
                "The following paths do not exist:\n"
                + "\n".join(nonexistent[:100])
                + ("\n..." if len(nonexistent) > 100 else "")
            )

        # Create a UID out of the paths that session was created from and the
        # timestamp
        crypto = hashlib.sha256()
        for fspath in fspaths:
            crypto.update(str(fspath.absolute()).encode())
        run_uid: str = crypto.hexdigest()[:6] + datetime.strftime(
            datetime.now(),
            "%Y%m%d%H%M%S",
        )

        if not isinstance(datatypes, ty.Sequence):
            datatypes = [datatypes]

        from_paths_kwargs = {}

        # Sort loaded series by StudyInstanceUID (imaging session)
        logger.info(f"Loading {datatypes} from {files_path}...")
        filesets = from_paths(
            fspaths,
            *datatypes,
            ignore=".*",
            **from_paths_kwargs,  # type: ignore[arg-type]
        )
        sessions: ty.Dict[ty.Tuple[str, str, str] | str, Self] = {}

        for fileset in tqdm(
            filesets,
            "Sorting resources into XNAT tree structure...",
        ):
            session_uid = IDSpec.get_values(fileset, session_uid_field)
            scan_id = IDSpec.get_values(fileset, scan_id_field)
            scan_type = IDSpec.get_values(fileset, scan_desc_field)
            # XNAT requires DICOM datasets to have in 'DICOM' and 'secondary'
            # resource labels otherwise some features don't work
            if isinstance(fileset, DicomCollection):
                try:
                    image_type = fileset.contents[0].metadata["ImageType"]
                except (KeyError, IndexError):
                    resource_label = "DICOM"
                else:
                    resource_label = dicom_image_type_to_resource_label(image_type)

            else:
                resource_label = IDSpec.get_values(fileset, resource_field)
            try:
                session = sessions[session_uid]
            except KeyError:
                session = cls(
                    uid=session_uid,
                    run_uid=run_uid,
                )
                sessions[session_uid] = session
            logger.debug(
                "Adding resource '%s' to %s scan in %s session",
                resource_label,
                scan_type,
                session_uid,
            )
            session.add_resource(
                scan_id,
                scan_type,
                resource_label,
                fileset,
                avoid_clashes=avoid_clashes,
            )
        return list(sessions.values())

    def assign(
        self,
        project_field: str,
        subject_field: str,
        visit_field: str | None = None,
        session_field: str | None = None,
        constant_project_id: str | None = None,
    ) -> ty.List[Self]:
        """Assigns project, subject, session IDS, scan IDS and descriptions and resource labels
        to the session and each scan/resource within it.

        Parameters
        ----------
        project_field : list[IdField]
            the metadata field that contains the XNAT project ID for the imaging session,
            by default "StudyID"
        subject_field : list[IdField]
            the metadata field that contains the XNAT subject ID for the imaging session,
            by default "PatientID"
        visit_field : list[IdField]
            the metadata field that contains the XNAT visit ID for the imaging session,
            by default "AccessionNumber"
        session_field: list[IdField], optional
            when provided, the value of this field is used directly as the XNAT session label
            instead of concatenating subject and visit IDs. Mutually exclusive with visit_field
            being used as the label source.
        constant_project_id : str
            Override the project ID loaded from the metadata (useful when invoking
            manually)


        Returns
        -------
        list[ImagingSession]
            all imaging sessions that are present in list of dicom paths

        Raises
        ------
        ImagingSessionParseError
            if values extracted from IDs across the DICOM scans are not consistent across
            DICOM files within the session
        """
        if visit_field is not None and session_field is not None:
            raise ValueError(
                f"Both 'visit_field' ({visit_field}) and 'session_field' ({session_field}) cannot be "
                "provided concurrently"
            )
        if visit_field is None and session_field is None:
            raise ValueError(
                "At least one of 'visit_field' or 'session_field' needs to be provided"
            )

        if constant_project_id is None:
            self.project_id = self.metadata[project_field]
        else:
            self.project_id = constant_project_id
        self.subject_id = self.metadata[subject_field]
        if visit_field is not None:
            self.visit_id = self.metadata[visit_field]
        else:
            self.session_id = self.metadata[session_field]

    @classmethod
    def from_orthanc(
        cls,
        url: str,
        output_dir: Path,
        store_dir: Path,
        user: str,
        password: str,
        processed_label: str = "xnat-sorted",
    ) -> ty.List["ImagingSession"]:
        """Stage DICOM studies from Orthanc directly into output_dir using hardlinks.
        Requires orthanc_storage_dir and output_dir to be on the same filesystem.

        Parameters
        ----------
        url : str
            Base URL of the Orthanc REST API, e.g. 'http://orthanc:8042'
        output_dir : Path
            Staging directory. Hardlinks land here directly, must be on the same
            filesystem as orthanc_storage_dir.
        store_dir : Path
            Orthanc's StorageDirectory as mounted.
        user : str, optional
            Orthanc basic auth credentials username
        password : str, optional
            Orthanc basic auth credentials password
        processed_label : str, optional
            Label applied after staging to prevent re-processing, by default 'xnat-sorted'.
            Remove via the Orthanc UI to re-sort a study.

        Returns
        -------
        list[ImagingSession]
            Staged sessions loaded from output_dir.
        """
        auth = (user, password) if user else None

        def get_json(path: str) -> ty.Any:
            resp = requests.get(f"{url}{path}", auth=auth)
            resp.raise_for_status()
            return resp.json()

        class _TagsAdapter:
            """Adapter so FieldSpec.get_value() can read an Orthanc tag dict."""

            def __init__(self, tags: dict) -> None:
                self.metadata = tags

        def eval_field(specs: list[IDSpec], tags: dict, escape: bool = False) -> str:
            """Return the first FieldSpec value that resolves, or 'UNKNOWN'.

            Parameters
            ----------
            escape : bool
                If True, replace non-alphanumeric/underscore characters with '_'
            """
            adapter = _TagsAdapter(tags)
            for spec in specs:
                try:
                    val = spec.get_value(adapter)
                    if val:
                        if escape:
                            val = IDSpec.xnat_id_escape_re.sub("_", val)
                        return val
                except Exception:
                    continue
            return "UNKNOWN"

        resp = requests.post(
            f"{url}/tools/find",
            auth=auth,
            json={
                "Level": "Study",
                "Query": {},
                "Labels": [processed_label],
                "LabelsConstraint": "None",
            },
        )
        resp.raise_for_status()
        study_ids = resp.json()
        logger.info("Found %d unstaged studies in Orthanc at '%s'", len(study_ids), url)

        staged: list[ImagingSession] = []
        for study_id in tqdm(study_ids, "Staging studies from Orthanc"):
            study = get_json(f"/studies/{study_id}")
            study_tags = {**study["MainDicomTags"], **study["PatientMainDicomTags"]}

            session_uid = eval_field("StudyInstanceUID", study_tags, escape=True)
            session_dir = output_dir / f"_.{session_uid}"
            session_dir.mkdir(parents=True, exist_ok=True)

            modalities: set[str] = set()
            for series_id in study["Series"]:
                series = get_json(f"/series/{series_id}")
                if modality := series["MainDicomTags"].get("Modality"):
                    modalities.add(modality)
                all_tags = {**study_tags, **series["MainDicomTags"]}
                scan_id = eval_field("SeriesNumber", all_tags)
                scan_type = eval_field("SeriesDescription", all_tags)

                if "ImageType" in all_tags:
                    resource_label = dicom_image_type_to_resource_label(
                        eval_field("ImageType", all_tags)
                    )
                else:
                    resource_label = "DICOM"
                resource_dir = session_dir / f"{scan_id}.{scan_type}" / resource_label
                resource_dir.mkdir(parents=True, exist_ok=True)

                instances = get_json(f"/series/{series_id}/instances")
                checksums: dict[str, str] = {}
                for instance in instances:
                    instance_id = instance["ID"]
                    sop_uid = instance["MainDicomTags"].get(
                        "SOPInstanceUID", instance_id
                    )
                    fname = f"{sop_uid}.dcm"
                    dest_path = resource_dir / fname
                    if dest_path.exists():
                        continue
                    attachment = get_json(
                        f"/instances/{instance_id}/attachments/dicom/info"
                    )
                    if attachment["CompressedSize"] != attachment["UncompressedSize"]:
                        raise ValueError(
                            f"Instance '{instance_id}' in series '{series_id}' is stored "
                            "compressed in Orthanc — disable StorageCompression in the "
                            "Orthanc config to use hardlink sorting."
                        )
                    uuid = attachment["Uuid"]
                    src_path = Path(store_dir) / uuid[0:2] / uuid[2:4] / uuid
                    os.link(src_path, dest_path)
                    checksums[fname] = attachment["UncompressedMD5"]

                manifest = {"datatype": "medimage/dicom-series", "checksums": checksums}
                with open(resource_dir / ImagingResource.MANIFEST_FNAME, "w") as f:
                    json.dump(manifest, f, indent=2)

            metadata_path = session_dir / Metadata.FNAME
            if not metadata_path.exists():
                if modalities:
                    study_tags["Modality"] = (
                        next(iter(modalities))
                        if len(modalities) == 1
                        else list(modalities)
                    )
                with open(metadata_path, "w") as f:
                    yaml.dump(study_tags, f, indent=4)

            requests.put(
                f"{url}/studies/{study_id}/labels/{processed_label}", auth=auth
            ).raise_for_status()
            logger.info(
                "Staged and labelled study '%s' -> '%s'", study_id, session_dir.name
            )
            staged.append(cls.load(session_dir))

        return staged

    def deidentify(
        self,
        dest_dir: Path,
        specs: dict[type[FileSet], ty.Any] = None,
        copy_mode: FileSet.CopyMode = FileSet.CopyMode.hardlink_or_copy,
        avoid_clashes: bool = False,
        require_matching_spec: bool = True,
    ) -> tuple[Self, dict[str, ty.Any]]:
        """Creates a new session with deidentified images

        Parameters
        ----------
        dest_dir : Path
            the directory to save the deidentified files into
        specs : dict[type[FileSet], Any], optional
            a project-specific specification that defines how to deidentify the different
            file types within the imaging session. The keys of the project spec are
            the mime-like of the file types (see https://arcanaframework.github.io/fileformats/)
            and the values are arbitrary file-format-specific specifications.
        copy_mode : FileSet.CopyMode, optional
            the mode to use to copy the files that don't need to be deidentified,
            by default FileSet.CopyMode.hardlink_or_copy
        avoid_clashes : bool, optional
            when copying a file that doesn't need to be deidentified, if a resource
            with the same name already exists in the scan, increment the
            resource name by appending _1, _2 etc. to the name until a unique name is found,
            by default False
        require_matching_spec : bool, optional
            whether to require a matching specification for each fileset, by default True

        Returns
        -------
        ImagingSession
            a new session with deidentified images
        dict[str, Any]
            a mapping containing the original values of metadata fields that
            have been removed or modified
        """
        if specs is None:
            specs = {}

        def select_spec(fileset: FileSet) -> ty.Any:
            """Select the appropriate deidentification specification for the
            resource based on its file type
            """
            matching_specs = {k: v for k, v in specs.items() if isinstance(fileset, k)}
            if not matching_specs:
                return None
            elif len(matching_specs) > 1:
                for k in matching_specs:
                    if all(issubclass(k, other_k) for other_k in matching_specs):
                        return matching_specs[k]
                raise KeyError(
                    f"Multiple deidentification specifications found for '{to_mime(type(fileset))}'"
                    f"file types. Please provide a more specific formats to map the specification"
                    f"specifications: {list(matching_specs)}"
                )
            return next(iter(matching_specs.values()))

        # Create a new session to save the deidentified files into
        deidentified = self.new_empty()
        reid_series = []
        for scan in self.scans.values():
            for resource_name, resource in scan.resources.items():
                resource_dest_dir = dest_dir / scan.id / resource_name
                if not getattr(resource.fileset, "contains_phi", False):
                    deid_resource = resource.fileset.copy(
                        resource_dest_dir,
                        mode=copy_mode,
                        new_stem=resource_name,
                        avoid_clashes=True,
                    )
                else:
                    resource_spec = select_spec(resource.fileset)
                    if resource_spec is None:
                        msg = (
                            "No deidentification specification found for %s fileset in %s/%s resource. "
                            "Please provide a project specification for %s in the file format hierarchy to "
                            "deidentify this resource. Returning None and copying the files without "
                            "deidentification, which may lead to PHI being uploaded to XNAT if the fileset "
                            "contains PHI. Matching specifications found in project spec: %s"
                        )
                        msg_vars = (
                            type(resource.fileset).__name__,
                            scan.id,
                            resource_name,
                            type(resource.fileset).__name__,
                            list(specs),
                        )
                        if require_matching_spec:
                            raise KeyError(msg % msg_vars)
                        else:
                            logger.warning(msg, *msg_vars)
                    deid_resource, reid_mdata = resource.fileset.deidentify(
                        out_dir=resource_dest_dir, spec=resource_spec
                    )
                    reid_series.append(reid_mdata)
                deidentified.add_resource(
                    scan.id,
                    scan.type,
                    resource_name,
                    deid_resource,
                    avoid_clashes=avoid_clashes,
                )
        return deidentified, collate_metadata_series(reid_series)

    def associate_files(
        self,
        patterns: ty.List[AssociatedFiles],
        spaces_to_underscores: bool = True,
        avoid_clashes: bool = False,
    ) -> list[FileSet]:
        """Adds files associated with the primary files to the session

        Parameters
        ----------
        patterns : list[AssociatedFiles]
            list of patterns to associate files with the primary files in the session
        spaces_to_underscores : bool, optional
            when building associated file globs, convert spaces underscores in fields
            extracted from source file metadata, false by default
        """
        all_associated = []
        for associated_files in patterns:
            # substitute string templates int the glob template with values from the
            # DICOM metadata to construct a glob pattern to select files associated
            # with current session
            associated_fspaths: ty.Set[Path] = set()
            primary_parents = self.primary_parents
            if primary_parents:
                for parent_dir in primary_parents:
                    assoc_glob = str(
                        parent_dir / associated_files.glob.format(**self.metadata)
                    )
                    if spaces_to_underscores:
                        assoc_glob = assoc_glob.replace(" ", "_")
                    # Select files using the constructed glob pattern
                    associated_fspaths.update(
                        Path(p) for p in glob(assoc_glob, recursive=True)
                    )
            elif self._metadata:
                assoc_glob = associated_files.glob.format(**self.metadata)
                if spaces_to_underscores:
                    assoc_glob = assoc_glob.replace(" ", "_")
                associated_fspaths.update(
                    Path(p) for p in glob(assoc_glob, recursive=True)
                )

            logger.info(
                "Found %s associated file paths matching '%s'",
                len(associated_fspaths),
                associated_files.glob,
            )

            # Identify scan id, type and resource names from deidentified file paths
            assoc_re = re.compile(associated_files.identity_pattern)
            for fspath in tqdm(associated_fspaths, "sorting files into resources"):
                match = assoc_re.match(str(fspath))
                if not match:
                    raise RuntimeError(
                        f"Regular expression '{associated_files.identity_pattern}' "
                        f"did not match file path {fspath}"
                    )
                scan_id = match.group("id")
                resource_name = match.group("resource")
                try:
                    scan_type = match.group("type")
                except IndexError:
                    scan_type = scan_id
                fspaths = from_paths([fspath], associated_files.datatype)
                self.add_resource(
                    scan_id,
                    scan_type,
                    resource_name,
                    fspaths[0],
                    associated=associated_files,
                    avoid_clashes=avoid_clashes,
                )
                all_associated.extend(fspaths)
        return all_associated

    def add_resource(
        self,
        scan_id: str,
        scan_type: str,
        resource_name: str,
        fileset: FileSet,
        overwrite: bool = False,
        associated: AssociatedFiles | None = None,
        avoid_clashes: bool = False,
    ) -> None:
        """Adds a resource to the imaging session

        Parameters
        ----------
        scan_id : str
            the ID of the scan to add the resource to
        scan_type : str
            short description of the type of the scan
        resource_name: str
            the name of the resource to add
        fileset : FileSet
            the fileset to add as the resource
        overwrite : bool
            whether to overwrite existing resource
        associated : bool, optional
            whether the resource is primary or associated to a primary resource
        avoid_clashes : bool, optional
            if a resource with the same name already exists in the scan, increment the
            resource name by appending _1, _2 etc. to the name until a unique name is found,
            by default False

        Raises
        ------
        KeyError
            if a resource with the same name already exists in the scan and
            `avoid_clashes` and `overwrite` are both False
        """
        if overwrite and avoid_clashes:
            raise ValueError(
                "Cannot set both 'overwrite' and 'avoid_clashes' to True when adding a "
                "resource"
            )
        try:
            scan = self.scans[scan_id]
        except KeyError:
            scan = self.scans[scan_id] = ImagingScan(
                id=scan_id, type=scan_type, associated=associated, session=self
            )
        else:
            if scan.type != scan_type:
                raise ValueError(
                    f"Non-matching scan types ({scan.type} and {scan_type}) "
                    f"for scan ID {scan_id}"
                )
            if associated != scan.associated:
                raise ValueError(
                    f"Non-matching associated files ({scan.associated} and {associated}) "
                    f"for scan ID {scan_id}"
                )
        resource = ImagingResource(name=resource_name, fileset=fileset, scan=scan)
        try:
            existing = scan.resources[resource_name]
        except KeyError:
            pass
        else:
            if resource.checksums == existing.checksums:
                logger.info(
                    "Not adding resource '%s' to %s scan in %s session as it is identical "
                    "to a resource that is already present %s",
                    resource_name,
                    scan_id,
                    self.name,
                    existing,
                )
                return
            elif overwrite:
                logger.warning(
                    "Overwriting existing resource '%s' in %s scan in %s session",
                    resource_name,
                    scan_id,
                    self.name,
                )
                del scan.resources[resource_name]
            elif avoid_clashes:
                match = re.match(r"^(.*)__(\d+)$", resource_name)
                if match:
                    base_name, num = match.groups()
                    num = int(num) + 1
                else:
                    base_name = resource_name
                    num = 2
                while resource_name in scan.resources:
                    resource_name = f"{base_name}__{num}"
                    num += 1
                logger.warning(
                    "Incremented resource name to '%s' to avoid clash with existing resources",
                    resource_name,
                )
                resource = ImagingResource(
                    name=resource_name, fileset=fileset, scan=scan
                )
            else:
                raise KeyError(
                    f"Clash between resource names ('{resource_name}') for {scan_id} scan in "
                    f"{self.name} session. Use 'overwrite=True' to overwrite the existing resource or "
                    "'avoid_clashes=True' to increment the resource name",
                )
        scan.resources[resource_name] = resource

    def add_session_resource(
        self,
        resource_name: str,
        fileset: FileSet,
        overwrite: bool = False,
    ) -> None:
        """Adds a session-level resource

        Parameters
        ----------
        resource_name : str
            the name of the resource
        fileset : FileSet
            the fileset to add as the resource
        overwrite : bool
            whether to overwrite an existing resource with the same name
        """
        resource = ImagingResource(name=resource_name, fileset=fileset)
        if resource_name in self.session_resources:
            existing = self.session_resources[resource_name]
            if resource.checksums == existing.checksums:
                return
            if not overwrite:
                raise KeyError(
                    f"Session resource '{resource_name}' already exists in {self.name}. "
                    "Use 'overwrite=True' to overwrite."
                )
        self.session_resources[resource_name] = resource

    @classmethod
    def from_metadata_yaml(cls, yaml_path: Path) -> Self:
        """Creates a metadata-only session from a __metadata__/ YAML file.

        Parameters
        ----------
        yaml_path : Path
            path to a YAML file named PROJECT.SUBJECT.VISIT.yaml

        Returns
        -------
        ImagingSession
            a session with no scans but with metadata populated
        """
        stem = yaml_path.stem
        parts = stem.split(".")
        if len(parts) != 3:
            raise ValueError(
                f"Expected metadata YAML filename to have format "
                f"PROJECT.SUBJECT.VISIT.yaml, got '{yaml_path.name}'"
            )
        project_id, subject_id, visit_id = parts
        session = cls(
            project_id=project_id,
            subject_id=subject_id,
            visit_id=visit_id,
        )
        with open(yaml_path) as f:
            session._metadata = yaml.safe_load(f)
        return session

    @classmethod
    def load(
        cls,
        session_dir: Path,
        require_manifest: bool = True,
        check_checksums: bool = True,
    ) -> Self:
        """Loads a session from a directory. Assumes that the name of the directory is
        the name of the session dir and the parent directory is the subject ID and the
        grandparent directory is the project ID. The scan information is loaded from a YAML
        along with the scan type, resources and fileformats. If the YAML file is not found
        or `use_manifest` is set to True, the session is loaded based on the directory
        structure.

        Parameters
        ----------
        session_dir : Path
            the path to the directory where the session is saved
        require_manifiest: bool, optional
            whether a manifest file is required to load the resources in the session,
            if true, resources will only be loaded if the manifest file is found,
            if false, resources will be loaded as FileSet types and checksums will not
            be checked, by default True
        check_checksums: bool, optional
            whether to check the checksums of the files in the session, by default True

        Returns
        -------
        ImagingSession
            the loaded session
        """
        if "." in session_dir.name:
            parts = session_dir.name.split(".")
        else:
            # Backwards compatibility with old delimiter
            parts = session_dir.name.split("-")
        if len(parts) == 4:
            project_id, subject_id, visit_id, run_uid = parts
        else:
            project_id, subject_id, visit_id = parts
            run_uid = None
        session = cls(
            project_id=project_id,
            subject_id=subject_id,
            visit_id=visit_id,
            run_uid=run_uid,
        )
        for item in session_dir.iterdir():
            if not item.is_dir():
                continue
            if "." in item.name:
                # scan directory: <scan_id>.<scan_type>
                scan = ImagingScan.load(
                    item,
                    require_manifest=require_manifest,
                    check_checksums=check_checksums,
                )
                scan.session = session
                session.scans[scan.id] = scan
            else:
                # session resource directory: <resource_name> (no dot)
                resource = ImagingResource.load(
                    item,
                    require_manifest=require_manifest,
                    check_checksums=check_checksums,
                )
                session.session_resources[resource.name] = resource
        metadata_path = session_dir / cls.METADATA_FNAME
        if metadata_path.exists():
            raw_meta = Yaml(metadata_path).load() or {}
            session.explicit_session_id = raw_meta.pop("__session_id__", None)
            session._metadata = raw_meta if raw_meta else None
        return session

    def save(
        self,
        dest_dir: Path,
        available_projects: ty.Optional[ty.List[str]] = None,
        copy_mode: FileSet.CopyMode = FileSet.CopyMode.hardlink_or_copy,
        collation_map: dict[ty.Type[FileSet], FileSet.CopyCollation] | None = None,
        cache_metadata: bool = True,
    ) -> tuple[Self, Path]:
        r"""Saves the session to a directory. The session will be saved to a directory
        with the project, subject and session IDs as subdirectories of this directory,
        along with the scans manifest

        Parameters
        ----------
        dest_dir : Path
            destination directory to save the deidentified files. The session will be saved
            to a directory with the project, subject and session IDs as subdirectories of
            this directory, along with the scans manifest
        available_projects : list[str], optional
            list of available project IDs on the XNAT server, if the project ID of the
            session is not in this list, it will be prefixed with "INVALID_UNRECOGNISED_"
            to avoid upload errors, by default None
        copy_mode : FileSet.CopyMode, optional
            the mode to use to copy the files that don't need to be deidentified,
            by default FileSet.CopyMode.hardlink_or_copy
        cache_metadata : bool or Path, optional
            whether to save the session metadata to a YAML file in the session directory,
            if True, the metadata will be saved to a file named "METADATA.yaml" in the session
            directory, if a Path, the metadata will be saved to this path, by default False

        Returns
        -------
        ImagingSession
            a deidentified session with updated paths
        Path
            the path to the directory where the session is saved
        """
        saved = self.new_empty()
        if available_projects is None or self.project_id in available_projects:
            project_id = self.project_id
        else:
            project_id = "INVALID_UNRECOGNISED_" + self.project_id
        session_dirname = ".".join((project_id, self.subject_id, self.visit_id))
        if self.run_uid:
            session_dirname += f".{self.run_uid}"
        session_dir = dest_dir / session_dirname
        session_dir.mkdir(parents=True, exist_ok=True)
        for scan in tqdm(self.scans.values(), f"Staging sessions to {session_dir}"):
            saved_scan = scan.save(
                session_dir, copy_mode=copy_mode, collation_map=collation_map
            )
            saved_scan.session = saved
            saved.scans[saved_scan.id] = saved_scan
        for resource in self.session_resources.values():
            saved_resource = resource.save(session_dir, copy_mode=copy_mode)
            saved.session_resources[saved_resource.name] = saved_resource
        if cache_metadata or self.explicit_session_id is not None:
            metadata_path = (
                session_dir / self.METADATA_FNAME
                if isinstance(cache_metadata, bool)
                else cache_metadata
            )
            logger.debug("Saving session metadata to '%s'", metadata_path)
            meta = dict(self.metadata) if cache_metadata and self.metadata else {}
            if self.explicit_session_id is not None:
                meta["__session_id__"] = self.explicit_session_id
            with open(metadata_path, "w") as f:
                yaml.dump(meta, f, indent=4)
        return saved, session_dir

    @classmethod
    def move_dir(cls, src: Path, dest: Path):
        with SoftFileLock(dest.with_suffix(".lock")):
            if dest.exists():
                logger.info(
                    "Merging sorted session '%s' into existing directory '%s'",
                    src.name,
                    dest,
                )
                for scan_dir in src.iterdir():
                    if scan_dir.is_dir():
                        scan_dir.rename(dest / scan_dir.name)
                exist_mdata_path = dest / cls.METADATA_FNAME
                new_mdata_path = src / cls.METADATA_FNAME
                if new_mdata_path.exists():
                    if exist_mdata_path.exists():
                        # Merge metadata files
                        mdata = Yaml(exist_mdata_path).load()
                        new_mdata = Yaml(new_mdata_path).load()
                        for key in set(mdata) & set(new_mdata):
                            if mdata[key] != new_mdata[key]:
                                raise ValueError(
                                    f"Conflict in metadata for key '{key}' between existing session at "
                                    f"'{exist_mdata_path}' and new session at '{new_mdata_path}'"
                                )
                        mdata.update(new_mdata)
                        Yaml(exist_mdata_path).save(mdata)
                    else:
                        new_mdata_path.rename(exist_mdata_path)
                if remaining := list(src.iterdir()):
                    raise ValueError(
                        f"Unexpected files/directories {remaining} found in saved session directory '{src}' "
                        f"after merging with existing session directory '{dest}'"
                    )
                src.rmdir()
            else:
                src.rename(dest)

    MANIFEST_FILENAME = "MANIFEST.yaml"

    def unlink(self) -> None:
        """Unlink all resources in the session"""
        for scan in self.scans.values():
            for resource in scan.resources.values():
                resource.unlink()

    def last_modified(self) -> int:
        """Returns the timestamp of the most recently modified file in the session
        in nanoseconds

        Returns
        -------
        int
            the mtime of the most recently modified file in the session in nanoseconds
        """
        return max(
            resource.fileset.last_modified
            for scan in self.scans.values()
            for resource in scan.resources.values()
        )


def fix_long_path(p: str | Path) -> Path:
    r"""Add \\?\ or \\?\UNC\ prefix on Windows for long paths."""
    if platform.system() != "Windows":
        return Path(p)

    path = Path(p)
    path_str = str(path.absolute())

    # Already has prefix, don't double-apply
    if path_str.startswith("\\\\?\\"):
        return path

    # UNC path: \\server\share\... -> \\?\UNC\server\share\...
    if path_str.startswith("\\\\"):
        return Path(f"\\\\?\\UNC\\{path_str[2:]}")

    # Local path: C:\... -> \\?\C:\...
    return Path(f"\\\\?\\{path_str}")


from .store import ImagingSessionMockStore  # noqa: E402


def json_serializer(obj: ty.Any) -> ty.Any:
    if isinstance(obj, Path):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def dicom_image_type_to_resource_label(image_type: list[str]) -> str:
    """Maps the image type of a DICOM series to the hard-coded resource names
    required by XNAT"""
    if image_type[:2] == [
        "DERIVED",
        "SECONDARY",
    ]:
        resource_label = "secondary"
    else:
        resource_label = "DICOM"  # special case
    return resource_label
