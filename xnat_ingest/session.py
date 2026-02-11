import hashlib
import logging
import re
import typing as ty
from collections import Counter, defaultdict
from datetime import datetime
from functools import cached_property
from glob import glob
from itertools import chain
from pathlib import Path

import attrs
from fileformats.core import FileSet, from_mime, from_paths
from fileformats.medimage import DicomCollection, MedicalImage
from frametree.core.exceptions import FrameTreeDataMatchError
from frametree.core.frameset import FrameSet
from tqdm import tqdm
from typing_extensions import Self

from .exceptions import ImagingSessionParseError, StagingError
from .resource import ImagingResource
from .scan import ImagingScan
from .utils import AssociatedFiles, FieldSpec

logger = logging.getLogger("xnat-ingest")


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
    project_id: str
    subject_id: str
    visit_id: str
    scans: ty.Dict[str, ImagingScan] = attrs.field(
        factory=dict,
        converter=scans_converter,
        validator=attrs.validators.instance_of(dict),
    )
    run_uid: ty.Optional[str] = attrs.field(default=None)

    def __attrs_post_init__(self) -> None:
        for scan in self.scans.values():
            scan.session = self

    id_escape_re = re.compile(r"[^a-zA-Z0-9_]+")

    def __getitem__(self, fieldname: str) -> ty.Any:
        return self.metadata[fieldname]

    @property
    def name(self) -> str:
        return f"{self.project_id}-{self.subject_id}-{self.visit_id}"

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
        return ["-".join([self.project_id, self.subject_id, self.visit_id])]

    @property
    def session_id(self) -> str:
        return self.make_session_id(self.project_id, self.subject_id, self.visit_id)

    @classmethod
    def make_session_id(cls, project_id: str, subject_id: str, visit_id: str) -> str:
        return f"{subject_id}_{visit_id}"

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
        return [r for p in self.scans.values() for r in p.resources.values()]

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
        )

    def select_resources(
        self,
        dataset: ty.Optional[FrameSet],
        always_include: ty.Sequence[str] = (),
    ) -> ty.Iterator[ImagingResource]:
        """Returns selected resources that match the columns in the dataset definition

        Parameters
        ----------
        dataset : FrameSet
            Arcana dataset definition
        always_include : sequence[str]
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
            if mime_like == "all":
                fileformat = FileSet
            else:
                fileformat = from_mime(mime_like)  # type: ignore[assignment]
                if not issubclass(fileformat, FileSet):
                    raise ValueError(
                        f"{mime_like!r} does not correspond to a file format ({fileformat})"
                    )
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

    @cached_property
    def metadata(self) -> dict[str, ty.Any]:
        primary_resources = self.primary_resources
        all_keys = [list(d.metadata.keys()) for d in primary_resources if d.metadata]
        common_keys = [
            k for k in set(chain(*all_keys)) if all(k in keys for keys in all_keys)
        ]
        collated = {k: primary_resources[0].metadata[k] for k in common_keys}
        for i, resource in enumerate(primary_resources[1:], start=1):
            for key in common_keys:
                if not resource.metadata:
                    continue
                val = resource.metadata[key]
                if val != collated[key]:
                    # Check whether the value is the same as the values in the previous
                    # images in the series
                    if (
                        not isinstance(collated[key], list)
                        or isinstance(val, list)
                        and not isinstance(collated[key][0], list)
                    ):
                        collated[key] = [collated[key]] * i + [val]
                    collated[key].append(val)
        return collated

    @classmethod
    def from_paths(
        cls,
        files_path: str | Path,
        datatypes: ty.Union[ty.Type[FileSet], ty.Sequence[ty.Type[FileSet]]],
        project_field: list[FieldSpec],
        subject_field: list[FieldSpec],
        visit_field: list[FieldSpec],
        scan_id_field: list[FieldSpec],
        scan_desc_field: list[FieldSpec],
        resource_field: list[FieldSpec],
        session_field: list[FieldSpec],
        project_id: list[FieldSpec] | None = None,
        avoid_clashes: bool = False,
        recursive: bool = False,
    ) -> ty.List[Self]:
        """Loads all imaging sessions from a list of DICOM files

        Parameters
        ----------
        files_path : str or Path
            Path to a directory containging the resources to load the sessions from, or a
            glob string that selects the paths
        datatypes : type or list[type]
            the fileformats to load from the paths, e.g. DicomSeries or
            [DicomSeries, NiftiGz]
        project_field : list[IdField]
            the metadata field that contains the XNAT project ID for the imaging session,
            by default "StudyID"
        subject_field : list[IdField]
            the metadata field that contains the XNAT subject ID for the imaging session,
            by default "PatientID"
        visit_field : list[IdField]
            the metadata field that contains the XNAT visit ID for the imaging session,
            by default "AccessionNumber"
        scan_id_field: list[IdField]
            the metadata field that contains the XNAT scan ID for the imaging session,
            by default "SeriesNumber"
        scan_desc_field: list[IdField]
            the metadata field that contains the XNAT scan description for the imaging session,
            by default "SeriesDescription"
        resource_field: list[IdField]
            the metadata field that contains the XNAT resource ID for the imaging session,
            by default {FileSet: "ImageType[-1]"}
        session_field: list[IdField], optional
            the name of the metadata field that uniquely identifies the session, used
            to check that the values extracted from the IDs across the DICOM scans are
            consistent across DICOM files within the session, by default "StudyInstanceUID"
        project_id : str
            Override the project ID loaded from the metadata (useful when invoking
            manually)
        avoid_clashes : bool, optional
            if a resource with the same name already exists in the scan, increment the
            resource name by appending _1, _2 etc. to the name until a unique name is found,
            by default False
        recursive : bool, optional
            recurse into directories passed as file paths (i.e. by appending '**/*' and running a glob),
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
        fspaths = []
        for fspath in files_path:
            if isinstance(fspath, Path) or "*" not in fspath:
                fspath = Path(fspath)
                if not fspath.exists():
                    raise ValueError(f"Provided DICOMs path '{fspath}' does not exist")
                if fspath.is_dir():
                    if recursive:
                        fspaths.extend(
                            Path(p) for p in glob(str(fspath) + "/**/*", recursive=True)
                        )
                    else:
                        fspaths.extend(Path(fspath).iterdir())
                else:
                    fspaths.append(fspath)
            else:
                fspaths.extend(Path(p) for p in glob(fspath, recursive=True))

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
        # Optimise the reading of DICOM metadata by only selecting the specific tags that are required
        specific_tags = from_paths_kwargs["specific_tags"] = []
        for spec in (
            project_field,
            subject_field,
            visit_field,
            scan_id_field,
            scan_desc_field,
            resource_field,
            session_field,
        ):
            for field in spec:
                if issubclass(field.datatype, DicomCollection):
                    specific_tags.append(field.field_name)

        # Sort loaded series by StudyInstanceUID (imaging session)
        logger.info(f"Loading {datatypes} from {files_path}...")
        resources = from_paths(
            fspaths,
            *datatypes,
            ignore=".*",
            **from_paths_kwargs,  # type: ignore[arg-type]
        )
        sessions: ty.Dict[ty.Tuple[str, str, str] | str, Self] = {}
        multiple_sessions: ty.DefaultDict[str, ty.Set[ty.Tuple[str, str, str]]] = (
            defaultdict(set)
        )
        missing_ids: dict[str, dict[str, str]] = defaultdict(dict)
        explicit_project_id = project_id is not None
        for resource in tqdm(
            resources,
            "Sorting resources into XNAT tree structure...",
        ):
            session_uid = (
                FieldSpec.get_value_from_fields(resource, session_field)
                if session_field
                else None
            )
            missing_ids_session = (
                missing_ids[session_uid] if session_uid is not None else None
            )

            if not explicit_project_id:
                project_id = FieldSpec.get_value_from_fields(
                    resource, project_field, missing_ids_session
                )
            subject_id = FieldSpec.get_value_from_fields(
                resource, subject_field, missing_ids_session
            )
            visit_id = FieldSpec.get_value_from_fields(
                resource, visit_field, missing_ids_session
            )
            scan_id = FieldSpec.get_value_from_fields(
                resource, scan_id_field, missing_ids_session
            )
            scan_type = FieldSpec.get_value_from_fields(
                resource, scan_desc_field, missing_ids_session
            )

            if isinstance(resource, DicomCollection):
                try:
                    image_type = resource.contents[0].metadata["ImageType"]
                except (KeyError, IndexError):
                    resource_label = "DICOM"
                else:
                    if image_type[:2] == [
                        "DERIVED",
                        "SECONDARY",
                    ]:
                        resource_label = "secondary"
                    else:
                        resource_label = "DICOM"  # special case
            else:
                resource_label = FieldSpec.get_value_from_fields(
                    resource, resource_field
                )
            if session_uid is None:
                session_uid = (project_id, subject_id, visit_id)
            try:
                session = sessions[session_uid]
            except KeyError:
                session = cls(
                    project_id=project_id,
                    subject_id=subject_id,
                    visit_id=visit_id,
                    run_uid=run_uid,
                )
                sessions[session_uid] = session
            else:
                if (session.project_id, session.subject_id, session.visit_id) != (
                    project_id,
                    subject_id,
                    visit_id,
                ):
                    # Record all issues with the session IDs for raising exception at the end
                    multiple_sessions[session_uid].add(
                        (project_id, subject_id, visit_id)
                    )
                    multiple_sessions[session_uid].add(
                        (session.project_id, session.subject_id, session.visit_id)
                    )
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
                resource,
                avoid_clashes=avoid_clashes,
            )
        if multiple_sessions:
            raise ImagingSessionParseError(
                "Multiple session UIDs found with the same project/subject/visit ID triplets: "
                + "\n".join(
                    f"{i} -> " + str(["{p}:{s}:{v}" for p, s, v in sess])
                    for i, sess in multiple_sessions.items()
                )
            )
        return list(sessions.values())

    def deidentify(
        self,
        dest_dir: Path,
        copy_mode: FileSet.CopyMode = FileSet.CopyMode.copy,
        avoid_clashes: bool = False,
    ) -> Self:
        """Creates a new session with deidentified images

        Parameters
        ----------
        dest_dir : Path
            the directory to save the deidentified files into
        copy_mode : FileSet.CopyMode, optional
            the mode to use to copy the files that don't need to be deidentified,
            by default FileSet.CopyMode.copy

        Returns
        -------
        ImagingSession
            a new session with deidentified images
        """
        # Create a new session to save the deidentified files into
        deidentified = self.new_empty()
        for scan in self.scans.values():
            for resource_name, resource in scan.resources.items():
                resource_dest_dir = dest_dir / scan.id / resource_name
                if not isinstance(resource.fileset, MedicalImage):
                    deid_resource = resource.fileset.copy(
                        resource_dest_dir, mode=copy_mode, new_stem=resource_name
                    )
                else:
                    deid_resource = resource.fileset.deidentify(
                        resource_dest_dir, copy_mode=copy_mode, new_stem=resource_name
                    )
                deidentified.add_resource(
                    scan.id,
                    scan.type,
                    resource_name,
                    deid_resource,
                    avoid_clashes=avoid_clashes,
                )
        return deidentified

    def associate_files(
        self,
        patterns: ty.List[AssociatedFiles],
        spaces_to_underscores: bool = True,
        avoid_clashes: bool = False,
    ) -> None:
        """Adds files associated with the primary files to the session

        Parameters
        ----------
        patterns : list[AssociatedFiles]
            list of patterns to associate files with the primary files in the session
        spaces_to_underscores : bool, optional
            when building associated file globs, convert spaces underscores in fields
            extracted from source file metadata, false by default
        """
        for associated_files in patterns:
            # substitute string templates int the glob template with values from the
            # DICOM metadata to construct a glob pattern to select files associated
            # with current session
            associated_fspaths: ty.Set[Path] = set()
            for parent_dir in self.primary_parents:
                assoc_glob = str(
                    parent_dir / associated_files.glob.format(**self.metadata)
                )
                if spaces_to_underscores:
                    assoc_glob = assoc_glob.replace(" ", "_")
                # Select files using the constructed glob pattern
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
                self.add_resource(
                    scan_id,
                    scan_type,
                    resource_name,
                    from_paths([fspath], associated_files.datatype)[0],
                    associated=associated_files,
                    avoid_clashes=avoid_clashes,
                )

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
        for scan_dir in session_dir.iterdir():
            if scan_dir.is_dir():
                scan = ImagingScan.load(
                    scan_dir,
                    require_manifest=require_manifest,
                    check_checksums=check_checksums,
                )
                scan.session = session
                session.scans[scan.id] = scan
        return session

    def save(
        self,
        dest_dir: Path,
        available_projects: ty.Optional[ty.List[str]] = None,
        copy_mode: FileSet.CopyMode = FileSet.CopyMode.hardlink_or_copy,
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
        session_dirname = "-".join((project_id, self.subject_id, self.visit_id))
        if self.run_uid:
            session_dirname += f"-{self.run_uid}"
        session_dir = dest_dir / session_dirname
        session_dir.mkdir(parents=True, exist_ok=True)
        for scan in tqdm(self.scans.values(), f"Staging sessions to {session_dir}"):
            saved_scan = scan.save(session_dir, copy_mode=copy_mode)
            saved_scan.session = saved
            saved.scans[saved_scan.id] = saved_scan
        return saved, session_dir

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


from .store import ImagingSessionMockStore  # noqa: E402
