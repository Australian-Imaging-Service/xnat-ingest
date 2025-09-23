import typing as ty
import re
from glob import glob
import logging
from functools import cached_property
import random
import hashlib
from datetime import datetime
import string
from itertools import chain
from collections import defaultdict, Counter
from pathlib import Path
from typing_extensions import Self
import attrs
from tqdm import tqdm
from fileformats.medimage import MedicalImage, DicomSeries
from fileformats.core import from_paths, FileSet, from_mime
from frametree.core.frameset import FrameSet
from frametree.core.exceptions import FrameTreeDataMatchError
from .exceptions import ImagingSessionParseError, StagingError
from .utils import AssociatedFiles, invalid_path_chars_re
from .scan import ImagingScan
from .resource import ImagingResource

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
                if isinstance(fileformat, FileSet):
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
        datatypes: ty.Union[
            ty.Type[FileSet], ty.Sequence[ty.Type[FileSet]]
        ] = DicomSeries,
        project_field: str = "StudyID",
        subject_field: str = "PatientID",
        visit_field: str = "AccessionNumber",
        scan_id_field: str = "SeriesNumber",
        scan_desc_field: str = "SeriesDescription",
        resource_field: str = "ImageType[-1]",
        session_field: str | None = "StudyInstanceUID",
        project_id: str | None = None,
    ) -> ty.List[Self]:
        """Loads all imaging sessions from a list of DICOM files

        Parameters
        ----------
        files_path : str or Path
            Path to a directory containging the resources to load the sessions from, or a
            glob string that selects the paths
        project_field : str
            the metadata field that contains the XNAT project ID for the imaging session,
            by default "StudyID"
        subject_field : str
            the metadata field that contains the XNAT subject ID for the imaging session,
            by default "PatientID"
        visit_field : str
            the metadata field that contains the XNAT visit ID for the imaging session,
            by default "AccessionNumber"
        scan_id_field: str
            the metadata field that contains the XNAT scan ID for the imaging session,
            by default "SeriesNumber"
        scan_desc_field: str
            the metadata field that contains the XNAT scan description for the imaging session,
            by default "SeriesDescription"
        resource_field: str
            the metadata field that contains the XNAT resource ID for the imaging session,
            by default "ImageType[-1]"
        session_field : str, optional
            the name of the metadata field that uniquely identifies the session, used
            to check that the values extracted from the IDs across the DICOM scans are
            consistent across DICOM files within the session, by default "StudyInstanceUID"
        project_id : str
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
        if isinstance(files_path, Path) or "*" not in files_path:
            files_path = Path(files_path)
            if not files_path.exists():
                raise ValueError(f"Provided DICOMs path '{files_path}' does not exist")
            if files_path.is_dir():
                fspaths = list(Path(files_path).iterdir())
            else:
                fspaths = [files_path]
        else:
            fspaths = [Path(p) for p in glob(files_path, recursive=True)]

        # Create a UID out of the paths that session was created from and the
        # timestamp
        crypto = hashlib.sha256()
        for fspath in fspaths:
            crypto.update(str(fspath.absolute()).encode())
        run_uid: str = crypto.hexdigest()[:6] + datetime.strftime(
            datetime.now(),
            "%Y%m%d%H%M%S",
        )

        from_paths_kwargs = {}
        if datatypes is DicomSeries:
            from_paths_kwargs["specific_tags"] = [
                project_field,
                subject_field,
                visit_field,
                session_field,
                scan_id_field,
                scan_desc_field,
                resource_field,
            ]

        if not isinstance(datatypes, ty.Sequence):
            datatypes = [datatypes]

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
            session_uid = resource.metadata[session_field] if session_field else None

            def get_id(field_type: str, field_name: str) -> str:
                if match := re.match(r"(\w+)\[([\-\d]+)\]", field_name):
                    field_name, index = match.groups()
                    index = int(index)
                else:
                    index = None
                try:
                    value = resource.metadata[field_name]
                except KeyError:
                    value = ""
                if not value:
                    if session_uid and field_type in ("project", "subject", "visit"):
                        try:
                            value = missing_ids[session_uid][field_type]
                        except KeyError:
                            value = missing_ids[session_uid][field_type] = (
                                "INVALID_MISSING_"
                                + field_type.upper()
                                + "_"
                                + "".join(
                                    random.choices(
                                        string.ascii_letters + string.digits, k=8
                                    )
                                )
                            )
                    else:
                        raise ImagingSessionParseError(
                            f"Did not find '{field_name}' field in {resource!r}, "
                            "cannot uniquely identify the resource, found:\n"
                            + "\n".join(resource.metadata)
                        )
                if index is not None:
                    value = value[index]
                elif isinstance(value, list):
                    frequency = Counter(value)
                    value = frequency.most_common(1)[0]
                value_str = str(value)
                value_str = invalid_path_chars_re.sub("_", value_str)
                return value_str

            if not explicit_project_id:
                project_id = get_id("project", project_field)
            subject_id = get_id("subject", subject_field)
            visit_id = get_id("visit", visit_field)
            scan_id = get_id("scan", scan_id_field)
            scan_type = get_id("scan type", scan_desc_field)
            if isinstance(resource, DicomSeries):
                resource_id = "DICOM"
            else:
                resource_id = get_id("resource", resource_field)

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
            session.add_resource(scan_id, scan_type, resource_id, resource)
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
        self, dest_dir: Path, copy_mode: FileSet.CopyMode = FileSet.CopyMode.copy
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
                )
        return deidentified

    def associate_files(
        self,
        patterns: ty.List[AssociatedFiles],
        spaces_to_underscores: bool = True,
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
                )

    def add_resource(
        self,
        scan_id: str,
        scan_type: str,
        resource_name: str,
        fileset: FileSet,
        overwrite: bool = False,
        associated: AssociatedFiles | None = None,
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
        """
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
        if resource_name in scan.resources and not overwrite:
            raise KeyError(
                f"Clash between resource names ('{resource_name}') for {scan_id} scan in "
                f"{self.name} session. Use 'overwrite=True' to overwrite the existing resource"
            )
        scan.resources[resource_name] = ImagingResource(
            name=resource_name, fileset=fileset, scan=scan
        )

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
                scan = ImagingScan.load(scan_dir, require_manifest=require_manifest)
                scan.session = session
                session.scans[scan.id] = scan
        return session

    def save(
        self,
        dest_dir: Path,
        available_projects: ty.Optional[ty.List[str]] = None,
        copy_mode: FileSet.CopyMode = FileSet.CopyMode.hardlink_or_copy,
    ) -> tuple[Self, Path]:
        r"""Stages and deidentifies files by removing the fields listed `FIELDS_TO_ANONYMISE` and
        replacing birth date with 01/01/<BIRTH-YEAR> and returning new imaging session

        Parameters
        ----------
        dest_dir : Path
            destination directory to save the deidentified files. The session will be saved
            to a directory with the project, subject and session IDs as subdirectories of
            this directory, along with the scans manifest
        associated_file_groups : Collection[AssociatedFiles], optional
            Glob pattern used to select the non-dicom files to include in the session. Note
            that the pattern is relative to the parent directory containing the DICOM files
            NOT the current working directory.
            The glob pattern can contain string template placeholders corresponding to DICOM
            metadata (e.g. '{PatientName.family_name}_{PatientName.given_name}'), which
            are substituted before the string is used to glob the non-DICOM files. In
            order to deidentify the filenames, the pattern must explicitly reference all
            identifiable fields in string template placeholders. By default, None

            Used to extract the scan ID & type/resource from the associated filename. Should
            be a regular-expression (Python syntax) with named groups called 'id' and 'type', e.g.
            '[^\.]+\.[^\.]+\.(?P<id>\d+)\.(?P<type>\w+)\..*'
        remove_original : bool, optional
            delete original files after they have been staged, false by default
        deidentify : bool, optional
            deidentify the scans in the staging process, true by default
        project_list : list[str], optional
            list of available projects in the store, used to check whether the project ID
            is valid
        spaces_to_underscores : bool, optional
            when building associated file globs, convert spaces underscores in fields
            extracted from source file metadata, false by default

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
