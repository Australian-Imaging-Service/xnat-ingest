import typing as ty
import re
from glob import glob
import logging
import os.path
import subprocess as sp
from functools import cached_property
import shutil
import yaml
import attrs
from itertools import chain
from collections import defaultdict
from pathlib import Path
import pydicom
from fileformats.application import Dicom
from fileformats.medimage import DicomSeries
from fileformats.core import from_paths, FileSet, DataType
from fileformats.generic import File, Directory
from arcana.core.data.set import Dataset
from arcana.core.data.space import DataSpace
from arcana.core.data.row import DataRow
from arcana.core.data.store import DataStore
from arcana.core.data.entry import DataEntry
from arcana.core.data.tree import DataTree
from arcana.core.exceptions import ArcanaDataMatchError
from .exceptions import DicomParseError, StagingError
from .utils import add_exc_note, transform_paths

logger = logging.getLogger("xnat-ingest")


def dicoms_converter(
    multi_dicom_series: ty.Union[ty.List[DicomSeries], ty.Dict[str, DicomSeries]]
) -> ty.Dict[str, DicomSeries]:
    if isinstance(multi_dicom_series, ty.Sequence):
        multi_dicom_series = {str(s["SeriesNumber"]): s for s in multi_dicom_series}
    return multi_dicom_series


@attrs.define(slots=False)
class ImagingSession:
    project_id: str
    subject_id: str
    session_id: str
    dicoms: ty.Dict[str, DicomSeries] = attrs.field(
        factory=dict, converter=dicoms_converter
    )
    associated_files_pattern: str | None = None
    associated_file_fspaths: ty.List[Path] = attrs.field(factory=list)

    def __getitem__(self, fieldname: str) -> ty.Any:
        return self.metadata[fieldname]

    @property
    def name(self):
        return f"{self.project_id}-{self.subject_id}-{self.session_id}"

    @cached_property
    def modalities(self) -> ty.Set[str]:
        modalities = self["Modality"]
        if not isinstance(modalities, str):
            modalities = set(modalities)
        return modalities

    @property
    def dicom_dir(self) -> Path:
        "A common parent directory for all the top-level paths in the file-set"
        return Path(os.path.commonpath(p.parent for p in self.dicoms.values()))  # type: ignore

    def select_resources(
        self,
        dataset: Dataset,
        include_all_dicoms: bool = False,
    ) -> ty.Iterator[ty.Tuple[str, str, str, FileSet]]:
        """Returns selected resources that match the columns in the dataset definition

        Parameters
        ----------
        dataset : Dataset
            Arcana dataset definition
        include_all_dicoms : bool
            whether to include all dicoms in upload or only those that are explicitly
            specified by a column in the dataset

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
        store = MockDataStore(self)

        uploaded: ty.Set[FileSet] = set()

        if include_all_dicoms:
            for scan_id, dcm in self.dicoms.items():
                uploaded.add(dcm)
                yield scan_id, dcm["SeriesDescription"], "DICOM", dcm

        for column in dataset.columns.values():
            try:
                entry = column.match_entry(store.row)
            except ArcanaDataMatchError as e:
                raise StagingError(
                    f"Did not find matching entry for {column} column in {dataset} from "
                    f"{self.name} session"
                ) from e
            else:
                scan = column.datatype(entry.item)
                if scan in uploaded:
                    logger.warning(
                        "scan %s already matched by previous columns (or `included_all_dicoms` option), "
                        "skipping match with %s",
                        scan,
                        column,
                    )
                    continue
                resource_name = (
                    column.datatype.mime_like.split("/")[-1]
                    .replace("-", "_")
                    .split(".")[-1]
                )
                if resource_name == "dicom_series":
                    resource_name = "DICOM"
                if isinstance(scan, (DicomSeries, Dicom)):
                    scan_id = str(scan.metadata["SeriesNumber"])
                    scan_type = str(scan["SeriesDescription"])
                elif isinstance(scan, Dicom):
                    scan_id = scan["Series"]
                else:
                    scan_id = column.name
                    if column.is_regex and re.compile(column.path).groups:
                        pattern = column.path
                    else:
                        pattern = r"^[\._\-]*(.*[a-zA-Z0-9])[\._\-]*"
                    if isinstance(scan, File):
                        scan_type = scan.stem
                    elif isinstance(scan, Directory):
                        scan_type = scan.fspath.name
                    else:
                        scan_type = scan.parent
                    match = re.match(pattern, scan_type)
                    if not match:
                        raise RuntimeError(f"{pattern} did not match {scan_type}")
                    scan_type = match.group(1)
            uploaded.add(scan)
            yield scan_id, scan_type, resource_name, scan

    @cached_property
    def metadata(self):
        all_dicoms = list(self.dicoms.values())
        all_keys = [list(d.metadata.keys()) for d in all_dicoms]
        common_keys = [
            k for k in set(chain(*all_keys)) if all(k in keys for keys in all_keys)
        ]
        collated = {k: all_dicoms[0][k] for k in common_keys}
        for i, series in enumerate(all_dicoms[1:], start=1):
            for key in common_keys:
                val = series[key]
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
    def construct(
        cls,
        dicoms_path: str | Path,
        associated_files_pattern: str | None = None,
        project_field: str = "StudyID",
        subject_field: str = "PatientID",
        session_field: str = "AccessionNumber",
        project_id: str | None = None,
    ) -> ty.List["ImagingSession"]:
        """Loads all imaging sessions from a list of DICOM files

        Parameters
        ----------
        dicoms_path : str or Path
            Path to a directory containging the DICOMS to load the sessions from, or a
            glob string that selects the paths
        associated_files_pattern : str
            Pattern used to select the non-dicom files to include in the session. The
            pattern can contain string template placeholders corresponding to DICOM
            metadata (e.g. '{PatientName.given_name}_{PatientName.family_name}'), which
            are substituted before the string is used to glob the non-DICOM files. In
            order to deidentify the filenames, the pattern must explicitly reference all
            identifiable fields in string template placeholders.
        project_field : str
            the name of the DICOM field that is to be interpreted as the corresponding
            XNAT project
        subject_field : str
            the name of the DICOM field that is to be interpreted as the corresponding
            XNAT project
        session_field : str
            the name of the DICOM field that is to be interpreted as the corresponding
            XNAT project
        project_id : str
            Override the project ID loaded from the DICOM header (useful when invoking
            manually)

        Returns
        -------
        list[ImagingSession]
            all imaging sessions that are present in list of dicom paths

        Raises
        ------
        DicomParseError
            if values extracted from IDs across the DICOM scans are not consistent across
            DICOM files within the session
        """
        if isinstance(dicoms_path, Path) or "*" not in dicoms_path:
            dicom_fspaths = list(Path(dicoms_path).iterdir())
        else:
            dicom_fspaths = [Path(p) for p in glob(dicoms_path)]

        # Sort loaded series by StudyInstanceUID (imaging session)
        logger.info("Loading DICOM series from %s", str(dicoms_path))
        dicom_sessions = defaultdict(list)
        for series in from_paths(dicom_fspaths, DicomSeries):
            dicom_sessions[series["StudyInstanceUID"]].append(series)

        # Construct sessions from sorted series
        logger.info("Searching for associated files ")
        sessions = []
        for session_dicom_series in dicom_sessions.values():

            def get_id(field):
                ids = set(s[field.keyword] for s in session_dicom_series)
                if len(ids) > 1:
                    raise DicomParseError(
                        f"Multiple values for '{field}' tag found across scans in session: "
                        f"{session_dicom_series}"
                    )
                id_ = next(iter(ids))
                if isinstance(id_, list):
                    raise DicomParseError(
                        f"Multiple values for '{field}' tag found within scans in session: "
                        f"{session_dicom_series}"
                    )
                id_ = id_.replace(" ", "_")
                return id_

            if associated_files_pattern:
                associated_files_path = Path(
                    os.path.commonpath(dicom_fspaths)
                ) / associated_files_pattern.format(**session_dicom_series[0].metadata)
                associated_file_fspaths = [
                    Path(p) for p in glob(str(associated_files_path))
                ]
            else:
                associated_file_fspaths = []

            sessions.append(
                cls(
                    dicoms={str(s["SeriesNumber"]): s for s in session_dicom_series},
                    associated_file_fspaths=associated_file_fspaths,
                    associated_files_pattern=associated_files_pattern,
                    project_id=(project_id if project_id else get_id(project_field)),
                    subject_id=get_id(subject_field),
                    session_id=get_id(session_field),
                )
            )

        return sessions

    @classmethod
    def load(cls, save_dir: Path):
        """Override IDs extracted from DICOM metadata with manually specified IDs loaded
        from a YAML

        Parameters
        ----------
        yaml_file : Path
            name of the file to load the manually specified IDs from (YAML format)
        """
        yaml_file = save_dir / cls.SAVE_FILENAME
        try:
            with open(yaml_file) as f:
                dct = yaml.load(f, Loader=yaml.SafeLoader)
        except Exception as e:
            add_exc_note(
                e,
                f"Loading saved session from {yaml_file}, please check that it "
                "is a valid YAML file",
            )
            raise e
        dct["dicoms"] = {k: DicomSeries(v) for k, v in dct["dicoms"].items()}
        dct["associated_file_fspaths"] = [
            Path(f) for f in dct["associated_file_fspaths"]
        ]
        return cls(**dct)

    def save(self, save_dir: Path):
        """Save the project/subject/session IDs loaded from the session to a YAML file,
        so they can be manually overridden.

        Parameters
        ----------
        yaml_file : Path
            name of the file to load the manually specified IDs from (YAML format)
        """
        dct = attrs.asdict(self, recurse=True)
        dct["associated_file_fspaths"] = [
            str(p) for p in dct["associated_file_fspaths"]
        ]
        dct["dicoms"] = {k: [str(p) for p in v["fspaths"]] for k, v in dct["dicoms"].items()}
        yaml_file = save_dir / self.SAVE_FILENAME
        with open(yaml_file, "w") as f:
            yaml.dump(
                dct,
                f,
            )

    def deidentify(self, dest_dir: Path) -> "ImagingSession":
        """Deidentify files by removing the fields listed `FIELDS_TO_ANONYMISE` and
        replacing birth date with 01/01/<BIRTH-YEAR> and returning new imaging session

        Parameters
        ----------
        dest_dir : Path
            destination directory to save the deidentified files

        Returns
        -------
        ImagingSession
            a deidentified session with updated paths
        """
        if not self._dcmedit_path:
            logger.warning(
                "Did not find `dcmedit` tool from the MRtrix package on the system path, "
                "de-identification will be performed by pydicom instead and may be slower"
            )
        new_dicoms = []
        for series_number, dicom_series in self.dicoms.items():
            scan_dir = dest_dir / series_number / "DICOM"
            scan_dir.mkdir(parents=True, exist_ok=True)
            new_dicom_paths = []
            for dicom_file in dicom_series.fspaths:
                new_dicom_paths.append(
                    self.deidentify_dicom(dicom_file, scan_dir / dicom_file.name)
                )
            new_dicom_series = DicomSeries(new_dicom_paths)
            new_dicoms.append(new_dicom_series)
        new_metadata = new_dicom_series.metadata
        if self.associated_files_pattern:
            transformed_fspaths = transform_paths(
                self.associated_file_fspaths,
                self.associated_files_pattern,
                self.metadata,
                new_metadata,
            )
            new_associated_file_fspaths = []
            for old, new in zip(self.associated_file_fspaths, transformed_fspaths):
                dest_path = dest_dir / new.name
                if Dicom.matches(old):
                    self.deidentify_dicom(old, dest_path)
                else:
                    shutil.copyfile(old, dest_path)
                new_associated_file_fspaths.append(dest_path)

        return type(self)(
            dicoms=new_dicoms,
            associated_file_fspaths=new_associated_file_fspaths,
            project_id=self.project_id,
            subject_id=self.subject_id,
            session_id=self.session_id,
        )

    def delete(self):
        """Delete all data associated with the session"""
        for fspath in chain(
            self.associated_file_fspaths, *(d.fspaths for d in self.dicoms.values())
        ):
            os.unlink(fspath)

    def deidentify_dicom(self, dicom_file: Path, new_path: Path) -> Path:
        if self._dcmedit_path:
            # Get year of birth
            # yob = (
            #     sp.check_output(
            #         [self._dcminfo_path, "-tag", "0010", "0030", str(dicom_file)]
            #     )
            #     .decode("utf-8")
            #     .split(" ")[-4:]
            # )
            # Copy to new path
            shutil.copyfile(dicom_file, new_path)
            # Replace date of birth date with 1st of Jan
            args = [
                self._dcmedit_path,
                "-anonymise",
                str(new_path),
            ]
            sp.check_call(args)
            # sp.check_call(
            #     [
            #         self._dcmedit_path,
            #         "-tag",
            #         "0010",
            #         "0030",
            #         f"0101{yob}",
            #         str(new_path),
            #     ]
            # )
            # # Clear remaining identifable fields
            # sp.check_call(
            #     (
            #         [self._dcmedit_path]
            #         + list(
            #             chain(*(("-tag",) + t + ("",) for t in self.FIELDS_TO_CLEAR))
            #         )
            #         + [str(new_path)]
            #     )
            # )
        else:
            dcm = pydicom.dcmread(dicom_file)
            dcm.PatientBirthDate = ""  # dcm.PatientBirthDate[:4] + "0101"
            for field in self.FIELDS_TO_CLEAR:
                try:
                    elem = dcm[field]  # type: ignore
                except KeyError:
                    pass
                else:
                    elem.value = ""
            dcm.save_as(new_path)
        return new_path

    @cached_property
    def _dcmedit_path(self) -> str:
        try:
            return sp.check_output("which dcmedit", shell=True).decode("utf-8").strip()
        except sp.CalledProcessError:
            return None

    @cached_property
    def _dcminfo_path(self) -> str:
        try:
            return sp.check_output("which dcminfo", shell=True).decode("utf-8").strip()
        except sp.CalledProcessError:
            return None

    FIELDS_TO_CLEAR = [
        ("0008", "0014"),  # Instance Creator UID
        ("0008", "1111"),  # Referenced Performed Procedure Step SQ
        ("0008", "1120"),  # Referenced Patient SQ
        ("0008", "1140"),  # Referenced Image SQ
        ("0008", "0096"),  # Referring Physician Identification SQ
        ("0008", "1032"),  # Procedure Code SQ
        ("0008", "1048"),  # Physician(s) of Record
        ("0008", "1049"),  # Physician(s) of Record Identification SQ
        ("0008", "1050"),  # Performing Physicians' Name
        ("0008", "1052"),  # Performing Physician Identification SQ
        ("0008", "1060"),  # Name of Physician(s) Reading Study
        ("0008", "1062"),  # Physician(s) Reading Study Identification SQ
        ("0008", "1110"),  # Referenced Study SQ
        ("0008", "1111"),  # Referenced Performed Procedure Step SQ
        ("0008", "1250"),  # Related Series SQ
        ("0008", "9092"),  # Referenced Image Evidence SQ
        ("0008", "0080"),  # Institution Name
        ("0008", "0081"),  # Institution Address
        ("0008", "0082"),  # Institution Code Sequence
        ("0008", "0092"),  # Referring Physician's Address
        ("0008", "0094"),  # Referring Physician's Telephone Numbers
        ("0008", "009C"),  # Consulting Physician's Name
        ("0008", "1070"),  # Operators' Name
        ("0010", "4000"),  # Patient Comments
        ("0010", "0010"),  # Patient's Name
        ("0010", "0021"),  # Issuer of Patient ID
        ("0010", "0032"),  # Patient's Birth Time
        ("0010", "0050"),  # Patient's Insurance Plan Code SQ
        ("0010", "0101"),  # Patient's Primary Language Code SQ
        ("0010", "1000"),  # Other Patient IDs
        ("0010", "1001"),  # Other Patient Names
        ("0010", "1002"),  # Other Patient IDs SQ
        ("0010", "1005"),  # Patient's Birth Name
        ("0010", "1010"),  # Patient's Age
        ("0010", "1040"),  # Patient's Address
        ("0010", "1060"),  # Patient's Mother's Birth Name
        ("0010", "1080"),  # Military Rank
        ("0010", "1081"),  # Branch of Service
        ("0010", "1090"),  # Medical Record Locator
        ("0010", "2000"),  # Medical Alerts
        ("0010", "2110"),  # Allergies
        ("0010", "2150"),  # Country of Residence
        ("0010", "2152"),  # Region of Residence
        ("0010", "2154"),  # Patient's Telephone Numbers
        ("0010", "2160"),  # Ethnic Group
        ("0010", "2180"),  # Occupation
        ("0010", "21A0"),  # Smoking Status
        ("0010", "21B0"),  # Additional Patient History
        ("0010", "21C0"),  # Pregnancy Status
        ("0010", "21D0"),  # Last Menstrual Date
        ("0010", "21F0"),  # Patient's Religious Preference
        ("0010", "2203"),  # Patient's Sex Neutered
        ("0010", "2297"),  # Responsible Person
        ("0010", "2298"),  # Responsible Person Role
        ("0010", "2299"),  # Responsible Organization
        ("0020", "9221"),  # Dimension Organization SQ
        ("0020", "9222"),  # Dimension Index SQ
        ("0038", "0010"),  # Admission ID
        ("0038", "0011"),  # Issuer of Admission ID
        ("0038", "0060"),  # Service Episode ID
        ("0038", "0061"),  # Issuer of Service Episode ID
        ("0038", "0062"),  # Service Episode Description
        ("0038", "0500"),  # Patient State
        ("0038", "0100"),  # Pertinent Documents SQ
        ("0040", "0260"),  # Performed Protocol Code SQ
        ("0088", "0130"),  # Storage Media File-Set ID
        ("0088", "0140"),  # Storage Media File-Set UID
        ("0400", "0561"),  # Original Attributes Sequence
        ("5200", "9229"),  # Shared Functional Groups SQ
    ]

    SAVE_FILENAME = "saved-session.yaml"


@attrs.define
class MockDataStore(DataStore):
    """Mock data store so we can use the column.match_entry method on the "entries" in
    the data row
    """

    session: ImagingSession

    @property
    def row(self):
        return DataRow(
            ids={DummySpace._: None},
            dataset=Dataset(id=None, store=self, hierarchy=[], space=DummySpace),
            frequency=DummySpace._,
        )

    def populate_row(self, row: DataRow):
        """
        Populate a row with all data entries found in the corresponding node in the data
        store (e.g. files within a directory, scans within an XNAT session) using the
        ``DataRow.add_entry`` method. Within a node/row there are assumed to be two types
        of entries, "primary" entries (e.g. acquired scans) common to all analyses performed
        on the dataset and "derivative" entries corresponding to intermediate outputs
        of previously performed analyses. These types should be stored in separate
        namespaces so there is no chance of a derivative overriding a primary data item.

        The name of the dataset/analysis a derivative was generated by is appended to
        to a base path, delimited by "@", e.g. "brain_mask@my_analysis". The dataset
        name is left blank by default, in which case "@" is just appended to the
        derivative path, i.e. "brain_mask@".

        Parameters
        ----------
        row : DataRow
            The row to populate with entries
        """
        for series_number, dcm in self.session.dicoms.items():
            row.add_entry(
                path=dcm["SeriesDescription"],
                datatype=DicomSeries,
                uri=f"dicom::{dcm['SeriesNumber']}",
            )
        for non_dcm_fspath in self.session.associated_file_fspaths:
            row.add_entry(
                path=non_dcm_fspath.name,
                datatype=FileSet,
                uri=f"associated_file::{non_dcm_fspath}",
            )

    def get(self, entry: DataEntry, datatype: type) -> DataType:
        """
        Gets the data item corresponding to the given entry

        Parameters
        ----------
        entry : DataEntry
            the data entry to update
        datatype : type
            the datatype to interpret the entry's item as

        Returns
        -------
        item : DataType
            the item stored within the specified entry
        """
        file_category, path = entry.uri.split("::")
        if file_category == "dicom":
            fileset = datatype(self.session.dicoms[path])
        else:
            fileset = datatype(path)
        return fileset

    ######################################
    # The following methods can be empty #
    ######################################

    def populate_tree(self, tree: DataTree):
        pass

    def connect(self) -> ty.Any:
        pass

    def disconnect(self, session: ty.Any):
        pass

    def create_data_tree(
        self,
        id: str,
        leaves: ty.List[ty.Tuple[str, ...]],
        hierarchy: ty.List[str],
        space: type,
        **kwargs,
    ):
        raise NotImplementedError

    ###################################
    # The following shouldn't be used #
    ###################################

    def put(self, item: DataType, entry: DataEntry) -> DataType:
        raise NotImplementedError

    def put_provenance(self, provenance: ty.Dict[str, ty.Any], entry: DataEntry):
        raise NotImplementedError

    def get_provenance(self, entry: DataEntry) -> ty.Dict[str, ty.Any]:
        raise NotImplementedError

    def save_dataset_definition(
        self, dataset_id: str, definition: ty.Dict[str, ty.Any], name: str
    ):
        raise NotImplementedError

    def load_dataset_definition(
        self, dataset_id: str, name: str
    ) -> ty.Dict[str, ty.Any]:
        raise NotImplementedError

    def site_licenses_dataset(self):
        raise NotImplementedError

    def create_entry(self, path: str, datatype: type, row: DataRow) -> DataEntry:
        raise NotImplementedError


class DummySpace(DataSpace):
    _ = 0b0
