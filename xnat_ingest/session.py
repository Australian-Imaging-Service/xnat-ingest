import typing as ty
from glob import glob
import logging
import os.path
from functools import cached_property
from copy import copy
import shutil
import yaml
import attrs
from collections import defaultdict
from pathlib import Path
import pydicom
from fileformats.medimage import DicomSeries
from fileformats.core import from_paths, FileSet
from arcana.core.data.set import Dataset
from .exceptions import DicomParseError
from .utils import add_exc_note, transform_paths

logger = logging.getLogger("xnat-ingest")


@attrs.define
class ImagingSession:
    project_id: str
    subject_id: str
    session_id: str
    dicoms: ty.List[DicomSeries] = attrs.field(factory=list)
    non_dicoms_pattern: str | None = None
    non_dicom_fspaths: ty.List[Path] = attrs.field(factory=list)

    def __getitem__(self, fieldname: str) -> ty.Any:
        return self.metadata[fieldname]

    @property
    def name(self):
        return f"{self.project_id}-{self.subject_id}-{self.session_id}"

    @cached_property
    def modalities(self) -> ty.Set[str]:
        return set(self["Modality"])

    @property
    def dicom_dir(self) -> Path:
        "A common parent directory for all the top-level paths in the file-set"
        return Path(os.path.commonpath(p.parent for p in self.dicoms))  # type: ignore

    def select_resources(
        self, dataset: Dataset
    ) -> ty.Iterator[ty.Tuple[str, str, FileSet]]:
        """Returns selected resources that match the columns in the dataset definition

        Parameters
        ----------
        dataset : Dataset
            Arcana dataset definition

        Yields
        ------
        scan_id : str
            the ID of the scan should be uploaded to
        scan_type : str
            the desc/type to assign to the scan
        scan : FileSet
            a fileset to upload
        """
        raise NotImplementedError

    @cached_property
    def metadata(self):
        collated = copy(self.dicoms[0].metadata)
        if len(self.dicoms) > 1:
            for key, val in self.dicoms[1].metadata.items():
                if val != collated[key]:  # Turn field into list
                    collated[key] = [collated[key], val]
        for dicom in self.dicoms[2:]:
            for key, val in dicom.metadata.items():
                if val != collated[key]:
                    collated[key].append(val)
        return collated

    @classmethod
    def load(
        cls,
        dicoms_path: str | Path,
        non_dicoms_pattern: str | None = None,
        project_field: str = "StudyID",
        subject_field: str = "PatientID",
        session_field: str = "AccessionNumber",
    ) -> ty.List["ImagingSession"]:
        """Loads all imaging sessions from a list of DICOM files

        Parameters
        ----------
        dicoms_path : str or Path
            Path to a directory containging the DICOMS to load the sessions from, or a
            glob string that selects the paths
        non_dicoms_pattern : str
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
        session_dicoms = defaultdict(list)
        for series in from_paths(dicom_fspaths, DicomSeries):
            session_dicoms[series["StudyInstanceUID"]].append(series)

        # Construct sessions from sorted series
        sessions = []
        for dicoms in session_dicoms.values():

            def get_id(field):
                ids = set(s[field] for s in dicoms)
                if len(ids) > 1:
                    raise DicomParseError(
                        f"Multiple values for '{field}' tag found across scans in session: "
                        f"{dicoms}"
                    )
                id_ = next(iter(ids))
                if isinstance(id_, list):
                    raise DicomParseError(
                        f"Multiple values for '{field}' tag found within scans in session: "
                        f"{dicoms}"
                    )
                return id_

            if non_dicoms_pattern:
                non_dicoms_path = Path(
                    os.path.commonpath(dicom_fspaths)
                ) / non_dicoms_pattern.format(**dicoms[0].metadata)
                non_dicom_fspaths = [Path(p) for p in glob(str(non_dicoms_path))]
            else:
                non_dicom_fspaths = []

            sessions.append(
                cls(
                    dicoms=dicoms,
                    non_dicom_fspaths=non_dicom_fspaths,
                    non_dicoms_pattern=non_dicoms_pattern,
                    project_id=get_id(project_field),
                    subject_id=get_id(subject_field),
                    session_id=get_id(session_field),
                )
            )

        return sessions

    def override_ids(self, yaml_file: Path):
        """Override IDs extracted from DICOM metadata with manually specified IDs loaded
        from a YAML

        Parameters
        ----------
        yaml_file : Path
            name of the file to load the manually specified IDs from (YAML format)
        """
        try:
            with open(yaml_file) as f:
                spec = yaml.load(f, Loader=yaml.SafeLoader)
        except Exception as e:
            add_exc_note(
                e,
                f"Loading manual override of IDs from {yaml_file}, please check that it "
                "is a valid YAML file",
            )
            raise e
        for name, val in spec.items():
            setattr(self, name, val)

    def save_ids(self, yaml_file):
        """Save the project/subject/session IDs loaded from the session to a YAML file,
        so they can be manually overridden.

        Parameters
        ----------
        yaml_file : Path
            name of the file to load the manually specified IDs from (YAML format)
        """
        yaml.dump(
            {
                "project": self.project_id,
                "subject": self.subject_id,
                "session": self.session_id,
            },
            yaml_file,
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
        new_dicoms = []
        for dicom_series in self.dicoms:
            scan_dir = dest_dir / dicom_series.series_number / "DICOM"
            scan_dir.mkdir(parents=True, exists_ok=True)
            new_dicom_paths = []
            for dicom_file in dicom_series.fspaths:
                dcm = pydicom.dcmread(dicom_file)
                dcm.PatientBirthDate = dcm.PatientBirthDate[:4] + "0101"
                for field in self.FIELDS_TO_ANONYMISE:
                    try:
                        del dcm[(int(field[0]), int(field[1]))]
                    except KeyError:
                        pass
                new_path = scan_dir / dicom_file.name
                dcm.save_as(new_path)
                new_dicom_paths.append(new_path)
            new_dicom_series = DicomSeries(new_dicom_paths)
            new_dicoms.append(new_dicom_series)
        new_metadata = dicom_series.metadata
        if self.non_dicoms_pattern:
            new_non_dicom_fspaths = transform_paths(
                self.non_dicom_fspaths,
                self.non_dicoms_pattern,
                self.metadata,
                new_metadata,
            )
            for old, new in zip(self.non_dicom_fspaths, new_non_dicom_fspaths):
                shutil.copy(old, new)

        return type(self)(
            dicoms=new_dicoms,
            non_dicom_fspaths=new_non_dicom_fspaths,
            project_id=self.project_id,
            subject_id=self.subject_id,
            session_id=self.session_id,
        )

    def delete(self):
        """Delete all data associated with the session"""
        raise NotImplementedError

    FIELDS_TO_ANONYMISE = [
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
