import typing as ty
import yaml
import logging
import attrs
from pathlib import Path
from fileformats.medimage import DicomSeries
from fileformats.generic import File
from .exceptions import DicomParseError

logger = logging.getLogger("xnat-ingest")


@attrs.define
class ImagingSession:

    dicoms: ty.List[DicomSeries]
    non_dicoms: ty.List[File]

    project_id: str | None = None
    subject_id: str | None = None
    session_id: str | None = None
    non_dicom_dir_name: str | None = None

    @classmethod
    def load(
        self,
        dicom_files: Path,
        non_dicoms_dir: Path,
        project_field: str = "StudyID",
        subject_field: str = "PatientID",
        session_field: str = "AccessionNumber",
        non_dicom_pattern: str = "{PatientName.given_name}_{PatientName.last_name}"
    ) -> ty.List["ImagingSession"]:


        dicoms = DicomSeries.from_paths()
        

        def get_id(field):
            ids = set(s[field] for s in dicoms)
            if len(ids) > 1:
                raise DicomParseError(
                    f"Multiple values for '{field}' tag found in dicom session"
                )
            return next(iter(ids))

        if self.project_id is None:
            self.project_id = get_id(project_field)
        if self.subject_id is None:
            self.subject_id = get_id(subject_field)
        if self.session_id is None:
            self.session_id = get_id(session_field)

        if self.non_dicom_dir_name:
            names = set(non_dicom_pattern.format(**s.metadata) for s in dicoms)
            if len(names) > 1:
                raise DicomParseError(
                    "Multiple values for non-DICOM directory name found in dicom session"
                )
            self.non_dicom_dir_name = next(iter(names))

    @classmethod
    def load(cls, file_name) -> "ImagingSession":
        with open(file_name) as f:
            spec = yaml.load(f, Loader=yaml.SafeLoader)
        logger.info(f"Loaded IDs from '{file_name}':\n{spec}")
        return ImagingSession(**spec)

    def save(self, file_name):
        yaml.dump(
            {
                "project": self.project_id,
                "subject": self.subject_id,
                "session": self.session_id,
                "overwrite": False,
            },
            file_name,
        )

    @attrs.define
    class Spec:

        @attrs.define
        class DicomSpec:

            type: str

        @attrs.define
        class NonDicomSpec:

            name: str

        dicoms: ty.List[DicomSpec]
        non_dicoms: ty.List[NonDicomSpec]

        @classmethod
        def load(cls, file_name) -> "ImagingSession.Spec":
            with open(file_name) as f:
                spec = yaml.load(f, Loader=yaml.SafeLoader)
            logger.info(f"Loaded upload spec from '{file_name}':\n{spec}")
            return ImagingSession.Spec(**spec)

        def save(self, file_name):
            yaml.dump(
                attrs.asdict(self, recurse=True),
                file_name,
            )


DICOM_FIELDS_TO_ANONYMISE = [
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
