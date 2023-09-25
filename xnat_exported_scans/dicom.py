import typing as ty
from collections import defaultdict
from pathlib import Path
from copy import copy
import attrs


@attrs.define
class DicomScan:

    Tag = ty.NewType("Tag", ty.Tuple[str, str])

    modality: str
    files: list[Path] = attrs.field(factory=list)
    ids: dict[str, str] = attrs.field(factory=dict)

    DEFAULT_ID_FIELDS = {
        "project": "StudyID",
        "subject": "PatientID",
        "session": "AccessionNumber",
    }

            
    
    @classmethod
    def from_files(
        cls,
        dicom_files: ty.Sequence[Path],
        ids: ty.Optional[dict[str, str]] = None,
        **id_fields: dict[str, ty.Union[str, Tag, tuple[str, ty.Callable], tuple[Tag, ty.Callable]]],
    ) -> "ty.Sequence[DicomScan]":
        """Loads a series of DICOM scans from a list of dicom files, grouping the files
        by series number and pulling various session-identifying fields from the headers

        Parameters
        ----------
        dicom_files: Sequence[Path]
            The dicom files to sort
        ids : dict[str, str]
            IDs to specifiy manually, overrides those loaded from the DICOM headers
        **id_fields : dict[str, ty.Union[str, Tag, tuple[str, ty.Callable], tuple[Tag, ty.Callable]]]
            The DICOM fields to extractx the IDs from. Values of the dictionary
            can either be the DICOM field name or tag as a tuple (e.g. `("0001", "0008")`)
            or a tuple containging the str/tag and a callable used to extract the
            ID from. For regex expressions you can use the DicomScan.id_exractor method
        """
        id_fields = copy(cls.DEFAULT_ID_FIELDS)
        id_fields.update(id_fields)

        scans: dict[str, DicomScan] = {}
        ids_dct = defaultdict(list)
        subject_id_dct = defaultdict(list)
        project_id_dct = defaultdict(list)
        # TESTNAME_GePhantom_20230825_155050
        for dcm_file in dicom_files:
            dcm = pydicom.dcmread(dcm_file)
            scan_id = dcm.SeriesNumber
            if "SECONDARY" in dcm.ImageType:
                modality = "SC"
            else:
                modality = dcm.Modality
            try:
                scan = scans[scan_id]
            except KeyError:
                scan = scans[scan_id] = Scan(modality=modality)
            else:
                # Get scan modality (should be the same for all dicoms with the same series
                # number)
                assert modality == scan.modality
            scan.files.append(dcm_file)
            project_id_dct[dcm.get(project_field.keyword)].append(dcm_file)
            subject_id_dct[dcm.get(subject_field.keyword)].append(dcm_file)
            session_id_dct[dcm.get(session_field.keyword)].append(dcm_file)
        errors: list[str] = []
        project_id: str = spec.get("project_id")  # type: ignore
        subject_id: str = spec.get("subject_id")  # type: ignore
        session_id: str = spec.get("session_id")  # type: ignore
        if project_id is None:
            project_ids = list(project_id_dct)
            if len(list(project_ids)) > 1:
                errors.append(
                    f"Incosistent project IDs found in {project_field}:\n"
                    + json.dumps(project_id_dct, indent=4)
                )
            else:
                project_id = project_ids[0]
                if not project_id:
                    logger.error(f"Project ID ({project_field}) not provided")
        if subject_id is None:
            subject_ids = list(subject_id_dct)
            if len(subject_ids) > 1:
                errors.append(
                    f"Incosistent subject IDs found in {subject_field}:\n"
                    + json.dumps(subject_id_dct, indent=4)
                )
            else:
                # FIXME: space is present in test data, but shouldn't be in prod
                subject_id = subject_ids[0].replace(" ", "_")
                if not subject_id:
                    errors.append(f"Subject ID ({subject_field}) not provided")
        if session_id is None:
            session_ids = list(session_id_dct)
            if len(session_ids) > 1:
                errors.append(
                    f"Incosistent session IDs found in {session_field}:\n"
                    + json.dumps(session_id_dct, indent=4)
                )
            else:
                session_id = session_ids[0]
                if not session_id:
                    errors.append(f"Session ID ({session_field}) not provided")
        if errors:
            raise DicomParseError("\n".join(errors))
        non_dicom_dir_name = "_".join(dcm.PatientName.split("^")) + "_" + dcm.StudyDate
        return scans, SessionMetadata(
            project_id, subject_id, session_id, non_dicom_dir_name
        )


DEFAULT_FIELDS_TO_ANONYMISE = [
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
