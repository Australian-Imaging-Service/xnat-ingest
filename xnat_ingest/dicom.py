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
        associated_file_dir_name = "_".join(dcm.PatientName.split("^")) + "_" + dcm.StudyDate
        return scans, SessionMetadata(
            project_id, subject_id, session_id, associated_file_dir_name
        )
