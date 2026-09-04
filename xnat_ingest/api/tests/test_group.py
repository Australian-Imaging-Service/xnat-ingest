import shutil
from pathlib import Path

import pytest
from fileformats.application import Json, Zip
from fileformats.core.exceptions import FormatRecognitionError
from fileformats.medimage import DicomDir, DicomSeries
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (
    get_image as get_pet_image,  # type: ignore[import-untyped]
)

from xnat_ingest.api.group_api import BUILD_NAME_DEFAULT, group
from xnat_ingest.helpers.arg_types import IDSpec, MetadataTable
from xnat_ingest.model.resource import ImagingResource
from xnat_ingest.model.session import ImagingSession

SESSION_FIELD = [IDSpec("StudyInstanceUID", "medimage/dicom-collection")]
SCAN_FIELD = [IDSpec("SeriesNumber", "medimage/dicom-collection")]
RESOURCE_FIELD = [IDSpec("ImageType[2:]", "medimage/dicom-collection")]


@pytest.fixture(scope="module")
def dicom_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    dicom_dir = tmp_path_factory.mktemp("dicom")
    get_pet_image(out_dir=dicom_dir)
    return dicom_dir


def test_group_creates_pre_assign_session_dir(dicom_dir: Path, tmp_path: Path) -> None:
    output_dir = tmp_path / "grouped"
    output_dir.mkdir()

    errors = group(
        input_paths=[str(dicom_dir)],
        output_dir=output_dir,
        datatypes=[DicomSeries],
        session=SESSION_FIELD,
        scan=SCAN_FIELD,
        resource=RESOURCE_FIELD,
    )

    assert errors == []
    session_dirs = [
        d for d in output_dir.iterdir() if d.is_dir() and d.name != BUILD_NAME_DEFAULT
    ]
    assert len(session_dirs) == 1
    session_dir = session_dirs[0]
    # Not yet assigned project/subject/visit IDs, so flagged with the pre-assign prefix
    assert session_dir.name.startswith(ImagingSession.PRE_ASSIGN_PREFIX)
    scan_dirs = [d for d in session_dir.iterdir() if d.is_dir()]
    assert len(scan_dirs) == 1
    # scan description is now resolved at 'assign' time, not 'group' time, so the
    # scan directory is saved with a trailing dot and no description
    assert scan_dirs[0].name.endswith(".")


def test_group_output_reloadable_with_no_assigned_ids(
    dicom_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "grouped"
    output_dir.mkdir()

    group(
        input_paths=[str(dicom_dir)],
        output_dir=output_dir,
        datatypes=[DicomSeries],
        session=SESSION_FIELD,
        scan=SCAN_FIELD,
        resource=RESOURCE_FIELD,
    )
    session_dir = next(
        d
        for d in output_dir.iterdir()
        if d.is_dir() and d.name.startswith(ImagingSession.PRE_ASSIGN_PREFIX)
    )

    reloaded = ImagingSession.load(session_dir)

    assert reloaded.project_id is None
    assert reloaded.subject_id is None
    assert reloaded.session_id is None
    assert reloaded.uid == session_dir.name[len(ImagingSession.PRE_ASSIGN_PREFIX) :]
    # scan description hasn't been resolved yet either
    scan = next(iter(reloaded.scans.values()))
    assert scan.type is None


def test_group_collects_errors_without_raising(tmp_path: Path) -> None:
    output_dir = tmp_path / "grouped"
    output_dir.mkdir()
    empty_input = tmp_path / "empty"
    empty_input.mkdir()

    errors = group(
        input_paths=[str(empty_input)],
        output_dir=output_dir,
        datatypes=[DicomSeries],
        session=SESSION_FIELD,
        scan=SCAN_FIELD,
        resource=RESOURCE_FIELD,
    )

    # No files found, so no sessions and no errors either
    assert errors == []


def test_group_unrecognised_file_raises_without_ignore_paths(
    dicom_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "grouped"
    output_dir.mkdir()
    input_dir = tmp_path / "input"
    shutil.copytree(dicom_dir, input_dir)
    (input_dir / "notes.txt").write_text("not a recognised format")

    with pytest.raises(FormatRecognitionError, match="notes.txt"):
        group(
            input_paths=[str(input_dir)],
            output_dir=output_dir,
            datatypes=[DicomSeries],
            session=SESSION_FIELD,
            scan=SCAN_FIELD,
            resource=RESOURCE_FIELD,
        )


def test_group_ignore_paths_skips_matching_unrecognised_files(
    dicom_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "grouped"
    output_dir.mkdir()
    input_dir = tmp_path / "input"
    shutil.copytree(dicom_dir, input_dir)
    (input_dir / "notes.txt").write_text("not a recognised format")

    errors = group(
        input_paths=[str(input_dir)],
        output_dir=output_dir,
        datatypes=[DicomSeries],
        session=SESSION_FIELD,
        scan=SCAN_FIELD,
        resource=RESOURCE_FIELD,
        ignore_paths=[r"notes\.txt"],
    )

    assert errors == []
    session_dirs = [
        d for d in output_dir.iterdir() if d.is_dir() and d.name != BUILD_NAME_DEFAULT
    ]
    assert len(session_dirs) == 1


def test_group_ignore_paths_pattern_not_matching_still_raises(
    dicom_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "grouped"
    output_dir.mkdir()
    input_dir = tmp_path / "input"
    shutil.copytree(dicom_dir, input_dir)
    (input_dir / "notes.txt").write_text("not a recognised format")

    with pytest.raises(FormatRecognitionError, match="notes.txt"):
        group(
            input_paths=[str(input_dir)],
            output_dir=output_dir,
            datatypes=[DicomSeries],
            session=SESSION_FIELD,
            scan=SCAN_FIELD,
            resource=RESOURCE_FIELD,
            ignore_paths=[r"unrelated-pattern"],
        )


def test_group_ignore_datatypes_excludes_recognised_but_unwanted_files(
    dicom_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "grouped"
    output_dir.mkdir()
    input_dir = tmp_path / "input"
    shutil.copytree(dicom_dir, input_dir)
    (input_dir / "notes.json").write_text("{}")

    errors = group(
        input_paths=[str(input_dir)],
        output_dir=output_dir,
        datatypes=[DicomSeries],
        session=SESSION_FIELD,
        scan=SCAN_FIELD,
        resource=RESOURCE_FIELD,
        ignore_datatypes=[Json],
    )

    assert errors == []
    session_dirs = [
        d for d in output_dir.iterdir() if d.is_dir() and d.name != BUILD_NAME_DEFAULT
    ]
    assert len(session_dirs) == 1
    scan_dir = next(d for d in session_dirs[0].iterdir() if d.is_dir())
    resource_dir = next(d for d in scan_dir.iterdir() if d.is_dir())
    assert not list(resource_dir.rglob("notes.json"))


def test_group_without_ignore_datatypes_raises_on_recognised_extra_type(
    dicom_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "grouped"
    output_dir.mkdir()
    input_dir = tmp_path / "input"
    shutil.copytree(dicom_dir, input_dir)
    (input_dir / "notes.json").write_text("{}")

    with pytest.raises(FormatRecognitionError, match="notes.json"):
        group(
            input_paths=[str(input_dir)],
            output_dir=output_dir,
            datatypes=[DicomSeries],
            session=SESSION_FIELD,
            scan=SCAN_FIELD,
            resource=RESOURCE_FIELD,
        )


def test_group_ignore_datatypes_contradicting_datatype_raises(
    dicom_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "grouped"
    output_dir.mkdir()

    with pytest.raises(ValueError, match="listed for both"):
        group(
            input_paths=[str(dicom_dir)],
            output_dir=output_dir,
            datatypes=[DicomSeries],
            session=SESSION_FIELD,
            scan=SCAN_FIELD,
            resource=RESOURCE_FIELD,
            ignore_datatypes=[DicomSeries],
        )


def test_group_creates_build_dir(tmp_path: Path) -> None:
    output_dir = tmp_path / "grouped"
    output_dir.mkdir()
    empty_input = tmp_path / "empty"
    empty_input.mkdir()

    group(
        input_paths=[str(empty_input)],
        output_dir=output_dir,
        datatypes=[DicomSeries],
        session=SESSION_FIELD,
        scan=SCAN_FIELD,
        resource=RESOURCE_FIELD,
    )

    assert (output_dir / BUILD_NAME_DEFAULT).exists()


@pytest.fixture
def patient_id(dicom_dir: Path) -> str:
    """The PatientID of the dummy dataset, discovered by a throwaway load so the
    metadata-table tests don't have to hard-code it"""
    sessions = ImagingSession.from_paths(
        dicom_dir, [DicomSeries], SESSION_FIELD, SCAN_FIELD, RESOURCE_FIELD
    )
    resource = next(iter(next(iter(sessions[0].scans.values())).resources.values()))
    return str(resource.fileset.metadata["PatientID"])


def test_group_injects_session_metadata_from_table(
    dicom_dir: Path, patient_id: str, tmp_path: Path
) -> None:
    output_dir = tmp_path / "grouped"
    output_dir.mkdir()
    table = tmp_path / "clinical.csv"
    table.write_text(
        "PatientID,StudyComment,Cohort\n"
        f"{patient_id},injected-via-table,control\n"
        "someone-else,should-not-be-used,case\n"
    )

    errors = group(
        input_paths=[str(dicom_dir)],
        output_dir=output_dir,
        datatypes=[DicomSeries],
        session=SESSION_FIELD,
        scan=SCAN_FIELD,
        resource=RESOURCE_FIELD,
        metadata_tables=[
            MetadataTable(str(table), "session", "PatientID=PatientID"),
        ],
    )

    assert errors == []
    session_dir = next(
        d
        for d in output_dir.iterdir()
        if d.is_dir() and d.name.startswith(ImagingSession.PRE_ASSIGN_PREFIX)
    )
    reloaded = ImagingSession.load(session_dir)
    assert reloaded.metadata["StudyComment"] == "injected-via-table"
    assert reloaded.metadata["Cohort"] == "control"


def test_from_paths_injects_resource_metadata_from_table(
    dicom_dir: Path, patient_id: str, tmp_path: Path
) -> None:
    table = tmp_path / "clinical.csv"
    table.write_text(f"PatientID,Radiotracer\n{patient_id},FDG\nother,ignored\n")

    sessions = ImagingSession.from_paths(
        dicom_dir,
        [DicomSeries],
        SESSION_FIELD,
        SCAN_FIELD,
        RESOURCE_FIELD,
        metadata_tables=[
            MetadataTable(str(table), "resource", "PatientID=PatientID"),
        ],
    )

    resource = next(iter(next(iter(sessions[0].scans.values())).resources.values()))
    assert resource.metadata["Radiotracer"] == "FDG"


def test_group_converts_directory_to_zip(dicom_dir: Path, tmp_path: Path):
    output_dir = tmp_path / "grouped"
    output_dir.mkdir()
    directory_dir = tmp_path / "directory"
    dicom0_dir = directory_dir / "dicom0"
    shutil.copytree(dicom_dir, dicom0_dir)

    errors = group(
        # input_paths=[str(dicom_dir)],
        input_paths=[str(directory_dir)],
        output_dir=output_dir,
        datatypes=[DicomDir],
        session=SESSION_FIELD,
        scan=SCAN_FIELD,
        resource=RESOURCE_FIELD,
        conversion_map={DicomDir: Zip},
    )

    assert errors == []
    session_dir = next(
        d
        for d in output_dir.iterdir()
        if d.is_dir() and d.name.startswith(ImagingSession.PRE_ASSIGN_PREFIX)
    )
    scan_dir = next(d for d in session_dir.iterdir() if d.is_dir())
    resource_dir = next(d for d in scan_dir.iterdir() if d.is_dir())
    manifest = Json(resource_dir / ImagingResource.MANIFEST_FNAME).load()
    assert manifest["datatype"] == Zip.mime_like
    assert (resource_dir / f"{dicom0_dir.name}.zip").exists()
