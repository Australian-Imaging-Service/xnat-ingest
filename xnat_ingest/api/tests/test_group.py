import shutil
from pathlib import Path

import pytest
from fileformats.core.exceptions import FormatRecognitionError
from fileformats.medimage import DicomSeries
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (
    get_image as get_pet_image,
)  # type: ignore[import-untyped]

from xnat_ingest.api.group_api import (
    BUILD_NAME_DEFAULT,
    group,
)
from xnat_ingest.helpers.arg_types import IDSpec
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


def test_group_unrecognised_file_raises_without_ignore(
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


def test_group_ignore_skips_matching_unrecognised_files(
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
        ignore=r"notes\.txt",
    )

    assert errors == []
    session_dirs = [
        d for d in output_dir.iterdir() if d.is_dir() and d.name != BUILD_NAME_DEFAULT
    ]
    assert len(session_dirs) == 1


def test_group_ignore_pattern_not_matching_still_raises(
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
            ignore=r"unrelated-pattern",
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
