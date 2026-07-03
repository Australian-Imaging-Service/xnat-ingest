from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fileformats.core import FileSet
from fileformats.medimage import DicomSeries
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (
    get_image as get_pet_image,
)  # type: ignore[import-untyped]

from xnat_ingest.api.assign_ import assign
from xnat_ingest.api.group_ import group
from xnat_ingest.helpers.arg_types import IDSpec
from xnat_ingest.model.session import ImagingSession

PROJECT_FIELD = "StudyID"
SUBJECT_FIELD = "PatientID"
SESSION_FIELD = "AccessionNumber"
SCAN_FIELD = "SeriesDescription"


@pytest.fixture
def grouped_dir(tmp_path: Path) -> Path:
    """A directory containing a single grouped-but-not-yet-assigned session"""
    d = tmp_path / "grouped"
    d.mkdir()
    (d / f"{ImagingSession.PRE_ASSIGN_PREFIX}some-uid").mkdir()
    return d


def test_assign_calls_load_assign_save_for_each_session(
    grouped_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()

    mock_session = MagicMock()
    with patch.object(ImagingSession, "load", return_value=mock_session) as mock_load:
        errors = assign(
            input_dir=grouped_dir,
            output_dir=output_dir,
            project_field=PROJECT_FIELD,
            subject_field=SUBJECT_FIELD,
            session_field=SESSION_FIELD,
        )

    assert errors == []
    mock_load.assert_called_once()
    mock_session.assign.assert_called_once_with(
        project_field=PROJECT_FIELD,
        subject_field=SUBJECT_FIELD,
        session_field=SESSION_FIELD,
        constant_project_id=None,
        scan_field=None,
    )
    mock_session.save.assert_called_once_with(
        dest_dir=output_dir,
        copy_mode=FileSet.CopyMode.hardlink_or_copy,
    )


def test_assign_passes_scan_field(grouped_dir: Path, tmp_path: Path) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()

    mock_session = MagicMock()
    with patch.object(ImagingSession, "load", return_value=mock_session):
        assign(
            input_dir=grouped_dir,
            output_dir=output_dir,
            project_field=PROJECT_FIELD,
            subject_field=SUBJECT_FIELD,
            session_field=SESSION_FIELD,
            scan_field=SCAN_FIELD,
        )

    assert mock_session.assign.call_args.kwargs["scan_field"] == SCAN_FIELD


def test_assign_passes_constant_project_id(grouped_dir: Path, tmp_path: Path) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()

    mock_session = MagicMock()
    with patch.object(ImagingSession, "load", return_value=mock_session):
        assign(
            input_dir=grouped_dir,
            output_dir=output_dir,
            project_field=PROJECT_FIELD,
            subject_field=SUBJECT_FIELD,
            session_field=SESSION_FIELD,
            project_id="FIXED_PROJECT",
        )

    assert (
        mock_session.assign.call_args.kwargs["constant_project_id"] == "FIXED_PROJECT"
    )


def test_assign_collects_errors_without_raising(
    grouped_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()

    mock_session = MagicMock()
    mock_session.assign.side_effect = RuntimeError("simulated assign failure")
    with patch.object(ImagingSession, "load", return_value=mock_session):
        errors = assign(
            input_dir=grouped_dir,
            output_dir=output_dir,
            project_field=PROJECT_FIELD,
            subject_field=SUBJECT_FIELD,
            session_field=SESSION_FIELD,
        )

    assert len(errors) == 1
    assert "simulated assign failure" in errors[0]
    mock_session.save.assert_not_called()


def test_assign_raise_errors_propagates(grouped_dir: Path, tmp_path: Path) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()

    mock_session = MagicMock()
    mock_session.assign.side_effect = RuntimeError("simulated assign failure")
    with patch.object(ImagingSession, "load", return_value=mock_session):
        with pytest.raises(RuntimeError, match="simulated assign failure"):
            assign(
                input_dir=grouped_dir,
                output_dir=output_dir,
                project_field=PROJECT_FIELD,
                subject_field=SUBJECT_FIELD,
                session_field=SESSION_FIELD,
                raise_errors=True,
            )


def test_assign_deletes_source_dir_on_success_when_delete_true(
    grouped_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()
    session_dir = next(grouped_dir.iterdir())
    (session_dir / "some_file.txt").write_text("data")

    mock_session = MagicMock()
    with patch.object(ImagingSession, "load", return_value=mock_session):
        errors = assign(
            input_dir=grouped_dir,
            output_dir=output_dir,
            project_field=PROJECT_FIELD,
            subject_field=SUBJECT_FIELD,
            session_field=SESSION_FIELD,
            delete=True,
        )

    assert errors == []
    assert not session_dir.exists()


def test_assign_leaves_source_dir_when_delete_false(
    grouped_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()
    session_dir = next(grouped_dir.iterdir())

    mock_session = MagicMock()
    with patch.object(ImagingSession, "load", return_value=mock_session):
        assign(
            input_dir=grouped_dir,
            output_dir=output_dir,
            project_field=PROJECT_FIELD,
            subject_field=SUBJECT_FIELD,
            session_field=SESSION_FIELD,
            delete=False,
        )

    assert session_dir.exists()


def test_assign_does_not_delete_on_failure_even_if_delete_true(
    grouped_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()
    session_dir = next(grouped_dir.iterdir())

    mock_session = MagicMock()
    mock_session.assign.side_effect = RuntimeError("simulated assign failure")
    with patch.object(ImagingSession, "load", return_value=mock_session):
        assign(
            input_dir=grouped_dir,
            output_dir=output_dir,
            project_field=PROJECT_FIELD,
            subject_field=SUBJECT_FIELD,
            session_field=SESSION_FIELD,
            delete=True,
        )

    assert session_dir.exists()


# ── end-to-end integration test using real (dummy) DICOM data ────────────────


@pytest.fixture(scope="module")
def dicom_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    dicom_dir = tmp_path_factory.mktemp("dicom")
    get_pet_image(out_dir=dicom_dir)
    return dicom_dir


def test_assign_end_to_end_resolves_ids_from_grouped_metadata(
    dicom_dir: Path, tmp_path: Path
) -> None:
    grouped_dir = tmp_path / "grouped"
    grouped_dir.mkdir()
    group_errors = group(
        input_paths=[str(dicom_dir)],
        output_dir=grouped_dir,
        datatypes=[DicomSeries],
        session=[IDSpec("StudyInstanceUID", "medimage/dicom-collection")],
        scan=[IDSpec("SeriesNumber", "medimage/dicom-collection")],
        resource=[IDSpec("ImageType[2:]", "medimage/dicom-collection")],
    )
    assert group_errors == []

    output_dir = tmp_path / "assigned"
    output_dir.mkdir()

    errors = assign(
        input_dir=grouped_dir,
        output_dir=output_dir,
        project_field=PROJECT_FIELD,
        subject_field=SUBJECT_FIELD,
        session_field=SESSION_FIELD,
        scan_field=SCAN_FIELD,
    )

    assert errors == []
    session_dirs = list(output_dir.iterdir())
    assert len(session_dirs) == 1
    # PatientID = "Session Label", AccessionNumber = "987654321" in the dummy data
    # (spaces are escaped to underscores in resolved IDs)
    assert session_dirs[0].name == "PROJECT_ID.Session_Label.987654321"

    # The scan description ('SeriesDescription') has now been resolved, so the scan
    # directory should no longer have a trailing dot
    scan_dir = next(d for d in session_dirs[0].iterdir() if d.is_dir())
    assert not scan_dir.name.endswith(".")
