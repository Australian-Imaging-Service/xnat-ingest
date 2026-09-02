from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fileformats.core import FileSet
from fileformats.generic import File
from fileformats.medimage import DicomSeries
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (
    get_image as get_pet_image,  # type: ignore[import-untyped]
)

from xnat_ingest.api.assign_api import INVALID_DIRNAME, assign
from xnat_ingest.api.group_api import group
from xnat_ingest.helpers.arg_types import IDSpec
from xnat_ingest.helpers.remotes import list_session_dirs
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


def _mock_session(dirname: str = "PROJ.SUBJ.SESS") -> MagicMock:
    """A mock session whose save() actually creates its output directory.

    assign materialises a session that does not exist yet under __build__ and
    renames it into place, so a save that writes nothing leaves nothing to
    rename. The mock also has to answer staging_dirname(), which assign uses to
    work out where the output WILL land before saving it.
    """
    mock = MagicMock()

    def _save(dest_dir: Path, **kwargs):
        saved_dir = Path(dest_dir) / dirname
        saved_dir.mkdir(parents=True, exist_ok=True)
        return mock, saved_dir

    mock.staging_dirname.return_value = dirname
    mock.save.side_effect = _save
    return mock

def test_assign_calls_load_assign_save_for_each_session(
    grouped_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()

    mock_session = _mock_session()
    mock_session.assign.return_value = {}
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
    # save() is handed the BUILD directory, not the output directory: a session
    # that does not exist yet is built under __build__ and renamed into place, so
    # that a run which dies part-way cannot leave a partial session under its real
    # name for upload to collect.
    saved_kwargs = mock_session.save.call_args.kwargs
    assert saved_kwargs["copy_mode"] == FileSet.CopyMode.hardlink_or_copy
    assert saved_kwargs["include"] == ()
    build_dir = saved_kwargs["dest_dir"]
    assert build_dir.parent.name == "__build__"
    assert build_dir.parent.parent == output_dir
    # and the finished session is what actually appears in the output
    assert (output_dir / "PROJ.SUBJ.SESS").is_dir()
    assert not build_dir.parent.exists(), "the build directory was left behind"


def test_assign_leaves_no_partial_session_under_its_real_name_when_save_dies(
    grouped_dir: Path, tmp_path: Path
) -> None:
    """A run that dies part-way through save() must leave nothing collectable.

    This is the live path. With de-identification done in Orthanc, which is how
    every site is configured, `upload` reads the assigned directory DIRECTLY.
    save() creates the session directory and then fills it one scan at a time, so
    a crash used to leave a prefix of the scans under the session's real name.
    Each surviving resource is internally consistent, so upload's per-resource
    manifest and checksum checks all pass; there is no session-level completeness
    check; the partial tree's mtimes go quiet and it satisfies the settle window
    on both upload paths. The result is a partial session in XNAT, silently.
    """
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()

    mock_session = _mock_session()
    # ids resolve cleanly, so this session is headed for the real output
    # directory and not the invalid one
    mock_session.assign.return_value = {}

    def dying_save(dest_dir: Path, **kwargs):
        # a prefix of the session, exactly as an interrupted save would leave it
        partial = Path(dest_dir) / "PROJ.SUBJ.SESS" / "1.scan" / "DICOM"
        partial.mkdir(parents=True)
        (partial / "inst.dcm").write_text("half a session")
        raise RuntimeError("died part-way through save")

    mock_session.save.side_effect = dying_save
    with patch.object(ImagingSession, "load", return_value=mock_session):
        errors = assign(
            input_dir=grouped_dir,
            output_dir=output_dir,
            project_field=PROJECT_FIELD,
            subject_field=SUBJECT_FIELD,
            session_field=SESSION_FIELD,
        )

    assert len(errors) == 1
    assert "died part-way through save" in errors[0]
    collectable = [p for p in output_dir.iterdir() if not p.name.startswith("__")]
    assert collectable == [], (
        f"a partial session was left where upload would collect it: {collectable}"
    )
    assert not list(output_dir.rglob("inst.dcm")), "the partial data survived"


def test_assign_routes_invalid_ids_to_invalid_subdir(
    grouped_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()

    mock_session = _mock_session()
    mock_session.assign.return_value = {"PatientID": "INVALID_MISSING_PATIENTID_abc123"}
    with patch.object(ImagingSession, "load", return_value=mock_session):
        errors = assign(
            input_dir=grouped_dir,
            output_dir=output_dir,
            project_field=PROJECT_FIELD,
            subject_field=SUBJECT_FIELD,
            session_field=SESSION_FIELD,
        )

    assert len(errors) == 1
    assert "PatientID" in errors[0]
    saved_kwargs = mock_session.save.call_args.kwargs
    assert saved_kwargs["copy_mode"] == FileSet.CopyMode.hardlink_or_copy
    assert saved_kwargs["include"] == ()
    # built under the INVALID directory, and promoted into it
    assert saved_kwargs["dest_dir"].parent.parent == output_dir / INVALID_DIRNAME
    assert (output_dir / INVALID_DIRNAME / "PROJ.SUBJ.SESS").is_dir()


def test_assign_passes_scan_field(grouped_dir: Path, tmp_path: Path) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()

    mock_session = _mock_session()
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

    mock_session = _mock_session()
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

    mock_session = _mock_session()
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

    mock_session = _mock_session()
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


def test_assign_deletes_source_dir_on_success_when_unlink_source_all(
    grouped_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()
    session_dir = next(grouped_dir.iterdir())
    (session_dir / "some_file.txt").write_text("data")

    mock_session = _mock_session()
    mock_session.assign.return_value = {}
    with patch.object(ImagingSession, "load", return_value=mock_session):
        errors = assign(
            input_dir=grouped_dir,
            output_dir=output_dir,
            project_field=PROJECT_FIELD,
            subject_field=SUBJECT_FIELD,
            session_field=SESSION_FIELD,
            unlink_source="all",
        )

    assert errors == []
    assert not session_dir.exists()


def test_assign_leaves_source_dir_when_unlink_source_none(
    grouped_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()
    session_dir = next(grouped_dir.iterdir())

    mock_session = _mock_session()
    with patch.object(ImagingSession, "load", return_value=mock_session):
        assign(
            input_dir=grouped_dir,
            output_dir=output_dir,
            project_field=PROJECT_FIELD,
            subject_field=SUBJECT_FIELD,
            session_field=SESSION_FIELD,
            unlink_source=None,
        )

    assert session_dir.exists()


def test_assign_unlink_source_keep_metadata_keeps_metadata_skeleton(
    grouped_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()
    session_dir = next(grouped_dir.iterdir())

    mock_session = _mock_session()
    mock_session.assign.return_value = {}
    with patch.object(ImagingSession, "load", return_value=mock_session):
        errors = assign(
            input_dir=grouped_dir,
            output_dir=output_dir,
            project_field=PROJECT_FIELD,
            subject_field=SUBJECT_FIELD,
            session_field=SESSION_FIELD,
            unlink_source="keep-metadata",
        )

    assert errors == []
    # the grouped session directory itself is untouched by 'keep-metadata' mode
    # (only the loaded session's own resource data would be removed, leaving its
    # session/scan-level metadata behind)
    assert session_dir.exists()
    mock_session.unlink.assert_called_once_with(keep_metadata=True)


def test_assign_does_not_delete_on_failure_even_if_unlink_source_all(
    grouped_dir: Path, tmp_path: Path
) -> None:
    output_dir = tmp_path / "assigned"
    output_dir.mkdir()
    session_dir = next(grouped_dir.iterdir())

    mock_session = _mock_session()
    mock_session.assign.side_effect = RuntimeError("simulated assign failure")
    with patch.object(ImagingSession, "load", return_value=mock_session):
        assign(
            input_dir=grouped_dir,
            output_dir=output_dir,
            project_field=PROJECT_FIELD,
            subject_field=SUBJECT_FIELD,
            session_field=SESSION_FIELD,
            unlink_source="all",
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
    # directory should no longer have a trailing dot. Unlike project/subject/session
    # IDs, scan descriptions are not escaped, so spaces are preserved
    scan_dir = next(d for d in session_dirs[0].iterdir() if d.is_dir())
    assert not scan_dir.name.endswith(".")
    assert scan_dir.name == "4.PET SWB 8MIN"


def test_assign_end_to_end_routes_datatypes_to_separate_projects(
    dicom_dir: Path, tmp_path: Path
) -> None:
    grouped_dir = tmp_path / "grouped"
    grouped_dir.mkdir()
    assert (
        group(
            input_paths=[str(dicom_dir)],
            output_dir=grouped_dir,
            datatypes=[DicomSeries],
            session=[IDSpec("StudyInstanceUID", "medimage/dicom-collection")],
            scan=[IDSpec("SeriesNumber", "medimage/dicom-collection")],
            resource=[IDSpec("ImageType[2:]", "medimage/dicom-collection")],
        )
        == []
    )

    grouped_session = ImagingSession.load(next(iter(list_session_dirs(grouped_dir))))
    grouped_session.add_session_resource("report", File.sample(seed=42))
    grouped_session.save(grouped_dir)

    dicom_output = tmp_path / "dicom-assigned"
    report_output = tmp_path / "report-assigned"
    common_args = {
        "input_dir": grouped_dir,
        "project_field": PROJECT_FIELD,
        "subject_field": SUBJECT_FIELD,
        "session_field": SESSION_FIELD,
    }
    assert (
        assign(
            output_dir=dicom_output,
            project_id="DICOM_PROJECT",
            include=[DicomSeries],
            **common_args,
        )
        == []
    )
    assert (
        assign(
            output_dir=report_output,
            project_id="REPORT_PROJECT",
            include=[File],
            **common_args,
        )
        == []
    )

    dicom_session = ImagingSession.load(next(iter(list_session_dirs(dicom_output))))
    report_session = ImagingSession.load(next(iter(list_session_dirs(report_output))))
    assert dicom_session.project_id == "DICOM_PROJECT"
    assert report_session.project_id == "REPORT_PROJECT"
    assert dicom_session.resources
    assert all(
        isinstance(resource.fileset, DicomSeries)
        for resource in dicom_session.resources
    )
    assert report_session.scans == {}
    assert len(report_session.session_resources) == 1
    assert isinstance(report_session.resources[0].fileset, File)
    assert (grouped_dir / grouped_session.staging_relpath[0]).exists()


def test_assign_end_to_end_unresolvable_project_field_goes_to_invalid_dir(
    dicom_dir: Path, tmp_path: Path
) -> None:
    """A session whose project field can't be resolved from its metadata should be
    saved with a placeholder ID under __invalid__, not dropped"""
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
        project_field="ThisFieldDoesNotExistInTheMetadata",
        subject_field=SUBJECT_FIELD,
        session_field=SESSION_FIELD,
    )

    assert len(errors) == 1
    assert "ThisFieldDoesNotExistInTheMetadata" in errors[0]
    assert list(output_dir.iterdir()) == [output_dir / INVALID_DIRNAME]
    session_dirs = list((output_dir / INVALID_DIRNAME).iterdir())
    assert len(session_dirs) == 1
    project_id = session_dirs[0].name.split(".")[0]
    assert project_id.startswith("INVALID_MISSING_")

    reloaded = ImagingSession.load(session_dirs[0])
    assert reloaded.invalid_ids


def test_assign_skips_duplicate_invalid_sessions(
    dicom_dir: Path, tmp_path: Path
) -> None:
    """Running assign twice on the same unresolvable session should not create
    a second directory in __invalid__."""
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

    errors1 = assign(
        input_dir=grouped_dir,
        output_dir=output_dir,
        project_field="NonExistentField",
        subject_field=SUBJECT_FIELD,
        session_field=SESSION_FIELD,
    )
    assert len(errors1) == 1
    invalid_dir = output_dir / INVALID_DIRNAME
    entries_after_first = list(invalid_dir.iterdir())
    assert len(entries_after_first) == 1

    # Second run should skip
    errors2 = assign(
        input_dir=grouped_dir,
        output_dir=output_dir,
        project_field="NonExistentField",
        subject_field=SUBJECT_FIELD,
        session_field=SESSION_FIELD,
    )
    assert errors2 == []
    entries_after_second = list(invalid_dir.iterdir())
    assert len(entries_after_second) == 1
    assert entries_after_second[0] == entries_after_first[0]
