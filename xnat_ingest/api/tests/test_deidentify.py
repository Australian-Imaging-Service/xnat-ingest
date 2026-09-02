import json
from pathlib import Path
from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet
from fileformats.generic import File
from fileformats.medimage import DicomSeries
from fileformats.testing import MyFormat, MyFormatGz

import xnat_ingest.specs as _specs_pkg
from xnat_ingest.api.deidentify_api import DEFAULT_SPEC_DIR, deidentify, load_specs
from xnat_ingest.model.scan import ImagingScan
from xnat_ingest.model.session import ImagingSession

SHIPPED_SPECS_DIR = Path(_specs_pkg.__path__[0])

PROJECT_ID = "PROJ"
SUBJECT_ID = "SUBJ"
VISIT_ID = "SESS"
SESSION_NAME = f"{PROJECT_ID}.{SUBJECT_ID}.{VISIT_ID}"

REID_MDATA = {"PatientName": "John Doe", "DOB": "19800101", "PatientID": "PID001"}


@pytest.fixture
def dirs(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    reid_dir = tmp_path / "reid"
    for d in [input_dir, output_dir, reid_dir]:
        d.mkdir(parents=True)
    (input_dir / SESSION_NAME).mkdir()
    return input_dir, output_dir, SHIPPED_SPECS_DIR, reid_dir


def _mock_deidentify(self, dest_dir, **kwargs) -> tuple[ImagingSession, dict]:
    return self.new_empty(), dict(REID_MDATA)


def _mock_deidentify_passthrough(self, dest_dir, **kwargs) -> tuple[ImagingSession, dict]:
    """Produces a COMPLETE output: the same session, unmodified.

    _mock_deidentify returns an EMPTY session, which is an incomplete output by
    definition, so it cannot be used to test the unlink: the completeness gate
    correctly refuses to delete the input in that case.
    """
    return self, dict(REID_MDATA)


def test_deidentify_deletes_source_dir_on_success_when_unlink_source_all(
    dirs: tuple[Path, Path, Path, Path],
):
    """A session directory is full of scan directories, so the unlink has to be
    recursive. assign does the same job with shutil.rmtree; this mirrors its test
    (test_assign_deletes_source_dir_on_success_when_unlink_source_all)."""
    input_dir, output_dir, spec_dir, reid_dir = dirs
    session_dir = input_dir / SESSION_NAME
    (session_dir / "1.scan" / "DICOM").mkdir(parents=True)
    (session_dir / "1.scan" / "DICOM" / "inst.dcm").write_text("data")

    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
            require_manifest=False,
            unlink_source="all",
        )

    assert errors == []
    assert not session_dir.exists()


def test_deidentify_discards_a_stale_build_from_a_crashed_run(
    dirs: tuple[Path, Path, Path, Path],
):
    """A build tree left by a run that died before promotion must not be adopted.

    save() does mkdir(exist_ok=True), so without wiping the promote directory first
    the survivor would be merged into and then renamed in as though this run had
    produced it. Stale files would reach XNAT, and could pad an incomplete run up to
    n_out == n_in and unlock the unlink.

    This is the first-materialisation path, which is the one that promotes: the
    output does not exist yet, so the session is built under __build__ and renamed
    into place.
    """
    input_dir, output_dir, spec_dir, reid_dir = dirs
    session_dir = input_dir / SESSION_NAME
    (session_dir / "1.scan" / "DICOM").mkdir(parents=True)
    (session_dir / "1.scan" / "DICOM" / "inst.dcm").write_text("data")

    stale = (
        output_dir / "__build__" / f"promote_{SESSION_NAME}" / SESSION_NAME / "9.ghost"
    )
    stale.mkdir(parents=True)
    (stale / "ghost.dcm").write_text("from a crashed run")

    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
            require_manifest=False,
        )

    promoted = list(output_dir.rglob("ghost.dcm"))
    assert not promoted, f"a stale build was adopted into the output: {promoted}"


def test_deidentify_keeps_source_when_output_is_incomplete(
    dirs: tuple[Path, Path, Path, Path],
):
    """The gate that stops a short run deleting the only copy that can repair it.

    _mock_deidentify returns an empty session, so the output is 0 files against 1
    input file. Even with unlink_source=all the input must survive.
    """
    input_dir, output_dir, spec_dir, reid_dir = dirs
    session_dir = input_dir / SESSION_NAME
    (session_dir / "1.scan" / "DICOM").mkdir(parents=True)
    (session_dir / "1.scan" / "DICOM" / "inst.dcm").write_text("data")

    with patch.object(ImagingSession, "deidentify", _mock_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
            require_manifest=False,
            unlink_source="all",
        )

    assert session_dir.exists(), "an incomplete run must not delete its input"
    # and it must SAY so. Logging alone would leave the caller with an empty error
    # list, a "completed successfully" line and exit 0, so a permanently stuck
    # session would be invisible to everything above this function.
    assert errors, "an incomplete run must report an error, not just log one"
    assert "incomplete" in errors[0]


def test_deidentify_leaves_source_dir_when_unlink_source_none(
    dirs: tuple[Path, Path, Path, Path],
):
    input_dir, output_dir, spec_dir, reid_dir = dirs
    session_dir = input_dir / SESSION_NAME
    (session_dir / "1.scan" / "DICOM").mkdir(parents=True)
    (session_dir / "1.scan" / "DICOM" / "inst.dcm").write_text("data")

    with patch.object(ImagingSession, "deidentify", _mock_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
            require_manifest=False,
        )

    assert session_dir.exists()
    # The mock produces an empty output, so this run IS incomplete. That must be
    # reported whether or not --unlink-source was passed: incompleteness is a
    # property of the output, not of the deletion policy. No site passes the flag
    # today, so tying the report to it would silence every real occurrence.
    assert errors, "an incomplete run must report an error even without unlink_source"
    assert "not unlinked" not in errors[0], "nothing was going to be unlinked here"


def test_deidentify_plain_json(dirs: tuple[Path, Path, Path, Path]):
    input_dir, output_dir, spec_dir, reid_dir = dirs

    with patch.object(ImagingSession, "deidentify", _mock_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
        )

    assert errors == []
    reid_file = reid_dir / f"{SESSION_NAME}.json"
    assert reid_file.exists()
    assert json.loads(reid_file.read_bytes()) == {
        "session_uid": SESSION_NAME,
        "changed_fields": REID_MDATA,
    }


def test_deidentify_encrypted(dirs: tuple[Path, Path, Path, Path]) -> None:
    input_dir, output_dir, spec_dir, reid_dir = dirs
    key = Fernet.generate_key()

    with patch.object(ImagingSession, "deidentify", _mock_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
            reid_encrypt_key=key,
        )

    assert errors == []
    enc_file = reid_dir / f"{SESSION_NAME}.json.enc"
    assert enc_file.exists()
    assert not (reid_dir / f"{SESSION_NAME}.json").exists()
    decrypted = json.loads(Fernet(key).decrypt(enc_file.read_bytes()))
    assert decrypted == {
        "session_uid": SESSION_NAME,
        "changed_fields": REID_MDATA,
    }


def test_deidentify_wrong_key_fails(dirs: tuple[Path, Path, Path, Path]):
    input_dir, output_dir, spec_dir, reid_dir = dirs
    encrypt_key = Fernet.generate_key()
    wrong_key = Fernet.generate_key()

    with patch.object(ImagingSession, "deidentify", _mock_deidentify):
        deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
            reid_encrypt_key=encrypt_key,
        )

    enc_file = reid_dir / f"{SESSION_NAME}.json.enc"
    with pytest.raises(Exception):
        Fernet(wrong_key).decrypt(enc_file.read_bytes())


def test_deidentify_error_collected(dirs: tuple[Path, Path, Path, Path]):
    input_dir, output_dir, spec_dir, reid_dir = dirs

    def failing_deidentify(self, dest_dir, **kwargs):
        raise RuntimeError("simulated deidentification failure")

    with patch.object(ImagingSession, "deidentify", failing_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
        )

    assert len(errors) == 1
    assert "simulated deidentification failure" in errors[0]
    assert not list(reid_dir.iterdir())


def test_deidentify_raise_errors(dirs: tuple[Path, Path, Path, Path]):
    input_dir, output_dir, spec_dir, reid_dir = dirs

    def failing_deidentify(self, dest_dir, **kwargs):
        raise RuntimeError("simulated deidentification failure")

    with patch.object(ImagingSession, "deidentify", failing_deidentify):
        with pytest.raises(RuntimeError, match="simulated deidentification failure"):
            deidentify(
                input_dir=input_dir,
                output_dir=output_dir,
                spec_dir=spec_dir,
                reid_dir=reid_dir,
                raise_errors=True,
            )


def test_deidentify_multiple_sessions(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    reid_dir = tmp_path / "reid"
    for d in [input_dir, output_dir, reid_dir]:
        d.mkdir()

    session_names = [f"PROJ.SUBJ{i}.SESS{i}" for i in range(3)]
    for name in session_names:
        (input_dir / name).mkdir()

    with patch.object(ImagingSession, "deidentify", _mock_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=SHIPPED_SPECS_DIR,
            reid_dir=reid_dir,
        )

    assert errors == []
    for name in session_names:
        reid_file = reid_dir / f"{name}.json"
        assert reid_file.exists()
        assert json.loads(reid_file.read_bytes())["session_uid"] == name


def test_deidentify_missing_spec_collected(dirs, tmp_path):
    input_dir, output_dir, _, reid_dir = dirs
    empty_spec_dir = tmp_path / "empty_specs"
    empty_spec_dir.mkdir()

    with patch.object(ImagingSession, "deidentify", _mock_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=empty_spec_dir,
            reid_dir=reid_dir,
        )

    assert len(errors) == 1


# ── load_specs unit tests ────────────────────────────────────────────────────


def test_load_specs_nonexistent_dir_returns_none(tmp_path: Path) -> None:
    assert load_specs(tmp_path / "nonexistent") is None


def _write_spec(spec_dir: Path, mime_like: str, content: str = "{}") -> Path:
    """Helper to create a spec file at spec_dir/<category>/<format>."""
    category, format_name = mime_like.split("/", 1)
    (spec_dir / category).mkdir(parents=True, exist_ok=True)
    spec_file = spec_dir / category / format_name
    spec_file.write_text(content)
    return spec_file


def test_load_specs_empty_dir_returns_empty_dict(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    assert load_specs(spec_dir) == ({}, {})


def test_load_specs_ignores_non_category_files(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    (spec_dir / "README").write_text("docs")
    (spec_dir / "config.json").write_text("{}")
    assert load_specs(spec_dir) == ({}, {})


def test_load_specs_single_mime_like(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    spec_file = _write_spec(spec_dir, "medimage/dicom-series")
    specs, transforms = load_specs(spec_dir)
    assert specs == {DicomSeries: spec_file}
    assert transforms == {}


def test_load_specs_multiple_mime_likes(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    dcm_file = _write_spec(spec_dir, "medimage/dicom-series")
    mf_file = _write_spec(spec_dir, "testing/my-format")
    mfgz_file = _write_spec(spec_dir, "testing/my-format-gz")
    specs, transforms = load_specs(spec_dir)
    assert specs == {DicomSeries: dcm_file, MyFormat: mf_file, MyFormatGz: mfgz_file}
    assert transforms == {}


def test_load_specs_mixed_files_only_picks_mime_names(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    spec_file = _write_spec(spec_dir, "medimage/dicom-series")
    (spec_dir / "README").write_text("docs")
    specs, transforms = load_specs(spec_dir)
    assert specs == {DicomSeries: spec_file}
    assert transforms == {}


# ── deidentify fallback / error tests ────────────────────────────────────────


def test_deidentify_falls_back_to_default_when_no_project_spec(
    dirs: tuple[Path, Path, Path, Path],
    tmp_path: Path,
) -> None:
    input_dir, output_dir, _, reid_dir = dirs
    # Provide only a default spec, no project-specific one
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    default_dir = spec_dir / DEFAULT_SPEC_DIR
    default_dir.mkdir()
    _write_spec(default_dir, "medimage/dicom-series")

    with patch.object(ImagingSession, "deidentify", _mock_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
        )

    assert errors == []
    assert (reid_dir / f"{SESSION_NAME}.json").exists()


def test_deidentify_uses_project_spec_over_default(
    dirs: tuple[Path, Path, Path, Path],
    tmp_path: Path,
) -> None:
    input_dir, output_dir, _, reid_dir = dirs
    # Both project and default specs exist; project spec should be used
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    default_dir = spec_dir / DEFAULT_SPEC_DIR
    default_dir.mkdir()
    _write_spec(default_dir, "medimage/dicom-series", '{"default": true}')
    project_dir = spec_dir / PROJECT_ID
    project_dir.mkdir()
    project_spec_file = _write_spec(
        project_dir, "medimage/dicom-series", '{"project": true}'
    )

    received_specs: list = []

    def capturing_deidentify(self, *_, **kwargs):
        received_specs.append(kwargs.get("specs"))
        return self.new_empty(), dict(REID_MDATA)

    with patch.object(ImagingSession, "deidentify", capturing_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
        )

    assert errors == []
    assert len(received_specs) == 1
    # The spec passed in should map DicomSeries to the project file, not the default
    assert received_specs[0].get(DicomSeries) == project_spec_file


def test_deidentify_passes_max_workers_through(
    dirs: tuple[Path, Path, Path, Path],
) -> None:
    """max_workers should be forwarded from deidentify_api.deidentify() to
    ImagingSession.deidentify() unchanged.
    """
    input_dir, output_dir, spec_dir, reid_dir = dirs

    received_max_workers: list = []

    def capturing_deidentify(self, *_, **kwargs):
        received_max_workers.append(kwargs.get("max_workers"))
        return self.new_empty(), dict(REID_MDATA)

    with patch.object(ImagingSession, "deidentify", capturing_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
            max_workers=4,
        )

    assert errors == []
    assert received_max_workers == [4]


def test_deidentify_max_workers_defaults_to_none(
    dirs: tuple[Path, Path, Path, Path],
) -> None:
    """When not given explicitly, max_workers should be forwarded as None."""
    input_dir, output_dir, spec_dir, reid_dir = dirs

    received_max_workers: list = []

    def capturing_deidentify(self, *_, **kwargs):
        received_max_workers.append(kwargs.get("max_workers"))
        return self.new_empty(), dict(REID_MDATA)

    with patch.object(ImagingSession, "deidentify", capturing_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
        )

    assert errors == []
    assert received_max_workers == [None]


# ── end-to-end test (no mocking of ImagingSession.deidentify) ───────────────


def test_deidentify_end_to_end_directory_structure(tmp_path: Path) -> None:
    """Runs deidentify() against a real session directory (written by
    ImagingSession.save()) without mocking ImagingSession.deidentify, and checks
    that the output directory structure is a single, correctly-named session
    directory that can be reloaded with ImagingSession.load().

    Regression test for a bug where deidentify_api.deidentify() passed
    `output_dir` directly (rather than a scratch directory) as the `dest_dir`
    for `session.deidentify()`, and then called
    `deidentified_session.save(output_dir / session_listing.name)` on top of
    that, resulting in files written twice: once as stray `<scan_id>/<resource>`
    directories directly under `output_dir`, and again correctly nested under
    `output_dir/<session_name>`.
    """
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    reid_dir = tmp_path / "reid"
    for d in (input_dir, output_dir, reid_dir):
        d.mkdir()

    scan_id = "1"
    resource_name = "RESOURCE"
    # A generic File has no `contains_phi` attribute, so it takes the plain-copy
    # path in ImagingSession.deidentify() and doesn't require a real deid spec.
    session = ImagingSession(
        uid=SESSION_NAME,
        project_id=PROJECT_ID,
        subject_id=SUBJECT_ID,
        session_id=VISIT_ID,
        scans=[
            ImagingScan(
                id=scan_id,
                type="SomeScanType",
                resources={resource_name: File.sample(seed=1)},
            )
        ],
    )
    session.save(input_dir)

    errors = deidentify(
        input_dir=input_dir,
        output_dir=output_dir,
        spec_dir=SHIPPED_SPECS_DIR,
        reid_dir=reid_dir,
    )

    assert errors == []

    session_out_dir = output_dir / SESSION_NAME
    assert session_out_dir.is_dir()
    # The deidentified session must not be nested inside another directory of
    # the same name
    assert not (session_out_dir / SESSION_NAME).exists()
    # No stray scan directories should be written directly under output_dir
    assert not (output_dir / scan_id).exists()

    reloaded = ImagingSession.load(session_out_dir)
    assert reloaded.name == SESSION_NAME
    assert reloaded.scans[scan_id].resources[resource_name].fileset.fspath.exists()
