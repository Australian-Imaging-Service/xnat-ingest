import json
from pathlib import Path
from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet
from fileformats.medimage import DicomSeries
from fileformats.testing import MyFormat, MyFormatGz

from xnat_ingest.api.deidentify_api import DEFAULT_SPEC_DIR, deidentify, load_specs
from xnat_ingest.model.session import ImagingSession

PROJECT_ID = "PROJ"
SUBJECT_ID = "SUBJ"
VISIT_ID = "SESS"
SESSION_NAME = f"{PROJECT_ID}.{SUBJECT_ID}.{VISIT_ID}"

REID_MDATA = {"PatientName": "John Doe", "DOB": "19800101", "PatientID": "PID001"}


@pytest.fixture
def dirs(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    spec_dir = tmp_path / "spec"
    reid_dir = tmp_path / "reid"
    for d in [input_dir, output_dir, spec_dir, reid_dir]:
        d.mkdir(parents=True)
    (input_dir / SESSION_NAME).mkdir()
    (spec_dir / PROJECT_ID).mkdir()
    return input_dir, output_dir, spec_dir, reid_dir


def _mock_deidentify(self, dest_dir, **kwargs) -> tuple[ImagingSession, dict]:
    return self.new_empty(), dict(REID_MDATA)


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
    assert json.loads(reid_file.read_bytes()) == REID_MDATA


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
    assert decrypted == REID_MDATA


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
    spec_dir = tmp_path / "spec"
    reid_dir = tmp_path / "reid"
    for d in [input_dir, output_dir, spec_dir, reid_dir]:
        d.mkdir()

    session_names = [f"PROJ.SUBJ{i}.SESS{i}" for i in range(3)]
    for name in session_names:
        (input_dir / name).mkdir()
    (spec_dir / "PROJ").mkdir()

    with patch.object(ImagingSession, "deidentify", _mock_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
        )

    assert errors == []
    for name in session_names:
        assert (reid_dir / f"{name}.json").exists()


def test_deidentify_missing_spec_collected(dirs):
    input_dir, output_dir, spec_dir, reid_dir = dirs
    (spec_dir / PROJECT_ID).rmdir()

    with patch.object(ImagingSession, "deidentify", _mock_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
        )

    assert len(errors) == 1


# ── load_specs unit tests ────────────────────────────────────────────────────


def test_load_specs_nonexistent_dir_returns_none(tmp_path: Path) -> None:
    assert load_specs(tmp_path / "nonexistent") is None


def test_load_specs_empty_dir_returns_empty_dict(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    assert load_specs(spec_dir) == {}


def test_load_specs_ignores_files_without_at(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    (spec_dir / "README").write_text("docs")
    (spec_dir / "config.json").write_text("{}")
    assert load_specs(spec_dir) == {}


def test_load_specs_single_mime_like(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    # Spec files are named as mime-like with '/' replaced by '@', no extension
    spec_file = spec_dir / "medimage@dicom-series"
    spec_file.write_text("{}")
    result = load_specs(spec_dir)
    assert result == {DicomSeries: spec_file}


def test_load_specs_multiple_mime_likes(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    dcm_file = spec_dir / "medimage@dicom-series"
    dcm_file.write_text("{}")
    mf_file = spec_dir / "testing@my-format"
    mf_file.write_text("{}")
    mfgz_file = spec_dir / "testing@my-format-gz"
    mfgz_file.write_text("{}")
    result = load_specs(spec_dir)
    assert result == {DicomSeries: dcm_file, MyFormat: mf_file, MyFormatGz: mfgz_file}


def test_load_specs_mixed_files_only_picks_at_names(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    spec_file = spec_dir / "medimage@dicom-series"
    spec_file.write_text("{}")
    (spec_dir / "README").write_text("docs")
    result = load_specs(spec_dir)
    assert result == {DicomSeries: spec_file}


# ── deidentify fallback / error tests ────────────────────────────────────────


def test_deidentify_falls_back_to_default_when_no_project_spec(
    dirs: tuple[Path, Path, Path, Path],
) -> None:
    input_dir, output_dir, spec_dir, reid_dir = dirs
    # Remove the project-specific spec dir but provide a default
    (spec_dir / PROJECT_ID).rmdir()
    default_dir = spec_dir / DEFAULT_SPEC_DIR
    default_dir.mkdir()
    (default_dir / "medimage@dicom-series").write_text("{}")

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
) -> None:
    input_dir, output_dir, spec_dir, reid_dir = dirs
    # Both project and default specs exist; project spec should be used
    default_dir = spec_dir / DEFAULT_SPEC_DIR
    default_dir.mkdir()
    (default_dir / "medimage@dicom-series").write_text('{"default": true}')
    project_spec_file = spec_dir / PROJECT_ID / "medimage@dicom-series"
    project_spec_file.write_text('{"project": true}')

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
