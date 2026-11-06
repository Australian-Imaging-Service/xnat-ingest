import hashlib
import hmac
import json
from importlib.resources import files
from pathlib import Path
from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet
from fileformats.generic import Directory, File
from fileformats.medimage import DicomSeries
from fileformats.testing import MyFormat, MyFormatGz

from accessionflow.api.deidentify_api import (
    DEFAULT_RECIPE_DIR,
    deidentify,
    load_recipe_files,
)
from accessionflow.helpers.metadata import Metadata
from accessionflow.model.accession import Accession
from accessionflow.model.acquisition import Acquisition

SHIPPED_RECIPES_DIR = Path(str(files("accessionflow.assets") / "deid_recipes"))

PROJECT_ID = "PROJ"
SUBJECT_ID = "SUBJ"
VISIT_ID = "SESS"
SESSION_NAME = f"{PROJECT_ID}.{SUBJECT_ID}.{VISIT_ID}"
SESSION_LABELS = dict(zip(("project", "subject", "session"), SESSION_NAME.split(".")))

REID_MDATA = {"PatientName": "John Doe", "DOB": "19800101", "PatientID": "PID001"}


@pytest.fixture
def dirs(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    reid_dir = tmp_path / "reid"
    for d in [input_dir, output_dir, reid_dir]:
        d.mkdir(parents=True)
    _stage(input_dir, SESSION_NAME)
    return input_dir, output_dir, SHIPPED_RECIPES_DIR, reid_dir


def _stage(input_dir: Path, name: str) -> Path:
    """Stage one resource with its manifest and metadata."""
    project_id, subject_id, session_id = name.split(".")
    Accession(
        uid=name,
        labels={"project": project_id, "subject": subject_id, "session": session_id},
        scans=[Acquisition(id="1", type="T", resources={"RES": File.sample(seed=1)})],
    ).save(input_dir)
    return input_dir / name


def _mock_deidentify(self, dest_dir, **kwargs) -> tuple[Accession, dict]:
    new_sess = self.new_empty()
    # Return a changed non-DICOM file in the input scan.
    new_sess.add_resource("1", "T", "RES", File.sample(seed=42))
    return new_sess, dict(REID_MDATA)


def _mock_deidentify_passthrough(self, dest_dir, **kwargs) -> tuple[Accession, dict]:
    """Copy the loaded input without changing its resources."""
    return self, dict(REID_MDATA)


def test_deidentify_deletes_source_dir_on_success_when_unlink_source_all(
    dirs: tuple[Path, Path, Path, Path],
):
    """Remove recorded files, then their empty directories, after success."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    session_dir = input_dir / SESSION_NAME

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
            require_manifest=False,
            unlink_source="all",
        )

    assert errors == []
    assert not session_dir.exists()


def test_deidentify_discards_a_stale_build_from_a_crashed_run(
    dirs: tuple[Path, Path, Path, Path],
):
    """A new output must not include files from a failed earlier build."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    session_dir = input_dir / SESSION_NAME
    (session_dir / "2.scan" / "DICOM").mkdir(parents=True)
    (session_dir / "2.scan" / "DICOM" / "inst.dcm").write_text("data")

    stale = (
        output_dir / "__build__" / f"promote_{SESSION_NAME}" / SESSION_NAME / "9.ghost"
    )
    stale.mkdir(parents=True)
    (stale / "ghost.dcm").write_text("from a crashed run")

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
            require_manifest=False,
        )

    promoted = list(output_dir.rglob("ghost.dcm"))
    assert not promoted, f"a stale build was adopted into the output: {promoted}"


def test_deidentify_does_not_skip_an_output_that_is_newer_but_short(
    dirs: tuple[Path, Path, Path, Path],
):
    """A partial output must be reprocessed, however new it is.

    Skipping on mtime alone treats "newer than its input" as "finished". A
    session left half-written by a run that died is also newer than its input,
    so it would be skipped on this cycle and every cycle after it, permanently,
    while sitting in the directory upload reads.
    """
    input_dir, output_dir, recipe_dir, reid_dir = dirs

    # an output that is NEWER than the input but missing its data
    stale = output_dir / SESSION_NAME
    (stale / "2.scan" / "RES").mkdir(parents=True)
    (stale / "__METADATA__.json").write_text("{}")

    calls: list = []

    def counting_deidentify(self, dest_dir, **kwargs):
        calls.append(self.name)
        return self, dict(REID_MDATA)

    with patch.object(Accession, "deidentify", counting_deidentify):
        deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
        )

    assert calls, "a short output was skipped, so the partial session is permanent"


def test_deidentify_does_not_skip_a_corrupt_output_with_the_right_file_count(
    dirs: tuple[Path, Path, Path, Path],
):
    """A file that is present but wrong must still be reprocessed.

    A re-save writes in place, so a crash can leave a file truncated under its
    real name. The count is still right and the mtime is still newer than the
    input, so a count-only currency check would skip it for ever. Skipping also
    bypasses the repair: Resource.save is what overwrites a bad output,
    and it only runs if the session is not skipped.
    """
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    calls: list = []

    def counting_deidentify(self, dest_dir, **kwargs):
        calls.append(self.name)
        return self, dict(REID_MDATA)

    with patch.object(Accession, "deidentify", counting_deidentify):
        deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
        )
    assert len(calls) == 1
    produced = output_dir / SESSION_NAME
    assert produced.is_dir()

    # corrupt one output file in place, keeping the file count identical
    data_files = [
        f for f in produced.rglob("*") if f.is_file() and not f.name.startswith("__")
    ]
    assert data_files, "no data file in the output to corrupt"
    data_files[0].write_bytes(b"truncated")

    with patch.object(Accession, "deidentify", counting_deidentify):
        deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
        )

    assert len(calls) == 2, "a corrupt output was skipped, so it can never be repaired"


def test_deidentify_processes_and_retires_a_backlog_session(
    dirs: tuple[Path, Path, Path, Path],
):
    """Turning unlink-source on processes and retires the remaining input."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    session_dir = input_dir / SESSION_NAME

    # first pass with no unlink: produces the output, leaves the input
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
        )
    assert session_dir.exists(), "nothing should have been unlinked yet"
    assert (output_dir / SESSION_NAME).is_dir()

    # Second pass with unlink: process the input again, then retire it.
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
            unlink_source="all",
        )

    assert errors == []
    assert not session_dir.exists(), (
        "a skipped session was never unlinked, so enabling the flag would "
        "leave the existing backlog in place for ever"
    )


def test_deidentify_processes_input_even_when_output_is_current(
    dirs: tuple[Path, Path, Path, Path],
):
    """An existing output does not suppress processing of present input."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    calls: list = []

    def counting_deidentify(self, dest_dir, **kwargs):
        calls.append(self.name)
        return self, dict(REID_MDATA)

    with patch.object(Accession, "deidentify", counting_deidentify):
        for _ in range(2):
            deidentify(
                input_dir=input_dir,
                output_dir=output_dir,
                recipe_dir=recipe_dir,
                reid_dir=reid_dir,
            )

    assert len(calls) == 2


def test_deidentify_reports_a_session_with_no_data_files(
    dirs: tuple[Path, Path, Path, Path],
):
    """An empty input directory without metadata is not a staged session."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    empty = input_dir / "PROJ.SUBJ.EMPTY"
    empty.mkdir()

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
        )

    assert any("no data files" in e for e in errors), errors
    assert not (output_dir / "PROJ.SUBJ.EMPTY").exists(), (
        "an output was produced for a session that had no input"
    )


def test_deidentify_skips_a_metadata_only_skeleton_without_reporting_it(
    dirs: tuple[Path, Path, Path, Path],
):
    """`--unlink-source keep-metadata` leaves a session with no data files.

    That skeleton is the designed steady state of the mode, not a fault, and
    under --loop it is re-examined every cycle. Reporting it would emit an error
    every interval for every session ever processed.
    """
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    skeleton = input_dir / "PROJ.SUBJ.SKEL"
    (skeleton / "2.scan").mkdir(parents=True)
    # what survives a keep-metadata unlink: the metadata, and no resource dirs
    (skeleton / "__METADATA__.json").write_text("{}")
    (skeleton / "2.scan" / "__METADATA__.json").write_text("{}")

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
        )

    assert not any("SKEL" in e for e in errors), errors
    assert not (output_dir / "PROJ.SUBJ.SKEL").exists()


def test_deidentify_accepts_a_method_that_removes_files(
    dirs: tuple[Path, Path, Path, Path],
):
    """A method may deliberately omit files or resources from its output."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    session_dir = input_dir / SESSION_NAME
    (session_dir / "2.scan" / "DICOM").mkdir(parents=True)
    (session_dir / "2.scan" / "DICOM" / "inst.dcm").write_text("data")

    with patch.object(Accession, "deidentify", _mock_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
            require_manifest=False,
            unlink_source="all",
        )

    assert errors == []
    assert not session_dir.exists()


def test_deidentify_leaves_source_dir_when_unlink_source_none(
    dirs: tuple[Path, Path, Path, Path],
):
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    session_dir = input_dir / SESSION_NAME
    (session_dir / "2.scan" / "DICOM").mkdir(parents=True)
    (session_dir / "2.scan" / "DICOM" / "inst.dcm").write_text("data")

    with patch.object(Accession, "deidentify", _mock_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
            require_manifest=False,
        )

    assert session_dir.exists()
    assert errors == []


def test_deidentify_plain_json(dirs: tuple[Path, Path, Path, Path]):
    input_dir, output_dir, recipe_dir, reid_dir = dirs

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
        )

    assert errors == []
    reid_file = reid_dir / f"{SESSION_NAME}.json"
    assert reid_file.exists()
    assert json.loads(reid_file.read_bytes()) == {
        "session_uid": SESSION_NAME,
        "changed_fields": REID_MDATA,
    }


def test_deidentify_no_reid_dir_discards_metadata(
    dirs: tuple[Path, Path, Path, Path],
) -> None:
    input_dir, output_dir, recipe_dir, reid_dir = dirs

    with patch.object(Accession, "deidentify", _mock_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
        )

    assert errors == []
    # deidentified output still produced, but nothing written to reid_dir
    assert (output_dir / SESSION_NAME).exists()
    assert not list(reid_dir.iterdir())


def test_deidentify_encrypt_key_without_reid_dir_warns(
    dirs: tuple[Path, Path, Path, Path],
    caplog: pytest.LogCaptureFixture,
) -> None:
    input_dir, output_dir, recipe_dir, _ = dirs

    with (
        patch.object(Accession, "deidentify", _mock_deidentify),
        caplog.at_level("WARNING", logger="accessionflow"),
    ):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_encrypt_key=Fernet.generate_key(),
        )

    assert errors == []
    assert "no --reid-dir" in caplog.text


def test_deidentify_encrypted(dirs: tuple[Path, Path, Path, Path]) -> None:
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    key = Fernet.generate_key()

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
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
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    encrypt_key = Fernet.generate_key()
    wrong_key = Fernet.generate_key()

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
            reid_encrypt_key=encrypt_key,
        )

    enc_file = reid_dir / f"{SESSION_NAME}.json.enc"
    with pytest.raises(Exception):
        Fernet(wrong_key).decrypt(enc_file.read_bytes())


def test_deidentify_error_collected(dirs: tuple[Path, Path, Path, Path]):
    input_dir, output_dir, recipe_dir, reid_dir = dirs

    def failing_deidentify(self, dest_dir, **kwargs):
        raise RuntimeError("simulated deidentification failure")

    with patch.object(Accession, "deidentify", failing_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
        )

    assert len(errors) == 1
    assert "simulated deidentification failure" in errors[0]
    assert not list(reid_dir.iterdir())


def test_deidentify_raise_errors(dirs: tuple[Path, Path, Path, Path]):
    input_dir, output_dir, recipe_dir, reid_dir = dirs

    def failing_deidentify(self, dest_dir, **kwargs):
        raise RuntimeError("simulated deidentification failure")

    with patch.object(Accession, "deidentify", failing_deidentify):
        with pytest.raises(RuntimeError, match="simulated deidentification failure"):
            deidentify(
                input_dir=input_dir,
                output_dir=output_dir,
                recipe_dir=recipe_dir,
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
        _stage(input_dir, name)

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=SHIPPED_RECIPES_DIR,
            reid_dir=reid_dir,
        )

    assert errors == []
    for name in session_names:
        reid_file = reid_dir / f"{name}.json"
        assert reid_file.exists()
        assert json.loads(reid_file.read_bytes())["session_uid"] == name


def test_deidentify_missing_spec_collected(dirs, tmp_path):
    input_dir, output_dir, _, reid_dir = dirs
    empty_recipe_dir = tmp_path / "empty_recipes"
    empty_recipe_dir.mkdir()

    with patch.object(Accession, "deidentify", _mock_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=empty_recipe_dir,
            reid_dir=reid_dir,
        )

    assert len(errors) == 1


# ── load_recipe_files unit tests ────────────────────────────────────────────────────


def test_load_recipe_files_nonexistent_dir_returns_none(tmp_path: Path) -> None:
    assert load_recipe_files(tmp_path / "nonexistent") is None


def _write_recipe(recipe_dir: Path, mime_like: str, content: str = "{}") -> Path:
    """Helper to create a spec file at recipe_dir/<category>/<format>."""
    category, format_name = mime_like.split("/", 1)
    (recipe_dir / category).mkdir(parents=True, exist_ok=True)
    recipe_file = recipe_dir / category / format_name
    recipe_file.write_text(content)
    return recipe_file


def test_load_recipe_files_empty_dir_returns_empty_dict(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "recipes"
    recipe_dir.mkdir()
    assert load_recipe_files(recipe_dir) == {}


def test_load_recipe_files_ignores_non_category_files(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "recipes"
    recipe_dir.mkdir()
    (recipe_dir / "README").write_text("docs")
    (recipe_dir / "config.json").write_text("{}")
    assert load_recipe_files(recipe_dir) == {}


def test_load_recipe_files_single_mime_like(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "recipes"
    recipe_dir.mkdir()
    recipe_file = _write_recipe(recipe_dir, "medimage/dicom-series")
    recipe_files = load_recipe_files(recipe_dir)
    assert recipe_files == {DicomSeries: recipe_file}


def test_load_recipe_files_multiple_mime_likes(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "recipes"
    recipe_dir.mkdir()
    dcm_file = _write_recipe(recipe_dir, "medimage/dicom-series")
    mf_file = _write_recipe(recipe_dir, "testing/my-format")
    mfgz_file = _write_recipe(recipe_dir, "testing/my-format-gz")
    recipe_files = load_recipe_files(recipe_dir)
    assert recipe_files == {
        DicomSeries: dcm_file,
        MyFormat: mf_file,
        MyFormatGz: mfgz_file,
    }


def test_load_recipe_files_mixed_files_only_picks_mime_names(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "recipes"
    recipe_dir.mkdir()
    recipe_file = _write_recipe(recipe_dir, "medimage/dicom-series")
    (recipe_dir / "README").write_text("docs")
    recipe_files = load_recipe_files(recipe_dir)
    assert recipe_files == {DicomSeries: recipe_file}


def test_load_recipe_files_skips_side_cars(tmp_path: Path) -> None:
    """Side-cars of the recipe, including ones whose stem resolves to the file type
    (e.g. "dicom-series.salt"), aren't treated as recipe files"""
    recipe_dir = tmp_path / "recipes"
    recipe_dir.mkdir()
    recipe_file = _write_recipe(
        recipe_dir, "medimage/dicom-series.deid", "FORMAT dicom\n\n%header\n"
    )
    (recipe_dir / "medimage" / "dicom-series.transforms.yaml").write_text(
        'version: "0.1"\n'
    )
    (recipe_dir / "medimage" / "dicom-series.salt").write_bytes(b"key")
    assert load_recipe_files(recipe_dir) == {DicomSeries: recipe_file}


def test_load_recipe_files_multiple_recipes_raises(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "recipes"
    recipe_dir.mkdir()
    _write_recipe(recipe_dir, "medimage/dicom-series.deid", "FORMAT dicom\n")
    _write_recipe(recipe_dir, "medimage/dicom-series.json", "FORMAT dicom\n")
    with pytest.raises(
        ValueError, match="Expected exactly one deidentification recipe"
    ):
        load_recipe_files(recipe_dir)


# ── deidentify fallback / error tests ────────────────────────────────────────


def test_deidentify_falls_back_to_default_when_no_project_spec(
    dirs: tuple[Path, Path, Path, Path],
    tmp_path: Path,
) -> None:
    input_dir, output_dir, _, reid_dir = dirs
    # Provide only a default spec, no project-specific one
    recipe_dir = tmp_path / "recipes"
    recipe_dir.mkdir()
    default_dir = recipe_dir / DEFAULT_RECIPE_DIR
    default_dir.mkdir()
    _write_recipe(default_dir, "medimage/dicom-series")

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
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
    recipe_dir = tmp_path / "recipes"
    recipe_dir.mkdir()
    default_dir = recipe_dir / DEFAULT_RECIPE_DIR
    default_dir.mkdir()
    _write_recipe(default_dir, "medimage/dicom-series", '{"default": true}')
    project_dir = recipe_dir / PROJECT_ID
    project_dir.mkdir()
    project_spec_file = _write_recipe(
        project_dir, "medimage/dicom-series", '{"project": true}'
    )

    received_recipe_files: list = []

    def capturing_deidentify(self, *_, **kwargs):
        received_recipe_files.append(kwargs.get("recipe_files"))
        # Preserve the resource while testing spec selection.
        return self, dict(REID_MDATA)

    with patch.object(Accession, "deidentify", capturing_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
        )

    assert errors == []
    assert len(received_recipe_files) == 1
    # The recipe files passed in should map DicomSeries to the project file, not the
    # default
    assert received_recipe_files[0].get(DicomSeries) == project_spec_file


def test_deidentify_passes_max_workers_through(
    dirs: tuple[Path, Path, Path, Path],
) -> None:
    """max_workers should be forwarded from deidentify_api.deidentify() to
    Accession.deidentify() unchanged.
    """
    input_dir, output_dir, recipe_dir, reid_dir = dirs

    received_max_workers: list = []

    def capturing_deidentify(self, *_, **kwargs):
        received_max_workers.append(kwargs.get("max_workers"))
        new_sess = self.new_empty()
        # add something to the session so it isn't empty
        new_sess.add_resource("1", "T", "RES", File.sample(seed=42))
        return new_sess, dict(REID_MDATA)

    with patch.object(Accession, "deidentify", capturing_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
            max_workers=4,
        )

    assert errors == []
    assert received_max_workers == [4]


def test_deidentify_max_workers_defaults_to_none(
    dirs: tuple[Path, Path, Path, Path],
) -> None:
    """When not given explicitly, max_workers should be forwarded as None."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs

    received_max_workers: list = []

    def capturing_deidentify(self, *_, **kwargs):
        received_max_workers.append(kwargs.get("max_workers"))
        new_sess = self.new_empty()
        # add something to the session so it isn't empty
        new_sess.add_resource("1", "T", "RES", File.sample(seed=42))
        return new_sess, dict(REID_MDATA)

    with patch.object(Accession, "deidentify", capturing_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
        )

    assert errors == []
    assert received_max_workers == [None]


# ── end-to-end test (no mocking of Accession.deidentify) ───────────────


def test_deidentify_end_to_end_directory_structure(tmp_path: Path) -> None:
    """Runs deidentify() against a real session directory (written by
    Accession.save()) without mocking Accession.deidentify, and checks
    that the output directory structure is a single, correctly-named session
    directory that can be reloaded with Accession.load().

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
    # path in Accession.deidentify() and doesn't require a real deid spec.
    session = Accession(
        uid=SESSION_NAME,
        labels={"project": PROJECT_ID, "subject": SUBJECT_ID, "session": VISIT_ID},
        scans=[
            Acquisition(
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
        recipe_dir=SHIPPED_RECIPES_DIR,
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

    reloaded = Accession.load(session_out_dir)
    assert reloaded.name == SESSION_NAME
    assert reloaded.scans[scan_id].resources[resource_name].fileset.fspath.exists()


def _retiring_kwargs(input_dir, output_dir, recipe_dir, reid_dir):
    """The arguments of a run that retires its input: no manifest required,
    the input unlinked once the output is complete."""
    return dict(
        input_dir=input_dir,
        output_dir=output_dir,
        recipe_dir=recipe_dir,
        reid_dir=reid_dir,
        require_manifest=False,
        unlink_source="all",
    )


def test_deidentify_retires_a_late_scan_with_existing_output(
    dirs: tuple[Path, Path, Path, Path],
):
    """A one-scan late input succeeds alongside the earlier output scan."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    kwargs = _retiring_kwargs(input_dir, output_dir, recipe_dir, reid_dir)
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    output = output_dir / SESSION_NAME
    assert sorted(p.name for p in output.iterdir() if p.is_dir()) == ["1.T"]

    # The late scan, alone, under the same session name.
    Accession(
        uid=SESSION_NAME,
        labels={"project": PROJECT_ID, "subject": SUBJECT_ID, "session": VISIT_ID},
        scans=[
            Acquisition(id="2", type="late", resources={"RES": File.sample(seed=2)})
        ],
    ).save(input_dir)
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)

    assert errors == []
    assert not (input_dir / SESSION_NAME).exists(), "a complete run retires its input"
    assert sorted(p.name for p in output.iterdir() if p.is_dir()) == ["1.T", "2.late"]


def _late_scan(input_dir: Path, resources: dict) -> None:
    """A late scan, alone, staged under the same session name."""
    Accession(
        uid=SESSION_NAME,
        labels={"project": PROJECT_ID, "subject": SUBJECT_ID, "session": VISIT_ID},
        scans=[Acquisition(id="2", type="late", resources=resources)],
    ).save(input_dir)


def test_deidentify_processes_a_late_scan_whose_output_is_current(
    dirs: tuple[Path, Path, Path, Path],
):
    """A retry processes the late scan and then removes its recorded input."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    kwargs = dict(
        input_dir=input_dir,
        output_dir=output_dir,
        recipe_dir=recipe_dir,
        reid_dir=reid_dir,
        require_manifest=False,
    )
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs, unlink_source="all") == []
    _late_scan(input_dir, {"RES": File.sample(seed=2)})
    # Deidentified, but not unlinked (as after a death between the two).
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    assert (input_dir / SESSION_NAME).exists()

    with patch.object(
        Accession,
        "deidentify",
        autospec=True,
        side_effect=_mock_deidentify_passthrough,
    ) as run:
        errors = deidentify(**kwargs, unlink_source="all")

    assert errors == [] and run.call_count == 1
    assert not (input_dir / SESSION_NAME).exists()
    output = output_dir / SESSION_NAME
    assert sorted(p.name for p in output.iterdir() if p.is_dir()) == ["1.T", "2.late"]


def test_deidentify_accepts_removed_late_files_with_older_output(
    dirs: tuple[Path, Path, Path, Path],
):
    """A successful method may remove a late resource and retain older scans."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    kwargs = _retiring_kwargs(input_dir, output_dir, recipe_dir, reid_dir)
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    output = output_dir / SESSION_NAME
    (output / "1.T" / "EXTRA").mkdir()
    (output / "1.T" / "EXTRA" / "old.txt").write_text("an older file")
    _late_scan(input_dir, {"RES": File.sample(seed=2), "RES2": File.sample(seed=3)})

    def drop_one(self, dest_dir, **kwargs):
        new_sess = self.new_empty()
        new_sess.add_resource("2", "late", "RES", File.sample(seed=2))
        return new_sess, dict(REID_MDATA)

    with patch.object(Accession, "deidentify", drop_one):
        errors = deidentify(**kwargs)

    assert errors == []
    assert not (input_dir / SESSION_NAME).exists()
    assert (output / "1.T" / "RES").is_dir(), "the older scan is untouched"


def test_deidentify_does_not_gate_new_input_on_unrelated_old_checksums(
    dirs: tuple[Path, Path, Path, Path],
):
    """Historical output corruption does not change the new run verdict."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    kwargs = _retiring_kwargs(input_dir, output_dir, recipe_dir, reid_dir)
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    output = output_dir / SESSION_NAME
    old_file = next(
        f
        for f in (output / "1.T" / "RES").iterdir()
        if f.is_file() and not f.name.startswith(".") and f.suffix != ".json"
    )
    old_file.write_bytes(b"corrupt")
    _late_scan(input_dir, {"RES": File.sample(seed=2)})

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)

    assert errors == []
    assert not (input_dir / SESSION_NAME).exists()
    assert old_file.read_bytes() == b"corrupt"


def _data_names(resource_dir: Path) -> list[str]:
    return sorted(f.name for f in resource_dir.iterdir() if not f.name.startswith("__"))


def test_deidentify_runs_for_new_files_of_a_scan_the_output_already_has(
    dirs: tuple[Path, Path, Path, Path],
):
    """A new file in an existing scan is processed and saved."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    kwargs = _retiring_kwargs(input_dir, output_dir, recipe_dir, reid_dir)
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    output = output_dir / SESSION_NAME
    first = _data_names(output / "1.T" / "RES")
    # The fixture's input has scan "1.T"/RES with File.sample(seed=1); seed 7 has
    # another name.
    Accession(
        uid=SESSION_NAME,
        labels={"project": PROJECT_ID, "subject": SUBJECT_ID, "session": VISIT_ID},
        scans=[Acquisition(id="1", type="T", resources={"RES": File.sample(seed=7)})],
    ).save(input_dir)
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)

    assert errors == []
    assert not (input_dir / SESSION_NAME).exists()
    new_name = File.sample(seed=7).fspath.name
    assert new_name in _data_names(output / "1.T" / "RES") and new_name not in first
    # NOTE: saving into an existing resource replaces that resource's files
    # (Resource.save), so the first file is not kept beside the new one.
    # The deidentified copy is a staging copy: the originals are in the source
    # and, by then, usually on XNAT. A file-level merge is not part of this change.


def test_deidentify_runs_for_another_resource_of_a_scan_the_output_already_has(
    dirs: tuple[Path, Path, Path, Path],
):
    """Same scan ID, another resource: not covered by the output, so the run
    happens and the input is retired only after it."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    kwargs = _retiring_kwargs(input_dir, output_dir, recipe_dir, reid_dir)
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    Accession(
        uid=SESSION_NAME,
        labels={"project": PROJECT_ID, "subject": SUBJECT_ID, "session": VISIT_ID},
        scans=[Acquisition(id="1", type="T", resources={"OTHER": File.sample(seed=1)})],
    ).save(input_dir)
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)

    assert errors == []
    assert not (input_dir / SESSION_NAME).exists()
    output = output_dir / SESSION_NAME
    assert sorted(d.name for d in (output / "1.T").iterdir() if d.is_dir()) == [
        "OTHER",
        "RES",
    ]


def test_deidentify_keeps_an_input_that_gained_files_during_the_run(
    dirs: tuple[Path, Path, Path, Path],
):
    """Retire the recorded input and leave new paths for the next cycle."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    session_dir = input_dir / SESSION_NAME
    kwargs = _retiring_kwargs(input_dir, output_dir, recipe_dir, reid_dir)

    def passthrough_while_a_file_arrives(self, dest_dir, **kw):
        late = session_dir / "3.late" / "DICOM"
        late.mkdir(parents=True)
        (late / "late.dcm").write_text("late")
        return self, dict(REID_MDATA)

    with patch.object(Accession, "deidentify", passthrough_while_a_file_arrives):
        errors = deidentify(**kwargs)

    assert errors == [], "the run itself was complete for what it read"
    assert (session_dir / "3.late" / "DICOM" / "late.dcm").exists(), "input kept"
    assert not (output_dir / SESSION_NAME / "3.late").exists()

    assert not (session_dir / "1.T").exists()

    # Next cycle: only the late input remains and is processed.
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)
    assert errors == []
    assert not session_dir.exists()
    assert (output_dir / SESSION_NAME / "3.late" / "DICOM" / "late.dcm").exists()


def test_deidentify_runs_again_for_a_copied_file_with_new_content(
    dirs: tuple[Path, Path, Path, Path], tmp_path: Path
):
    """Always process new input, including a reused name with changed bytes."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    kwargs = _retiring_kwargs(input_dir, output_dir, recipe_dir, reid_dir)
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    output = output_dir / SESSION_NAME
    [name] = _data_names(output / "1.T" / "RES")
    corrected = tmp_path / "corrected" / name
    corrected.parent.mkdir()
    corrected.write_bytes(b"the corrected result")
    Accession(
        uid=SESSION_NAME,
        labels={"project": PROJECT_ID, "subject": SUBJECT_ID, "session": VISIT_ID},
        scans=[Acquisition(id="1", type="T", resources={"RES": File(corrected)})],
    ).save(input_dir)

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)

    assert errors == []
    assert not (input_dir / SESSION_NAME).exists()
    assert (output / "1.T" / "RES" / name).read_bytes() == b"the corrected result"


def test_deidentify_saves_a_directory_resource(tmp_path: Path):
    """A directory resource is saved with its files and its input is retired."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    for d in (input_dir, output_dir):
        d.mkdir()
    contents = tmp_path / "contents"
    contents.mkdir()
    (contents / "a.txt").write_text("a")
    (contents / "b.txt").write_text("b")
    Accession(
        uid=SESSION_NAME,
        labels={"project": PROJECT_ID, "subject": SUBJECT_ID, "session": VISIT_ID},
        scans=[Acquisition(id="1", type="T", resources={"DIR": Directory(contents)})],
    ).save(input_dir)

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=SHIPPED_RECIPES_DIR,
            reid_dir=None,
            require_manifest=False,
            unlink_source="all",
        )

    assert errors == []
    assert not (input_dir / SESSION_NAME).exists()
    assert sorted(
        f.name for f in (output_dir / SESSION_NAME / "1.T" / "DIR").rglob("*.txt")
    ) == ["a.txt", "b.txt"]


def test_deidentify_does_not_trust_a_stale_input_manifest(tmp_path: Path):
    """A directory resource includes its present files, even with a stale manifest."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    for d in (input_dir, output_dir):
        d.mkdir()
    contents = tmp_path / "contents"
    contents.mkdir()
    (contents / "a.txt").write_text("a")
    (contents / "b.txt").write_text("b")
    kwargs = dict(
        input_dir=input_dir,
        output_dir=output_dir,
        recipe_dir=SHIPPED_RECIPES_DIR,
        reid_dir=None,
        require_manifest=False,
        unlink_source="all",
    )

    def stage() -> Path:
        Accession(
            uid=SESSION_NAME,
            labels={"project": PROJECT_ID, "subject": SUBJECT_ID, "session": VISIT_ID},
            scans=[
                Acquisition(id="1", type="T", resources={"DIR": Directory(contents)})
            ],
        ).save(input_dir)
        return input_dir / SESSION_NAME

    stage()
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    assert (output_dir / SESSION_NAME).is_dir(), "the first run produced the output"
    session_dir = stage()  # the same input again, with its manifest
    a_file = next(session_dir.rglob("a.txt"))
    (a_file.parent / "c.txt").write_text("not in the manifest yet")

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)

    assert errors == []
    assert not session_dir.exists()
    assert [f.name for f in (output_dir / SESSION_NAME).rglob("c.txt")] == ["c.txt"]


def test_deidentify_retires_recorded_session_files_after_success(
    dirs: tuple[Path, Path, Path, Path],
):
    """The all policy removes recorded files throughout the input session."""
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    kwargs = _retiring_kwargs(input_dir, output_dir, recipe_dir, reid_dir)
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    _stage(input_dir, SESSION_NAME)
    (input_dir / SESSION_NAME / "stray.txt").write_text("a file nothing owns")

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)

    assert errors == []
    assert not (input_dir / SESSION_NAME).exists()


@pytest.mark.parametrize("existing_output", [False, True])
def test_deidentify_keeps_only_uid_in_output_metadata(
    dirs: tuple[Path, Path, Path, Path],
    existing_output: bool,
):
    input_dir, output_dir, recipe_dir, _ = dirs
    source_dir = input_dir / SESSION_NAME
    metadata_paths = [
        Path("__METADATA__.json"),
        Path("1.T/__METADATA__.json"),
        Path("1.T/RES/__METADATA__.json"),
    ]

    for relative_path in metadata_paths:
        path = source_dir / relative_path
        metadata = json.loads(path.read_text())
        metadata["PatientName"] = "Source name"
        path.write_text(json.dumps(metadata))

    originals = {path: (source_dir / path).read_bytes() for path in metadata_paths}

    if existing_output:
        Accession.load(source_dir).save(output_dir)

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
        )

    assert errors == []
    output_session = output_dir / SESSION_NAME

    for relative_path in metadata_paths:
        expected = (
            {"__uid__": SESSION_NAME, "__labels__": SESSION_LABELS}
            if relative_path == Path("__METADATA__.json")
            else {}
        )
        assert json.loads((output_session / relative_path).read_text()) == expected
        assert (source_dir / relative_path).read_bytes() == originals[relative_path]

    reloaded = Accession.load(output_session)
    assert reloaded.uid == SESSION_NAME
    assert reloaded.scans["1"].resources["RES"].checksums


def _with_uid(session_dir: Path, uid: str) -> None:
    metadata_path = session_dir / "__METADATA__.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["__uid__"] = uid
    metadata_path.write_text(json.dumps(metadata))


def test_deidentify_hashes_uid_and_records_it_for_reid(
    dirs: tuple[Path, Path, Path, Path],
):
    input_dir, output_dir, recipe_dir, reid_dir = dirs
    original = "1.2.840.original.study.uid"
    _with_uid(input_dir / SESSION_NAME, original)

    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            reid_dir=reid_dir,
            hash_uid_key="site-secret",
        )

    assert errors == []
    expected = hmac.new(b"site-secret", original.encode(), hashlib.sha256).hexdigest()
    output_session = output_dir / SESSION_NAME
    assert json.loads((output_session / "__METADATA__.json").read_text()) == {
        "__uid__": expected,
        # The labels are always kept, as they are needed to reload the session
        "__labels__": SESSION_LABELS,
    }
    assert Accession.load(output_session).uid == expected
    # The original isn't left in any sidecar of the output
    for sidecar in output_session.rglob("__METADATA__.json"):
        assert original not in sidecar.read_text()
    reid = json.loads((reid_dir / f"{SESSION_NAME}.json").read_text())
    assert reid["session_uid"] == original
    assert reid["deidentified_session_uid"] == expected


def test_deidentify_uid_hash_is_stable_and_depends_on_the_key(
    dirs: tuple[Path, Path, Path, Path],
):
    input_dir, output_dir, recipe_dir, _ = dirs
    uids = []
    for key in ("key-a", "key-a", "key-b"):
        with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
            assert not deidentify(
                input_dir=input_dir,
                output_dir=output_dir,
                recipe_dir=recipe_dir,
                hash_uid_key=key,
            )
        metadata = (output_dir / SESSION_NAME / "__METADATA__.json").read_text()
        uids.append(json.loads(metadata)["__uid__"])
    assert uids[0] == uids[1]
    assert uids[0] != uids[2]


def test_deidentify_keeps_original_uid_without_a_key(
    dirs: tuple[Path, Path, Path, Path],
):
    input_dir, output_dir, recipe_dir, _ = dirs
    _with_uid(input_dir / SESSION_NAME, "original-uid")
    with patch.object(Accession, "deidentify", _mock_deidentify_passthrough):
        assert not deidentify(
            input_dir=input_dir, output_dir=output_dir, recipe_dir=recipe_dir
        )
    metadata = (output_dir / SESSION_NAME / "__METADATA__.json").read_text()
    assert json.loads(metadata)["__uid__"] == "original-uid"


def test_deidentify_refuses_an_empty_hash_key(dirs: tuple[Path, Path, Path, Path]):
    input_dir, output_dir, recipe_dir, _ = dirs
    with pytest.raises(ValueError, match="must not be empty"):
        deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            recipe_dir=recipe_dir,
            hash_uid_key="",
        )


def test_allowlist_never_removes_reserved_metadata(tmp_path: Path):
    metadata = Metadata(
        {
            "__uid__": "uid",
            "__store_type__": "xnat",
            "PatientName": "Source name",
            "Modality": "MR",
        },
        None,
    )
    metadata._read = True

    metadata.save(tmp_path, allowlist={"Modality"})

    assert json.loads((tmp_path / Metadata.FNAME).read_text()) == {
        "__uid__": "uid",
        "__store_type__": "xnat",
        "Modality": "MR",
    }
