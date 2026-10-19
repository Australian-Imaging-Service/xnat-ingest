import json
from pathlib import Path
from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet
from fileformats.generic import Directory, File
from fileformats.medimage import DicomSeries
from fileformats.testing import MyFormat, MyFormatGz

import xnat_ingest.specs as _specs_pkg
from xnat_ingest.api.deidentify_api import (
    DEFAULT_SPEC_DIR,
    deidentify,
    load_recipe_files,
)
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
    _stage(input_dir, SESSION_NAME)
    return input_dir, output_dir, SHIPPED_SPECS_DIR, reid_dir


def _stage(input_dir: Path, name: str) -> Path:
    """Stage a valid session with one data file in it.

    Deliberately NOT an empty directory. deidentify refuses a session holding no
    data files, because the completeness gate compares an output count against an
    input count and 0 == 0 would pass an empty session off as a complete one.
    These tests are about spec selection, error collection and the
    re-identification sidecar, so they need a session that is merely valid.

    Written through save() rather than by hand so that the resource gets its
    manifest; loading one without a manifest is an error unless the caller opts
    out with require_manifest=False.
    """
    project_id, subject_id, session_id = name.split(".")
    ImagingSession(
        uid=name,
        project_id=project_id,
        subject_id=subject_id,
        session_id=session_id,
        scans=[ImagingScan(id="1", type="T", resources={"RES": File.sample(seed=1)})],
    ).save(input_dir)
    return input_dir / name


def _mock_deidentify(self, dest_dir, **kwargs) -> tuple[ImagingSession, dict]:
    new_sess = self.new_empty()
    # One file under the scan the staged input has ("1.T"), so that the output
    # matches the input by scan ID. The completeness check counts the output
    # files of the scans the input holds; a file elsewhere would not count.
    new_sess.add_resource("1", "T", "RES", File.sample(seed=42))
    return new_sess, dict(REID_MDATA)


def _mock_deidentify_passthrough(
    self, dest_dir, **kwargs
) -> tuple[ImagingSession, dict]:
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
    # The fixture already stages a session with a scan directory in it, which is
    # what makes the unlink recursive. An extra hand-made resource here would be
    # counted as input but not carried to the output, so the completeness gate
    # would correctly refuse the very unlink this test is asserting.
    session_dir = input_dir / SESSION_NAME

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


def test_deidentify_does_not_skip_an_output_that_is_newer_but_short(
    dirs: tuple[Path, Path, Path, Path],
):
    """A partial output must be reprocessed, however new it is.

    Skipping on mtime alone treats "newer than its input" as "finished". A
    session left half-written by a run that died is also newer than its input,
    so it would be skipped on this cycle and every cycle after it, permanently,
    while sitting in the directory upload reads.
    """
    input_dir, output_dir, spec_dir, reid_dir = dirs

    # an output that is NEWER than the input but missing its data
    stale = output_dir / SESSION_NAME
    (stale / "1.scan" / "RES").mkdir(parents=True)
    (stale / "__METADATA__.json").write_text("{}")

    calls: list = []

    def counting_deidentify(self, dest_dir, **kwargs):
        calls.append(self.name)
        return self, dict(REID_MDATA)

    with patch.object(ImagingSession, "deidentify", counting_deidentify):
        deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
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
    bypasses the repair: ImagingResource.save is what overwrites a bad output,
    and it only runs if the session is not skipped.
    """
    input_dir, output_dir, spec_dir, reid_dir = dirs
    calls: list = []

    def counting_deidentify(self, dest_dir, **kwargs):
        calls.append(self.name)
        return self, dict(REID_MDATA)

    with patch.object(ImagingSession, "deidentify", counting_deidentify):
        deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
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

    with patch.object(ImagingSession, "deidentify", counting_deidentify):
        deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
        )

    assert len(calls) == 2, "a corrupt output was skipped, so it can never be repaired"


def test_deidentify_unlinks_a_backlog_session_it_skips(
    dirs: tuple[Path, Path, Path, Path],
):
    """Turning --unlink-source on must clear sessions processed before it was.

    A session whose output already exists is skipped as current on every cycle,
    so if only the just-produced path unlinked, the backlog would sit in the
    input directory for ever. The skip has already verified the output is
    complete and hashes to its manifest, which is stronger than the file count
    the post-run gate applies, so retiring the input there is safe.
    """
    input_dir, output_dir, spec_dir, reid_dir = dirs
    session_dir = input_dir / SESSION_NAME

    # first pass with no unlink: produces the output, leaves the input
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
        )
    assert session_dir.exists(), "nothing should have been unlinked yet"
    assert (output_dir / SESSION_NAME).is_dir()

    # second pass WITH unlink: the session is skipped as current, and retired
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
            unlink_source="all",
        )

    assert errors == []
    assert not session_dir.exists(), (
        "a skipped session was never unlinked, so enabling the flag would "
        "leave the existing backlog in place for ever"
    )


def test_deidentify_skips_an_output_that_is_complete_and_current(
    dirs: tuple[Path, Path, Path, Path],
):
    """Re-deidentifying an unchanged session is wasted work.

    Measured at roughly 80 seconds for a 535-instance series, which a
    60-second loop cannot absorb once several sessions are staged.
    """
    input_dir, output_dir, spec_dir, reid_dir = dirs
    calls: list = []

    def counting_deidentify(self, dest_dir, **kwargs):
        calls.append(self.name)
        return self, dict(REID_MDATA)

    with patch.object(ImagingSession, "deidentify", counting_deidentify):
        for _ in range(2):
            deidentify(
                input_dir=input_dir,
                output_dir=output_dir,
                spec_dir=spec_dir,
                reid_dir=reid_dir,
            )

    assert len(calls) == 1, f"the second cycle reprocessed the session: {calls}"


def test_deidentify_reports_a_session_with_no_data_files(
    dirs: tuple[Path, Path, Path, Path],
):
    """An empty session directory must not pass as a complete run.

    The completeness gate compares an output count against an input count, and
    0 == 0 is vacuously true, so an empty input used to produce an output
    directory, report no errors and exit 0. On tier-1 `upload` reads that
    directory directly, and ImagingSession.load resolves the project and session
    ids from the DIRECTORY NAME, so an empty directory is uploadable: XNAT gets an
    experiment with no data in it.
    """
    input_dir, output_dir, spec_dir, reid_dir = dirs
    empty = input_dir / "PROJ.SUBJ.EMPTY"
    empty.mkdir()

    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
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
    input_dir, output_dir, spec_dir, reid_dir = dirs
    skeleton = input_dir / "PROJ.SUBJ.SKEL"
    (skeleton / "1.scan").mkdir(parents=True)
    # what survives a keep-metadata unlink: the metadata, and no resource dirs
    (skeleton / "__METADATA__.json").write_text("{}")
    (skeleton / "1.scan" / "__METADATA__.json").write_text("{}")

    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
        )

    assert not any("SKEL" in e for e in errors), errors
    assert not (output_dir / "PROJ.SUBJ.SKEL").exists()


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

    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
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


def test_deidentify_no_reid_dir_discards_metadata(
    dirs: tuple[Path, Path, Path, Path],
) -> None:
    input_dir, output_dir, spec_dir, reid_dir = dirs

    with patch.object(ImagingSession, "deidentify", _mock_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
        )

    assert errors == []
    # deidentified output still produced, but nothing written to reid_dir
    assert (output_dir / SESSION_NAME).exists()
    assert not list(reid_dir.iterdir())


def test_deidentify_encrypt_key_without_reid_dir_warns(
    dirs: tuple[Path, Path, Path, Path],
    caplog: pytest.LogCaptureFixture,
) -> None:
    input_dir, output_dir, spec_dir, _ = dirs

    with (
        patch.object(ImagingSession, "deidentify", _mock_deidentify),
        caplog.at_level("WARNING", logger="xnat-ingest"),
    ):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_encrypt_key=Fernet.generate_key(),
        )

    assert errors == []
    assert "no --reid-dir" in caplog.text


def test_deidentify_encrypted(dirs: tuple[Path, Path, Path, Path]) -> None:
    input_dir, output_dir, spec_dir, reid_dir = dirs
    key = Fernet.generate_key()

    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
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

    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
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
        _stage(input_dir, name)

    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
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


# ── load_recipe_files unit tests ────────────────────────────────────────────────────


def test_load_recipe_files_nonexistent_dir_returns_none(tmp_path: Path) -> None:
    assert load_recipe_files(tmp_path / "nonexistent") is None


def _write_spec(spec_dir: Path, mime_like: str, content: str = "{}") -> Path:
    """Helper to create a spec file at spec_dir/<category>/<format>."""
    category, format_name = mime_like.split("/", 1)
    (spec_dir / category).mkdir(parents=True, exist_ok=True)
    spec_file = spec_dir / category / format_name
    spec_file.write_text(content)
    return spec_file


def test_load_recipe_files_empty_dir_returns_empty_dict(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    assert load_recipe_files(spec_dir) == {}


def test_load_recipe_files_ignores_non_category_files(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    (spec_dir / "README").write_text("docs")
    (spec_dir / "config.json").write_text("{}")
    assert load_recipe_files(spec_dir) == {}


def test_load_recipe_files_single_mime_like(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    spec_file = _write_spec(spec_dir, "medimage/dicom-series")
    recipe_files = load_recipe_files(spec_dir)
    assert recipe_files == {DicomSeries: spec_file}


def test_load_recipe_files_multiple_mime_likes(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    dcm_file = _write_spec(spec_dir, "medimage/dicom-series")
    mf_file = _write_spec(spec_dir, "testing/my-format")
    mfgz_file = _write_spec(spec_dir, "testing/my-format-gz")
    recipe_files = load_recipe_files(spec_dir)
    assert recipe_files == {
        DicomSeries: dcm_file,
        MyFormat: mf_file,
        MyFormatGz: mfgz_file,
    }


def test_load_recipe_files_mixed_files_only_picks_mime_names(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    spec_file = _write_spec(spec_dir, "medimage/dicom-series")
    (spec_dir / "README").write_text("docs")
    recipe_files = load_recipe_files(spec_dir)
    assert recipe_files == {DicomSeries: spec_file}


def test_load_recipe_files_skips_side_cars(tmp_path: Path) -> None:
    """Side-cars of the recipe, including ones whose stem resolves to the file type
    (e.g. "dicom-series.salt"), aren't treated as recipe files"""
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    spec_file = _write_spec(
        spec_dir, "medimage/dicom-series.deid", "FORMAT dicom\n\n%header\n"
    )
    (spec_dir / "medimage" / "dicom-series.transforms.yaml").write_text(
        'version: "0.1"\n'
    )
    (spec_dir / "medimage" / "dicom-series.salt").write_bytes(b"key")
    assert load_recipe_files(spec_dir) == {DicomSeries: spec_file}


def test_load_recipe_files_multiple_recipes_raises(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    _write_spec(spec_dir, "medimage/dicom-series.deid", "FORMAT dicom\n")
    _write_spec(spec_dir, "medimage/dicom-series.json", "FORMAT dicom\n")
    with pytest.raises(
        ValueError, match="Expected exactly one deidentification recipe"
    ):
        load_recipe_files(spec_dir)


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

    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
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

    received_recipe_files: list = []

    def capturing_deidentify(self, *_, **kwargs):
        received_recipe_files.append(kwargs.get("recipe_files"))
        # `self`, not new_empty(): an empty output is an INCOMPLETE one and the
        # gate reports it, so this test would fail on an assertion about specs.
        return self, dict(REID_MDATA)

    with patch.object(ImagingSession, "deidentify", capturing_deidentify):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
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
    ImagingSession.deidentify() unchanged.
    """
    input_dir, output_dir, spec_dir, reid_dir = dirs

    received_max_workers: list = []

    def capturing_deidentify(self, *_, **kwargs):
        received_max_workers.append(kwargs.get("max_workers"))
        new_sess = self.new_empty()
        # add something to the session so it isn't empty
        new_sess.add_resource("1", "T", "RES", File.sample(seed=42))
        return new_sess, dict(REID_MDATA)

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
        new_sess = self.new_empty()
        # add something to the session so it isn't empty
        new_sess.add_resource("1", "T", "RES", File.sample(seed=42))
        return new_sess, dict(REID_MDATA)

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


def test_deidentify_counts_only_its_own_scans_in_an_existing_output(
    dirs: tuple[Path, Path, Path, Path],
):
    """A series that arrives after the session was deidentified and uploaded is
    staged as a session with only the new scan, and its output is saved into the
    existing deidentified session directory. The completeness check must count
    the scans this run carried, not the ones that were already there: counting
    the whole directory called the run incomplete, kept its input and repeated it
    on every cycle, and the new scan never reached the upload stage."""
    input_dir, output_dir, spec_dir, reid_dir = dirs
    kwargs = dict(
        input_dir=input_dir,
        output_dir=output_dir,
        spec_dir=spec_dir,
        reid_dir=reid_dir,
        require_manifest=False,
        unlink_source="all",
    )
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    output = output_dir / SESSION_NAME
    assert sorted(p.name for p in output.iterdir() if p.is_dir()) == ["1.T"]

    # The late scan, alone, under the same session name.
    ImagingSession(
        uid=SESSION_NAME,
        project_id=PROJECT_ID,
        subject_id=SUBJECT_ID,
        session_id=VISIT_ID,
        scans=[
            ImagingScan(id="2", type="late", resources={"RES": File.sample(seed=2)})
        ],
    ).save(input_dir)
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)

    assert errors == []
    assert not (input_dir / SESSION_NAME).exists(), "a complete run retires its input"
    assert sorted(p.name for p in output.iterdir() if p.is_dir()) == ["1.T", "2.late"]


def _late_scan(input_dir: Path, resources: dict) -> None:
    """A late scan, alone, staged under the same session name."""
    ImagingSession(
        uid=SESSION_NAME,
        project_id=PROJECT_ID,
        subject_id=SUBJECT_ID,
        session_id=VISIT_ID,
        scans=[ImagingScan(id="2", type="late", resources=resources)],
    ).save(input_dir)


def test_deidentify_skips_a_late_scan_whose_output_is_current(
    dirs: tuple[Path, Path, Path, Path],
):
    """The skip path judges the merged output the same way: by the scans of the
    input in front of it. A late scan already deidentified into the existing
    output (the run died before the unlink) is skipped and its input retired,
    without re-deidentifying it on every cycle."""
    input_dir, output_dir, spec_dir, reid_dir = dirs
    kwargs = dict(
        input_dir=input_dir,
        output_dir=output_dir,
        spec_dir=spec_dir,
        reid_dir=reid_dir,
        require_manifest=False,
    )
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs, unlink_source="all") == []
    _late_scan(input_dir, {"RES": File.sample(seed=2)})
    # Deidentified, but not unlinked (as after a death between the two).
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    assert (input_dir / SESSION_NAME).exists()

    with patch.object(ImagingSession, "deidentify") as run:
        errors = deidentify(**kwargs, unlink_source="all")

    assert errors == [] and not run.called
    assert not (input_dir / SESSION_NAME).exists()
    output = output_dir / SESSION_NAME
    assert sorted(p.name for p in output.iterdir() if p.is_dir()) == ["1.T", "2.late"]


def test_deidentify_keeps_a_late_scan_that_is_short_despite_the_older_files(
    dirs: tuple[Path, Path, Path, Path],
):
    """Old files in the output must not pad a short run. The late scan has two
    resources; the run carries one. 2 old + 1 new is more than the 2 input
    files, and the run is incomplete all the same."""
    input_dir, output_dir, spec_dir, reid_dir = dirs
    kwargs = dict(
        input_dir=input_dir,
        output_dir=output_dir,
        spec_dir=spec_dir,
        reid_dir=reid_dir,
        require_manifest=False,
        unlink_source="all",
    )
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    output = output_dir / SESSION_NAME
    (output / "1.T" / "EXTRA").mkdir()
    (output / "1.T" / "EXTRA" / "old.txt").write_text("an older file")
    _late_scan(input_dir, {"RES": File.sample(seed=2), "RES2": File.sample(seed=3)})

    def drop_one(self, dest_dir, **kwargs):
        new_sess = self.new_empty()
        new_sess.add_resource("2", "late", "RES", File.sample(seed=2))
        return new_sess, dict(REID_MDATA)

    with patch.object(ImagingSession, "deidentify", drop_one):
        errors = deidentify(**kwargs)

    assert errors and "incomplete (1 of 2 files)" in errors[0]
    assert (input_dir / SESSION_NAME).exists(), "a short run must not delete its input"
    assert (output / "1.T" / "RES").is_dir(), "the older scan is untouched"


def test_deidentify_keeps_a_late_scan_when_an_older_output_file_is_corrupt(
    dirs: tuple[Path, Path, Path, Path],
):
    """A corrupt file among the older scans fails the checksum verification of
    the output, so the new input is kept and the problem is reported. Nothing is
    deleted on the strength of a directory that does not verify."""
    input_dir, output_dir, spec_dir, reid_dir = dirs
    kwargs = dict(
        input_dir=input_dir,
        output_dir=output_dir,
        spec_dir=spec_dir,
        reid_dir=reid_dir,
        require_manifest=False,
        unlink_source="all",
    )
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    output = output_dir / SESSION_NAME
    old_file = next(
        f
        for f in (output / "1.T" / "RES").iterdir()
        if f.is_file() and not f.name.startswith(".") and f.suffix != ".json"
    )
    old_file.write_bytes(b"corrupt")
    _late_scan(input_dir, {"RES": File.sample(seed=2)})

    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)

    assert errors and "incomplete" in errors[0]
    assert (input_dir / SESSION_NAME).exists()


def _data_names(resource_dir: Path) -> list[str]:
    return sorted(f.name for f in resource_dir.iterdir() if not f.name.startswith("__"))


def test_deidentify_runs_for_new_files_of_a_scan_the_output_already_has(
    dirs: tuple[Path, Path, Path, Path],
):
    """Same scan, same resource, same number of files, OTHER files (a series
    sent again with new instances while the first deidentified copy is still
    there). A count would call the output current and delete the new input.
    The files are matched by name: the run happens, the new file is in the
    output, and only then is the input retired."""
    input_dir, output_dir, spec_dir, reid_dir = dirs
    kwargs = dict(
        input_dir=input_dir,
        output_dir=output_dir,
        spec_dir=spec_dir,
        reid_dir=reid_dir,
        require_manifest=False,
        unlink_source="all",
    )
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    output = output_dir / SESSION_NAME
    first = _data_names(output / "1.T" / "RES")
    # The fixture's input has scan "1.T"/RES with File.sample(seed=1); seed 7 has
    # another name.
    ImagingSession(
        uid=SESSION_NAME,
        project_id=PROJECT_ID,
        subject_id=SUBJECT_ID,
        session_id=VISIT_ID,
        scans=[ImagingScan(id="1", type="T", resources={"RES": File.sample(seed=7)})],
    ).save(input_dir)
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)

    assert errors == []
    assert not (input_dir / SESSION_NAME).exists()
    new_name = File.sample(seed=7).fspath.name
    assert new_name in _data_names(output / "1.T" / "RES") and new_name not in first
    # NOTE: saving into an existing resource replaces that resource's files
    # (ImagingResource.save), so the first file is not kept beside the new one.
    # The deidentified copy is a staging copy: the originals are in the source
    # and, by then, usually on XNAT. A file-level merge is not part of this change.


def test_deidentify_runs_for_another_resource_of_a_scan_the_output_already_has(
    dirs: tuple[Path, Path, Path, Path],
):
    """Same scan ID, another resource: not covered by the output, so the run
    happens and the input is retired only after it."""
    input_dir, output_dir, spec_dir, reid_dir = dirs
    kwargs = dict(
        input_dir=input_dir,
        output_dir=output_dir,
        spec_dir=spec_dir,
        reid_dir=reid_dir,
        require_manifest=False,
        unlink_source="all",
    )
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    ImagingSession(
        uid=SESSION_NAME,
        project_id=PROJECT_ID,
        subject_id=SUBJECT_ID,
        session_id=VISIT_ID,
        scans=[ImagingScan(id="1", type="T", resources={"OTHER": File.sample(seed=1)})],
    ).save(input_dir)
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
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
    """The assign stage merges a late series into a session it has already
    written. A file that arrives while that session is being deidentified is
    not in the output; removing the input directory would remove it too. The
    input is kept, and deidentified in full on the next cycle."""
    input_dir, output_dir, spec_dir, reid_dir = dirs
    session_dir = input_dir / SESSION_NAME
    kwargs = dict(
        input_dir=input_dir,
        output_dir=output_dir,
        spec_dir=spec_dir,
        reid_dir=reid_dir,
        require_manifest=False,
        unlink_source="all",
    )

    def passthrough_while_a_file_arrives(self, dest_dir, **kw):
        late = session_dir / "3.late" / "DICOM"
        late.mkdir(parents=True)
        (late / "late.dcm").write_text("late")
        return self, dict(REID_MDATA)

    with patch.object(ImagingSession, "deidentify", passthrough_while_a_file_arrives):
        errors = deidentify(**kwargs)

    assert errors == [], "the run itself was complete for what it read"
    assert (session_dir / "3.late" / "DICOM" / "late.dcm").exists(), "input kept"
    assert not (output_dir / SESSION_NAME / "3.late").exists()

    # Next cycle: the whole input, late file included, is deidentified and retired.
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)
    assert errors == []
    assert not session_dir.exists()
    assert (output_dir / SESSION_NAME / "3.late" / "DICOM" / "late.dcm").exists()


def test_deidentify_runs_again_for_a_copied_file_with_new_content(
    dirs: tuple[Path, Path, Path, Path], tmp_path: Path
):
    """A format that is copied through (anything but DICOM) is matched by content,
    not by name alone: a result file sent again with other bytes under the same
    name is deidentified (copied) again, and its input retired only after that."""
    input_dir, output_dir, spec_dir, reid_dir = dirs
    kwargs = dict(
        input_dir=input_dir,
        output_dir=output_dir,
        spec_dir=spec_dir,
        reid_dir=reid_dir,
        require_manifest=False,
        unlink_source="all",
    )
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    output = output_dir / SESSION_NAME
    [name] = _data_names(output / "1.T" / "RES")
    corrected = tmp_path / "corrected" / name
    corrected.parent.mkdir()
    corrected.write_bytes(b"the corrected result")
    ImagingSession(
        uid=SESSION_NAME,
        project_id=PROJECT_ID,
        subject_id=SUBJECT_ID,
        session_id=VISIT_ID,
        scans=[ImagingScan(id="1", type="T", resources={"RES": File(corrected)})],
    ).save(input_dir)

    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)

    assert errors == []
    assert not (input_dir / SESSION_NAME).exists()
    assert (output / "1.T" / "RES" / name).read_bytes() == b"the corrected result"


def test_deidentify_counts_the_files_inside_a_directory_resource(tmp_path: Path):
    """A directory-backed resource is one path in the saved session but holds
    several files; the completeness gate counts the files, as the input does."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    for d in (input_dir, output_dir):
        d.mkdir()
    contents = tmp_path / "contents"
    contents.mkdir()
    (contents / "a.txt").write_text("a")
    (contents / "b.txt").write_text("b")
    ImagingSession(
        uid=SESSION_NAME,
        project_id=PROJECT_ID,
        subject_id=SUBJECT_ID,
        session_id=VISIT_ID,
        scans=[ImagingScan(id="1", type="T", resources={"DIR": Directory(contents)})],
    ).save(input_dir)

    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=SHIPPED_SPECS_DIR,
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
    """A producer interrupted between writing a file and updating the manifest
    leaves a file the manifest does not list. The skip path reads the files on
    disk, not the manifest: the unlisted file is not in the output, so the
    session is deidentified again and the input retired only after that."""
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
        spec_dir=SHIPPED_SPECS_DIR,
        reid_dir=None,
        require_manifest=False,
        unlink_source="all",
    )

    def stage() -> Path:
        ImagingSession(
            uid=SESSION_NAME,
            project_id=PROJECT_ID,
            subject_id=SUBJECT_ID,
            session_id=VISIT_ID,
            scans=[
                ImagingScan(id="1", type="T", resources={"DIR": Directory(contents)})
            ],
        ).save(input_dir)
        return input_dir / SESSION_NAME

    stage()
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    assert (output_dir / SESSION_NAME).is_dir(), "the first run produced the output"
    session_dir = stage()  # the same input again, with its manifest
    a_file = next(session_dir.rglob("a.txt"))
    (a_file.parent / "c.txt").write_text("not in the manifest yet")

    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)

    assert errors == []
    assert not session_dir.exists()
    assert [f.name for f in (output_dir / SESSION_NAME).rglob("c.txt")] == ["c.txt"]


def test_deidentify_does_not_skip_an_input_with_a_file_outside_its_resources(
    dirs: tuple[Path, Path, Path, Path],
):
    """A data file at the session root belongs to no resource and reaches no
    output; the output cannot be current for such an input. The run happens,
    produces nothing for the stray file, and keeps the input."""
    input_dir, output_dir, spec_dir, reid_dir = dirs
    kwargs = dict(
        input_dir=input_dir,
        output_dir=output_dir,
        spec_dir=spec_dir,
        reid_dir=reid_dir,
        require_manifest=False,
        unlink_source="all",
    )
    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        assert deidentify(**kwargs) == []
    _stage(input_dir, SESSION_NAME)
    (input_dir / SESSION_NAME / "stray.txt").write_text("a file nothing owns")

    with patch.object(ImagingSession, "deidentify", _mock_deidentify_passthrough):
        errors = deidentify(**kwargs)

    assert errors and "incomplete" in errors[0]
    assert (input_dir / SESSION_NAME / "stray.txt").exists()
