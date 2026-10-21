import os
from pathlib import Path
from unittest.mock import patch

import pytest
from fileformats.core import FileSet
from fileformats.generic import File

from xnat_ingest.api.deidentify_api import deidentify
from xnat_ingest.helpers.metadata import Metadata
from xnat_ingest.model.resource import ImagingResource
from xnat_ingest.model.session import ImagingSession

from .test_deidentify import SESSION_NAME, SHIPPED_SPECS_DIR, _stage


@pytest.mark.parametrize("unlink_source", ["all", "keep-metadata"])
def test_retire_recorded_files_and_keep_late_file(tmp_path, unlink_source):
    input_dir = tmp_path / "input"
    source = _stage(input_dir, SESSION_NAME)
    resource = source / "1.T" / "RES"
    recorded = set(resource.iterdir())
    late = resource / "late.txt"

    def process(session, dest_dir, **kwargs):
        late.write_text("next cycle")
        return session, {}

    with patch.object(ImagingSession, "deidentify", process):
        errors = deidentify(
            input_dir,
            tmp_path / "output",
            SHIPPED_SPECS_DIR,
            unlink_source=unlink_source,
        )
    assert errors == []
    assert late.read_text() == "next cycle"
    assert not any(p.exists() for p in recorded)
    assert (source / Metadata.FNAME).exists() == (unlink_source == "keep-metadata")


def test_method_can_remove_all_resources(tmp_path):
    input_dir = tmp_path / "input"
    source = _stage(input_dir, SESSION_NAME)
    with patch.object(
        ImagingSession,
        "deidentify",
        lambda session, *a, **kw: (session.new_empty(), {}),
    ):
        errors = deidentify(
            input_dir, tmp_path / "output", SHIPPED_SPECS_DIR, unlink_source="all"
        )
    assert errors == []
    assert not source.exists()


def test_present_input_is_processed_again(tmp_path):
    input_dir = tmp_path / "input"
    _stage(input_dir, SESSION_NAME)
    calls = []

    def process(session, *args, **kwargs):
        calls.append(session)
        return session, {}

    with patch.object(ImagingSession, "deidentify", process):
        for _ in range(2):
            assert deidentify(input_dir, tmp_path / "output", SHIPPED_SPECS_DIR) == []
    assert len(calls) == 2


def test_resource_copy_is_hidden_until_complete(tmp_path, monkeypatch):
    source = _stage(tmp_path / "input", SESSION_NAME)
    session = ImagingSession.load(source)
    resource = session.scans["1"].resources["RES"]
    previous = {p.name: p.read_bytes() for p in resource.fileset.fspaths}
    replacement = tmp_path / "replacement.dat"
    replacement.write_bytes(b"complete replacement")
    resource.fileset = File(replacement)
    original_copy = FileSet.copy

    def copy_with_reader(fileset, destination, *args, **kwargs):
        destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        partial = destination / "partial.dat"
        partial.write_bytes(b"partial")
        # A consumer can run while the copy is in progress. It must see the old
        # complete resource, without any of the replacement's partial bytes.
        loaded = ImagingSession.load(source)
        visible = loaded.scans["1"].resources["RES"].fileset
        assert {p.name: p.read_bytes() for p in visible.fspaths} == previous
        assert not destination.is_relative_to(source)
        partial.unlink()
        return original_copy(fileset, destination, *args, **kwargs)

    monkeypatch.setattr(FileSet, "copy", copy_with_reader)
    session.save(source.parent, copy_mode=FileSet.CopyMode.copy)
    loaded = ImagingSession.load(source)
    assert (
        next(iter(loaded.scans["1"].resources["RES"].fileset.fspaths)).read_bytes()
        == b"complete replacement"
    )


@pytest.mark.parametrize("mode", ["all", "keep-metadata"])
@pytest.mark.parametrize("failure", ["method", "save", "reid"])
def test_failure_keeps_recorded_input(tmp_path, mode, failure):
    input_dir = tmp_path / "input"
    source = _stage(input_dir, SESSION_NAME)
    recorded = {
        p.relative_to(source): p.read_bytes() for p in source.rglob("*") if p.is_file()
    }
    reid_dir = tmp_path / "reid"

    def process(session, *args, **kwargs):
        if failure == "method":
            raise RuntimeError("method failed")
        if failure == "reid":
            # A directory at the requested file path makes the final write fail.
            (reid_dir / f"{SESSION_NAME}.json").mkdir()
        if failure == "save":
            session.save = lambda *a, **kw: (_ for _ in ()).throw(
                RuntimeError("save failed")
            )
        return session, {}

    with patch.object(ImagingSession, "deidentify", process):
        errors = deidentify(
            input_dir,
            tmp_path / "output",
            SHIPPED_SPECS_DIR,
            reid_dir=reid_dir,
            unlink_source=mode,
        )
    assert len(errors) == 1
    assert {
        p.relative_to(source): p.read_bytes() for p in source.rglob("*") if p.is_file()
    } == recorded


@pytest.mark.parametrize("mode", ["all", "keep-metadata"])
def test_arrival_during_cleanup_survives(tmp_path, monkeypatch, mode):
    source = _stage(tmp_path / "input", SESSION_NAME)
    resource = source / "1.T" / "RES"
    late = resource / "late.dat"
    unlink = Path.unlink

    def unlink_with_arrival(path, *args, **kwargs):
        if path.parent == resource and not late.exists():
            late.write_bytes(b"next cycle")
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", unlink_with_arrival)
    with ImagingSession.load_for_processing(source, unlink_source=mode):
        pass
    assert list(resource.iterdir()) == [late]


@pytest.mark.parametrize("mode", ["all", "keep-metadata"])
def test_snapshot_precedes_load_and_covers_session_resources(
    tmp_path, monkeypatch, mode
):
    source = _stage(tmp_path / "input", SESSION_NAME)
    session = ImagingSession.load(source)
    session.session_resources["SESSION"] = ImagingResource(
        "SESSION", File.sample(seed=77)
    )
    session.save(source.parent)
    original_load = ImagingSession.load
    late = source / "SESSION" / "late.dat"

    def load(*args, **kwargs):
        loaded = original_load(*args, **kwargs)
        late.write_bytes(b"arrived during load")
        new_resource = source / "2.new" / "NEW"
        new_resource.mkdir(parents=True)
        (new_resource / "new.dat").write_bytes(b"new resource")
        return loaded

    monkeypatch.setattr(ImagingSession, "load", load)
    with ImagingSession.load_for_processing(source, unlink_source=mode):
        pass
    assert list(late.parent.iterdir()) == [late]
    assert (source / "2.new" / "NEW" / "new.dat").read_bytes() == b"new resource"
    assert not (source / "1.T" / "RES").exists()
    assert (source / "1.T" / Metadata.FNAME).exists() == (mode == "keep-metadata")


def test_changed_non_dicom_bytes_are_saved(tmp_path):
    input_dir = tmp_path / "input"
    source = _stage(input_dir, SESSION_NAME)
    original = next(
        iter(ImagingSession.load(source).resources[0].fileset.fspaths)
    ).read_bytes()

    def process(session, destination, **kwargs):
        changed = destination / "changed.dat"
        changed.write_bytes(b"deidentified non-DICOM content")
        result = session.new_empty()
        result.add_resource("1", "T", "RES", File(changed))
        return result, {}

    with patch.object(ImagingSession, "deidentify", process):
        errors = deidentify(
            input_dir, tmp_path / "output", SHIPPED_SPECS_DIR, unlink_source="all"
        )
    assert errors == []
    assert not source.exists()
    saved = ImagingSession.load(tmp_path / "output" / SESSION_NAME)
    content = next(iter(saved.resources[0].fileset.fspaths)).read_bytes()
    assert content == b"deidentified non-DICOM content"
    assert content != original


def test_failed_resource_copy_keeps_previous_resource(tmp_path, monkeypatch):
    source = _stage(tmp_path / "input", SESSION_NAME)
    session = ImagingSession.load(source)
    old = {
        p.relative_to(source): p.read_bytes() for p in source.rglob("*") if p.is_file()
    }
    replacement = tmp_path / "replacement.dat"
    replacement.write_bytes(b"replacement")
    session.resources[0].fileset = File(replacement)

    def fail(*args, **kwargs):
        raise OSError("copy failed")

    monkeypatch.setattr(FileSet, "copy", fail)
    with pytest.raises(OSError, match="copy failed"):
        session.save(source.parent)
    assert {
        p.relative_to(source): p.read_bytes() for p in source.rglob("*") if p.is_file()
    } == old
    assert not list((source.parent / "__build__").iterdir())


def test_failed_resource_publish_restores_previous_resource(tmp_path, monkeypatch):
    source = _stage(tmp_path / "input", SESSION_NAME)
    session = ImagingSession.load(source)
    old = {
        p.relative_to(source): p.read_bytes() for p in source.rglob("*") if p.is_file()
    }
    replacement = tmp_path / "replacement.dat"
    replacement.write_bytes(b"replacement")
    session.resources[0].fileset = File(replacement)
    replace = os.replace

    def fail_publication(src, dest):
        if Path(src).name == "new" and Path(dest).name == "RES":
            raise OSError("publish failed")
        return replace(src, dest)

    monkeypatch.setattr(os, "replace", fail_publication)
    with pytest.raises(OSError, match="publish failed"):
        session.save(source.parent)
    assert {
        p.relative_to(source): p.read_bytes() for p in source.rglob("*") if p.is_file()
    } == old


@pytest.mark.parametrize("mode", ["all", "keep-metadata"])
def test_retirement_unlinks_symlinks_without_removing_targets(tmp_path, mode):
    source = _stage(tmp_path / "input", SESSION_NAME)
    resource = source / "1.T" / "RES"
    target = tmp_path / "external"
    target.mkdir()
    (target / "keep.dat").write_bytes(b"keep")
    (resource / "file-link").symlink_to(target / "keep.dat")
    (resource / "directory-link").symlink_to(target, target_is_directory=True)
    (resource / "broken-link").symlink_to(target / "missing")
    with ImagingSession.load_for_processing(source, unlink_source=mode):
        pass
    assert not resource.exists()
    assert (target / "keep.dat").read_bytes() == b"keep"
