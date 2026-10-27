"""Source-path cleanup with real resources and a file added after the save."""

import os
from unittest.mock import patch

import pytest
from fileformats.core import FileSet
from fileformats.generic import File

from xnat_ingest.api.assign_api import assign
from xnat_ingest.helpers.metadata import Metadata
from xnat_ingest.model.resource import ImagingResource
from xnat_ingest.model.scan import ImagingScan
from xnat_ingest.model.session import ImagingSession


@pytest.fixture
def source(tmp_path):
    seed = tmp_path / "original.txt"
    seed.write_text("original")
    session_seed = tmp_path / "session.txt"
    session_seed.write_text("session resource")
    session = ImagingSession(
        uid="paths",
        scans=[
            ImagingScan(
                "1", "scan", resources={"RAW": ImagingResource("RAW", File(seed))}
            )
        ],
        session_resources={"SESSION": ImagingResource("SESSION", File(session_seed))},
    )
    for key, value in (
        ("StudyID", "PROJECT"),
        ("PatientID", "SUBJECT"),
        ("AccessionNumber", "SESSION"),
    ):
        session.metadata[key] = value
    grouped = tmp_path / "grouped"
    _, root = session.save(grouped, copy_mode=FileSet.CopyMode.copy)
    return grouped, root, root / "1.scan/RAW/original.txt"


def run(grouped, output, mode):
    return assign(
        grouped,
        output,
        "StudyID",
        "PatientID",
        "AccessionNumber",
        unlink_source=mode,
        copy_mode=FileSet.CopyMode.copy,
        raise_errors=True,
    )


@pytest.mark.parametrize("mode", ["all", "keep-metadata"])
@pytest.mark.parametrize("resource", ["scan", "session"])
def test_file_at_new_path_survives_cleanup(source, tmp_path, mode, resource):
    grouped, root, payload = source
    if resource == "session":
        payload = root / "SESSION" / "session.txt"
    late = payload.with_name("late.txt")
    save = ImagingSession.save

    def save_then_add(self, *args, **kwargs):
        result = save(self, *args, **kwargs)
        late.write_text("late")
        return result

    with patch.object(ImagingSession, "save", save_then_add):
        assert run(grouped, tmp_path / "assigned", mode) == []
    assert late.read_text() == "late"
    assert not payload.exists()
    assert next((tmp_path / "assigned").rglob("original.txt")).read_text() == "original"
    if mode == "keep-metadata":
        assert (root / Metadata.FNAME).is_file()
        assert (root / "1.scan" / Metadata.FNAME).is_file()


@pytest.mark.parametrize("mode", ["all", "keep-metadata"])
@pytest.mark.parametrize("resource", ["scan", "session"])
def test_new_resource_directory_survives_cleanup(source, tmp_path, mode, resource):
    grouped, root, payload = source
    parent = root / "1.scan" if resource == "scan" else root
    late = parent / "LATE" / "late.txt"
    save = ImagingSession.save

    def save_then_add(self, *args, **kwargs):
        result = save(self, *args, **kwargs)
        late.parent.mkdir()
        late.write_text("late resource")
        return result

    with patch.object(ImagingSession, "save", save_then_add):
        assert run(grouped, tmp_path / "assigned", mode) == []
    assert late.read_text() == "late resource"
    assert not payload.parent.exists()
    assert not (root / "SESSION").exists()
    if mode == "keep-metadata":
        assert (root / Metadata.FNAME).is_file()
        assert (root / "1.scan" / Metadata.FNAME).is_file()


@pytest.mark.parametrize("resource", ["scan", "session"])
@pytest.mark.parametrize("target_exists", [False, True])
def test_resource_symlink_cleanup_preserves_late_paths(
    source, tmp_path, resource, target_exists
):
    grouped, root, payload = source
    resource_dir = payload.parent if resource == "scan" else root / "SESSION"
    outside = tmp_path / "outside"
    if target_exists:
        outside.mkdir()
        (outside / "data").write_text("outside")
    recorded = resource_dir / "recorded-link"
    recorded.symlink_to(outside)
    empty = resource_dir / "empty" / "nested"
    empty.mkdir(parents=True)
    late = resource_dir / "late-link"
    save = ImagingSession.save

    def save_then_add(self, *args, **kwargs):
        result = save(self, *args, **kwargs)
        late.symlink_to(outside)
        return result

    with patch.object(ImagingSession, "save", save_then_add):
        assert run(grouped, tmp_path / "assigned", "keep-metadata") == []
    assert not recorded.is_symlink()
    assert late.is_symlink()
    assert not empty.parent.exists()
    assert not payload.exists()
    if target_exists:
        assert (outside / "data").read_text() == "outside"


@pytest.mark.parametrize("mode", ["all", "keep-metadata"])
@pytest.mark.parametrize(
    "extra_kind",
    [
        None,
        "dangling",
        "directory",
        pytest.param(
            "fifo",
            marks=pytest.mark.skipif(
                not hasattr(os, "mkfifo"), reason="FIFOs unavailable"
            ),
        ),
    ],
)
def test_normal_cleanup_retains_requested_metadata(source, tmp_path, mode, extra_kind):
    grouped, root, payload = source
    sentinels = [root / "unloaded.txt", root / "1.scan" / "unloaded.txt"]
    for path in sentinels:
        path.write_text("unloaded")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "data").write_text("outside")
    extras = [root / ".extra", root / "1.scan" / ".extra"] if extra_kind else []
    for path in extras:
        if extra_kind == "fifo":
            os.mkfifo(path)
        else:
            path.symlink_to(
                outside if extra_kind == "directory" else tmp_path / "absent"
            )
    assert run(grouped, tmp_path / "assigned", mode) == []
    assert not payload.exists()
    assert not (root / "SESSION").exists()
    if mode == "all":
        assert not root.exists()
    else:
        assert (root / Metadata.FNAME).exists()
        assert (root / "1.scan" / Metadata.FNAME).exists()
        assert not payload.parent.exists()
        assert all(path.read_text() == "unloaded" for path in sentinels)
        assert all(path.exists() or path.is_symlink() for path in extras)
    assert (outside / "data").read_text() == "outside"
    assert next((tmp_path / "assigned").rglob("original.txt")).read_text() == "original"


@pytest.mark.parametrize("mode", ["all", "keep-metadata"])
def test_failed_save_preserves_input(source, tmp_path, mode):
    grouped, root, payload = source
    with patch.object(ImagingSession, "save", side_effect=RuntimeError("save failed")):
        with pytest.raises(RuntimeError, match="save failed"):
            run(grouped, tmp_path / "assigned", mode)
    assert payload.read_text() == "original"
    ImagingSession.load(root)
