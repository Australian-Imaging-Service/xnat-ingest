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
    session = ImagingSession(
        uid="paths",
        scans=[
            ImagingScan(
                "1", "scan", resources={"RAW": ImagingResource("RAW", File(seed))}
            )
        ],
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


def test_file_at_new_path_survives_cleanup(source, tmp_path):
    grouped, root, payload = source
    late = payload.with_name("late.txt")
    save = ImagingSession.save

    def save_then_add(self, *args, **kwargs):
        result = save(self, *args, **kwargs)
        late.write_text("late")
        return result

    with patch.object(ImagingSession, "save", save_then_add):
        assert run(grouped, tmp_path / "assigned", "all") == []
    assert late.read_text() == "late"
    assert not payload.exists()
    assert next((tmp_path / "assigned").rglob("original.txt")).read_text() == "original"


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


def test_failed_save_preserves_input(source, tmp_path):
    grouped, root, payload = source
    with patch.object(ImagingSession, "save", side_effect=RuntimeError("save failed")):
        with pytest.raises(RuntimeError, match="save failed"):
            run(grouped, tmp_path / "assigned", "all")
    assert payload.read_text() == "original"
    ImagingSession.load(root)
