import logging
import os
import time
from pathlib import Path

import pytest
from fileformats.core import FileSet
from fileformats.core.exceptions import FormatMismatchError
from fileformats.generic import File

from xnat_ingest.model.resource import ImagingResource


def test_load_legacy_manifest_fname(tmp_path: Path) -> None:
    """ImagingResource.load should fall back to the legacy manifest filename
    (OLD_MANIFEST_FNAME) for resources saved before MANIFEST_FNAME was changed"""
    src_dir = tmp_path / "in"
    src_dir.mkdir()
    fpath = src_dir / "a-file.txt"
    fpath.write_text("content")
    resource = ImagingResource(name="my-resource", fileset=File(fpath))

    dest_dir = tmp_path / "out"
    dest_dir.mkdir()
    saved = resource.save(dest_dir)

    resource_dir = dest_dir / "my-resource"
    # Simulate a resource saved by an older version of xnat-ingest that wrote the
    # manifest under the legacy filename
    (resource_dir / ImagingResource.MANIFEST_FNAME).rename(
        resource_dir / ImagingResource.OLD_MANIFEST_FNAME
    )

    loaded = ImagingResource.load(resource_dir)
    assert loaded.checksums == saved.checksums
    assert loaded.datatype == saved.datatype


def _resource(tmp_path: Path, age: float = 0.0) -> ImagingResource:
    src_dir = tmp_path / "in"
    src_dir.mkdir()
    fpath = src_dir / "a-file.txt"
    fpath.write_text("content")
    if age:
        # An input that did not change since an earlier pass (assign, or a
        # package pass-through) is older than all files that pass wrote
        mtime = time.time() - age
        os.utime(fpath, (mtime, mtime))
    return ImagingResource(name="my-resource", fileset=File(fpath))


def test_save_repairs_an_unreadable_manifest(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A producer killed while it wrote the manifest leaves a file that is not
    JSON. The resource cannot be loaded, and a later complete save of the same
    resource replaces the directory instead of failing the same way"""
    resource = _resource(tmp_path)
    dest_dir = tmp_path / "out"
    dest_dir.mkdir()
    saved = resource.save(dest_dir)
    resource_dir = dest_dir / "my-resource"
    manifest = resource_dir / ImagingResource.MANIFEST_FNAME
    manifest.write_text("{")
    with pytest.raises(FormatMismatchError):
        ImagingResource.load(resource_dir)

    with caplog.at_level(logging.WARNING):
        repaired = resource.save(dest_dir)

    assert repaired.checksums == saved.checksums
    assert ImagingResource.load(resource_dir).checksums == saved.checksums
    assert (resource_dir / "a-file.txt").read_text() == "content"
    assert ImagingResource.MANIFEST_FNAME in caplog.text


def test_save_publishes_the_manifest_in_one_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The manifest is written beside its final name and moved into place, and
    the whole resource is published by one rename. A process that dies before
    the move leaves no manifest, never a partial one, and nothing at the visible
    path; the next save publishes the directory"""
    resource = _resource(tmp_path)
    dest_dir = tmp_path / "out"
    dest_dir.mkdir()

    def die_before_the_move(src: str, dst: str) -> None:
        raise OSError("killed before the manifest was published")

    monkeypatch.setattr(os, "replace", die_before_the_move)
    with pytest.raises(OSError):
        resource.save(dest_dir)
    monkeypatch.undo()

    resource_dir = dest_dir / "my-resource"
    assert not resource_dir.exists()
    repaired = resource.save(dest_dir, overwrite=True)
    assert ImagingResource.load(resource_dir).checksums == repaired.checksums
    assert (resource_dir / "a-file.txt").read_text() == "content"
    assert not (resource_dir / (ImagingResource.MANIFEST_FNAME + ".tmp")).exists()


@pytest.mark.parametrize(
    "copy_mode",
    [FileSet.CopyMode.copy, FileSet.CopyMode.hardlink],
    ids=["copy", "hardlink"],
)
@pytest.mark.parametrize("died", ["before-the-move", "before-the-manifest"])
def test_save_replaces_a_resource_without_a_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    copy_mode: FileSet.CopyMode,
    died: str,
) -> None:
    """A producer that dies after it wrote the data files and before it published
    the manifest leaves nothing at the visible path: a resource is built in a
    temporary directory and published by one rename, so a reader never sees a
    directory without its manifest. The next save of the same input succeeds and
    leaves no temporary manifest behind"""
    resource = _resource(tmp_path, age=3600.0)
    dest_dir = tmp_path / "out"
    dest_dir.mkdir()
    resource_dir = dest_dir / "my-resource"
    tmp_manifest = resource_dir / (ImagingResource.MANIFEST_FNAME + ".tmp")

    def die(*args: object) -> None:
        raise OSError(f"killed {died}")

    if died == "before-the-move":
        monkeypatch.setattr(os, "replace", die)
    else:
        monkeypatch.setattr(ImagingResource, "write_manifest", die)
    with pytest.raises(OSError):
        resource.save(dest_dir, copy_mode=copy_mode)
    monkeypatch.undo()
    assert not resource_dir.exists()
    assert [p for p in dest_dir.iterdir() if not p.name.startswith(".")] == []

    repaired = resource.save(dest_dir, copy_mode=copy_mode)

    assert repaired.checksums == resource.checksums
    assert ImagingResource.load(resource_dir).checksums == resource.checksums
    assert (resource_dir / "a-file.txt").read_text() == "content"
    assert not tmp_manifest.exists()


def test_save_replaces_an_empty_resource_directory(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A producer that dies after it made the resource directory and before it
    wrote the first file leaves an empty directory. The next save replaces it. It
    does not fail on every pass because there are no files to load"""
    resource = _resource(tmp_path, age=3600.0)
    dest_dir = tmp_path / "out"
    resource_dir = dest_dir / "my-resource"
    resource_dir.mkdir(parents=True)

    with caplog.at_level(logging.WARNING):
        repaired = resource.save(dest_dir)

    assert repaired.checksums == resource.checksums
    assert ImagingResource.load(resource_dir).checksums == resource.checksums
    assert "has no manifest" in caplog.text
