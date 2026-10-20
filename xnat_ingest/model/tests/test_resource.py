import logging
import os
from pathlib import Path

import pytest
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


def _resource(tmp_path: Path) -> ImagingResource:
    src_dir = tmp_path / "in"
    src_dir.mkdir()
    fpath = src_dir / "a-file.txt"
    fpath.write_text("content")
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
    """The manifest is written beside its final name and moved into place. A
    process that dies before the move leaves no manifest, never a partial one,
    and the next save repairs the directory"""
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
    assert not (resource_dir / ImagingResource.MANIFEST_FNAME).exists()
    assert (resource_dir / "a-file.txt").read_text() == "content"
    repaired = resource.save(dest_dir, overwrite=True)
    assert ImagingResource.load(resource_dir).checksums == repaired.checksums
    assert not (resource_dir / (ImagingResource.MANIFEST_FNAME + ".tmp")).exists()
