from pathlib import Path

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
