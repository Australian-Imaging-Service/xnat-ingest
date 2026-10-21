"""A copied manifest publication temporary file is not resource data."""

from pathlib import Path

import boto3
import pytest
from fileformats.core import FileSet
from fileformats.generic import Directory
from moto import mock_aws

from xnat_ingest.exceptions import DifferingCheckumsException
from xnat_ingest.helpers.arg_types import StoreCredentials
from xnat_ingest.helpers.remotes import iterate_s3_sessions
from xnat_ingest.model.resource import ImagingResource
from xnat_ingest.model.session import ImagingSession


def _saved(tmp_path: Path, directory: bool = False) -> Path:
    source = tmp_path / "input"
    source.mkdir()
    data = source / "payload.txt"
    data.write_text("complete data")
    fileset = Directory(source) if directory else FileSet(data)
    scan = tmp_path / "edge" / "PROJECT.SUBJECT.SESSION" / "1.test"
    scan.mkdir(parents=True)
    ImagingResource(name="DATA", fileset=fileset).save(scan)
    resource = scan / "DATA"
    (resource / ImagingResource.MANIFEST_TMP_FNAME).write_text("{unfinished")
    return resource


@pytest.mark.parametrize("directory", [False, True])
def test_resource_load_ignores_only_publication_temp(
    tmp_path: Path, directory: bool
) -> None:
    resource_dir = _saved(tmp_path, directory)
    temporary = resource_dir / ImagingResource.MANIFEST_TMP_FNAME

    loaded = ImagingResource.load(resource_dir)

    assert len(loaded.checksums) == 1
    assert loaded.calculate_checksums() == loaded.checksums
    assert temporary.read_text() == "{unfinished", "a reader must not delete it"


def test_temporary_file_cannot_replace_required_manifest(tmp_path: Path) -> None:
    resource = _saved(tmp_path)
    (resource / ImagingResource.MANIFEST_FNAME).unlink()
    with pytest.raises(FileNotFoundError, match="Manifest file not found"):
        ImagingResource.load(resource)


def test_manifest_optional_load_does_not_upload_temporary_file(tmp_path: Path) -> None:
    resource = _saved(tmp_path)
    (resource / ImagingResource.MANIFEST_FNAME).unlink()

    loaded = ImagingResource.load(resource, require_manifest=False)

    assert loaded.checksums is None
    assert set(loaded.calculate_checksums()) == {"payload.txt"}


@pytest.mark.parametrize(
    "name",
    [
        "unlisted.txt",
        "other.tmp",
        "__MANIFEST__.json.tmp.bak",
        "nested/__MANIFEST__.json.tmp",
    ],
)
def test_other_unlisted_files_are_still_refused(tmp_path: Path, name: str) -> None:
    resource = _saved(tmp_path)
    extra = resource / name
    extra.parent.mkdir(exist_ok=True)
    extra.write_text("not listed in the manifest")

    with pytest.raises(DifferingCheckumsException):
        ImagingResource.load(resource)


def test_temporary_file_does_not_hide_changed_resource_data(tmp_path: Path) -> None:
    resource = _saved(tmp_path)
    (resource / "payload.txt").write_text("different data")
    with pytest.raises(DifferingCheckumsException):
        ImagingResource.load(resource)


@pytest.mark.parametrize("extra_file", [False, True])
def test_management_load_of_s3_copy_with_publication_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra_file: bool
) -> None:
    """Exercise the actual S3 listing/download and session loader used by upload."""
    resource = _saved(tmp_path)
    session = resource.parent.parent
    if extra_file:
        (resource / "unlisted.txt").write_text("must still refuse")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket="manifest-copy-test")
        for path in session.rglob("*"):
            if path.is_file():
                client.put_object(
                    Bucket="manifest-copy-test",
                    Key=f"staged/{session.name}/{path.relative_to(session)}",
                    Body=path.read_bytes(),
                )
        listings = iterate_s3_sessions(
            "s3://manifest-copy-test/staged",
            StoreCredentials(access_key="test", access_secret="test"),
            tmp_path / "management-cache",
            wait_period=0,
        )
        try:
            assert next(listings) == 1
            listing = next(listings)
            copied = listing.cache_path
            temporary = copied / "1.test" / "DATA" / ImagingResource.MANIFEST_TMP_FNAME
            assert temporary.read_text() == "{unfinished"
            if extra_file:
                with pytest.raises(DifferingCheckumsException):
                    ImagingSession.load(copied)
            else:
                loaded = ImagingSession.load(copied)
                data = loaded.scans["1"].resources["DATA"]
                assert set(data.checksums) == {"payload.txt"}
                assert data.calculate_checksums() == data.checksums
            assert temporary.exists()
        finally:
            listings.close()
