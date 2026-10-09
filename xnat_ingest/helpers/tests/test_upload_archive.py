"""File upload methods keep xnatpy's protocol without its in-memory spool."""

import os
import tarfile
import tracemalloc
import typing as ty
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from xnat.exceptions import XNATValueError
from xnat.mixin import AbstractResource
from xnat.session import BaseXNATSession

from xnat_ingest.api.upload_api import _staged_upload_batch, upload
from xnat_ingest.helpers.remotes import SessionOnlyListing
from xnat_ingest.helpers.upload import upload_resource_directory


class ArchiveCapture:
    def upload_data(
        self, stream: ty.BinaryIO, remote_name: str, **kwargs: ty.Any
    ) -> None:
        self.stream = stream
        self.remote_name = remote_name
        self.options = kwargs
        assert stream.tell() == 0
        with tarfile.open(fileobj=stream, mode="r:*") as archive:
            self.members = []
            for member in archive:
                contents = archive.extractfile(member) if member.isfile() else None
                self.members.append(
                    (
                        member.name,
                        member.type,
                        member.size,
                        member.mode,
                        member.mtime,
                        member.linkname,
                        contents.read() if contents else None,
                    )
                )
        assert kwargs["upload_size"] == stream.seek(0, os.SEEK_END)


@pytest.mark.parametrize("method", ["tar_file", "tgz_file", None, ""])
@pytest.mark.parametrize("overwrite", [False, True])
def test_archive_matches_xnatpy(
    tmp_path: Path, method: str | None, overwrite: bool
) -> None:
    source = tmp_path / "resource"
    source.mkdir()
    (source / "empty").mkdir()
    (source / "nested").mkdir()
    data = source / "nested" / "scan with spaces.dat"
    data.write_bytes(b"synthetic scan\x00\xff")
    (source / "zero-length.dat").touch()
    (source / "hardlink.dat").hardlink_to(data)
    (source / "symlink.dat").symlink_to("nested/scan with spaces.dat")
    options = {"file_content": "RAW", "file_format": "DICOM", "timeout": 30}
    expected = ArchiveCapture()
    actual = ArchiveCapture()

    AbstractResource.upload_dir(
        expected, source, method=method, overwrite=overwrite, **options
    )
    upload_resource_directory(
        actual, str(source), method=method, overwrite=overwrite, **options
    )

    assert actual.members == expected.members
    assert actual.remote_name == expected.remote_name
    assert actual.options == expected.options
    assert actual.stream.closed


@pytest.mark.parametrize("method", ["per_file", "tar_memory", "tgz_memory", "invalid"])
def test_other_methods_are_delegated(tmp_path: Path, method: str) -> None:
    resource = MagicMock()

    upload_resource_directory(
        resource, tmp_path, method=method, overwrite=True, timeout=30
    )

    resource.upload_dir.assert_called_once_with(
        tmp_path, method=method, overwrite=True, timeout=30
    )
    resource.upload_data.assert_not_called()


@pytest.mark.parametrize("method", ["tar_file", "tgz_file"])
def test_invalid_directory_keeps_xnatpy_error(tmp_path: Path, method: str) -> None:
    source = tmp_path / "missing"
    with pytest.raises(XNATValueError) as expected:
        AbstractResource.upload_dir(MagicMock(), source, method=method)
    with pytest.raises(XNATValueError) as actual:
        upload_resource_directory(MagicMock(), source, method=method)
    assert str(actual.value) == str(expected.value)


def test_archive_is_closed_when_upload_fails(tmp_path: Path) -> None:
    resource = MagicMock()
    resource.upload_data.side_effect = ConnectionError("synthetic upload failure")

    with pytest.raises(ConnectionError, match="synthetic upload failure"):
        upload_resource_directory(resource, tmp_path)

    assert resource.upload_data.call_args.args[0].closed


def test_batch_archives_keep_only_the_selected_files(tmp_path: Path) -> None:
    source = tmp_path / "resource"
    (source / "nested").mkdir(parents=True)
    files = [source / "one.dat", source / "nested" / "two.dat"]
    for path in files:
        path.write_bytes(path.name.encode())
    (source / "already-uploaded.dat").write_bytes(b"leave on XNAT")

    for path in files:
        resource = ArchiveCapture()
        with _staged_upload_batch(source, [path]) as batch:
            upload_resource_directory(resource, batch)
        actual = {m[0]: m[-1] for m in resource.members if m[1] == tarfile.REGTYPE}
        assert actual == {str(path.relative_to(source)): path.name.encode()}
        assert not batch.exists()

    assert all(path.exists() for path in files)
    assert (source / "already-uploaded.dat").exists()


class StreamingSession:
    """Run xnatpy's real streaming path, replacing only the HTTP transport."""

    upload_stream = BaseXNATSession.upload_stream

    def __init__(self) -> None:
        self.logger = MagicMock()
        self.interface = MagicMock()
        self.interface.put.side_effect = self._receive
        self.received = 0

    def _check_connection(self) -> None:
        pass

    def _check_response(self, response: ty.Any) -> None:
        pass

    def _format_uri(self, uri: str, **kwargs: ty.Any) -> str:
        return uri

    def _receive(self, uri: str, data: ty.BinaryIO, **kwargs: ty.Any) -> MagicMock:
        assert data.tell() == 0
        while chunk := data.read(128 * 1024):
            self.received += len(chunk)
        return MagicMock(status_code=200)


class StreamingResource:
    upload_data = AbstractResource.upload_data
    uri = "/data/experiments/TEST/resources/1"

    def __init__(self) -> None:
        self.xnat_session = StreamingSession()
        self.files = MagicMock()


@pytest.mark.parametrize("method", ["tar_file", "tgz_file"])
def test_large_archive_has_bounded_python_memory(tmp_path: Path, method: str) -> None:
    # Random data prevents compression from hiding an archive-sized buffer.
    with (tmp_path / "scan.dat").open("wb") as data:
        for _ in range(50):
            data.write(os.urandom(1024 * 1024))
    resource = StreamingResource()

    tracemalloc.start()
    try:
        upload_resource_directory(
            resource, tmp_path, method=method, update_func=lambda *args: None
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert resource.xnat_session.received > 49 * 1024 * 1024
    assert peak < 8 * 1024 * 1024
    resource.files.clearcache.assert_called_once()


def test_session_only_upload_uses_archive_helper(tmp_path: Path) -> None:
    source = tmp_path / "SESSION" / "RAW"
    source.mkdir(parents=True)
    (source / "scan.dat").write_bytes(b"synthetic scan")
    resource = ArchiveCapture()
    repo = MagicMock()
    repo.connection.create_object.return_value = resource
    xsession = MagicMock(uri="/data/experiments/TEST")
    loaded_resource = MagicMock()
    loaded_resource.name = "RAW"

    with (
        patch.object(SessionOnlyListing, "all_uploaded", return_value=False),
        patch.object(SessionOnlyListing, "find_xnat_session", return_value=xsession),
        patch(
            "xnat_ingest.api.upload_api.ImagingResource.load",
            return_value=loaded_resource,
        ),
    ):
        assert upload(str(tmp_path), repo, raise_errors=True) == []

    assert resource.remote_name == "upload.tar.gz"
    assert any(
        m[0] == "scan.dat" and m[-1] == b"synthetic scan" for m in resource.members
    )
    assert resource.stream.closed
