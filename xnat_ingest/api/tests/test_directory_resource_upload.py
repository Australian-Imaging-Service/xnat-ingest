"""Upload the files in a Directory with the paths from its manifest."""

import errno
import hashlib
import math
import tarfile
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from fileformats.application import Zip
from fileformats.core import FileSet
from fileformats.generic import Directory, File

from xnat_ingest.api import upload_api
from xnat_ingest.helpers.remotes import calculate_checksums
from xnat_ingest.model.resource import ImagingResource
from xnat_ingest.model.session import ImagingSession


def _directory(tmp_path):
    data = tmp_path / "data"
    for name in ("flat.bin", "nested/leaf.bin", ".hidden.bin", ".hidden/leaf.bin"):
        path = data / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(name.encode())
    (data / "empty").mkdir()
    return Directory(data)


class _RemoteResource:
    def __init__(self, held=None):
        self.held = dict(held or {})
        self.batches = []
        self.file_uploads = []

    def upload_dir(self, directory, method):
        batch = {}
        for path in sorted(Path(directory).rglob("*")):
            if path.is_dir():
                continue
            assert not path.is_symlink(), "A batch must contain the target bytes"
            batch[str(path.relative_to(directory))] = hashlib.md5(
                path.read_bytes()
            ).hexdigest()
        self.batches.append(batch)
        self.held.update(batch)

    def upload(self, source, name):
        self.file_uploads.append(name)
        self.held[name] = hashlib.md5(Path(source).read_bytes()).hexdigest()

    def upload_data(self, stream, remote_name, **kwargs):
        batch = {}
        with tarfile.open(fileobj=stream, mode="r:*") as archive:
            for member in archive:
                if member.isdir():
                    continue
                assert not member.issym(), "A batch must contain the target bytes"
                with archive.extractfile(member) as contents:
                    batch[member.name] = hashlib.md5(contents.read()).hexdigest()
        self.batches.append(batch)
        self.held.update(batch)


def _upload(tmp_path, fileset, remote, only_files=None, batch_size=1):
    """Run upload with real files and a real session, without a server."""
    staging = tmp_path / "staged"
    (staging / "proj.subj.sess").mkdir(parents=True)
    session = ImagingSession(
        uid="uid", project_id="proj", subject_id="subj", session_id="sess"
    )
    session.add_resource("1", "scan", "DATA", fileset)
    connection = mock.MagicMock()
    connection.projects = {"proj": SimpleNamespace(experiments={})}

    def get_json(uri, query=None):
        if uri == "/data/archive/projects":
            rows = [{"ID": "proj", "name": "proj"}]
        elif uri == "/data/projects/proj/experiments":
            rows = []
        else:
            raise AssertionError(f"Unexpected REST read: {uri}")
        return {"ResultSet": {"Result": rows}}

    connection.get_json.side_effect = get_json
    repo = SimpleNamespace(server="fake://xnat", connection=connection)
    with (
        mock.patch.object(upload_api.ImagingSession, "load", return_value=session),
        mock.patch.object(upload_api.FrameSet, "load", side_effect=KeyError("none")),
        mock.patch.object(
            upload_api, "get_xnat_session", return_value=SimpleNamespace(id="E1")
        ),
        mock.patch.object(
            upload_api, "get_xnat_resource", return_value=(remote, only_files)
        ),
        mock.patch.object(
            upload_api,
            "get_xnat_checksums",
            side_effect=lambda resource: dict(resource.held),
        ),
    ):
        errors = upload_api.upload(
            str(staging),
            repo,
            always_include=["all"],
            check_checksums=True,
            num_files_per_batch=batch_size,
            xnat_max_workers=1,
            raise_errors=True,
        )
    assert errors == []


def test_directory_checksums_match_manifest_walk(tmp_path):
    fileset = _directory(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "excluded.bin").write_bytes(b"outside")
    (fileset.fspath / "dir-link").symlink_to(outside, target_is_directory=True)
    (fileset.fspath / "nested" / "alias.bin").symlink_to("../flat.bin")
    expected = ImagingResource("DATA", fileset).calculate_checksums()

    assert set(expected) == {
        "data/flat.bin",
        "data/nested/leaf.bin",
        "data/nested/alias.bin",
        "data/.hidden.bin",
        "data/.hidden/leaf.bin",
    }
    assert calculate_checksums(fileset, max_workers=2) == expected


def test_broken_link_checksum_keeps_fileformats_contract(tmp_path):
    fileset = _directory(tmp_path)
    (fileset.fspath / "broken.bin").symlink_to("missing.bin")
    expected = ImagingResource("DATA", fileset).calculate_checksums()
    assert expected["data/broken.bin"] == hashlib.md5(b"\0").hexdigest()
    assert calculate_checksums(fileset) == expected


@pytest.mark.parametrize("batch_size", [0, 1, 2])
def test_directory_upload_keeps_nested_paths_and_batches(tmp_path, batch_size):
    fileset = _directory(tmp_path)
    expected = ImagingResource("DATA", fileset).calculate_checksums()
    remote = _RemoteResource()
    _upload(tmp_path, fileset, remote, batch_size=batch_size)

    assert remote.held == expected
    assert len(remote.batches) == math.ceil(
        len(expected) / (batch_size or len(expected))
    )
    assert all(remote.batches)
    assert remote.file_uploads == []


def test_directory_repair_sends_only_missing_leaf(tmp_path):
    fileset = _directory(tmp_path)
    expected = ImagingResource("DATA", fileset).calculate_checksums()
    missing = "data/nested/leaf.bin"
    remote = _RemoteResource(
        {path: digest for path, digest in expected.items() if path != missing}
    )
    _upload(tmp_path, fileset, remote, only_files={missing})

    assert remote.batches == [{missing: expected[missing]}]
    assert remote.held == expected


def test_directory_repair_keeps_name_mismatch_error(tmp_path):
    fileset = _directory(tmp_path)
    remote = _RemoteResource()
    with pytest.raises(RuntimeError) as caught:
        _upload(tmp_path, fileset, remote, only_files={"not-in-manifest.bin"})
    assert "Refusing to repair" in str(caught.value.__cause__)
    assert remote.batches == []


def test_one_file_batch_contains_relative_link_target_bytes(tmp_path):
    fileset = _directory(tmp_path)
    (fileset.fspath / "nested" / "alias.bin").symlink_to("../flat.bin")
    expected = ImagingResource("DATA", fileset).calculate_checksums()
    remote = _RemoteResource()
    _upload(tmp_path, fileset, remote, batch_size=1)
    assert remote.held == expected
    assert {"data/nested/alias.bin": expected["data/flat.bin"]} in remote.batches


def test_directory_links_are_not_followed_in_upload(tmp_path):
    fileset = _directory(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "excluded.bin").write_bytes(b"outside")
    (fileset.fspath / "dir-link").symlink_to(outside, target_is_directory=True)
    remote = _RemoteResource()
    _upload(tmp_path, fileset, remote)
    assert remote.held == ImagingResource("DATA", fileset).calculate_checksums()
    assert not any("dir-link" in path for path in remote.held)


def test_broken_link_upload_fails_before_any_batch(tmp_path):
    fileset = _directory(tmp_path)
    (fileset.fspath / "broken.bin").symlink_to("missing.bin")
    remote = _RemoteResource()
    with pytest.raises(RuntimeError) as caught:
        _upload(tmp_path, fileset, remote)
    assert "not a file" in str(caught.value.__cause__)
    assert "broken.bin" in str(caught.value.__cause__)
    assert remote.batches == []


def test_empty_directory_upload_has_clear_error(tmp_path):
    data = tmp_path / "data"
    (data / "empty").mkdir(parents=True)
    fileset = Directory(data)
    assert ImagingResource("DATA", fileset).calculate_checksums() == {}
    remote = _RemoteResource()
    with pytest.raises(RuntimeError) as caught:
        _upload(tmp_path, fileset, remote)
    assert "no files to upload" in str(caught.value.__cause__)
    assert remote.batches == []


def test_overlapping_roots_upload_each_leaf_once(tmp_path):
    directory = _directory(tmp_path)
    fileset = FileSet([directory.fspath, directory.fspath / "flat.bin"])
    expected = ImagingResource("DATA", fileset).calculate_checksums()
    remote = _RemoteResource()
    _upload(tmp_path, fileset, remote, batch_size=0)
    assert remote.batches == [expected]


@pytest.mark.parametrize("file_type", [File, FileSet, Zip])
def test_file_based_resource_is_unchanged(tmp_path, file_type):
    first = tmp_path / "first.bin"
    first.write_bytes(b"first")
    second = tmp_path / "second.bin"
    second.write_bytes(b"second")
    if file_type is Zip:
        zipped = tmp_path / "files.zip"
        with zipfile.ZipFile(zipped, "w") as archive:
            archive.write(first, first.name)
        fileset = Zip(zipped)
    elif file_type is File:
        fileset = File(first)
    else:
        fileset = FileSet([first, second])
    expected = ImagingResource("DATA", fileset).calculate_checksums()
    assert calculate_checksums(fileset) == expected
    remote = _RemoteResource()
    _upload(tmp_path, fileset, remote)
    assert remote.held == expected
    assert bool(remote.file_uploads) is isinstance(fileset, File)


def test_parent_access_does_not_scale_with_file_count(tmp_path, monkeypatch):
    get_parent = FileSet.parent.fget
    access_counts = []
    for size in (2, 32):
        root = tmp_path / str(size)
        root.mkdir()
        paths = [root / f"{number}.bin" for number in range(size)]
        for path in paths:
            path.write_bytes(path.name.encode())
        fileset = FileSet(paths)
        reads = []

        def parent(instance):
            reads.append(instance)
            return get_parent(instance)

        with monkeypatch.context() as patch:
            patch.setattr(FileSet, "parent", property(parent))
            _upload(root, fileset, _RemoteResource(), batch_size=0)
        access_counts.append(len(reads))

    assert access_counts[0] == access_counts[1]


@pytest.mark.parametrize("cross_device", [False, True])
def test_batch_stages_absolute_file_link_bytes(tmp_path, monkeypatch, cross_device):
    source = tmp_path / "source"
    source.mkdir()
    target = tmp_path / "target.bin"
    target.write_bytes(b"target bytes")
    link = source / "link.bin"
    link.symlink_to(target)
    if cross_device:

        def exdev(*args, **kwargs):
            raise OSError(errno.EXDEV, "simulated cross-device target")

        monkeypatch.setattr(Path, "hardlink_to", exdev)
    with upload_api._staged_upload_batch(source, [link]) as batch:
        staged = batch / "link.bin"
        assert not staged.is_symlink()
        assert staged.read_bytes() == target.read_bytes()
        if not cross_device:
            assert staged.stat().st_ino == target.stat().st_ino


def test_regular_file_hardlink_failure_is_not_hidden(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "file.bin"
    path.write_bytes(b"file")

    def exdev(*args, **kwargs):
        raise OSError(errno.EXDEV, "simulated regular-file error")

    monkeypatch.setattr(Path, "hardlink_to", exdev)
    with pytest.raises(OSError, match="regular-file error"):
        with upload_api._staged_upload_batch(source, [path]):
            pytest.fail("The hardlink must fail")
