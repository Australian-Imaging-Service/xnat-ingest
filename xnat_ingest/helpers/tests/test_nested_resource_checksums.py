"""Real XNAT file listings use basenames in Name and retain paths in URI."""

from types import SimpleNamespace
from unittest import mock

import pytest
from fileformats.core import FileSet

from xnat_ingest.helpers.remotes import (
    S3SessionListing,
    compare_resource_with_xnat,
    get_xnat_checksums,
    get_xnat_resource,
)
from xnat_ingest.model.resource import ImagingResource

RESOURCE_URI = "/data/experiments/E1/resources/DATA"
CANONICAL_URI = "/data/experiments/E1/resources/97/files/"


def _row(path, digest="same"):
    return {
        "Name": path.rsplit("/", 1)[-1],
        "URI": CANONICAL_URI + path,
        "digest": digest,
    }


def _resource(rows):
    response = mock.Mock(status_code=200)
    response.json.return_value = {"ResultSet": {"Result": rows}}
    connection = mock.Mock()
    connection.get.return_value = response
    return SimpleNamespace(
        uri=RESOURCE_URI, id="97", label="DATA", xnat_session=connection
    )


def _local(tmp_path, names):
    files = []
    for index, name in enumerate(names):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"synthetic file {index}".encode())
        files.append(path)
    return ImagingResource("DATA", FileSet(files))


@pytest.mark.parametrize("relative_name", [False, True])
def test_nested_listing_matches_local_resource_checksums(tmp_path, relative_name):
    local = _local(tmp_path, ["flat.bin", "nested/scan with spaces.bin"])
    rows = [_row(path, digest) for path, digest in local.checksums.items()]
    if relative_name:
        for row, path in zip(rows, local.checksums):
            row["Name"] = path
    remote = _resource(rows)

    assert get_xnat_checksums(remote) == local.calculate_checksums()
    remote.xnat_session.get.assert_called_once_with(RESOURCE_URI + "/files")


@pytest.mark.parametrize("digest", ["same", ""])
def test_repeated_basenames_do_not_hide_files(digest):
    paths = ["a.bin", "first/a.bin", "second/a.bin"]
    remote = _resource([_row(path, digest) for path in paths])
    actual = get_xnat_checksums(remote)

    assert actual == {path: digest for path in paths}
    comparison = compare_resource_with_xnat({"a.bin": "same"}, actual)
    assert not comparison.complete
    assert comparison.extra == {"first/a.bin", "second/a.bin"}


@pytest.mark.parametrize("digest", ["same", ""])
def test_nested_file_cannot_stand_in_for_missing_root_file(digest):
    remote = _resource([_row("nested/a.bin", digest)])
    comparison = compare_resource_with_xnat(
        {"a.bin": "same"}, get_xnat_checksums(remote)
    )

    assert not comparison.complete
    assert comparison.missing == {"a.bin"}
    assert comparison.extra == {"nested/a.bin"}
    assert not comparison.repairable


@pytest.mark.parametrize(
    "path",
    [
        "dir with spaces/file with spaces.bin",
        "literal%20dir/literal%2F%25.bin",
        "dir#one/file?two+#three.bin",
        "nested/files/a.bin",
        "nested/resources/other/files/a.bin",
    ],
)
def test_listing_paths_are_preserved_verbatim(path):
    assert get_xnat_checksums(_resource([_row(path)])) == {path: "same"}


@pytest.mark.parametrize("uri", [None, "/not-a-resource/a.bin", CANONICAL_URI])
def test_missing_or_malformed_file_uri_is_not_treated_as_a_basename(uri):
    row = _row("a.bin")
    row["URI"] = uri
    with pytest.raises(ValueError, match="resource-relative path"):
        get_xnat_checksums(_resource([row]))


def test_encoded_uri_is_not_guessed_when_it_disagrees_with_name():
    row = _row("scan with spaces.bin")
    row["URI"] = CANONICAL_URI + "scan%20with%20spaces.bin"
    with pytest.raises(ValueError, match="does not match its Name"):
        get_xnat_checksums(_resource([row]))


def test_duplicate_full_path_does_not_silently_discard_a_digest():
    with pytest.raises(ValueError, match="repeats resource-relative path"):
        get_xnat_checksums(
            _resource([_row("nested/a.bin", "first"), _row("nested/a.bin", "second")])
        )


@pytest.mark.parametrize("missing", [False, True])
def test_nested_session_resource_completeness_and_repair(tmp_path, missing):
    local = _local(tmp_path, ["flat.bin", "nested/a.bin", "nested/b.bin"])
    rows = [
        _row(path, digest)
        for path, digest in local.checksums.items()
        if not (missing and path == "nested/b.bin")
    ]
    remote = _resource(rows)
    xsession = SimpleNamespace(
        scans={}, resources={"DATA": remote}, xnat_session=remote.xnat_session
    )
    connection = SimpleNamespace(
        projects={"proj": SimpleNamespace(experiments={"sess": xsession})}
    )

    class Listing(S3SessionListing):
        @property
        def resource_paths(self):
            return {"DATA"}

        @property
        def resource_manifests(self):
            return {"DATA": {"checksums": local.checksums}}

    listing = Listing(
        name="proj.subj.sess", objects=[], bucket=None, cache_path=tmp_path
    )
    assert listing.all_uploaded(connection) is not missing
    expected = (remote, {"nested/b.bin"}) if missing else (None, None)
    assert get_xnat_resource(local, xsession) == expected
    remote.xnat_session.put.assert_not_called()
    remote.xnat_session.post.assert_not_called()
