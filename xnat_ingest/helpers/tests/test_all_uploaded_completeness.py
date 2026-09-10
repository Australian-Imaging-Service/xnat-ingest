"""all_uploaded() must not call a short resource "uploaded".

It compared resource LABELS, so a resource holding 5 of 8 files satisfied it and
the whole session was skipped before any per-resource check could run. Nothing
downstream saw it: no error, no retry, and the session reported as uploaded.

Measured on a live XNAT: 3 of 8 files deleted from a resource, after which every
pass logged "Skipping upload of '<session>' as all the resources already exist
on XNAT". This is the gate that made both the incomplete-resource exception and
the operator alert unreachable in that case.
"""

import typing as ty
from unittest import mock

from xnat_ingest.helpers.remotes import S3SessionListing

RESOURCE_PATH = "1.scan_one/DICOM"
LOCAL = {"a.dcm": "d1", "b.dcm": "d2", "c.dcm": "d3"}


class FakeXnatResource:
    label = "DICOM"


class FakeScan:
    id = "1"
    type = "scan_one"

    def __init__(self) -> None:
        self.resources = {"DICOM": FakeXnatResource()}


class FakeExperiment:
    def __init__(self) -> None:
        self.scans = {"1": FakeScan()}
        self.resources: dict[str, ty.Any] = {}


class FakeConnection:
    def __init__(self) -> None:
        self.projects = {"proj": mock.MagicMock(experiments={"sess": FakeExperiment()})}


def _listing(tmp_path: ty.Any, manifest: dict[str, str]) -> S3SessionListing:
    listing = S3SessionListing(
        name="proj.subj.sess", objects=[], bucket=None, cache_path=tmp_path
    )
    # resource_paths and resource_manifests are what all_uploaded consults
    type(listing).resource_paths = property(lambda self: {RESOURCE_PATH})  # type: ignore[assignment]
    type(listing).resource_manifests = property(  # type: ignore[assignment]
        lambda self: {RESOURCE_PATH: {"checksums": manifest}}
    )
    return listing


def test_complete_resource_is_uploaded(tmp_path: ty.Any) -> None:
    listing = _listing(tmp_path, LOCAL)
    with mock.patch(
        "xnat_ingest.helpers.remotes.get_xnat_checksums", return_value=dict(LOCAL)
    ):
        assert listing.all_uploaded(FakeConnection()) is True


def test_short_resource_is_not_uploaded(tmp_path: ty.Any) -> None:
    """THE REGRESSION: the label is present but two files are not."""
    listing = _listing(tmp_path, LOCAL)
    with mock.patch(
        "xnat_ingest.helpers.remotes.get_xnat_checksums",
        return_value={"a.dcm": "d1"},
    ):
        assert listing.all_uploaded(FakeConnection()) is False, (
            "a resource holding 1 of 3 files must not count as uploaded, "
            "otherwise the session is skipped before anything can notice"
        )


def test_empty_digests_do_not_make_a_complete_resource_look_short(
    tmp_path: ty.Any,
) -> None:
    """XNAT reports digest '' until a catalog refresh; names are still valid."""
    listing = _listing(tmp_path, LOCAL)
    with mock.patch(
        "xnat_ingest.helpers.remotes.get_xnat_checksums",
        return_value={k: "" for k in LOCAL},
    ):
        assert listing.all_uploaded(FakeConnection()) is True


def test_no_manifest_falls_back_to_previous_behaviour(tmp_path: ty.Any) -> None:
    """Sessions staged without manifests must not become newly unskippable."""
    listing = _listing(tmp_path, {})
    with mock.patch("xnat_ingest.helpers.remotes.get_xnat_checksums", return_value={}):
        assert listing.all_uploaded(FakeConnection()) is True
