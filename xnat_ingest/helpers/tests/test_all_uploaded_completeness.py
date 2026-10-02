"""all_uploaded() must not call a short resource "uploaded".

It compared resource LABELS, and a resource holding 5 of 8 files carries the
same label as one holding all 8, so the whole session was skipped before any
per-resource check could run: no error, no retry, reported as uploaded.
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


def test_unreadable_manifests_do_not_vote_the_session_complete(
    tmp_path: ty.Any,
) -> None:
    """UNKNOWN IS NOT COMPLETE.

    This used to return True, and the caller logs True as "Skipping upload ...
    as all the resources already exist on XNAT" and moves on, so a resource
    holding a fraction of its files was passed over with a success-shaped line.

    On the S3 path the manifest read is one download and one JSON parse per
    resource, so a read timeout, a 5xx, a truncated body, a manifest still being
    written, or a single malformed file discards all of them for that session.
    Returning False costs a session download and then finds nothing to do,
    because get_xnat_resource compares each resource itself.
    """
    listing = _listing(tmp_path, LOCAL)
    type(listing).resource_manifests = property(  # type: ignore[assignment]
        lambda self: (_ for _ in ()).throw(OSError("read timeout from S3"))
    )
    with mock.patch(
        "xnat_ingest.helpers.remotes.get_xnat_checksums", return_value=dict(LOCAL)
    ):
        assert listing.all_uploaded(FakeConnection()) is False, (
            "a session whose completeness cannot be determined must not be "
            "reported as uploaded"
        )


# Upload can store a staged resource under a different label (from the DICOM SOP
# class) or scan ID (split by modality into '<scan-id>-<modality>'), so a session
# that is fully uploaded must still be recognised as such by its files.


class _XResource:
    def __init__(self, label: str, files: dict[str, str]) -> None:
        self.label = label
        self.files = files


class _XScan:
    def __init__(self, id: str, type: str, resources: list[_XResource]) -> None:
        self.id = id
        self.type = type
        self.resources = {r.label: r for r in resources}


class _XSession:
    def __init__(self, scans: list[_XScan]) -> None:
        self.scans = {s.id: s for s in scans}
        self.resources: dict[str, ty.Any] = {}


class _Connection:
    def __init__(self, xsession: _XSession) -> None:
        self.projects = {"proj": mock.MagicMock(experiments={"sess": xsession})}


def _renamed_listing(
    tmp_path: ty.Any, manifests: dict[str, dict[str, str]]
) -> S3SessionListing:
    # Subclass so the patched properties don't leak into other tests
    class Listing(S3SessionListing):
        @property
        def resource_paths(self) -> set[str]:
            return set(manifests)

        @property
        def resource_manifests(self) -> dict[str, dict[str, ty.Any]]:
            return {p: {"checksums": c} for p, c in manifests.items()}

    return Listing(name="proj.subj.sess", objects=[], bucket=None, cache_path=tmp_path)


def _all_uploaded(listing: S3SessionListing, scans: list[_XScan]) -> bool:
    with mock.patch(
        "xnat_ingest.helpers.remotes.get_xnat_checksums",
        side_effect=lambda xresource: dict(xresource.files),
    ):
        return listing.all_uploaded(_Connection(_XSession(scans)))  # type: ignore[arg-type]


def test_resource_relabelled_on_upload_is_uploaded(tmp_path: ty.Any) -> None:
    listing = _renamed_listing(tmp_path, {"501.Dose Report/DICOM": {"x.dcm": "d1"}})
    scans = [_XScan("501", "Dose Report", [_XResource("secondary", {"x.dcm": "d1"})])]
    assert _all_uploaded(listing, scans) is True


def test_scan_split_by_modality_on_upload_is_uploaded(tmp_path: ty.Any) -> None:
    listing = _renamed_listing(
        tmp_path, {"1.3D Application Data/DICOM": {"a.dcm": "d1", "b.dcm": "d2"}}
    )
    scans = [
        _XScan("1-OT", "3D Application Data", [_XResource("DICOM", {"a.dcm": "d1"})]),
        _XScan("1-CT", "3D Application Data", [_XResource("DICOM", {"b.dcm": "d2"})]),
    ]
    assert _all_uploaded(listing, scans) is True


def test_renamed_resource_missing_a_file_is_not_uploaded(tmp_path: ty.Any) -> None:
    listing = _renamed_listing(
        tmp_path, {"501.Dose Report/DICOM": {"x.dcm": "d1", "y.dcm": "d2"}}
    )
    scans = [_XScan("501", "Dose Report", [_XResource("secondary", {"x.dcm": "d1"})])]
    assert _all_uploaded(listing, scans) is False


def test_renamed_resource_with_differing_checksum_is_not_uploaded(
    tmp_path: ty.Any,
) -> None:
    listing = _renamed_listing(tmp_path, {"501.Dose Report/DICOM": {"x.dcm": "d1"}})
    scans = [
        _XScan("501", "Dose Report", [_XResource("secondary", {"x.dcm": "other"})])
    ]
    assert _all_uploaded(listing, scans) is False


def test_files_in_a_different_scan_are_not_matched(tmp_path: ty.Any) -> None:
    """Scan '1' must not pick up files from scan '10'."""
    listing = _renamed_listing(tmp_path, {"1.scan_one/DICOM": {"x.dcm": "d1"}})
    scans = [_XScan("10", "scan_ten", [_XResource("DICOM", {"x.dcm": "d1"})])]
    assert _all_uploaded(listing, scans) is False
