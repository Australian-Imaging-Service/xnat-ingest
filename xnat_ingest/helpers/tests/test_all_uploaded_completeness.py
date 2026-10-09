"""all_uploaded() must not call a short resource "uploaded".

It compared resource LABELS, and a resource holding 5 of 8 files carries the
same label as one holding all 8, so the whole session was skipped before any
per-resource check could run: no error, no retry, reported as uploaded.

all_uploaded() reads XNAT through REST calls, not xnatpy objects, so these
tests answer those calls from a small model of one XNAT session.
"""

import typing as ty
from pathlib import Path
from unittest import mock

import pytest
from xnat.core import XNATBaseListing, XNATBaseObject

from xnat_ingest.helpers.remotes import S3SessionListing, SessionOnlyListing

RESOURCE_PATH = "1.scan_one/DICOM"
LOCAL = {"a.dcm": "d1", "b.dcm": "d2", "c.dcm": "d3"}

# (scan ID, scan type, {resource label: {file name: digest}})
Scan = ty.Tuple[str, str, ty.Dict[str, ty.Dict[str, str]]]


class FakeResponse:
    def __init__(self, body: ty.Any, status_code: int = 200) -> None:
        self.status_code = status_code
        self._body = body

    def json(self) -> ty.Any:
        return self._body


class FakeXnat:
    """Answers the REST reads of all_uploaded(), in the shape XNAT sends them.

    Project 'proj' holds session 'sess' (ID 'E1'). Change `scans` between calls
    to change what XNAT holds. Like XNAT, it lists scans and resources only
    under /data/experiments/E1: under /data/projects/<P>/experiments/E1 XNAT
    returns the session document instead.
    """

    def __init__(
        self,
        scans: ty.List[Scan],
        session_resources: ty.Optional[ty.Dict[str, ty.Dict[str, str]]] = None,
        projects: ty.Optional[ty.List[ty.Dict[str, str]]] = None,
        experiments: ty.Optional[ty.List[ty.Dict[str, str]]] = None,
    ) -> None:
        self.scans = scans
        self.session_resources = session_resources or {}
        self.projects = projects or [{"ID": "proj", "name": "proj"}]
        self.experiments = experiments or [
            {"ID": "E1", "label": "sess", "xsiType": "xnat:mrSessionData"}
        ]
        self.calls: ty.List[str] = []

    def _resources(self) -> ty.Dict[str, ty.Tuple[str, str, ty.Dict[str, str]]]:
        """URI of each resource's file listing -> (parent URI, label, files)."""
        resources = {}
        for scan_id, _, scan_resources in self.scans:
            for i, (label, files) in enumerate(scan_resources.items()):
                parent = f"/data/experiments/E1/scans/{scan_id}"
                resources[f"{parent}/resources/{scan_id}{i}/files"] = (
                    parent,
                    label,
                    files,
                )
        for i, (label, files) in enumerate(self.session_resources.items()):
            resources[f"/data/experiments/E1/resources/s{i}/files"] = (
                "/data/experiments/E1",
                label,
                files,
            )
        return resources

    def get_json(
        self, uri: str, query: ty.Optional[ty.Dict[str, str]] = None
    ) -> ty.Any:
        self.calls.append(uri)
        rows: ty.List[ty.Dict[str, str]]
        if uri == "/data/archive/projects":
            rows = self.projects
        elif uri.startswith("/data/projects/") and uri.endswith("/experiments"):
            rows = self.experiments
        elif uri == "/data/experiments":
            rows = self.experiments
        elif uri == "/data/experiments/E1/scans":
            rows = [
                {"ID": s, "type": t, "xsiType": "xnat:mrScanData"}
                for s, t, _ in self.scans
            ]
        elif uri.endswith("/resources"):
            # XNAT sends resource rows without ID or URI columns
            rows = [
                {
                    "xnat_abstractresource_id": files_uri.split("/")[-2],
                    "label": label,
                    "element_name": "xnat:resourceCatalog",
                }
                for files_uri, (parent, label, _) in self._resources().items()
                if parent + "/resources" == uri
            ]
        else:
            raise AssertionError(f"unexpected GET {uri}")
        return {"ResultSet": {"Result": [dict(r) for r in rows]}}

    def get(self, uri: str) -> FakeResponse:
        self.calls.append(uri)
        _, _, files = self._resources()[uri]
        return FakeResponse(
            {"ResultSet": {"Result": [{"Name": n, "digest": d} for n, d in files.items()]}}
        )


def _listing(
    tmp_path: ty.Any, manifests: ty.Dict[str, ty.Dict[str, str]]
) -> S3SessionListing:
    # Subclass so the patched properties don't leak into other tests
    class Listing(S3SessionListing):
        @property
        def resource_paths(self) -> ty.Set[str]:
            return set(manifests)

        @property
        def resource_manifests(self) -> ty.Dict[str, ty.Dict[str, ty.Any]]:
            return {p: {"checksums": c} for p, c in manifests.items()}

    return Listing(name="proj.subj.sess", objects=[], bucket=None, cache_path=tmp_path)


def _one_scan(files: ty.Dict[str, str]) -> FakeXnat:
    return FakeXnat([("1", "scan_one", {"DICOM": files})])


def test_complete_resource_is_uploaded(tmp_path: ty.Any) -> None:
    listing = _listing(tmp_path, {RESOURCE_PATH: LOCAL})
    assert listing.all_uploaded(_one_scan(dict(LOCAL))) is True  # type: ignore[arg-type]


def test_short_resource_is_not_uploaded(tmp_path: ty.Any) -> None:
    """THE REGRESSION: the label is present but two files are not."""
    listing = _listing(tmp_path, {RESOURCE_PATH: LOCAL})
    assert listing.all_uploaded(_one_scan({"a.dcm": "d1"})) is False, (  # type: ignore[arg-type]
        "a resource holding 1 of 3 files must not count as uploaded, "
        "otherwise the session is skipped before anything can notice"
    )


def test_empty_digests_do_not_make_a_complete_resource_look_short(
    tmp_path: ty.Any,
) -> None:
    """XNAT reports digest '' until a catalog refresh; names are still valid."""
    listing = _listing(tmp_path, {RESOURCE_PATH: LOCAL})
    xnat = _one_scan({k: "" for k in LOCAL})
    assert listing.all_uploaded(xnat) is True  # type: ignore[arg-type]


def test_no_manifest_falls_back_to_previous_behaviour(tmp_path: ty.Any) -> None:
    """Sessions staged without manifests must not become newly unskippable."""
    listing = _listing(tmp_path, {RESOURCE_PATH: {}})
    assert listing.all_uploaded(_one_scan({})) is True  # type: ignore[arg-type]


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

    class Listing(S3SessionListing):
        @property
        def resource_paths(self) -> ty.Set[str]:
            return {RESOURCE_PATH}

        @property
        def resource_manifests(self) -> ty.Dict[str, ty.Dict[str, ty.Any]]:
            raise OSError("read timeout from S3")

    listing = Listing(name="proj.subj.sess", objects=[], bucket=None, cache_path=tmp_path)
    assert listing.all_uploaded(_one_scan(dict(LOCAL))) is False, (  # type: ignore[arg-type]
        "a session whose completeness cannot be determined must not be "
        "reported as uploaded"
    )


# Upload can store a staged resource under a different label (from the DICOM SOP
# class) or scan ID (split by modality into '<scan-id>-<modality>'), so a session
# that is fully uploaded must still be recognised as such by its files.


def test_resource_relabelled_on_upload_is_uploaded(tmp_path: ty.Any) -> None:
    listing = _listing(tmp_path, {"501.Dose Report/DICOM": {"x.dcm": "d1"}})
    xnat = FakeXnat([("501", "Dose Report", {"secondary": {"x.dcm": "d1"}})])
    assert listing.all_uploaded(xnat) is True  # type: ignore[arg-type]


def test_scan_split_by_modality_on_upload_is_uploaded(tmp_path: ty.Any) -> None:
    listing = _listing(
        tmp_path, {"1.3D Application Data/DICOM": {"a.dcm": "d1", "b.dcm": "d2"}}
    )
    xnat = FakeXnat(
        [
            ("1-OT", "3D Application Data", {"DICOM": {"a.dcm": "d1"}}),
            ("1-CT", "3D Application Data", {"DICOM": {"b.dcm": "d2"}}),
        ]
    )
    assert listing.all_uploaded(xnat) is True  # type: ignore[arg-type]


def test_renamed_resource_missing_a_file_is_not_uploaded(tmp_path: ty.Any) -> None:
    listing = _listing(
        tmp_path, {"501.Dose Report/DICOM": {"x.dcm": "d1", "y.dcm": "d2"}}
    )
    xnat = FakeXnat([("501", "Dose Report", {"secondary": {"x.dcm": "d1"}})])
    assert listing.all_uploaded(xnat) is False  # type: ignore[arg-type]


def test_renamed_resource_with_differing_checksum_is_not_uploaded(
    tmp_path: ty.Any,
) -> None:
    listing = _listing(tmp_path, {"501.Dose Report/DICOM": {"x.dcm": "d1"}})
    xnat = FakeXnat([("501", "Dose Report", {"secondary": {"x.dcm": "other"}})])
    assert listing.all_uploaded(xnat) is False  # type: ignore[arg-type]


def test_files_in_a_different_scan_are_not_matched(tmp_path: ty.Any) -> None:
    """Scan '1' must not pick up files from scan '10'."""
    listing = _listing(tmp_path, {"1.scan_one/DICOM": {"x.dcm": "d1"}})
    xnat = FakeXnat([("10", "scan_ten", {"DICOM": {"x.dcm": "d1"}})])
    assert listing.all_uploaded(xnat) is False  # type: ignore[arg-type]


def test_session_resource_is_found_by_label(tmp_path: ty.Any) -> None:
    listing = _listing(tmp_path, {"PROTOCOL": {"p.json": "d1"}})
    xnat = FakeXnat([], session_resources={"PROTOCOL": {"p.json": "d1"}})
    assert listing.all_uploaded(xnat) is True  # type: ignore[arg-type]


# The session lookup follows xnatpy's XNATListing: by ID first, then by the
# project's name or the session's label, and an ambiguous label matches nothing.


def test_missing_session_is_not_uploaded(tmp_path: ty.Any) -> None:
    listing = _listing(tmp_path, {RESOURCE_PATH: LOCAL})
    xnat = _one_scan(dict(LOCAL))
    xnat.experiments = [{"ID": "E9", "label": "other", "xsiType": "xnat:mrSessionData"}]
    assert listing.all_uploaded(xnat) is False  # type: ignore[arg-type]


def test_missing_project_raises(tmp_path: ty.Any) -> None:
    listing = _listing(tmp_path, {RESOURCE_PATH: LOCAL})
    xnat = _one_scan(dict(LOCAL))
    xnat.projects = [{"ID": "other", "name": "other"}]
    with pytest.raises(KeyError, match="Project 'proj' does not exist on XNAT"):
        listing.all_uploaded(xnat)  # type: ignore[arg-type]


def test_project_is_found_by_name(tmp_path: ty.Any) -> None:
    listing = _listing(tmp_path, {RESOURCE_PATH: LOCAL})
    xnat = _one_scan(dict(LOCAL))
    xnat.projects = [{"ID": "P1", "name": "proj"}]
    assert listing.all_uploaded(xnat) is True  # type: ignore[arg-type]
    assert "/data/projects/P1/experiments" in xnat.calls
    assert "/data/experiments/E1/scans" in xnat.calls


def test_session_is_found_by_id(tmp_path: ty.Any) -> None:
    listing = _listing(tmp_path, {RESOURCE_PATH: LOCAL})
    listing.name = "proj.subj.E1"
    assert listing.all_uploaded(_one_scan(dict(LOCAL))) is True  # type: ignore[arg-type]


def test_ambiguous_session_label_matches_nothing(tmp_path: ty.Any) -> None:
    listing = _listing(tmp_path, {RESOURCE_PATH: LOCAL})
    xnat = _one_scan(dict(LOCAL))
    xnat.experiments = [
        {"ID": "E1", "label": "sess", "xsiType": "xnat:mrSessionData"},
        {"ID": "E2", "label": "sess", "xsiType": "xnat:mrSessionData"},
    ]
    assert listing.all_uploaded(xnat) is False  # type: ignore[arg-type]


def test_rows_with_no_id_or_type_are_ignored(tmp_path: ty.Any) -> None:
    listing = _listing(tmp_path, {RESOURCE_PATH: LOCAL})
    xnat = _one_scan(dict(LOCAL))
    xnat.experiments = [
        {"ID": "", "label": "sess", "xsiType": "xnat:mrSessionData"},
        {"ID": "E1", "label": "sess", "xsiType": "xnat:mrSessionData"},
        {"ID": "E2", "label": "sess", "xsiType": ""},
    ]
    assert listing.all_uploaded(xnat) is True  # type: ignore[arg-type]


def test_session_only_listing_finds_session_by_label(tmp_path: ty.Any) -> None:
    listing = SessionOnlyListing(Path(tmp_path) / "sess")
    xnat = FakeXnat([])
    assert listing.find_xnat_session_uri(xnat) == "/data/experiments/E1"  # type: ignore[arg-type]
    xnat.experiments = [{"ID": "E9", "label": "other", "xsiType": "xnat:mrSessionData"}]
    assert listing.find_xnat_session_uri(xnat) is None  # type: ignore[arg-type]


def test_session_only_listing_raises_on_multiple_matches(tmp_path: ty.Any) -> None:
    listing = SessionOnlyListing(Path(tmp_path) / "sess")
    xnat = FakeXnat([])
    xnat.experiments = [
        {"ID": "E1", "label": "sess", "xsiType": "xnat:mrSessionData"},
        {"ID": "E2", "label": "sess", "xsiType": "xnat:petSessionData"},
    ]
    with pytest.raises(RuntimeError, match="Multiple XNAT sessions"):
        listing.find_xnat_session_uri(xnat)  # type: ignore[arg-type]


# `upload --loop` calls all_uploaded() for every staged session on every pass.


def test_each_call_reads_xnat_again(tmp_path: ty.Any) -> None:
    """A change in XNAT is seen on the next call. Nothing is cached."""
    listing = _listing(tmp_path, {RESOURCE_PATH: LOCAL})
    xnat = _one_scan(dict(LOCAL))
    assert listing.all_uploaded(xnat) is True  # type: ignore[arg-type]

    xnat.scans = [("1", "scan_one", {"DICOM": {"a.dcm": "d1"}})]  # files deleted

    assert listing.all_uploaded(xnat) is False  # type: ignore[arg-type]


def test_repeated_checks_build_no_xnatpy_objects(tmp_path: ty.Any) -> None:
    """THE LEAK: xnatpy keeps every listing object it builds in a class-level
    registry. A check that built them on every pass grew memory for as long as
    `upload --loop` ran. So building any xnatpy object here is an error."""
    listing = _listing(tmp_path, {RESOURCE_PATH: LOCAL})
    xnat = _one_scan(dict(LOCAL))

    def refuse(*args: ty.Any, **kwargs: ty.Any) -> None:
        raise AssertionError("all_uploaded() built an xnatpy object")

    with (
        mock.patch.object(XNATBaseListing, "__init__", refuse),
        mock.patch.object(XNATBaseObject, "__init__", refuse),
    ):
        for _ in range(200):
            assert listing.all_uploaded(xnat) is True  # type: ignore[arg-type]
