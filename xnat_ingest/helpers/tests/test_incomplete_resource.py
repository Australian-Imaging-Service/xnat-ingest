"""An incomplete resource on XNAT must not be reported as already uploaded.

`get_xnat_resource` returned None whenever the resource existed on XNAT, and the
caller logs "Skipping '<path>' resource as it is already uploaded" for None. So a
resource that XNAT holds only *part* of was skipped on every subsequent pass and
never repaired, while the session still took the `else` branch and logged
"Successfully uploaded all files".

The information needed to notice was already in hand: the function computes
`missing_paths` from the checksum comparison and logs it at ERROR, then discards
it and returns None anyway.

Measured on a real deployment: a 383-instance study had 16 of 95 files uploaded
into one scan while the staging bucket was still being written. Every later pass
skipped that scan as "already uploaded". 170 of 383 instances reached XNAT and
the uploader reported success on each run.
"""

import typing as ty
from unittest import mock

import pytest

from xnat_ingest.exceptions import IncompleteCheckumsException
from xnat_ingest.helpers.remotes import get_xnat_resource


class FakeResourceOnXnat:
    """The resource XNAT already holds."""


class FakeScan:
    def __init__(self, resources: dict[str, ty.Any]) -> None:
        self.resources = resources
        self.files = ["something"]  # non-empty: skip the catalog-refresh branch


class FakeXnatSession:
    def __init__(self, scan: FakeScan) -> None:
        self.scans = {"2": scan}
        self.xnat_session = mock.MagicMock()


class FakeStagedScan:
    id = "2"
    path = "test_project:SUBJ:SESS:2-t1_mprage_ax"


class FakeStagedResource:
    """The resource as staged locally, i.e. what we hold and want uploaded."""

    name = "DICOM"
    scan = FakeStagedScan()
    path = "test_project:SUBJ:SESS:2-t1_mprage_ax:DICOM"

    def __init__(self, checksums: dict[str, str]) -> None:
        self.checksums = checksums


def _staged(n: int) -> dict[str, str]:
    return {f"slice{i}.dcm": f"digest{i}" for i in range(n)}


def test_incomplete_resource_raises_rather_than_reporting_uploaded() -> None:
    """XNAT holding 16 of 95 files must not read as 'already uploaded'."""
    staged = _staged(95)
    on_xnat = {k: v for k, v in list(staged.items())[:16]}  # the short resource

    resource = FakeStagedResource(staged)
    xsession = FakeXnatSession(FakeScan({"DICOM": FakeResourceOnXnat()}))

    with mock.patch(
        "xnat_ingest.helpers.remotes.get_xnat_checksums", return_value=on_xnat
    ):
        with pytest.raises(IncompleteCheckumsException) as excinfo:
            get_xnat_resource(resource, xsession)

    msg = excinfo.value.msg
    assert "missing 79 file(s)" in msg, msg
    assert "2-t1_mprage_ax" in msg, msg


def test_complete_resource_still_skips_quietly() -> None:
    """An identical resource is genuinely already uploaded: still returns None."""
    staged = _staged(95)
    resource = FakeStagedResource(staged)
    xsession = FakeXnatSession(FakeScan({"DICOM": FakeResourceOnXnat()}))

    with mock.patch(
        "xnat_ingest.helpers.remotes.get_xnat_checksums", return_value=dict(staged)
    ):
        assert get_xnat_resource(resource, xsession) is None


def test_extra_files_on_xnat_still_returns_none() -> None:
    """XNAT holding files we do not is a conflict, not something to auto-repair.

    Re-uploading cannot resolve it, so the existing behaviour is kept: log and
    return None. Only *missing* files, which an upload could actually fix, raise.
    """
    staged = _staged(3)
    on_xnat = dict(staged)
    on_xnat["unexpected.dcm"] = "digestX"

    resource = FakeStagedResource(staged)
    xsession = FakeXnatSession(FakeScan({"DICOM": FakeResourceOnXnat()}))

    with mock.patch(
        "xnat_ingest.helpers.remotes.get_xnat_checksums", return_value=on_xnat
    ):
        assert get_xnat_resource(resource, xsession) is None
