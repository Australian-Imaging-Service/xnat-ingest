"""An incomplete resource on XNAT must be repaired, not reported as uploaded.

get_xnat_resource returned None whenever the resource existed, and the caller
reads None as "already uploaded", so a resource XNAT held only part of was
skipped on every later pass while the session reported success.

The missing files are now handed back for upload. Only differences an upload
CANNOT fix, files on XNAT we do not have or shared files with different
content, are still an error for a human to resolve.
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
    # not a DicomCollection, so the resource keeps the name it was staged under rather
    # than one derived from the SOP class -- these tests are about checksum repair
    fileset = None
    path = "test_project:SUBJ:SESS:2-t1_mprage_ax:DICOM"

    def __init__(self, checksums: dict[str, str]) -> None:
        self.checksums = checksums


def _staged(n: int) -> dict[str, str]:
    return {f"slice{i}.dcm": f"digest{i}" for i in range(n)}


def _call(staged: dict[str, str], on_xnat: dict[str, str]) -> ty.Any:
    resource = FakeStagedResource(staged)
    xsession = FakeXnatSession(FakeScan({"DICOM": FakeResourceOnXnat()}))
    with mock.patch(
        "xnat_ingest.helpers.remotes.get_xnat_checksums", return_value=on_xnat
    ):
        return get_xnat_resource(resource, xsession)


def test_incomplete_resource_is_returned_for_repair() -> None:
    """THE REGRESSION: XNAT holding 16 of 95 files must not read as done.

    This is the shape of the real incident. The resource comes back paired with
    the 79 names the caller has to upload, and with nothing else, so the files
    XNAT already holds are not sent again.
    """
    staged = _staged(95)
    on_xnat = {k: v for k, v in list(staged.items())[:16]}  # the short resource

    xresource, only_files = _call(staged, on_xnat)

    assert xresource is not None, "a repairable resource must not be skipped"
    assert only_files == set(staged) - set(on_xnat)
    assert len(only_files) == 79
    assert "slice0.dcm" not in only_files, "already on XNAT, must not be resent"


def test_complete_resource_still_skips_quietly() -> None:
    """An identical resource is genuinely already uploaded: nothing to do."""
    staged = _staged(95)
    assert _call(staged, dict(staged)) == (None, None)


def test_extra_files_on_xnat_are_not_repaired() -> None:
    """XNAT holding files we do not is a conflict, not something to auto-repair.

    An upload can only add, so it cannot resolve this. The existing behaviour is
    kept: log and skip, leaving it to a human.
    """
    staged = _staged(3)
    on_xnat = dict(staged)
    on_xnat["unexpected.dcm"] = "digestX"

    assert _call(staged, on_xnat) == (None, None)


def test_missing_alongside_extra_files_still_raises() -> None:
    """Missing files that CANNOT be repaired must still fail the session.

    Uploading here would append to a resource that is already wrong, so the
    exception stays: the session must not be reported as cleanly uploaded, and
    the operator is told to delete the resource on XNAT.
    """
    staged = _staged(5)
    on_xnat = {"slice0.dcm": "digest0", "unexpected.dcm": "digestX"}

    with pytest.raises(IncompleteCheckumsException) as excinfo:
        _call(staged, on_xnat)

    msg = excinfo.value.msg
    assert "missing 4 file(s)" in msg, msg
    assert "cannot be repaired" in msg, msg
    assert "Delete the resource on XNAT" in msg, msg


def test_differing_content_is_not_repaired() -> None:
    """A shared file with different bytes cannot be fixed by uploading either."""
    staged = _staged(3)
    on_xnat = dict(staged)
    on_xnat["slice1.dcm"] = "SOMETHING_ELSE"

    assert _call(staged, on_xnat) == (None, None)


def test_empty_digests_do_not_make_a_complete_resource_look_broken() -> None:
    """XNAT reports digest '' until a catalog refresh populates it.

    Every file present by name and no digests to compare means nothing is
    wrong, and treating it as a difference would re-upload healthy resources.
    """
    staged = _staged(4)
    assert _call(staged, {k: "" for k in staged}) == (None, None)


def test_empty_digests_still_repair_a_genuinely_missing_file() -> None:
    """Names are trustworthy even when digests are not."""
    staged = _staged(4)
    on_xnat = {k: "" for k in list(staged)[:2]}

    xresource, only_files = _call(staged, on_xnat)

    assert xresource is not None
    assert only_files == {"slice2.dcm", "slice3.dcm"}
