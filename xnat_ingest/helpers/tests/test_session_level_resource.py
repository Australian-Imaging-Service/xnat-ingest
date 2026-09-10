"""A session-level resource must be held to the same rule as a scan resource.

get_xnat_resource has two branches. The scan branch was fixed so an incomplete
resource is repaired or reported. The branch above it, for resources attached to
the session rather than to a scan (resource.scan is None), still logged and
returned None for every kind of difference, and the caller reads None as
"already uploaded". So a session-level resource that XNAT held only part of was
skipped on every pass and the session still reported as cleanly uploaded: the
exact rule the session verdict exists to enforce, surviving one branch up.

The old difference report could also raise KeyError. It indexed
resource.checksums by XNAT's keys, so a file present on XNAT but not staged
crashed the report that was meant to explain the problem.
"""

import typing as ty
from unittest import mock

import pytest

from xnat_ingest.exceptions import IncompleteCheckumsException
from xnat_ingest.helpers.remotes import get_xnat_resource


class FakeResourceOnXnat:
    pass


class FakeXnatSession:
    """A session that already holds a session-level resource named DICOM."""

    label = "SESS"
    uri = "/data/experiments/XNAT_E1"
    id = "XNAT_E1"

    def __init__(self) -> None:
        self.resources = {"REPORT": FakeResourceOnXnat()}
        self.xnat_session = mock.MagicMock()


class FakeStagedResource:
    """Staged at session level: scan is None."""

    name = "REPORT"
    scan = None
    path = "test_project:SUBJ:SESS:REPORT"

    def __init__(self, checksums: dict[str, str]) -> None:
        self.checksums = checksums


def _staged(n: int) -> dict[str, str]:
    return {f"page{i}.pdf": f"digest{i}" for i in range(n)}


def _call(staged: dict[str, str], on_xnat: dict[str, str]) -> ty.Any:
    with mock.patch(
        "xnat_ingest.helpers.remotes.get_xnat_checksums", return_value=on_xnat
    ):
        return get_xnat_resource(FakeStagedResource(staged), FakeXnatSession())


def test_incomplete_session_resource_is_returned_for_repair() -> None:
    """THE REGRESSION: short session-level resource must not read as done."""
    staged = _staged(5)
    on_xnat = {k: v for k, v in list(staged.items())[:2]}

    xresource, only_files = _call(staged, on_xnat)

    assert xresource is not None, "a repairable resource must not be skipped"
    assert only_files == {"page2.pdf", "page3.pdf", "page4.pdf"}


def test_complete_session_resource_still_skips() -> None:
    staged = _staged(5)
    assert _call(staged, dict(staged)) == (None, None)


def test_extra_files_on_xnat_do_not_raise_keyerror() -> None:
    """The old report indexed the staged checksums by XNAT's keys.

    A file on XNAT that was never staged is exactly the case that crashed it.
    """
    staged = _staged(3)
    on_xnat = dict(staged)
    on_xnat["surprise.pdf"] = "digestX"

    assert _call(staged, on_xnat) == (None, None), "must report, not crash"


def test_missing_alongside_extra_still_raises() -> None:
    """Not repairable by uploading, so the session must not report clean."""
    staged = _staged(4)
    on_xnat = {"page0.pdf": "digest0", "surprise.pdf": "digestX"}

    with pytest.raises(IncompleteCheckumsException) as excinfo:
        _call(staged, on_xnat)
    assert "missing 3 file(s)" in excinfo.value.msg


def test_empty_digests_do_not_make_a_complete_resource_look_short() -> None:
    staged = _staged(3)
    assert _call(staged, {k: "" for k in staged}) == (None, None)
