"""upload() must not decide from a stale view of XNAT, and must not grow memory.

`upload --loop` holds one XNAT connection for the life of the process, and
xnatpy caches what it reads on it. An operator who deletes a partially uploaded
session in XNAT expects the next pass to upload it again. If the pass answers
"does this already exist on XNAT?" from that cache, it logs "Skipping ... as all
the resources already exist on XNAT" and skips the session for ever.

So the check that runs on every pass, all_uploaded(), reads XNAT directly (see
test_all_uploaded_completeness.py). The upload steps use xnatpy objects, so the
cache is cleared before them. It is NOT cleared on every pass: that made xnatpy
build new listings on every pass, and xnatpy never frees them.
"""

import typing as ty
from pathlib import Path
from unittest import mock

from xnat_ingest.api.upload_api import upload
from xnat_ingest.helpers.remotes import LocalSessionListing


class RecordingConnection:
    """Records clearcache() and the first read of XNAT through xnatpy objects."""

    def __init__(self, events: ty.List[str]) -> None:
        self._events = events

    def __enter__(self) -> "RecordingConnection":
        return self

    def __exit__(self, *exc: ty.Any) -> ty.Literal[False]:
        return False

    def clearcache(self) -> None:
        self._events.append("clearcache")

    @property
    def projects(self) -> ty.Any:
        self._events.append("read_projects")
        return {}


class FakeRepo:
    def __init__(self, events: ty.List[str]) -> None:
        self.connection = RecordingConnection(events)


def _upload_one_session(tmp_path: Path, uploaded: bool) -> ty.List[str]:
    (tmp_path / "proj.subj.sess").mkdir()
    events: ty.List[str] = []
    with (
        mock.patch.object(LocalSessionListing, "all_uploaded", return_value=uploaded),
        mock.patch("xnat_ingest.api.upload_api.ImagingSession.load"),
    ):
        upload(input_dir=str(tmp_path), xnat_repo=FakeRepo(events), wait_period=0)
    return events


def test_cache_is_cleared_before_the_upload_steps_read_xnat(tmp_path: Path) -> None:
    """Clearing after the first read would serve it from the stale cache."""
    events = _upload_one_session(tmp_path, uploaded=False)
    assert "read_projects" in events, events
    assert "clearcache" in events[: events.index("read_projects")], (
        f"the cache must be cleared before the upload steps read XNAT through "
        f"xnatpy objects. events={events}"
    )


def test_a_pass_with_nothing_to_upload_builds_no_xnatpy_objects(
    tmp_path: Path,
) -> None:
    """THE LEAK: a pass that only finds complete sessions must not touch
    xnatpy's object cache. Clearing and refilling it on every pass is what made
    `upload --loop` grow."""
    events = _upload_one_session(tmp_path, uploaded=True)
    assert events == [], events
