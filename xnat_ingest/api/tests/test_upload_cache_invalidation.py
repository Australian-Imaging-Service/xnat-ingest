"""upload() must drop the cached view of XNAT before it decides anything.

`upload --loop` holds one XNAT connection for the life of the process, and
xnatpy caches project/subject/experiment listings on it. Without an explicit
invalidation, every pass answers "does this already exist on XNAT?" from a
snapshot taken at startup, so nothing an operator does in XNAT is ever visible.

The failure this guards against: an operator deletes a partially uploaded
session in XNAT so the pipeline will re-upload it. The next pass reads the
cache, still sees it, logs "Skipping ... as all the resources already exist on
XNAT", and skips it for ever. Only restarting the process recovers it.

The ordering assertion matters as much as the call itself. Clearing *after* the
existence checks would look correct in a diff and restore the bug.
"""

import typing as ty

from xnat_ingest.api.upload_api import upload


class RecordingConnection:
    """Records the order of clearcache vs the first read of XNAT state."""

    def __init__(self, events: list[str]) -> None:
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
    def __init__(self, events: list[str]) -> None:
        self.connection = RecordingConnection(events)


def test_upload_clears_the_connection_cache(tmp_path: "ty.Any") -> None:
    """A pass must invalidate the cached XNAT listings."""
    events: list[str] = []
    upload(input_dir=str(tmp_path), xnat_repo=FakeRepo(events), wait_period=0)
    assert "clearcache" in events, (
        "upload() did not clear the XNAT connection cache: a long-running "
        "`upload --loop` would answer existence checks from a snapshot taken "
        "when the process started"
    )


def test_cache_is_cleared_before_any_xnat_state_is_read(tmp_path: "ty.Any") -> None:
    """Clearing after the existence checks would restore the bug silently."""
    events: list[str] = []
    upload(input_dir=str(tmp_path), xnat_repo=FakeRepo(events), wait_period=0)

    # Unconditional: the very first thing a pass does must be the invalidation.
    # Asserting on events[0] rather than on relative indices avoids a branch that
    # only runs when the input directory happens to contain sessions.
    assert events and events[0] == "clearcache", (
        f"the first action of a pass must be clearing the XNAT cache, otherwise "
        f"the first existence check is served from the stale snapshot. "
        f"events={events}"
    )
