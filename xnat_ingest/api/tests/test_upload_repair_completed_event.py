"""upload() emits xnat_repair_completed only when the session's verdict is clean.

test_repair_completed_message.py pins the WORDING of the line. This pins WHERE
it is logged: inside upload()'s `if msg is None:` gate, once per session, after
every re-sent resource has been verified. Moving the logger call out of the
gate, or emitting it per resource as each upload finishes, passes the helper
tests and fails these.

No XNAT is needed. The session is staged on disk for real and upload() runs
for real, including the thread pool, the batch staging and the post-upload
checksum comparison. Only the four calls that would reach XNAT are replaced:
FrameSet.load, get_xnat_session, get_xnat_resource and get_xnat_checksums.
The fake resource "holds" the files XNAT would list, so a re-send that does
not land is caught by the real comparison, as a short resource is in
production.
"""

import json
import logging
import typing as ty
from pathlib import Path
from unittest import mock

import pytest

from xnat_ingest.api import upload_api
from xnat_ingest.api.upload_api import upload
from xnat_ingest.exceptions import IncompleteCheckumsException
from xnat_ingest.helpers.logging import JsonFormatter, logger

EVENT = "xnat_repair_completed"
UPLOADED = "Successfully uploaded all files in"
ATTEMPTED = "Repaired "  # the pre-verdict attempt line


# ---------------------------------------------------------------- XNAT fakes


class FakeXResource:
    """A resource XNAT holds only part of; `held` is what it would list."""

    def __init__(self, held: set[str], outcome: str) -> None:
        self.held = set(held)
        self.outcome = outcome  # "lands" | "raises" | "lost"

    def upload_dir(self, upload_dir: Path, method: str) -> None:
        if self.outcome == "raises":
            raise ConnectionError("simulated XNAT upload failure")
        if self.outcome == "lands":
            self.held |= {p.name for p in Path(upload_dir).rglob("*") if p.is_file()}
        # "lost": the call returns but XNAT never lists the files

    def listing(self) -> dict[str, str]:
        # Names only, as on a site without enableChecksums: the real
        # compare_resource_with_xnat still fails on a missing name.
        return {name: "" for name in self.held}


class FakeExperiment:
    id = "XNAT_E00001"


class FakeProject:
    experiments: dict[str, ty.Any] = {}  # session not on XNAT: all_uploaded() is False


class FakeConnection:
    def __init__(self) -> None:
        self.projects = {"proj": FakeProject()}

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *exc: ty.Any) -> ty.Literal[False]:
        return False

    def clearcache(self) -> None:
        pass

    def put(self, uri: str) -> None:  # triggerPipelines
        pass


class FakeRepo:
    server = "fake://xnat"

    def __init__(self) -> None:
        self.connection = FakeConnection()


# ------------------------------------------------------------------ helpers


def _stage(staging: Path, session: str, scans: ty.Iterable[str]) -> None:
    """Stage `session` with one DICOM resource per scan: a.dat on XNAT, b.dat not."""
    for scan in scans:
        res = staging / session / scan / "DICOM"
        res.mkdir(parents=True)
        (res / "a.dat").write_bytes(b"already on XNAT")
        (res / "b.dat").write_bytes(b"missing on XNAT")


class JsonCapture(logging.Handler):
    """Formats every record with the production JsonFormatter and parses it back."""

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.setFormatter(JsonFormatter())
        self.lines: list[dict[str, ty.Any]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(json.loads(self.format(record)))

    def events(self) -> list[dict[str, ty.Any]]:
        return [line for line in self.lines if line.get("event") == EVENT]

    def messages(self) -> list[str]:
        return [line["message"] for line in self.lines]


@pytest.fixture
def json_log() -> ty.Iterator[JsonCapture]:
    handler = JsonCapture()
    level = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        yield handler
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)


def _run(staging: Path, outcomes: dict[str, str], **kwargs: ty.Any) -> list[str]:
    """Drive the real upload() with each scan's re-send outcome, keyed by scan id."""

    def fake_get_xnat_resource(resource: ty.Any, xsession: ty.Any) -> ty.Any:
        outcome = outcomes[resource.scan.id]
        if outcome == "incomplete":  # short on XNAT and NOT repairable
            raise IncompleteCheckumsException(f"{resource.path} is short")
        return FakeXResource({"a.dat"}, outcome), {"b.dat"}

    with (
        mock.patch.object(upload_api.FrameSet, "load", side_effect=KeyError("none")),
        mock.patch.object(
            upload_api, "get_xnat_session", return_value=FakeExperiment()
        ),
        mock.patch.object(upload_api, "get_xnat_resource", fake_get_xnat_resource),
        mock.patch.object(
            upload_api, "get_xnat_checksums", lambda xresource: xresource.listing()
        ),
    ):
        return upload(
            input_dir=str(staging),
            xnat_repo=FakeRepo(),  # type: ignore[arg-type]
            always_include=["all"],
            require_manifest=False,
            check_checksums=True,  # the default, and what makes "completed" mean verified
            wait_period=0,
            **kwargs,
        )


# -------------------------------------------------------------------- tests


@pytest.mark.parametrize("outcome", ["raises", "lost"])
def test_failed_repair_emits_no_completion(
    tmp_path: Path, json_log: JsonCapture, outcome: str
) -> None:
    """(a) The attempt line is logged; the completion must not be."""
    _stage(tmp_path, "proj.subj.sess", ["1.t1"])

    errors = _run(tmp_path, {"1": outcome})

    assert errors, "the fake failure did not reach the verdict; the test is vacuous"
    assert any(m.startswith(ATTEMPTED) for m in json_log.messages()), (
        "no repair was attempted, so the absence below proves nothing"
    )
    assert json_log.events() == [], (
        "xnat_repair_completed was logged for a repair whose re-send failed: "
        "it must sit inside upload()'s `if msg is None:` gate"
    )
    assert not any(UPLOADED in m for m in json_log.messages())


@pytest.mark.parametrize("later", ["raises", "lost", "incomplete"])
def test_earlier_success_then_later_failure_emits_none(
    tmp_path: Path, json_log: JsonCapture, later: str
) -> None:
    """(b) One verdict per session, not one event per resource.

    xnat_max_workers=1 makes the executor run scan 1 (re-sent cleanly) before
    scan 2 (fails, or is unrepairable), so a per-resource emit would already
    have fired by the time the session fails.
    """
    _stage(tmp_path, "proj.subj.sess", ["1.t1", "2.t2"])

    errors = _run(tmp_path, {"1": "lands", "2": later}, xnat_max_workers=1)

    assert errors
    assert any(
        "Uploaded 'proj:subj:sess:1-t1:DICOM'" in m for m in json_log.messages()
    ), "scan 1 should have been re-sent successfully first"
    assert json_log.events() == [], (
        "a completion was logged for a session that did not upload cleanly"
    )


def test_successful_repair_emits_exactly_one_event(
    tmp_path: Path, json_log: JsonCapture
) -> None:
    """(c) One JSON line, carrying session and resources, before the success line."""
    _stage(tmp_path, "proj.subj.sess", ["1.t1", "2.t2"])

    errors = _run(tmp_path, {"1": "lands", "2": "lands"})

    assert errors == []
    events = json_log.events()
    assert len(events) == 1, events
    (event,) = events
    assert event["level"] == "INFO"
    assert event["logger"] == "xnat-ingest"
    assert event.get("session") == "proj.subj.sess", "extra= must carry session"
    assert event.get("resources") == 2, "extra= must carry the resource count"
    assert event["message"].startswith("Completed repair of 2 incomplete resource(s)")
    messages = json_log.messages()
    assert messages.index(event["message"]) < messages.index(
        f"{UPLOADED} 'proj.subj.sess'"
    ), "the completion shares the success line's gate and precedes it"


def test_one_pass_reports_each_session_on_its_own(
    tmp_path: Path, json_log: JsonCapture
) -> None:
    """Per-session state: no event for the failed session, no count carried over.

    Three sessions in one pass, each repairing one resource. Assertions are
    order-free (list_session_dirs follows iterdir order), and whichever order
    runs, at least one clean session follows another repair, so a list that
    leaked across sessions would inflate its count.
    """
    _stage(tmp_path, "proj.subj.sess1", ["1.t1"])
    _stage(tmp_path, "proj.subj.sess2", ["2.t2"])
    _stage(tmp_path, "proj.subj.sess3", ["3.t3"])

    _run(tmp_path, {"1": "lands", "2": "raises", "3": "lands"})

    assert sorted(
        (e.get("session"), e.get("resources")) for e in json_log.events()
    ) == [
        ("proj.subj.sess1", 1),
        ("proj.subj.sess3", 1),
    ]
