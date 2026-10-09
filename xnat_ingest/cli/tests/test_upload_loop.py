"""Unit tests for the `upload --loop` connection-lifecycle behaviour.

These cover the changes introduced across 1ec85ca ("Handle transient
connection errors in upload --loop mode"), 1e36402 ("hold one XNAT
connection across --loop iterations") and c2595d2 ("reconnect held XNAT
session on auth expiry in --loop mode"):

* one XNAT connection is held across loop iterations instead of being
  re-opened every time (the fix for the xnatpy schema-rebuild memory leak)
* transient network/XNAT errors close and re-open the held connection
  instead of crashing the daemon
* per-session errors that look like an XNAT auth failure (401/403) force a
  reconnect, since the held session can go stale server-side
* ordinary per-session errors must NOT force a reconnect, otherwise the
  memory-leak fix they were layered on top of would be defeated
* a reconnect attempt that itself fails is handled gracefully and retried
  on the next tick rather than crashing the loop

The XNAT connection and the `api.upload` call are mocked throughout, so
these don't need a running XNAT server (unlike test_cli.py's
test_stage_and_upload).
"""

import tempfile
import typing as ty
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests.exceptions

from conftest import show_cli_trace
from xnat_ingest.cli import upload_cmd

STAGED = "/staged"
SERVER = "https://xnat.example.org"
COMMON_ARGS = [STAGED, SERVER, "--user", "testuser", "--password", "testpass"]

# Unset any XINGEST_* env vars a developer may have exported locally (e.g. for
# manually testing against a real XNAT) so they can't leak into these tests,
# which rely on click falling back to the explicit CLI args/defaults above.
CLEAN_ENV = {
    "XINGEST_STORE_CREDENTIALS": None,
    "XINGEST_HOST": None,
    "XINGEST_USER": None,
    "XINGEST_PASS": None,
    "XINGEST_STAGED": None,
    "XINGEST_LOOP": None,
}


class _StopLoop(Exception):
    """Raised from the mocked time.sleep() to break out of the infinite
    `--loop` while-loop once the scenario under test has run enough
    iterations."""


def _run_loop(
    cli_runner: ty.Any,
    upload_side_effect: ty.Any,
    sleep_side_effect: ty.List[ty.Any],
    xnat_side_effect: ty.Optional[ty.List[ty.Any]] = None,
    extra_args: ty.Sequence[str] = (),
) -> ty.Tuple[ty.Any, MagicMock, MagicMock]:
    with (
        patch("xnat_ingest.cli.upload_cli.Xnat") as mock_xnat_cls,
        patch("xnat_ingest.cli.upload_cli.upload") as mock_upload,
        patch("xnat_ingest.cli.upload_cli.time.sleep") as mock_sleep,
    ):
        if xnat_side_effect is not None:
            mock_xnat_cls.side_effect = xnat_side_effect
        else:
            mock_xnat_cls.return_value = MagicMock()
        mock_upload.side_effect = upload_side_effect
        mock_sleep.side_effect = sleep_side_effect

        result = cli_runner(
            upload_cmd,
            COMMON_ARGS + ["--loop", "1"] + list(extra_args),
            env=CLEAN_ENV,
        )
    return result, mock_xnat_cls, mock_upload


def test_loop_holds_single_xnat_connection_across_iterations(
    cli_runner: ty.Any,
) -> None:
    """With no errors, --loop must reuse the one connection it opened at
    startup rather than reconnecting every iteration."""
    result, mock_xnat_cls, mock_upload = _run_loop(
        cli_runner,
        upload_side_effect=[[], [], []],
        sleep_side_effect=[None, None, _StopLoop()],
    )

    assert isinstance(result.exception, _StopLoop), show_cli_trace(result)
    assert mock_xnat_cls.call_count == 1
    assert mock_upload.call_count == 3


def test_loop_reopens_connection_after_transient_error(cli_runner: ty.Any) -> None:
    """A transient network error from upload() must close and re-open the
    held connection, then keep looping rather than crashing the daemon."""
    result, mock_xnat_cls, mock_upload = _run_loop(
        cli_runner,
        upload_side_effect=[
            requests.exceptions.ConnectionError("connection reset by peer"),
            [],
        ],
        sleep_side_effect=[None, _StopLoop()],
    )

    assert isinstance(result.exception, _StopLoop), show_cli_trace(result)
    # initial open + one reopen after the transient error
    assert mock_xnat_cls.call_count == 2
    assert mock_upload.call_count == 2


def test_loop_reconnects_after_detecting_auth_failure_in_returned_errors(
    cli_runner: ty.Any,
) -> None:
    """upload() doesn't raise on a stale/expired session -- it swallows the
    401/403 per session and returns it as an error string. --loop must
    detect that signature and force a reconnect, otherwise every
    subsequent session would be rejected until the process is restarted."""
    auth_error = [
        "Skipping upload of 'session1' due to error: \"XNATResponseError: "
        'Response status 401 for path /data/experiments"\n'
    ]
    result, mock_xnat_cls, mock_upload = _run_loop(
        cli_runner,
        upload_side_effect=[auth_error, []],
        sleep_side_effect=[None, _StopLoop()],
    )

    assert isinstance(result.exception, _StopLoop), show_cli_trace(result)
    assert mock_xnat_cls.call_count == 2
    assert mock_upload.call_count == 2


def test_loop_does_not_reconnect_for_ordinary_per_session_errors(
    cli_runner: ty.Any,
) -> None:
    """Ordinary per-session errors (e.g. a missing scan) are data problems,
    not connection problems. Reconnecting on every one of these would
    reintroduce the xnatpy schema-rebuild memory leak the held connection
    was added to avoid, so the connection must be left alone."""
    ordinary_error = [
        "Skipping upload of 'session1' due to error: \"ValueError: missing scan\"\n"
    ]
    result, mock_xnat_cls, mock_upload = _run_loop(
        cli_runner,
        upload_side_effect=[ordinary_error, ordinary_error, ordinary_error],
        sleep_side_effect=[None, None, _StopLoop()],
    )

    assert isinstance(result.exception, _StopLoop), show_cli_trace(result)
    assert mock_xnat_cls.call_count == 1
    assert mock_upload.call_count == 3


def test_loop_retries_gracefully_when_reconnect_itself_fails(
    cli_runner: ty.Any,
) -> None:
    """If the reconnect triggered by an auth failure itself fails (e.g. the
    server is briefly unreachable), the loop must not crash: it should log
    the failure, carry on with no connection, and successfully reconnect on
    a later tick."""
    auth_error = [
        "Skipping upload of 'session1' due to error: \"XNATResponseError: "
        'Response status 401 for path /data/experiments"\n'
    ]
    result, mock_xnat_cls, mock_upload = _run_loop(
        cli_runner,
        upload_side_effect=[auth_error, []],
        sleep_side_effect=[None, _StopLoop()],
        xnat_side_effect=[
            MagicMock(),  # initial open, succeeds
            ConnectionError("refused"),  # reconnect after the 401, fails
            MagicMock(),  # reconnect attempt at the top of the next iteration, succeeds
        ],
    )

    assert isinstance(result.exception, _StopLoop), show_cli_trace(result)
    assert mock_xnat_cls.call_count == 3
    assert mock_upload.call_count == 2


def test_loop_uses_one_s3_cache_dir_and_removes_it_on_exit(
    cli_runner: ty.Any,
) -> None:
    """Without --temp-dir, the loop must use one cache directory for the whole
    run and remove it when the run ends. A new directory for each pass was
    never removed, so the loop left one behind every pass."""
    cache_dirs: ty.List[Path] = []

    def record_cache_dir(**kwargs: ty.Any) -> ty.List[str]:
        cache_dirs.append(Path(kwargs["s3_cache_dir"]))
        assert cache_dirs[-1].is_dir()
        return []

    result, _, mock_upload = _run_loop(
        cli_runner,
        upload_side_effect=record_cache_dir,
        sleep_side_effect=[None, None, _StopLoop()],
    )

    assert isinstance(result.exception, _StopLoop), show_cli_trace(result)
    assert mock_upload.call_count == 3
    assert len(set(cache_dirs)) == 1
    assert not cache_dirs[0].exists()


def test_runs_that_share_a_temp_dir_use_their_own_cache_dir(
    cli_runner: ty.Any, tmp_path: Path, monkeypatch: ty.Any
) -> None:
    """Each run removes its own cache directory on exit. Two runs given the
    same --temp-dir must not share one, or the first to stop would remove the
    other's downloads. Files the caller keeps in --temp-dir must stay."""
    monkeypatch.setattr(tempfile, "tempdir", None)  # the CLI sets it
    callers_file = tmp_path / "callers-file.txt"
    callers_file.write_text("not ours")
    cache_dirs: ty.List[Path] = []

    def record_cache_dir(**kwargs: ty.Any) -> ty.List[str]:
        cache_dirs.append(Path(kwargs["s3_cache_dir"]))
        return []

    for _ in range(2):
        result, _, _ = _run_loop(
            cli_runner,
            upload_side_effect=record_cache_dir,
            sleep_side_effect=[_StopLoop()],
            extra_args=["--temp-dir", str(tmp_path)],
        )
        assert isinstance(result.exception, _StopLoop), show_cli_trace(result)

    assert len(set(cache_dirs)) == 2
    assert all(d.parent.parent == tmp_path for d in cache_dirs)
    assert not any(d.exists() for d in cache_dirs)
    assert callers_file.exists()


def test_reconnects_reuse_one_xnat_cache_dir_and_remove_it_on_exit(
    cli_runner: ty.Any,
) -> None:
    """Each connection used to get a new cache directory that was never
    removed. A reconnect must reuse the run's one directory."""
    result, mock_xnat_cls, _ = _run_loop(
        cli_runner,
        upload_side_effect=[
            requests.exceptions.ConnectionError("connection reset by peer"),
            [],
        ],
        sleep_side_effect=[None, _StopLoop()],
    )

    assert isinstance(result.exception, _StopLoop), show_cli_trace(result)
    cache_dirs = {c.kwargs["cache_dir"] for c in mock_xnat_cls.call_args_list}
    assert mock_xnat_cls.call_count == 2
    assert len(cache_dirs) == 1
    assert not next(iter(cache_dirs)).exists()


def test_one_shot_mode_reraises_transient_error_instead_of_looping(
    cli_runner: ty.Any,
) -> None:
    """Without --loop (one-shot mode), a transient error must propagate to
    the caller instead of being swallowed -- there's no daemon to keep
    alive, so callers need to see the failure."""
    with (
        patch("xnat_ingest.cli.upload_cli.Xnat") as mock_xnat_cls,
        patch("xnat_ingest.cli.upload_cli.upload") as mock_upload,
    ):
        mock_xnat_cls.return_value = MagicMock()
        mock_upload.side_effect = requests.exceptions.ConnectionError(
            "connection reset by peer"
        )

        result = cli_runner(upload_cmd, COMMON_ARGS, env=CLEAN_ENV)

    assert isinstance(result.exception, requests.exceptions.ConnectionError), (
        show_cli_trace(result)
    )
    assert mock_xnat_cls.call_count == 1
    assert mock_upload.call_count == 1
