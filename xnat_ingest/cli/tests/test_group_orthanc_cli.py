"""CLI-level plumbing tests for `xnat-ingest group-orthanc`.

ImagingSession.from_orthanc is mocked here (its behaviour, including the
to_process_label/processed_label filtering, is covered directly against a
faked Orthanc REST API in model/tests/test_from_orthanc.py) -- this just
checks that group_orthanc_cli actually runs and passes the CLI options
through to the api/model layers unchanged.

This command used to crash unconditionally: cli/group.py called
`group_orthanc(..., wait_period=wait_period)` but api/group_.py's
group_orthanc() has no `wait_period` parameter, so every invocation raised
`TypeError: group_orthanc() got an unexpected keyword argument
'wait_period'` before any Orthanc/network code ran. There was no test
coverage of this command at all, which is how it went unnoticed.
"""

import typing as ty
from pathlib import Path
from unittest.mock import patch

from conftest import show_cli_trace
from xnat_ingest.cli import group_orthanc_cmd
from xnat_ingest.model.session import ImagingSession


def test_group_orthanc_cli_runs_and_passes_labels_through(
    cli_runner: ty.Any, tmp_path: Path
) -> None:
    store_dir = tmp_path / "orthanc-store"
    store_dir.mkdir()
    output_dir = tmp_path / "staged"

    with patch.object(
        ImagingSession, "from_orthanc", return_value=[]
    ) as mock_from_orthanc:
        result = cli_runner(
            group_orthanc_cmd,
            [
                "http://orthanc.example.org:8042",
                str(store_dir),
                str(output_dir),
                "orthanc-user",
                "orthanc-pass",
                "--to-process-label",
                "ready-for-xnat",
                "--processed-label",
                "sent-to-xnat",
            ],
        )

    assert result.exit_code == 0, show_cli_trace(result)
    assert mock_from_orthanc.call_count == 1
    _, kwargs = mock_from_orthanc.call_args
    assert kwargs["to_process_label"] == "ready-for-xnat"
    assert kwargs["processed_label"] == "sent-to-xnat"


def test_group_orthanc_cli_defaults_to_no_to_process_label(
    cli_runner: ty.Any, tmp_path: Path
) -> None:
    """Without --to-process-label, every non-processed study is a
    candidate -- from_orthanc must be called with to_process_label=None
    rather than the option being silently dropped or defaulted wrong."""
    store_dir = tmp_path / "orthanc-store"
    store_dir.mkdir()
    output_dir = tmp_path / "staged"

    with patch.object(
        ImagingSession, "from_orthanc", return_value=[]
    ) as mock_from_orthanc:
        result = cli_runner(
            group_orthanc_cmd,
            [
                "http://orthanc.example.org:8042",
                str(store_dir),
                str(output_dir),
                "orthanc-user",
                "orthanc-pass",
            ],
        )

    assert result.exit_code == 0, show_cli_trace(result)
    _, kwargs = mock_from_orthanc.call_args
    assert kwargs["to_process_label"] is None
