import logging
import traceback

import click
from click.testing import CliRunner

from xnat_ingest.cli import assign_cmd


def show_cli_trace(result: click.testing.Result) -> str:
    """Show the exception traceback from CLIRunner results"""
    assert result.exc_info is not None
    exc_type, exc, tb = result.exc_info
    return "".join(traceback.format_exception(exc_type, value=exc, tb=tb))


runner = CliRunner()

WORK_DIR = "/Users/tclo7153/Data/ACEMID/uq-test"

logging.basicConfig(level=logging.DEBUG)

result = runner.invoke(
    assign_cmd,
    [
        f"{WORK_DIR}/grouped",
        f"{WORK_DIR}/assigned",
        "--subject",
        "SubjectID",
        "--session",
        "{SubjectID}_{CaptureDate:%Y%m%d}",
        "--constant-project-id",
        "ACEMID",
    ],  # XINGEST_DIR
    catch_exceptions=False,
)


if result.exit_code != 0:
    show_cli_trace(result)
