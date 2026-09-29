import logging
import traceback

import click
from click.testing import CliRunner

from xnat_ingest.cli import group_cmd


def show_cli_trace(result: click.testing.Result) -> str:
    """Show the exception traceback from CLIRunner results"""
    assert result.exc_info is not None
    exc_type, exc, tb = result.exc_info
    return "".join(traceback.format_exception(exc_type, value=exc, tb=tb))


runner = CliRunner()

WORK_DIR = "/Users/tclo7153/Data/ACEMID/uq-test"

logging.basicConfig(level=logging.DEBUG)

result = runner.invoke(
    group_cmd,
    [
        f"{WORK_DIR}/input",
        f"{WORK_DIR}/grouped",
        "--recursive",
        "--datatype",
        "image/png",
        "--datatype",
        "image/jpeg",
        "--datatype",
        "medimage/vnd.canfield.whole-body-analysis-dir",
        "--datatype",
        "medimage/vnd.canfield.dexi-data-dir",
        "--exclude-path",
        f"{WORK_DIR}/*/*/*.png",
        "--exclude-path",
        f"{WORK_DIR}/*/*/*.jpg",
        "--allow-unrecognised",
        ".*",
        "--session",
        "{subject_uid}_{CaptureDate:%Y%m%d}",
        "all",
        "--scan",
        "dermoscopy-{LesionID}",
        "image/png|image/jpeg",
        "--scan",
        "dexi-{CaptureTime}",
        "medimage/vnd.canfield.dexi-data-dir",
        "--scan",
        "analysis-{CaptureTime}",
        "medimage/vnd.canfield.whole-body-analysis-dir",
        "--resource",
        "CaptureDevice",
        "image/png|image/jpeg",
        "--on-resource-clash",
        "merge",
        "image/png|image/jpeg",
        "--path-metadata-regex",
        r".*/(?P<subject_uid>[\w-]+)/(?P<filename>[\w-]+\.(?:png|jpe?g))",
        "image/png|image/jpeg",
        "--path-metadata-regex",
        r".*/(?P<subject_uid>[\w-]+)/(?P<CaptureDate>\d{8})(?P<CaptureTime>\d+)/analysis",
        "medimage/vnd.canfield.whole-body-analysis-dir",
        "--path-metadata-regex",
        r".*/(?P<subject_uid>[\w-]+)/(?P<CaptureDate>\d{8})(?P<CaptureTime>\d+)/DexiData.*",
        "medimage/vnd.canfield.dexi-data-dir",
        "--metadata-table",
        f"{WORK_DIR}/input/Dermoscopy/LesionDermoscopyData_20260805155613.csv",
        "fileset[image/png|image/jpeg]",
        "ImagePath='=HYPERLINK(\"{subject_uid}/{filename}\")'",
    ],  # XINGEST_DIR
    catch_exceptions=False,
)


if result.exit_code != 0:
    show_cli_trace(result)
