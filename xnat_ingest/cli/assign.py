import datetime
import time
import typing as ty
from pathlib import Path

import click
from fileformats.core import FileSet

from xnat_ingest.cli.base import base_cli

from ..api.assign_ import assign
from ..helpers.arg_types import (
    CopyModeParamType,
    LoggerConfig,
)
from ..helpers.logging import logger, set_logger_handling


@base_cli.command(
    name="assign",
    help="""Assigns project, subject and session IDs, extracted from session
metadata, to sessions that have already been grouped into scans/resources

INPUT_DIR is the path to the directory containing the grouped-but-not-yet-assigned
sessions (the output of the 'group' command)

OUTPUT_DIR is the directory that the assigned sessions will be written to
""",
)
@click.argument(
    "input_dir",
    type=click.Path(exists=True, path_type=Path),
    envvar="XINGEST_INPUT_DIR",
)
@click.argument(
    "output_dir", type=click.Path(path_type=Path), envvar="XINGEST_OUTPUT_DIR"
)
@click.option(
    "--project",
    "project_field",
    type=str,
    default="StudyComments",
    envvar="XINGEST_PROJECT",
    help=(
        "The keyword of the metadata field to extract the XNAT project ID from "
        "(XINGEST_PROJECT env. var)"
    ),
)
@click.option(
    "--subject",
    "subject_field",
    type=str,
    default="PatientID",
    envvar="XINGEST_SUBJECT",
    help=(
        "The keyword of the metadata field to extract the XNAT subject ID from "
        "(XINGEST_SUBJECT env. var)"
    ),
)
@click.option(
    "--session",
    "session_field",
    type=str,
    default="AccessionNumber",
    envvar="XINGEST_SESSION",
    help=(
        "The keyword of the metadata field to extract the XNAT session ID from "
        "(XINGEST_SESSION env. var)"
    ),
)
@click.option(
    "--scan",
    "scan_field",
    type=str,
    default="SeriesDescription",
    envvar="XINGEST_SCAN_DESC",
    help=(
        "The keyword of the metadata field to extract a description for each scan from. "
        "Scans for which the field can't be resolved are left without a description "
        "(XINGEST_SCAN_DESC env. var)"
    ),
)
@click.option(
    "--constant-project-id",
    type=str,
    default=None,
    help=("Fix the project ID as a constant for all data matched by this command"),
)
@click.option(
    "--loop",
    type=int,
    default=-1,
    envvar="XINGEST_LOOP",
    help="Run the assign process continuously every LOOP seconds (XINGEST_LOOP env. var). ",
)
@click.option(
    "--copy-mode",
    type=CopyModeParamType(),
    default=FileSet.CopyMode.hardlink_or_copy,
    envvar="XINGEST_COPY_MODE",
    help="The method to use for copying files (XINGEST_COPY_MODE env. var)",
)
@click.option(
    "--delete/--dont-delete",
    default=False,
    envvar="XINGEST_DELETE",
    help=(
        "Whether to delete the grouped session directories after they have been "
        "assigned or not (XINGEST_DELETE env. var)"
    ),
)
@click.option(
    "--logger",
    "loggers",
    multiple=True,
    type=LoggerConfig.cli_type,
    envvar="XINGEST_LOGGERS",
    nargs=3,
    default=(),
    metavar="<logtype> <loglevel> <location>",
    help=(
        "Setup handles to capture logs that are generated (XINGEST_LOGGERS env. var)"
    ),
)
@click.option(
    "--additional-logger",
    "additional_loggers",
    type=str,
    multiple=True,
    default=(),
    envvar="XINGEST_ADDITIONAL_LOGGERS",
    help=(
        "The loggers to use for logging. By default just the 'xnat-ingest' logger is used. "
        "But additional loggers can be included (e.g. 'xnat') can be "
        "specified here (XINGEST_ADDITIONAL_LOGGERS env. var)"
    ),
)
@click.option(
    "--raise-errors/--dont-raise-errors",
    default=False,
    type=bool,
    help="Whether to raise errors instead of logging them (typically for debugging)",
)
def assign_cli(
    input_dir: Path,
    output_dir: Path,
    project_field: str,
    subject_field: str,
    session_field: str,
    constant_project_id: str | None,
    scan_field: str | None,
    delete: bool,
    loggers: ty.List[LoggerConfig],
    additional_loggers: ty.List[str],
    raise_errors: bool,
    loop: int,
    copy_mode: FileSet.CopyMode,
) -> None:

    if raise_errors and loop >= 0:
        raise ValueError(
            "Cannot use --raise-errors and --loop together as the loop will "
            "continue to run even if an error occurs"
        )

    set_logger_handling(
        logger_configs=loggers,
        additional_loggers=additional_loggers,
    )

    # Run the assign process in a loop if loop is set to a positive value, otherwise just run it once
    while True:
        start_time = datetime.datetime.now()
        errors = assign(
            input_dir=input_dir,
            output_dir=output_dir,
            project_field=project_field,
            subject_field=subject_field,
            session_field=session_field,
            project_id=constant_project_id,
            scan_field=scan_field,
            delete=delete,
            raise_errors=raise_errors,
            copy_mode=copy_mode,
        )
        if errors:
            logger.error(
                "Assign completed with %s errors:\n\n%s",
                len(errors),
                "\n".join(errors),
            )
        else:
            logger.info("Assign completed successfully")
        if loop < 0:
            break
        end_time = datetime.datetime.now()
        elapsed_seconds = (end_time - start_time).total_seconds()
        sleep_time = loop - elapsed_seconds
        logger.info(
            "Assign took %s seconds, waiting another %s seconds before running "
            "again (loop every %s seconds)",
            elapsed_seconds,
            sleep_time,
            loop,
        )
        time.sleep(loop)
