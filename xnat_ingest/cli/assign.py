import datetime
import time
import typing as ty
from pathlib import Path

import click
from fileformats.core import FileSet
from fileformats.medimage import DicomSeries

from xnat_ingest.cli.base import base_cli

from ..api.sort_ import sort
from ..helpers.arg_types import (
    CopyModeParamType,
    IDSpec,
    LoggerConfig,
    MimeType,
)
from ..helpers.logging import logger, set_logger_handling


@base_cli.command(
    name="sort",
    help="""Stages images found in the input directories into separate directories for each
imaging acquisition session

INPUT_DIR is the path to the sorted scans

OUTPUT_DIR is the directory that the assigned files will be written to
""",
)
@click.argument("input_dir", type=str, nargs=-1, envvar="XINGEST_INPUT_PATHS")
@click.argument(
    "output_dir", type=click.Path(path_type=Path), envvar="XINGEST_STAGING_DIR"
)
@click.option(
    "--project",
    type=IDSpec.cli_type,
    nargs=4,
    metavar="<datatype> <type> <specifier>",
    multiple=True,
    default=[],
    envvar="XINGEST_PROJECT",
    help=(
        "The keyword of the metadata field to extract the XNAT project ID from "
        "(XINGEST_PROJECT env. var)"
    ),
)
@click.option(
    "--subject",
    type=IDSpec.cli_type,
    nargs=4,
    multiple=True,
    default=[],
    envvar="XINGEST_SUBJECT",
    help=(
        "The keyword of the metadata field to extract the XNAT subject ID from "
        "(XINGEST_SUBJECT env. var)"
    ),
)
@click.option(
    "--visit",
    type=IDSpec.cli_type,
    nargs=4,
    multiple=True,
    default=[],
    envvar="XINGEST_VISIT",
    help=(
        "The keyword of the metadata field to extract the XNAT imaging session ID from "
        "(XINGEST_VISIT env. var)"
    ),
)
@click.option(
    "--session",
    type=IDSpec.cli_type,
    nargs=4,
    multiple=True,
    default=[],
    envvar="XINGEST_SESSION",
    help=(
        "The metadata field to use as the XNAT session label directly, instead of concatenating "
        "subject and visit IDs. (XINGEST_SESSION env. var)"
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
    help="Run the staging process continuously every LOOP seconds (XINGEST_LOOP env. var). ",
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
        "Whether to delete the session directories after they have been uploaded or "
        "not (XINGEST_DELETE env. var)"
    ),
)
@click.option(
    "--logger",
    "loggers",
    multiple=True,
    type=LoggerConfig.cli_type,
    envvar="XINGEST_LOGGERS",
    nargs=4,
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
    input_paths: list[str],
    staging_dir: Path,
    datatype: list[MimeType] | None,
    project: list[IDSpec],
    subject: list[IDSpec],
    visit: list[IDSpec],
    session: list[IDSpec],
    constant_project_id: str | None,
    delete: bool,
    loggers: ty.List[LoggerConfig],
    additional_loggers: ty.List[str],
    raise_errors: bool,
    loop: int,
    wait_period: int,
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
    datatypes: list[ty.Type[FileSet]]
    if not datatype:
        datatypes = [DicomSeries]
    else:
        datatypes = [dt.datatype for dt in datatype]  # type: ignore[misc]

    # Run the staging process in a loop if loop is set to a positive value, otherwise just run it once
    while True:
        start_time = datetime.datetime.now()
        errors = sort(
            input_paths=input_paths,
            output_dir=staging_dir,
            datatypes=datatypes,
            project=project,
            subject=subject,
            visit=visit,
            session=session,
            fixed_project_id=constant_project_id,
            delete=delete,
            raise_errors=raise_errors,
            copy_mode=copy_mode,
            wait_period=wait_period,
        )
        if errors:
            logger.error(
                "Staging completed with %s errors:\n\n%s",
                len(errors),
                "\n".join(errors),
            )
        else:
            logger.info("Staging completed successfully")
        if loop < 0:
            break
        end_time = datetime.datetime.now()
        elapsed_seconds = (end_time - start_time).total_seconds()
        sleep_time = loop - elapsed_seconds
        logger.info(
            "Stage took %s seconds, waiting another %s seconds before running "
            "again (loop every %s seconds)",
            elapsed_seconds,
            sleep_time,
            loop,
        )
        time.sleep(loop)
