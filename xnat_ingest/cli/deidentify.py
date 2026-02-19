import datetime
import time
import typing as ty
from pathlib import Path

import click
from fileformats.core import FileSet

from xnat_ingest.cli.base import base_cli

from ..helpers.arg_types import CopyModeParamType, LoggerConfig
from ..helpers.logging import logger, set_logger_handling

DEIDENTIFIED_NAME_DEFAULT = "DEIDENTIFIED"


@base_cli.command(
    help="""Stages images found in the input directories into separate directories for each
imaging acquisition session

INPUT_DIR is either the path to a directory containing the files to deidentify

OUTPUT_DIR is the directory that the files for each session are collated to before they
are uploaded to XNAT
""",
)
@click.argument(
    "input_dir",
    type=click.Path(path_type=Path, exists=True),
    nargs=-1,
    envvar="XINGEST_INPUT_DIR",
)
@click.argument(
    "output_dir", type=click.Path(path_type=Path), envvar="XINGEST_OUTPUT_DIR"
)
@click.option(
    "--delete/--dont-delete",
    default=False,
    envvar="XINGEST_DELETE",
    help=(
        "Whether to delete the session directories after they have been deidentified "
        "or not (XINGEST_DELETE env. var)"
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
@click.option(
    "--deidentify/--dont-deidentify",
    default=False,
    type=bool,
    envvar="XINGEST_DEIDENTIFY",
    help=(
        "whether to deidentify the file names and DICOM metadata before staging "
        "(XINGEST_DEIDENTIFY env. var)"
    ),
)
@click.option(
    "--copy-mode",
    type=CopyModeParamType(),
    default=FileSet.CopyMode.hardlink_or_copy,
    envvar="XINGEST_COPY_MODE",
    help="The method to use for copying files (XINGEST_COPY_MODE env. var)",
)
@click.option(
    "--loop",
    type=int,
    default=-1,
    envvar="XINGEST_LOOP",
    help="Run the staging process continuously every LOOP seconds (XINGEST_LOOP env. var). ",
)
@click.option(
    "--avoid-clashes/--dont-avoid-clashes",
    default=False,
    envvar="XINGEST_AVOID_CLASHES",
    help=(
        "If a resource with the same name already exists in the scan, increment the "
        "resource name by appending _1, _2 etc. to the name until a unique name is found "
        "(XINGEST_AVOID_CLASHES env. var)"
    ),
)
@click.option(
    "--recursive/--not-recursive",
    type=bool,
    default=False,
    help=("Whether to recursively search input directories for input files"),
)
def deidentify_cli(
    input_dir: Path,
    output_dir: Path,
    loggers: ty.List[LoggerConfig],
    additional_loggers: ty.List[str],
    require_manifest: bool,
    raise_errors: bool,
    deidentify: bool,
    copy_mode: FileSet.CopyMode,
    loop: int,
    avoid_clashes: bool,
    delete: bool,
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

    # Run the staging process in a loop if loop is set to a positive value, otherwise
    # just run it once
    while True:
        start_time = datetime.datetime.now()
        errors = deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            avoid_clashes=avoid_clashes,
            raise_errors=raise_errors,
            copy_mode=copy_mode,
            require_manifest=require_manifest,
            delete=delete,
        )
        if errors:
            logger.error(
                f"Staging completed with {len(errors)} errors:\n\n{''.join(errors)}"
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
