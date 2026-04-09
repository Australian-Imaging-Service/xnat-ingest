import datetime
import tempfile
import time
import typing as ty
from pathlib import Path

import click
from fileformats.core import FileSet

from ..api import associate
from ..cli.base import base_cli
from ..helpers.arg_types import AssociatedFiles, LoggerConfig
from ..helpers.logging import logger, set_logger_handling

PRE_STAGE_NAME_DEFAULT = "PRE-STAGE"
STAGED_NAME_DEFAULT = "STAGED"
INVALID_NAME_DEFAULT = "INVALID"
DEIDENTIFIED_NAME_DEFAULT = "DEIDENTIFIED"


class CopyModeParamType(click.ParamType):
    name = "copy_mode"

    def convert(
        self,
        value: str,
        param: ty.Optional[click.Parameter],
        ctx: ty.Optional[click.Context],
    ) -> FileSet.CopyMode:
        if isinstance(value, FileSet.CopyMode):
            return value
        try:
            # Allow case-insensitive matching on enum member names.
            return FileSet.CopyMode[value.lower()]
        except KeyError:
            self.fail(f"{value!r} is not a valid copy mode", param, ctx)


@base_cli.command(
    name="associate",
    help="""Stages images found in the input directories into separate directories for each
imaging acquisition session

FILES_PATH is either the path to a directory containing the files to upload, or
a glob pattern that selects the paths directly

OUTPUT_DIR is the directory that the files for each session are collated to before they
are uploaded to XNAT
""",
)
@click.argument(
    "input_dir",
    type=click.Path(path_type=Path, exists=True),
    envvar="XINGEST_INPUT_DIR",
)
@click.argument(
    "output_dir", type=click.Path(path_type=Path), envvar="XINGEST_OUTPUT_DIR"
)
@click.argument("datatype", type=str, envvar="XINGEST_DATATYPE")
@click.argument("glob", type=str, envvar="XINGEST_GLOB")
@click.argument("id_pattern", type=str, envvar="XINGEST_ID_PATTERN")
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
    help="Whether to delete the session directories after they have been uploaded or not (XINGEST_DELETE env. var)",
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
    "--spaces-to-underscores/--no-spaces-to-underscores",
    default=False,
    help="Whether to replace spaces with underscores in the filenames of associated files (XINGEST_SPACES_TO_UNDERSCORES env. var)",
    envvar="XINGEST_SPACES_TO_UNDERSCORES",
    type=bool,
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
        "resource name by appending _1, _2 etc. to the name until a unique name is found (XINGEST_AVOID_CLASHES env. var)"
    ),
)
@click.option(
    "--temp-dir",
    type=Path,
    default=None,
    envvar="XINGEST_TEMPDIR",
    help="The directory to use for temporary downloads (i.e. from s3)",
)
@click.option(
    "--require-manifest/--dont-require-manifest",
    default=None,
    envvar="XINGEST_REQUIRE_MANIFEST",
    help=("Whether to require manifest files in the staged resources or not"),
    type=bool,
)
def associate_cli(
    input_dir: Path,
    output_dir: Path,
    datatype: str,
    glob: str,
    id_pattern: str,
    loggers: ty.List[LoggerConfig],
    additional_loggers: ty.List[str],
    raise_errors: bool,
    spaces_to_underscores: bool,
    avoid_clashes: bool,
    loop: int,
    temp_dir: Path | None,
    copy_mode: FileSet.CopyMode,
    delete: bool,
    require_manifest: bool,
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

    # Set the directory to create temporary files/directories in away from system default
    if temp_dir:
        tempfile.tempdir = str(temp_dir)

    # Loop the upload process if loop is set to a positive value, otherwise just run it once
    while True:
        start_time = datetime.datetime.now()
        errors = associate(
            input_dir=input_dir,
            output_dir=output_dir,
            associated_files=[AssociatedFiles(datatype, glob, id_pattern)],
            spaces_to_underscores=spaces_to_underscores,
            avoid_clashes=avoid_clashes,
            raise_errors=raise_errors,
            require_manifest=require_manifest,
            copy_mode=copy_mode,
            delete=delete,
        )
        if errors:
            logger.error(
                f"Association completed with {len(errors)} errors:\n\n{''.join(errors)}"
            )
        else:
            logger.info("Association completed successfully without errors")
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

    if raise_errors and loop >= 0:
        raise ValueError(
            "Cannot use --raise-errors and --loop together as the loop will "
            "continue to run even if an error occurs"
        )
