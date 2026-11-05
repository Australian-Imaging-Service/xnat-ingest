import datetime
import tempfile
import time
import typing as ty
from pathlib import Path

import click
from fileformats.core import FileSet

from ..api.package_api import package
from ..helpers.arg_types import (
    ON_RESOURCE_CLASH,
    CopyModeParamType,
    LoggerConfig,
    MimeType,
    OnResourceClash,
    OutputResource,
)
from ..helpers.logging import logger, set_logger_handling
from .base import cli


@cli.command(
    name="package",
    help="""Replace staged scan resources with one or more named conversions.

INPUT_DIR is normally the output of the deidentify command. Every
OUTPUT_RESOURCE conversion receives the original matching SOURCE_DATATYPE
resource, allowing an archive and a sample to be produced independently without
reopening the archive.
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
@click.argument(
    "source_datatype", type=MimeType.cli_type, envvar="XINGEST_SOURCE_DATATYPE"
)
@click.option(
    "--output-resource",
    "output_resources",
    type=OutputResource.cli_type,
    multiple=True,
    required=True,
    nargs=2,
    metavar="<label> <target-spec>",
    envvar="XINGEST_OUTPUT_RESOURCES",
    help=(
        "Resource label and target datatype to produce from each matching source. "
        "May be repeated. Target specs accept converter options after ':', e.g. "
        "'application/zip:allowZip64=false'."
    ),
)
@click.option(
    "--copy-mode",
    type=CopyModeParamType(),
    default=FileSet.CopyMode.hardlink_or_copy,
    envvar="XINGEST_COPY_MODE",
    help="How converted and pass-through resources are copied to the output.",
)
@click.option(
    "--unlink-source",
    type=click.Choice(["all", "keep-metadata"]),
    default=None,
    envvar="XINGEST_UNLINK_SOURCE",
    help=(
        "Remove the input session after successful packaging, either entirely or "
        "leaving its session/scan metadata skeleton."
    ),
)
@click.option(
    "--on-resource-clash",
    type=click.Choice(ON_RESOURCE_CLASH),
    default="error",
    envvar="XINGEST_ON_RESOURCE_CLASH",
    help="How to handle output labels that clash within the same scan.",
)
@click.option(
    "--require-manifest/--dont-require-manifest",
    default=True,
    envvar="XINGEST_REQUIRE_MANIFEST",
    help="Whether staged input resources must contain manifests.",
)
@click.option(
    "--temp-dir",
    type=click.Path(path_type=Path),
    default=None,
    envvar="XINGEST_TEMPDIR",
    help="Directory in which converters create temporary data.",
)
@click.option(
    "--loop",
    type=int,
    default=-1,
    envvar="XINGEST_LOOP",
    help="Run repeatedly every LOOP seconds; a negative value runs once.",
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
)
@click.option(
    "--additional-logger",
    "additional_loggers",
    type=str,
    multiple=True,
    default=(),
    envvar="XINGEST_ADDITIONAL_LOGGERS",
)
@click.option(
    "--raise-errors/--dont-raise-errors",
    default=False,
    type=bool,
)
def package_cmd(
    input_dir: Path,
    output_dir: Path,
    source_datatype: MimeType,
    output_resources: tuple[OutputResource, ...],
    copy_mode: FileSet.CopyMode,
    unlink_source: str | None,
    on_resource_clash: OnResourceClash,
    require_manifest: bool,
    temp_dir: Path | None,
    loop: int,
    loggers: ty.List[LoggerConfig],
    additional_loggers: ty.List[str],
    raise_errors: bool,
) -> None:
    if raise_errors and loop >= 0:
        raise ValueError("Cannot use --raise-errors and --loop together")

    labels = [output.label for output in output_resources]
    if len(labels) != len(set(labels)):
        raise click.BadParameter(
            "Each --output-resource label must be unique",
            param_hint="--output-resource",
        )

    source = source_datatype.datatype
    resolved_outputs = {
        output.label: output.resolve(source) for output in output_resources
    }

    set_logger_handling(
        logger_configs=loggers,
        additional_loggers=additional_loggers,
    )
    if temp_dir:
        tempfile.tempdir = str(temp_dir)

    while True:
        started = datetime.datetime.now()
        errors = package(
            input_dir=input_dir,
            output_dir=output_dir,
            source_datatype=source,
            output_resources=resolved_outputs,
            on_resource_clash=on_resource_clash,
            raise_errors=raise_errors,
            require_manifest=require_manifest,
            copy_mode=copy_mode,
            unlink_source=unlink_source,
        )
        if errors:
            logger.error(
                "Packaging completed with %d errors:\n\n%s",
                len(errors),
                "\n".join(errors),
            )
        if loop < 0:
            break
        elapsed = (datetime.datetime.now() - started).total_seconds()
        sleep_time = max(loop - elapsed, 0)
        logger.info(
            "Package took %s seconds, waiting %s seconds before running again",
            elapsed,
            sleep_time,
        )
        time.sleep(sleep_time)
