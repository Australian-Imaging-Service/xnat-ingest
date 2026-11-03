import time
import typing as ty
from pathlib import Path

import click

from ..api.fields_api import fields
from ..helpers.arg_types import (
    FieldMapping,
    IDSpec,
    LoggerConfig,
)
from ..helpers.logging import logger, set_logger_handling
from .base import cli


@cli.command(
    name="fields",
    help="Extract session metadata into XNAT fields and write staged sessions.",
)
@click.argument(
    "input_dir",
    type=click.Path(exists=True, path_type=Path),
    envvar="XINGEST_INPUT_DIR",
)
@click.argument(
    "output_dir",
    type=click.Path(path_type=Path),
    envvar="XINGEST_OUTPUT_DIR",
)
@click.option(
    "--extract",
    "extract",
    type=FieldMapping.cli_type,
    multiple=True,
    required=True,
    nargs=2,
    metavar="<xnat-field> <specifier>",
    envvar="XINGEST_FIELDS",
    help=(
        "Expose session metadata as an XNAT field. May be repeated; separate "
        "entries with ';' in XINGEST_FIELDS."
    ),
)
@click.option("--loop", type=int, default=-1, envvar="XINGEST_LOOP")
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
    multiple=True,
    type=str,
    default=(),
    envvar="XINGEST_ADDITIONAL_LOGGERS",
)
@click.option("--raise-errors/--dont-raise-errors", default=False)
def fields_cmd(
    input_dir: Path,
    output_dir: Path,
    extract: tuple[FieldMapping, ...],
    loop: int,
    loggers: ty.Sequence[LoggerConfig],
    additional_loggers: ty.Sequence[str],
    raise_errors: bool,
) -> None:
    if raise_errors and loop >= 0:
        raise ValueError("Cannot use --raise-errors and --loop together")

    try:
        field_specs: dict[str, IDSpec] = FieldMapping.to_dict(extract)
    except ValueError as e:
        raise click.BadParameter(str(e), param_hint="--extract") from e

    set_logger_handling(
        logger_configs=loggers,
        additional_loggers=additional_loggers,
    )

    while True:
        started = time.monotonic()
        errors = fields(
            input_dir=input_dir,
            output_dir=output_dir,
            extract=field_specs,
            raise_errors=raise_errors,
        )
        if errors:
            logger.error(
                "Field extraction completed with %d errors:\n\n%s",
                len(errors),
                "\n".join(errors),
            )
        if loop < 0:
            break

        elapsed = time.monotonic() - started
        sleep_time = max(loop - elapsed, 0)
        logger.info(
            "Field extraction took %s seconds, waiting %s seconds",
            elapsed,
            sleep_time,
        )
        time.sleep(sleep_time)
