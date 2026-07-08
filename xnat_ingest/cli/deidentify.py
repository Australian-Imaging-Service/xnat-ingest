import datetime
import time
import typing as ty
from pathlib import Path

import click
from fileformats.core import FileSet

from xnat_ingest.cli.base import base_cli

from ..api.deidentify_api import deidentify
from ..helpers.arg_types import CopyModeParamType, LoggerConfig
from ..helpers.logging import logger, set_logger_handling

DEIDENTIFIED_NAME_DEFAULT = "DEIDENTIFIED"


@base_cli.command(
    name="deidentify",
    help="""Stages images found in the input directories into separate directories for each
imaging acquisition session

INPUT_DIR is the path to the directory containing the session directories to de-identify.
Each session directory should be named in the format <project_id>.<subject_id>.<session_id>
and contain subdirectories for each scan, which in turn contain the resource files for
each scan.

OUTPUT_DIR is the directory that the files for each session are collated to before they
are uploaded to XNAT

SPEC_DIR is the directory containing the project-specific deidentification specifications.
It should contain one subdirectory per project, named <project_id>, plus an optional
"__default__" subdirectory used as a fallback for projects that don't have their own.
Within each of these subdirectories, there is one JSON spec file per file format that
requires deidentification in that project, named after the format's MIME-like identifier
with '/' replaced by '@' (e.g. 'medimage/dicom-series' -> 'medimage@dicom-series.json').
Formats without a matching spec file are only deidentified if a spec is found for a
broader/parent format (e.g. a 'medimage/dicom-collection' spec also covers
'medimage/dicom-series').

REID_DIR is the directory to save the re-identification metadata to, which can be used to
re-identify the de-identified data if needed. The re-identification metadata is saved in
JSON format, with one JSON file per session, containing a list of mappings from original
to de-identified identifiers for each resource in the session, as well as any additional
metadata needed for re-identification (e.g. DICOM tags that were modified during de-identification).
The re-identification metadata files are named <session_id>.json (or <session_id>.json.enc if encrypted
by --reid-encrypt-key option) and saved in the REID_DIR.
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
@click.argument(
    "spec_dir",
    type=click.Path(path_type=Path, exists=True),
    envvar="XINGEST_SPEC_DIR",
)
@click.argument(
    "reid_dir",
    type=click.Path(path_type=Path),
    envvar="XINGEST_REID_DIR",
)
@click.option(
    "--unlink-source",
    type=click.Choice(["all", "keep-metadata"]),
    default=None,
    envvar="XINGEST_UNLINK_SOURCE",
    help=(
        "Whether to unlink the assigned session directories after they have been "
        "deidentified. 'all' removes the whole assigned session directory; "
        "'keep-metadata' removes the resource data but leaves the session/scan-"
        "level metadata behind, so a lightweight skeleton of the session survives "
        "(e.g. for 'associate' to use later). If not set, the assigned "
        "directories are left in place (XINGEST_UNLINK_SOURCE env. var)"
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
    help="Whether to raise errors instead of logging them (typically for debugging)",
)
@click.option(
    "--require-manifest/--no-require-manifest",
    default=True,
    envvar="XINGEST_REQUIRE_MANIFEST",
    help=(
        "Whether to require a MANIFEST.json file in each resource directory. "
        "If False, resources are loaded as generic FileSets without checksum validation "
        "(XINGEST_REQUIRE_MANIFEST env. var)"
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
    "--reid-encrypt-key",
    type=str,
    default=None,
    envvar="XINGEST_REID_ENCRYPT_KEY",
    help=(
        "An optional encryption key to use for encrypting the re-identification metadata "
        "(XINGEST_REID_ENCRYPT_KEY env. var). This should be a URL-safe base64-encoded 32-byte key, "
        "e.g. generated using `Fernet.generate_key()` from the cryptography package"
    ),
)
def deidentify_cli(
    input_dir: Path,
    output_dir: Path,
    spec_dir: Path,
    reid_dir: Path,
    loggers: ty.List[LoggerConfig],
    additional_loggers: ty.List[str],
    require_manifest: bool,
    raise_errors: bool,
    copy_mode: FileSet.CopyMode,
    loop: int,
    avoid_clashes: bool,
    unlink_source: str | None,
    reid_encrypt_key: str | None = None,
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

    encrypt_key_bytes: bytes | None = (
        reid_encrypt_key.encode() if reid_encrypt_key is not None else None
    )

    # Run the staging process in a loop if loop is set to a positive value, otherwise
    # just run it once
    while True:
        start_time = datetime.datetime.now()
        deidentify(
            input_dir=input_dir,
            output_dir=output_dir,
            spec_dir=spec_dir,
            reid_dir=reid_dir,
            avoid_clashes=avoid_clashes,
            raise_errors=raise_errors,
            copy_mode=copy_mode,
            require_manifest=require_manifest,
            unlink_source=unlink_source,
            reid_encrypt_key=encrypt_key_bytes,
        )
        if loop < 0:
            break
        end_time = datetime.datetime.now()
        elapsed_seconds = (end_time - start_time).total_seconds()
        sleep_time = loop - elapsed_seconds
        logger.info(
            "Deidentify took %s seconds, waiting another %s seconds before running "
            "again (loop every %s seconds)",
            elapsed_seconds,
            sleep_time,
            loop,
        )
        time.sleep(loop)
