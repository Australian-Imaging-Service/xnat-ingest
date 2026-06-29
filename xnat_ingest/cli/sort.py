import datetime
import time
import typing as ty
from pathlib import Path

import click
from fileformats.core import FileSet
from fileformats.medimage import DicomSeries

from xnat_ingest.cli.base import base_cli

from ..api.sort_ import sort, sort_from_orthanc
from ..helpers.arg_types import (
    CollationSpec,
    CopyModeParamType,
    IDSpec,
    LoggerConfig,
    MimeType,
    SaveMetadata,
    PathMetadata,
)
from ..helpers.logging import logger, set_logger_handling


@base_cli.command(
    name="sort",
    help="""Sorts images found in the input paths into separate resources, sorted into
scans and acquisition sessions

INPUT_PATHS are either paths to directories containing the files to upload, or
glob patterns that select the paths directly

OUTPUT_DIR is the directory that the files for each session are collated to before they
are uploaded to XNAT
""",
)
@click.argument("input_paths", type=str, nargs=-1, envvar="XINGEST_INPUT_PATHS")
@click.argument(
    "output_dir", type=click.Path(path_type=Path), envvar="XINGEST_STAGING_DIR"
)
@click.option(
    "--session-uid",
    type=IDSpec.cli_type,
    nargs=4,
    multiple=True,
    default=(("StudyInstanceUID", "medimage/dicom-collection"),),
    envvar="XINGEST_SESSION_UID",
    help=(
        "The metadata field used to group files into the same session before IDs are extracted "
        "(XINGEST_SESSION_UID env. var). Defaults to StudyInstanceUID."
    ),
)
@click.option(
    "--scan-uid",
    type=IDSpec.cli_type,
    nargs=4,
    multiple=True,
    default=(("SeriesNumber", "medimage/dicom-collection"),),
    envvar="XINGEST_SESSION_UID",
    help=(
        "The metadata field used to group files into the same session before IDs are extracted "
        "(XINGEST_SESSION_UID env. var). Defaults to StudyInstanceUID."
    ),
)
@click.option(
    "--datatype",
    type=MimeType.cli_type,
    metavar="<mime-type>",
    multiple=True,
    default=None,
    envvar="XINGEST_DATATYPES",
    help=(
        'The MIME-type(s) (or "MIME-like" see FileFormats docs) of potential datatype(s) '
        "of the primary files to to upload, defaults to 'medimage/dicom-series'. "
        "Any formats implemented in the FileFormats Python package "
        "(https://github.com/ArcanaFramework/fileformats) that implement the 'read_metadata' "
        '"extra" are supported, see FF docs on how to add support for new formats.'
    ),
)
@click.option(
    "--save-metadata",
    "-m",
    type=SaveMetadata.cli_type,
    metavar="<field> <level>",
    multiple=True,
    default=(),
    envvar="XINGEST_SAVE_METADATA",
    help=SaveMetadata.HELP_STR,
)
@click.option(
    "--path-metadata",
    type=PathMetadata.cli_type,
    metavar="<regex> <datatype>",
    nargs=2,
    multiple=True,
    default=(),
    help=(
        'Regular expressions to extract "metadata" values from resource paths to be saved '
        "with named groups for each of the metadata fields to be extracted. If not present, "
        "the extracted metadata fields will be added to the save-metadata list at the 'resource' "
        "level"
    ),
)
@click.option(
    "--loop",
    type=int,
    default=-1,
    envvar="XINGEST_LOOP",
    help="Run the staging process continuously every LOOP seconds (XINGEST_LOOP env. var). ",
)
@click.option(
    "--wait-period",
    type=int,
    default=0,
    envvar="XINGEST_WAIT_PERIOD",
    help=(
        "The number of seconds to wait since the last file modification in sessions "
        "in the S3 bucket or source file-system directory before uploading them to "
        "avoid uploading partial sessions (XINGEST_WAIT_PERIOD env. var)."
    ),
)
@click.option(
    "--recursive/--not-recursive",
    type=bool,
    default=False,
    help=("Whether to recursively search input directories for input files"),
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
@click.option(
    "--collate-resources",
    type=CollationSpec.cli_type,
    metavar="<mime-type> <collation>",
    nargs=2,
    multiple=True,
    default=(),
    envvar="XINGEST_COLLATE_RESOURCES",
    help=(
        "Flatten files of the given datatype into the resource directory during sort, "
        "regardless of source directory structure (e.g. when sorting from Orthanc). "
        "Collation level is one of 'any', 'siblings', or 'adjacent' (default 'siblings'). "
    ),
)
def sort_cli(
    input_paths: list[str],
    output_dir: Path,
    datatype: list[MimeType] | None,
    session_uid: list[IDSpec],
    scan_uid: list[IDSpec],
    save_metadata: list[SaveMetadata],
    path_metadata: list[PathMetadata],
    delete: bool,
    loggers: ty.List[LoggerConfig],
    additional_loggers: ty.List[str],
    raise_errors: bool,
    loop: int,
    wait_period: int,
    recursive: bool,
    copy_mode: FileSet.CopyMode,
    collate_resources: tuple[CollationSpec, ...],
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
            output_dir=output_dir,
            datatypes=datatypes,
            session_uid=session_uid,
            scan_uid=scan_uid,
            delete=delete,
            raise_errors=raise_errors,
            copy_mode=copy_mode,
            wait_period=wait_period,
            recursive=recursive,
            save_metadata=save_metadata,
            path_metadata=path_metadata,
            collation_map={cs.datatype: cs.collation_level for cs in collate_resources},
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


@base_cli.command(
    name="sort-from-orthanc",
    help="""Sorts images stored within an Orthanc instance into directories that can be processed by
subsequent processing steps.

URL of the Orthanc instance to connect to

STORE_DIR path to Orthanc's " "StorageDirectory as mounted in pod. DICOM files are hardlinked from the storage "
"directory directly to the staging directory. (XINGEST_ORTHANC_STORE_DIR env. var)"

OUTPUT_DIR is the directory that the files for each session are collated to before they
are uploaded to XNAT

USERNAME for the Orthanc user

PASSWORD for the Orthanc user
""",
)
@click.argument("url", type=str, envvar="XINGEST_ORTHANC_URL")
@click.argument(
    "store_dir",
    type=click.Path(path_type=Path, exist=True, file_okay=False),
    envvar="XINGEST_ORTHANC_STORE_DIR",
)
@click.argument("output_dir", type=click.Path(path_type=Path))
@click.argument("user", type=str, envvar="XINGEST_ORTHANC_USER")
@click.argument("password", type=str, envvar="XINGEST_ORTHANC_PASSWORD")
@click.option(
    "--metadata",
    "-m",
    type=SaveMetadata.cli_type,
    metavar="<field> <level>",
    multiple=True,
    default=(),
    envvar="XINGEST_SAVE_METADATA",
    help=SaveMetadata.HELP_STR,
)
@click.option(
    "--processed-label",
    type=str,
    default="xnat-sorted",
    envvar="XINGEST_ORTHANC_LABEL",
    help=(
        "Label applied to Orthanc studies after staging to prevent re-processing. "
        "Can be removed via the Orthanc UI "
        "(XINGEST_ORTHANC_LABEL env. var)"
    ),
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
    "--loop",
    type=int,
    default=-1,
    envvar="XINGEST_LOOP",
    help="Run the staging process continuously every LOOP seconds (XINGEST_LOOP env. var). ",
)
@click.option(
    "--wait-period",
    type=int,
    default=0,
    envvar="XINGEST_WAIT_PERIOD",
    help=(
        "The number of seconds to wait since the last file modification in sessions "
        "in the S3 bucket or source file-system directory before uploading them to "
        "avoid uploading partial sessions (XINGEST_WAIT_PERIOD env. var)."
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
def sort_from_orthanc_cli(
    url: str,
    store_dir: Path,
    output_dir: Path,
    user: str,
    password: str,
    delete: bool,
    raise_errors: bool,
    loop: int,
    wait_period: int,
    copy_mode: FileSet.CopyMode,
    loggers: ty.List[LoggerConfig],
    additional_loggers: ty.List[str],
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

    # Run the staging process in a loop if loop is set to a positive value, otherwise just run it once
    while True:
        start_time = datetime.datetime.now()
        errors = sort_from_orthanc(
            url=url,
            store_dir=store_dir,
            output_dir=output_dir,
            user=user,
            password=password,
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
            "Sorting from Orthan took %s seconds, waiting another %s seconds before running "
            "again (loop every %s seconds)",
            elapsed_seconds,
            sleep_time,
            loop,
        )
        time.sleep(loop)
