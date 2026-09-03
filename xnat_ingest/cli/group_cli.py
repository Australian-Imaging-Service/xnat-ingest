import datetime
import time
import typing as ty
from pathlib import Path

import click
from dateutil import tz
from fileformats.core import FileSet

from xnat_ingest.cli.base import cli

from ..api.group_api import group, group_orthanc
from ..helpers.arg_types import (
    ON_RESOURCE_CLASH,
    CollationSpec,
    Convert,
    CopyModeParamType,
    IDSpec,
    LoggerConfig,
    MetadataTable,
    MimeType,
    OnResourceClash,
    PathMetadataRegex,
)
from ..helpers.logging import logger, set_logger_handling


@cli.command(
    name="group",
    help="""Groups images found in the input paths into separate resources, grouped into
scans and acquisition sessions

INPUT_PATHS are either paths to directories containing the files to upload, or
glob patterns that select the paths directly

OUTPUT_DIR is the directory that the files for each session are collated to before they
are uploaded to XNAT
""",
)
@click.argument("input_paths", type=str, nargs=-1, envvar="XINGEST_INPUT_PATHS")
@click.argument(
    "output_dir", type=click.Path(path_type=Path), envvar="XINGEST_OUTPUT_DIR"
)
@click.option(
    "--session",
    type=IDSpec.cli_type,
    nargs=2,
    multiple=True,
    default=(("StudyInstanceUID", "all"),),
    envvar="XINGEST_SESSION",
    help=(
        "The metadata field used to group files into the same session before IDs are extracted "
        "(XINGEST_SESSION env. var). Defaults to StudyInstanceUID. Can also be a Python format "
        "string over several fields, e.g. '{PatientID}_{StudyDate:%Y%m%d}', to compose one."
    ),
)
@click.option(
    "--scan",
    type=IDSpec.cli_type,
    nargs=2,
    multiple=True,
    default=[["SeriesNumber", "all"]],
    metavar="<specifier> <datatype>",
    envvar="XINGEST_SCAN",
    help=(
        "The keyword of the metadata field to extract the XNAT imaging scan ID from, or a "
        "Python format string over several fields (see --session) (XINGEST_SCAN env. var)"
    ),
)
@click.option(
    "--resource",
    type=IDSpec.cli_type,
    nargs=2,
    multiple=True,
    default=(),
    metavar="<specifier> <datatype>",
    envvar="XINGEST_RESOURCE",
    help=(
        "The keywords of the metadata field to extract the XNAT imaging resource ID from "
        "for different datatypes (use `generic/file-set` as a catch-all if required), or a "
        "Python format string over several fields (see --session). If not given, each "
        "resource is labelled with the mime-like rendering of its fileset type name, "
        "e.g. 'vectra-export', 'sqlite3-db'. (XINGEST_RESOURCE env. var)"
    ),
)
@click.option(
    "--datatype",
    type=MimeType.cli_type,
    metavar="<mime-type>",
    multiple=True,
    default=["medimage/dicom-series"],
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
    "--path-metadata-regex",
    type=PathMetadataRegex.cli_type,
    multiple=True,
    nargs=2,
    metavar="<regex> <datatype>",
    envvar="XINGEST_PATH_METADATA_REGEX",
    default=(),
    help=(
        'Regular expressions to extract "metadata" values from resource file paths as named groups. '
        "using Python regular expression syntax. The named groups are used as metadata fields for "
        "the resource files, and the extracted values will be used to populate the corresponding "
        "metadata fields to complement the metadata read from the file headers."
    ),
)
@click.option(
    "--on-resource-clash",
    type=click.Choice(ON_RESOURCE_CLASH),
    default="avoid",
    envvar="XINGEST_ON_RESOURCE_CLASH",
    help=(
        "Determines the behavior when a resource with the same name already exists in the scan. "
        "Options are 'merge', 'avoid', 'error' (XINGEST_ON_RESOURCE_CLASH env. var)."
        "Default: 'avoid'"
    ),
)
@click.option(
    "--ignore-path",
    "ignore_paths",
    type=str,
    default=(),
    multiple=True,
    envvar="XINGEST_IGNORE_PATH",
    help=(
        "Regular expressions to match paths that should be ignored when grouping files into sessions. "
        "If None, no paths will be ignored. To ignore all paths by default, use '.*' as the value "
        "for this parameter. (XINGEST_IGNORE env. var)"
    ),
)
@click.option(
    "--ignore-type",
    "ignore_types",
    type=MimeType.cli_type,
    default=None,
    multiple=True,
    envvar="XINGEST_IGNORE_TYPE",
    help=(
        "Datatypes that should be ignored when grouping files into sessions. If None, paths "
        "that aren't recognised as part of the requested datatypes or filtered using "
        "ignore_paths will raise an error (XINGEST_IGNORE env. var)"
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
    type=click.IntRange(min=0),
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
    envvar="XINGEST_RECURSIVE",
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
    "--unlink-source",
    type=click.Choice(["all", "keep-metadata"]),
    default=None,
    envvar="XINGEST_UNLINK_SOURCE",
    help=(
        "Whether to unlink the source files after they have been successfully "
        "grouped. 'all' and 'keep-metadata' behave the same here (individual "
        "source files are removed one by one) since 'group's source isn't a "
        "directory tree that xnat-ingest owns and can't have a metadata skeleton "
        "left behind in it (XINGEST_UNLINK_SOURCE env. var)"
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
    "--collate-resources",
    type=CollationSpec.cli_type,
    metavar="<mime-type> <collation>",
    nargs=2,
    multiple=True,
    default=(),
    envvar="XINGEST_COLLATE_RESOURCES",
    help=(
        "Flatten files of the given datatype into the resource directory during grouping, "
        "regardless of source directory structure (e.g. when grouping from Orthanc). "
        "Collation level is one of 'any', 'siblings', or 'adjacent' (default 'siblings'). "
    ),
)
@click.option(
    "--convert",
    "conversions",
    type=Convert.cli_type,
    metavar="<src-mime-like> <tgt-mime-like>",
    nargs=2,
    multiple=True,
    default=(),
    envvar="XINGEST_CONVERT",
    help=("Convert resources of <src-mime-like> to <tgt-mime-like> during save. "),
)
@click.option(
    "--metadata-table",
    "metadata_tables",
    multiple=True,
    type=MetadataTable.cli_type,
    envvar="XINGEST_METADATA_TABLES",
    nargs=3,
    default=(),
    metavar="<path> <row-frequency> <join-exprs>",
    help=(
        "Specify metadata tables to extract and join metadata from input files (XINGEST_METADATA_TABLES env. var). "
        "The 'path' arg specifies the location of the metadata table file. Its format is auto-detected as CSV or "
        "TSV from the file extension; a different format can be forced by appending its mime-type in square "
        "brackets, e.g. 'path/to/table.dat[text/csv]'. "
        'The "row frequency" arg specifies what each row in the '
        "metadata table corresponds to in the data hierarchy, and can be one of 'session', 'scan', 'resource', "
        "'fileset', 'fileset[<mime-type>]'. When one or more mime-types are given in square brackets after 'fileset' "
        "they restrict the join to input files of those types (multiple mime-types can be '|'-separated, e.g. "
        "'fileset[image/png|image/jpeg]'); a bare 'fileset' matches any input file. "
        "The 'join-exprs' arg is a comma-separated list of '<column-name>=<cell-value>' expressions; a row is a "
        "match when every expression holds. The '<cell-value>' is either the name of an existing metadata field or "
        "a Python format string over one or more metadata fields, e.g. '{PatientID}_{SessionID}'. All columns of "
        "the matched row are then merged into the target's metadata. "
        "The example below extracts the relative path of an image file with `--path-metadata-regex` and uses it to "
        "join a table whose 'ImagePath' column holds spreadsheet HYPERLINK() formulas.\n\n"
        "    xnat-ingest group ...  \\\n"
        "        --path-metadata-regex '.*/(?P<relpath>[\\w-]+/[\\w-]+\\.(?:png|jpg))' image/png|image/jpeg \\\n"
        "        --metadata-table path/to/table.csv[text/csv] fileset[image/png|image/jpeg] "
        "'ImagePath=HYPERLINK(\"{relpath}\")'\n"
    ),
)
def group_cmd(
    input_paths: list[str],
    output_dir: Path,
    session: ty.Sequence[IDSpec],
    scan: ty.Sequence[IDSpec],
    resource: ty.Sequence[IDSpec],
    datatype: list[MimeType] | None,
    path_metadata_regex: list[PathMetadataRegex],
    unlink_source: str | None,
    loggers: list[LoggerConfig],
    additional_loggers: list[str],
    raise_errors: bool,
    on_resource_clash: OnResourceClash,
    ignore_paths: list[str] | None,
    ignore_types: list[MimeType] | None,
    loop: int,
    wait_period: int,
    recursive: bool,
    copy_mode: FileSet.CopyMode,
    collate_resources: tuple[CollationSpec, ...],
    conversions: tuple[MimeType, ...],
    metadata_tables: tuple[MetadataTable, ...],
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
        start_time = datetime.datetime.now(tz=tz.tzlocal())
        errors = group(
            input_paths=input_paths,
            output_dir=output_dir,
            datatypes=[dt.datatype for dt in datatype],
            session=session,
            scan=scan,
            resource=resource,
            unlink_source=unlink_source,
            raise_errors=raise_errors,
            copy_mode=copy_mode,
            ignore_paths=ignore_paths,
            ignore_types=[dt.datatype for dt in ignore_types],
            on_resource_clash=on_resource_clash,
            wait_period=wait_period,
            path_metadata_regex=path_metadata_regex,
            recursive=recursive,
            collation_map={cs.datatype: cs.collation_level for cs in collate_resources},
            conversion_map={c.src: c.tgt for c in conversions},
            metadata_tables=metadata_tables,
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
        end_time = datetime.datetime.now(tz=tz.tzlocal())
        elapsed_seconds = (end_time - start_time).total_seconds()
        sleep_time = max(loop - elapsed_seconds, 0)
        logger.info(
            "Group took %s seconds, waiting another %s seconds before running "
            "again (loop every %s seconds)",
            elapsed_seconds,
            sleep_time,
            loop,
        )
        time.sleep(sleep_time)


@cli.command(
    name="group-orthanc",
    help="""Groups images stored within an Orthanc instance into directories that can be processed by
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
    type=click.Path(path_type=Path, exists=True, file_okay=False),
    envvar="XINGEST_ORTHANC_STORE_DIR",
)
@click.argument("output_dir", type=click.Path(path_type=Path))
@click.argument("user", type=str, envvar="XINGEST_ORTHANC_USER")
@click.argument("password", type=str, envvar="XINGEST_ORTHANC_PASSWORD")
@click.option(
    "--to-process-label",
    type=str,
    default=None,
    envvar="XINGEST_ORTHANC_TO_PROCESS",
    help=(
        "Label applied to Orthanc studies after staging to prevent re-processing. "
        "Can be removed via the Orthanc UI "
        "(XINGEST_ORTHANC_TO_PROCESS env. var)"
    ),
)
@click.option(
    "--processed-label",
    type=str,
    default=None,
    envvar="XINGEST_ORTHANC_PROCESSED",
    help=(
        "Label applied to Orthanc studies after staging to prevent re-processing. "
        "Can be removed via the Orthanc UI "
        "(XINGEST_ORTHANC_PROCESSED env. var)"
    ),
)
@click.option(
    "--unlink-source",
    type=click.Choice(["all", "keep-metadata"]),
    default=None,
    envvar="XINGEST_UNLINK_SOURCE",
    help=(
        "Whether to unlink the source studies in Orthanc after they have been "
        "successfully grouped. Not yet implemented (XINGEST_UNLINK_SOURCE env. var)"
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
def group_orthanc_cmd(
    url: str,
    store_dir: Path,
    output_dir: Path,
    user: str,
    password: str,
    processed_label: str | None,
    to_process_label: str | None,
    unlink_source: str | None,
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
        group_orthanc(
            url=url,
            store_dir=store_dir,
            output_dir=output_dir,
            user=user,
            password=password,
            processed_label=processed_label,
            to_process_label=to_process_label,
            unlink_source=unlink_source,
            raise_errors=raise_errors,
            copy_mode=copy_mode,
            wait_period=wait_period,
        )
        if loop < 0:
            break
        end_time = datetime.datetime.now()
        elapsed_seconds = (end_time - start_time).total_seconds()
        sleep_time = loop - elapsed_seconds
        logger.info(
            "Grouping from Orthanc took %s seconds, waiting another %s seconds before running "
            "again (loop every %s seconds)",
            elapsed_seconds,
            sleep_time,
            loop,
        )
        time.sleep(loop)
