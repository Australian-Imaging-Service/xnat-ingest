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
    FieldSpec,
    LoggerConfig,
    MimeType,
    XnatLogin,
)
from ..helpers.logging import logger, set_logger_handling

PRE_STAGE_NAME_DEFAULT = "PRE-STAGE"
STAGED_NAME_DEFAULT = "STAGED"
INVALID_NAME_DEFAULT = "INVALID"
DEIDENTIFIED_NAME_DEFAULT = "DEIDENTIFIED"


@base_cli.command(
    help="""Stages images found in the input directories into separate directories for each
imaging acquisition session

FILES_PATH is either the path to a directory containing the files to upload, or
a glob pattern that selects the paths directly

OUTPUT_DIR is the directory that the files for each session are collated to before they
are uploaded to XNAT
""",
)
@click.argument("input_paths", type=str, nargs=-1, envvar="XINGEST_INPUT_PATHS")
@click.argument(
    "staging_dir", type=click.Path(path_type=Path), envvar="XINGEST_STAGING_DIR"
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
    "--project-field",
    type=FieldSpec.cli_type,
    nargs=2,
    multiple=True,
    default=[["StudyID", "generic/file-set"]],
    envvar="XINGEST_PROJECT",
    help=(
        "The keyword of the metadata field to extract the XNAT project ID from (XINGEST_PROJECT env. var)"
    ),
)
@click.option(
    "--subject-field",
    type=FieldSpec.cli_type,
    nargs=2,
    multiple=True,
    default=[["PatientID", "generic/file-set"]],
    envvar="XINGEST_SUBJECT",
    help=(
        "The keyword of the metadata field to extract the XNAT subject ID from (XINGEST_SUBJECT env. var)"
    ),
)
@click.option(
    "--visit-field",
    type=FieldSpec.cli_type,
    nargs=2,
    multiple=True,
    default=[["AccessionNumber", "generic/file-set"]],
    envvar="XINGEST_VISIT",
    help=(
        "The keyword of the metadata field to extract the XNAT imaging session ID from (XINGEST_VISIT env. var)"
    ),
)
@click.option(
    "--session-field",
    type=FieldSpec.cli_type,
    nargs=2,
    multiple=True,
    default=[["StudyInstanceUID", "generic/file-set"]],
    envvar="XINGEST_SESSION",
    help=(
        "The keyword of the metadata field to extract the XNAT imaging session ID from (XINGEST_SESSION env. var)"
    ),
)
@click.option(
    "--scan-id-field",
    type=FieldSpec.cli_type,
    nargs=2,
    multiple=True,
    default=[["SeriesNumber", "generic/file-set"]],
    envvar="XINGEST_SCAN_ID",
    help=(
        "The keyword of the metadata field to extract the XNAT imaging scan ID from (XINGEST_SCAN_ID env. var)"
    ),
)
@click.option(
    "--scan-desc-field",
    type=FieldSpec.cli_type,
    nargs=2,
    multiple=True,
    default=[["SeriesDescription", "generic/file-set"]],
    envvar="XINGEST_SCAN_DESC",
    help=(
        "The keyword of the metadata field to extract the XNAT imaging scan description from (XINGEST_SCAN_DESC env. var)"
    ),
)
@click.option(
    "--resource-field",
    type=FieldSpec.cli_type,
    nargs=2,
    multiple=True,
    default=[["ImageType[2:]", "generic/file-set"]],
    metavar="<field> <datatype>",
    envvar="XINGEST_RESOURCE",
    help=(
        "The keywords of the metadata field to extract the XNAT imaging resource ID from "
        "for different datatypes (use `generic/file-set` as a catch-all if required). (XINGEST_RESOURCE env. var)"
    ),
)
@click.option(
    "--project-id",
    type=str,
    default=None,
    help=("Override the project ID read from the DICOM headers"),
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
    "--xnat-login",
    nargs=3,
    type=XnatLogin.cli_type,
    default=None,
    metavar="<host> <user> <password>",
    help="The XNAT server to upload to plus the user and password to use for login (XINGEST_XNAT_LOGIN env. var)",
    envvar="XINGEST_XNAT_LOGIN",
)
@click.option(
    "--loop",
    type=int,
    default=-1,
    envvar="XINGEST_LOOP",
    help="Run the staging process continuously every LOOP seconds (XINGEST_LOOP env. var). ",
)
@click.option(
    "--pre-stage-dir-name",
    type=str,
    default=PRE_STAGE_NAME_DEFAULT,
    envvar="XINGEST_PRE_STAGE_DIR_NAME",
    help="The name of the directory to use for pre-staging the files (XINGEST_PRE_STAGE_DIR_NAME env. var)",
)
@click.option(
    "--staged-dir-name",
    type=str,
    default=STAGED_NAME_DEFAULT,
    envvar="XINGEST_STAGED_DIR_NAME",
    help="The name of the directory to use for staging the files (XINGEST_STAGED_DIR_NAME env. var)",
)
@click.option(
    "--invalid-dir-name",
    type=str,
    default=INVALID_NAME_DEFAULT,
    envvar="XINGEST_INVALID_DIR_NAME",
    help="The name of the directory to use for invalid files (XINGEST_INVALID_DIR_NAME env. var)",
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
    "--avoid-clashes/--dont-avoid-clashes",
    default=False,
    envvar="XINGEST_AVOID_CLASHES",
    help=(
        "If a resource with the same name already exists in the scan, increment the "
        "resource name by appending _1, _2 etc. to the name until a unique name is found (XINGEST_AVOID_CLASHES env. var)"
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
def stage(
    input_paths: list[str],
    staging_dir: Path,
    datatype: list[MimeType] | None,
    project_field: list[FieldSpec],
    subject_field: list[FieldSpec],
    visit_field: list[FieldSpec],
    session_field: list[FieldSpec] | None,
    scan_id_field: list[FieldSpec],
    scan_desc_field: list[FieldSpec],
    resource_field: list[FieldSpec],
    project_id: str | None,
    delete: bool,
    loggers: ty.List[LoggerConfig],
    additional_loggers: ty.List[str],
    raise_errors: bool,
    xnat_login: XnatLogin,
    pre_stage_dir_name: str,
    staged_dir_name: str,
    invalid_dir_name: str,
    loop: int,
    wait_period: int,
    avoid_clashes: bool,
    recursive: bool,
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
            staging_dir=staging_dir,
            datatypes=datatypes,
            project_field=project_field,
            subject_field=subject_field,
            visit_field=visit_field,
            session_field=session_field,
            scan_id_field=scan_id_field,
            scan_desc_field=scan_desc_field,
            resource_field=resource_field,
            project_id=project_id,
            delete=delete,
            raise_errors=raise_errors,
            copy_mode=copy_mode,
            pre_stage_dir_name=pre_stage_dir_name,
            staged_dir_name=staged_dir_name,
            invalid_dir_name=invalid_dir_name,
            wait_period=wait_period,
            avoid_clashes=avoid_clashes,
            recursive=recursive,
            xnat_login=xnat_login,
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
