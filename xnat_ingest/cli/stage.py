import datetime
import tempfile
import time
import traceback
import typing as ty
from pathlib import Path

import click
from fileformats.core import FileSet
from fileformats.medimage import DicomSeries
from frametree.xnat import Xnat  # type: ignore[import-untyped]
from tqdm import tqdm

from xnat_ingest.cli.base import cli
from xnat_ingest.session import ImagingSession
from xnat_ingest.utils import (
    AssociatedFiles,
    FieldSpec,
    LoggerConfig,
    MimeType,
    XnatLogin,
    logger,
    set_logger_handling,
)

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


@cli.command(
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
    "--associated-files",
    type=AssociatedFiles.cli_type,
    nargs=3,
    default=None,
    multiple=True,
    envvar="XINGEST_ASSOCIATED",
    metavar="<datatype> <glob> <id-pattern>",
    help=(
        'The "glob" arg is a glob pattern by which to detect associated files to be '
        "attached to the DICOM sessions. Note that when this pattern corresponds to a "
        "relative path it is considered to be relative to the parent directory containing "
        "the DICOMs for the session NOT the current working directory Can contain string "
        "templates corresponding to DICOM metadata fields, which are substituted before "
        "the glob is called. For example, "
        '"./associated/{PatientName.family_name}_{PatientName.given_name}/*)" '
        "will find all files under the subdirectory within '/path/to/dicoms/associated' that matches "
        "<GIVEN-NAME>_<FAMILY-NAME>. Will be interpreted as being relative to `dicoms_dir` "
        "if a relative path is provided.\n"
        'The "id-pattern" arg is a regular expression that is used to extract the scan ID & '
        "type/resource from the associated filename. Should be a regular-expression "
        "(Python syntax) with named groups called 'id' and 'type', e.g. "
        r"'[^\.]+\.[^\.]+\.(?P<id>\d+)\.(?P<type>\w+)\..*'"
        "(XINGEST_ASSOCIATED env. var)"
    ),
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
    "--deidentify/--dont-deidentify",
    default=False,
    type=bool,
    envvar="XINGEST_DEIDENTIFY",
    help="whether to deidentify the file names and DICOM metadata before staging (XINGEST_DEIDENTIFY env. var)",
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
    "--spaces-to-underscores/--no-spaces-to-underscores",
    default=False,
    help="Whether to replace spaces with underscores in the filenames of associated files (XINGEST_SPACES_TO_UNDERSCORES env. var)",
    envvar="XINGEST_SPACES_TO_UNDERSCORES",
    type=bool,
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
    "--deidentified-dir-name",
    type=str,
    default=DEIDENTIFIED_NAME_DEFAULT,
    envvar="XINGEST_DEIDENTIFIED_DIR_NAME",
    help="The name of the directory to use for deidentified files (XINGEST_DEIDENTIFIED_DIR_NAME env. var)",
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
def stage(
    input_paths: list[str],
    staging_dir: Path,
    datatype: list[MimeType] | None,
    associated_files: ty.List[AssociatedFiles],
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
    deidentify: bool,
    xnat_login: XnatLogin,
    spaces_to_underscores: bool,
    copy_mode: FileSet.CopyMode,
    pre_stage_dir_name: str,
    staged_dir_name: str,
    invalid_dir_name: str,
    deidentified_dir_name: str,
    loop: int,
    wait_period: int,
    avoid_clashes: bool,
    recursive: bool,
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

    if xnat_login:
        logger.info(
            "Logging into XNAT server '%s' as user '%s' to check project IDs",
            xnat_login.host,
            xnat_login.user,
        )
        xnat_repo = Xnat(
            server=xnat_login.host,
            user=xnat_login.user,
            password=xnat_login.password,
            cache_dir=Path(tempfile.mkdtemp()),
        )
        with xnat_repo.connection:
            project_list = [p.name for p in xnat_repo.connection.projects]
    else:
        logger.info("No XNAT login provided, will not check project IDs in XNAT")
        project_list = None

    msg = f"Loading {list(datatypes)} sessions from '{input_paths}'"

    for assoc_files in associated_files:
        msg += f" with associated files selected from '{assoc_files.glob}'"
        if not assoc_files.glob.startswith("/"):
            msg += " (relative to the directories in which the primary files are found)"

    logger.info(msg)

    # Create sub-directories of the output directory for the different phases of the
    # staging process
    prestage_dir = staging_dir / pre_stage_dir_name
    staged_dir = staging_dir / staged_dir_name
    invalid_dir = staging_dir / invalid_dir_name
    prestage_dir.mkdir(parents=True, exist_ok=True)
    staged_dir.mkdir(parents=True, exist_ok=True)
    invalid_dir.mkdir(parents=True, exist_ok=True)
    if deidentify:
        deidentified_dir = staging_dir / deidentified_dir_name
        deidentified_dir.mkdir(parents=True, exist_ok=True)

    def do_stage() -> None:
        sessions = ImagingSession.from_paths(
            files_path=input_paths,
            datatypes=datatypes,
            project_field=project_field,
            subject_field=subject_field,
            visit_field=visit_field,
            session_field=session_field,
            scan_id_field=scan_id_field,
            scan_desc_field=scan_desc_field,
            resource_field=resource_field,
            project_id=project_id,
            avoid_clashes=avoid_clashes,
            recursive=recursive,
        )

        logger.info("Staging sessions to '%s'", str(staging_dir))

        for session in tqdm(sessions, f"Staging resources found in '{input_paths}'"):

            if wait_period:
                last_mod = session.last_modified()
                if (time.time_ns() - last_mod) < wait_period * 1e9:
                    logger.info(
                        "Skipping staging of session '%s' as it was last modified "
                        "at %s which is less than %s seconds ago",
                        session.name,
                        last_mod,
                        wait_period,
                    )
                    continue

            try:
                if associated_files:
                    session.associate_files(
                        associated_files,
                        spaces_to_underscores=spaces_to_underscores,
                        avoid_clashes=avoid_clashes,
                    )
                if deidentify:
                    deidentified_session = session.deidentify(
                        deidentified_dir,
                        copy_mode=copy_mode,
                        avoid_clashes=avoid_clashes,
                    )
                    if delete:
                        session.unlink()
                    session = deidentified_session
                # We save the session into a temporary "pre-stage" directory first before
                # moving them into the final "staged" directory. This is to prevent the
                # files being transferred/deleted until the saved session is in a final state.
                _, saved_dir = session.save(
                    prestage_dir,
                    available_projects=project_list,
                    copy_mode=copy_mode,
                )
                logger.info(
                    "Successfully staged session '%s' to '%s'",
                    session.name,
                    str(saved_dir),
                )
                if "INVALID" in saved_dir.name:
                    saved_dir.rename(invalid_dir / saved_dir.relative_to(prestage_dir))
                else:
                    saved_dir.rename(staged_dir / saved_dir.relative_to(prestage_dir))
                if delete:
                    session.unlink()
            except Exception as e:
                if not raise_errors:
                    logger.error(
                        f"Skipping '{session.name}' session due to error in staging: \"{e}\""
                        f"\n{traceback.format_exc()}\n\n"
                    )
                    continue
                else:
                    raise

    if loop >= 0:
        while True:
            start_time = datetime.datetime.now()
            try:
                do_stage()
            except Exception as e:
                logger.error(
                    f'Error attempting to prepare stage of sessions: "{e}"'
                    f"\n{traceback.format_exc()}\n\n"
                )
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
    else:
        try:
            do_stage()
        except Exception as e:
            if not raise_errors:
                logger.error(
                    f'Error attempting to prepare stage of sessions: "{e}"'
                    f"\n{traceback.format_exc()}\n\n"
                )
            else:
                raise
        logger.info("Staging completed successfully")


if __name__ == "__main__":
    stage()
if __name__ == "__main__":
    stage()
