from pathlib import Path
import typing as ty
import traceback
import click
import datetime
import time
import tempfile
from tqdm import tqdm
from fileformats.core import FileSet
from xnat_ingest.cli.base import cli
from xnat_ingest.session import ImagingSession
from frametree.xnat import Xnat  # type: ignore[import-untyped]
from xnat_ingest.utils import (
    AssociatedFiles,
    logger,
    LoggerConfig,
    XnatLogin,
    set_logger_handling,
)

PRE_STAGE_NAME_DEFAULT = "PRE-STAGE"
STAGED_NAME_DEFAULT = "STAGED"
INVALID_NAME_DEFAULT = "INVALID"
DEIDENTIFIED_NAME_DEFAULT = "DEIDENTIFIED"


@cli.command(
    help="""Stages DICOM and associated files found in the input directories into separate
directories for each session

DICOMS_PATH is either the path to a directory containing the DICOM files to upload, or
a glob pattern that selects the DICOM paths directly

STAGING_DIR is the directory that the files for each session are collated to before they
are uploaded to XNAT
""",
)
@click.argument("files_path", type=str, envvar="XINGEST_DICOMS_PATH")
@click.argument("output_dir", type=click.Path(path_type=Path), envvar="XINGEST_DIR")
@click.option(
    "--datatype",
    type=str,
    metavar="<mime-type>",
    multiple=True,
    default=["medimage/dicom-series"],
    envvar="XINGEST_DATATYPE",
    help="The datatype of the primary files to to upload",
)
@click.option(
    "--project-field",
    type=str,
    default="StudyID",
    envvar="XINGEST_PROJECT",
    help=("The keyword of the metadata field to extract the XNAT project ID from "),
)
@click.option(
    "--subject-field",
    type=str,
    default="PatientID",
    envvar="XINGEST_SUBJECT",
    help=("The keyword of the metadata field to extract the XNAT subject ID from "),
)
@click.option(
    "--visit-field",
    type=str,
    default="AccessionNumber",
    envvar="XINGEST_VISIT",
    help=(
        "The keyword of the metadata field to extract the XNAT imaging session ID from "
    ),
)
@click.option(
    "--session-field",
    type=str,
    default=None,
    envvar="XINGEST_SESSION",
    help=(
        "The keyword of the metadata field to extract the XNAT imaging session ID from "
    ),
)
@click.option(
    "--scan-id-field",
    type=str,
    default="SeriesNumber",
    envvar="XINGEST_SCAN_ID",
    help=(
        "The keyword of the metadata field to extract the XNAT imaging scan ID from "
    ),
)
@click.option(
    "--scan-desc-field",
    type=str,
    default="SeriesDescription",
    envvar="XINGEST_SCAN_DESC",
    help=(
        "The keyword of the metadata field to extract the XNAT imaging scan description from "
    ),
)
@click.option(
    "--resource-field",
    type=str,
    default="ImageType[-1]",
    envvar="XINGEST_RESOURCE",
    help=(
        "The keyword of the metadata field to extract the XNAT imaging resource ID from "
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
    ),
)
@click.option(
    "--delete/--dont-delete",
    default=False,
    envvar="XINGEST_DELETE",
    help="Whether to delete the session directories after they have been uploaded or not",
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
    help=("Setup handles to capture logs that are generated"),
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
        "specified here"
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
    help="whether to deidentify the file names and DICOM metadata before staging",
)
@click.option(
    "--xnat-login",
    nargs=3,
    type=XnatLogin.cli_type,
    default=None,
    metavar="<host> <user> <password>",
    help="The XNAT server to upload to plus the user and password to use",
    envvar="XNAT_INGEST_TRANSFER_XNAT_LOGIN",
)
@click.option(
    "--spaces-to-underscores/--no-spaces-to-underscores",
    default=False,
    help="Whether to replace spaces with underscores in the filenames of associated files",
    envvar="XINGEST_SPACES_TO_UNDERSCORES",
    type=bool,
)
@click.option(
    "--work-dir",
    type=click.Path(path_type=Path),
    default=None,
    envvar="XINGEST_WORK_DIR",
    help=(
        "The working directory to use for temporary files. Should be on the same "
        "physical disk as the staging directory for optimal performance"
    ),
)
@click.option(
    "--copy-mode",
    type=FileSet.CopyMode,
    default=FileSet.CopyMode.hardlink_or_copy,
    envvar="XINGEST_COPY_MODE",
    help="The method to use for copying files",
)
@click.option(
    "--loop",
    type=int,
    default=None,
    envvar="XINGEST_LOOP",
    help="Run the staging process continuously every LOOP seconds",
)
@click.option(
    "--pre-stage-dir-name",
    type=str,
    default=PRE_STAGE_NAME_DEFAULT,
    envvar="XINGEST_PRE_STAGE_DIR_NAME",
    help="The name of the directory to use for pre-staging the files",
)
@click.option(
    "--staged-dir-name",
    type=str,
    default=STAGED_NAME_DEFAULT,
    envvar="XINGEST_STAGED_DIR_NAME",
    help="The name of the directory to use for staging the files",
)
@click.option(
    "--invalid-dir-name",
    type=str,
    default=INVALID_NAME_DEFAULT,
    envvar="XINGEST_INVALID_DIR_NAME",
    help="The name of the directory to use for invalid files",
)
@click.option(
    "--deidentified-dir-name",
    type=str,
    default=DEIDENTIFIED_NAME_DEFAULT,
    envvar="XINGEST_DEIDENTIFIED_DIR_NAME",
    help="The name of the directory to use for deidentified files",
)
def stage(
    files_path: str,
    output_dir: Path,
    datatype: str,
    associated_files: ty.List[AssociatedFiles],
    project_field: str,
    subject_field: str,
    visit_field: str,
    session_field: str | None,
    scan_id_field: str,
    scan_desc_field: str,
    resource_field: str,
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
    loop: int | None,
    work_dir: Path | None = None,
) -> None:
    set_logger_handling(
        logger_configs=loggers,
        additional_loggers=additional_loggers,
    )

    if xnat_login:
        xnat_repo = Xnat(
            server=xnat_login.host,
            user=xnat_login.user,
            password=xnat_login.password,
            cache_dir=Path(tempfile.mkdtemp()),
        )
        with xnat_repo.connection:
            project_list = list(xnat_repo.connection.projects)
    else:
        project_list = None

    if session_field is None and datatype == "medimage/dicom-series":
        session_field = "StudyInstanceUID"

    msg = f"Loading {datatype} sessions from '{files_path}'"

    for assoc_files in associated_files:
        msg += f" with associated files selected from '{assoc_files.glob}'"
        if not assoc_files.glob.startswith("/"):
            msg += " (relative to the directories in which the primary files are found)"

    logger.info(msg)

    # Create sub-directories of the output directory for the different phases of the
    # staging process
    prestage_dir = output_dir / pre_stage_dir_name
    staged_dir = output_dir / staged_dir_name
    invalid_dir = output_dir / invalid_dir_name
    prestage_dir.mkdir(parents=True, exist_ok=True)
    staged_dir.mkdir(parents=True, exist_ok=True)
    invalid_dir.mkdir(parents=True, exist_ok=True)
    if deidentify:
        deidentified_dir = output_dir / deidentified_dir_name
        deidentified_dir.mkdir(parents=True, exist_ok=True)

    def do_stage() -> None:
        sessions = ImagingSession.from_paths(
            files_path=files_path,
            project_field=project_field,
            subject_field=subject_field,
            visit_field=visit_field,
            session_field=session_field,
            scan_id_field=scan_id_field,
            scan_desc_field=scan_desc_field,
            resource_field=resource_field,
            project_id=project_id,
        )

        logger.info("Staging sessions to '%s'", str(output_dir))

        for session in tqdm(sessions, f"Staging resources found in '{files_path}'"):
            try:
                if associated_files:
                    session.associate_files(
                        associated_files,
                        spaces_to_underscores=spaces_to_underscores,
                    )
                if deidentify:
                    deidentified_session = session.deidentify(
                        deidentified_dir,
                        copy_mode=copy_mode,
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

    if loop:
        while True:
            start_time = datetime.datetime.now()
            do_stage()
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
        do_stage()


if __name__ == "__main__":
    stage()
