from pathlib import Path
import typing as ty
import traceback
import click
import tempfile
from tqdm import tqdm
from xnat_ingest.cli.base import cli
from xnat_ingest.session import ImagingSession
from arcana.xnat import Xnat
from xnat_ingest.utils import (
    DicomField,
    AssociatedFiles,
    logger,
    LogFile,
    LogEmail,
    MailServer,
    XnatLogin,
    set_logger_handling,
)


@cli.command(
    help="""Stages DICOM and associated files found in the input directories into separate
directories for each session

DICOMS_PATH is either the path to a directory containing the DICOM files to upload, or
a glob pattern that selects the DICOM paths directly

STAGING_DIR is the directory that the files for each session are collated to before they
are uploaded to XNAT
""",
)
@click.argument("dicoms_path", type=str, envvar="XNAT_INGEST_STAGE_DICOMS_PATH")
@click.argument(
    "staging_dir", type=click.Path(path_type=Path), envvar="XNAT_INGEST_STAGE_DIR"
)
@click.option(
    "--project-field",
    type=DicomField,
    default="StudyID",
    envvar="XNAT_INGEST_STAGE_PROJECT",
    help=("The keyword or tag of the DICOM field to extract the XNAT project ID from "),
)
@click.option(
    "--subject-field",
    type=DicomField,
    default="PatientID",
    envvar="XNAT_INGEST_STAGE_SUBJECT",
    help=("The keyword or tag of the DICOM field to extract the XNAT subject ID from "),
)
@click.option(
    "--visit-field",
    type=DicomField,
    default="AccessionNumber",
    envvar="XNAT_INGEST_STAGE_SESSION",
    help=(
        "The keyword or tag of the DICOM field to extract the XNAT imaging session ID from "
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
    nargs=2,
    default=None,
    envvar="XNAT_INGEST_STAGE_ASSOCIATED",
    metavar="<glob> <id-pattern>",
    help=(
        'The "glob" arg is a glob pattern by which to detect associated files to be '
        "attached to the DICOM sessions. Note that when this pattern corresponds to a "
        "relative path it is considered to be relative to the parent directory containing "
        "the DICOMs for the session NOT the current working directory Can contain string "
        "templates corresponding to DICOM metadata fields, which are substituted before "
        "the glob is called. For example, "
        '"./associated/{PatientName.given_name}_{PatientName.family_name}/*)" '
        "will find all files under the subdirectory within '/path/to/dicoms/associated' that matches "
        "<GIVEN-NAME>_<FAMILY-NAME>. Will be interpreted as being relative to `dicoms_dir` "
        "if a relative path is provided.\n"
        'The "id-pattern" arg is a regular expression that is used to extract the scan ID & '
        "type/resource from the associated filename. Should be a regular-expression "
        "(Python syntax) with named groups called 'id' and 'type', e.g. "
        r"--assoc-id-pattern '[^\.]+\.[^\.]+\.(?P<id>\d+)\.(?P<type>\w+)\..*'"
    ),
)
@click.option(
    "--delete/--dont-delete",
    default=False,
    envvar="XNAT_INGEST_STAGE_DELETE",
    help="Whether to delete the session directories after they have been uploaded or not",
)
@click.option(
    "--log-level",
    default="info",
    type=str,
    envvar="XNAT_INGEST_STAGE_LOGLEVEL",
    help=("The level of the logging printed to stdout"),
)
@click.option(
    "--log-file",
    "log_files",
    default=None,
    type=LogFile.cli_type,
    multiple=True,
    nargs=2,
    metavar="<path> <loglevel>",
    envvar="XNAT_INGEST_STAGE_LOGFILE",
    help=(
        'Location to write the output logs to, defaults to "upload-logs" in the '
        "export directory"
    ),
)
@click.option(
    "--log-email",
    "log_emails",
    type=LogEmail.cli_type,
    nargs=3,
    metavar="<address> <loglevel> <subject-preamble>",
    multiple=True,
    envvar="XNAT_INGEST_STAGE_LOGEMAIL",
    help=(
        "Email(s) to send logs to. When provided in an environment variable, "
        "mail and log level are delimited by ',' and separate destinations by ';'"
    ),
)
@click.option(
    "--mail-server",
    type=MailServer.cli_type,
    nargs=4,
    metavar="<host> <sender-email> <user> <password>",
    default=None,
    envvar="XNAT_INGEST_STAGE_MAILSERVER",
    help=(
        "the mail server to send logger emails to. When provided in an environment variable, "
        "args are delimited by ';'"
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
    envvar="XNAT_INGEST_STAGE_DEIDENTIFY",
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
def stage(
    dicoms_path: str,
    staging_dir: Path,
    associated_files: AssociatedFiles,
    project_field: DicomField,
    subject_field: DicomField,
    visit_field: DicomField,
    project_id: str | None,
    delete: bool,
    log_level: str,
    log_files: ty.List[LogFile],
    log_emails: ty.List[LogEmail],
    mail_server: MailServer,
    raise_errors: bool,
    deidentify: bool,
    xnat_login: XnatLogin,
):
    set_logger_handling(
        log_level=log_level,
        log_emails=log_emails,
        log_files=log_files,
        mail_server=mail_server,
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

    msg = f"Loading DICOM sessions from '{dicoms_path}'"

    if associated_files:
        msg += f" with associated files selected from '{associated_files.glob}'"
        if not associated_files.glob.startswith("/"):
            msg += " (relative to the directories in which the DICOMs are found)"

    logger.info(msg)

    sessions = ImagingSession.from_dicoms(
        dicoms_path=dicoms_path,
        project_field=project_field,
        subject_field=subject_field,
        visit_field=visit_field,
        project_id=project_id,
    )

    logger.info("Staging sessions to '%s'", str(staging_dir))

    for session in tqdm(sessions, f"Staging DICOM sessions found in '{dicoms_path}'"):
        try:
            session_staging_dir = staging_dir.joinpath(*session.staging_relpath)
            if session_staging_dir.exists():
                logger.info(
                    "Skipping %s session as staging directory %s already exists",
                    session.name,
                    str(session_staging_dir),
                )
                continue
            # Identify theDeidentify files if necessary and save them to the staging directory
            session.stage(
                staging_dir,
                associated_files=associated_files,
                remove_original=delete,
                deidentify=deidentify,
                project_list=project_list,
            )
        except Exception as e:
            if not raise_errors:
                logger.error(
                    f"Skipping '{session.name}' session due to error in staging: \"{e}\""
                    f"\n{traceback.format_exc()}\n\n"
                )
                continue
            else:
                raise


if __name__ == "__main__":
    stage()
