from pathlib import Path
import traceback
import click
from tqdm import tqdm
from .base import cli
from ..session import ImagingSession
from ..utils import logger
from .utils import DicomField, LogFile, LogEmail, MailServer, set_logger_handling


@cli.command(
    help="""Stages DICOM and associated files found in the input directories into separate
directories for each session

DICOMS_PATH is either the path to a directory containing the DICOM files to upload, or
a glob pattern that selects the DICOM paths directly

STAGING_DIR is the directory that the files for each session are collated to before they
are uploaded to XNAT
""",
)
@click.argument("dicoms_path", type=str)
@click.argument("staging_dir", type=click.Path(path_type=Path))
@click.option(
    "--project-field",
    type=DicomField,
    default="StudyID",
    envvar="XNAT_INGEST_PROJECT",
    help=("The keyword or tag of the DICOM field to extract the XNAT project ID from "),
)
@click.option(
    "--subject-field",
    type=DicomField,
    default="PatientID",
    envvar="XNAT_INGEST_SUBJECT",
    help=("The keyword or tag of the DICOM field to extract the XNAT subject ID from "),
)
@click.option(
    "--session-field",
    type=DicomField,
    default="AccessionNumber",
    envvar="XNAT_INGEST_SESSION",
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
    "--assoc-files-glob",
    type=str,
    default=None,
    envvar="XNAT_INGEST_NONDICOMSPATTERN",
    help=(
        "Glob pattern by which to detect non-DICOM files that "
        "corresponding to DICOM sessions. Can contain string templates corresponding to "
        "DICOM metadata fields, which are substituted before the glob is called. For "
        'example, "/path/to/non-dicoms/{PatientName.given_name}_{PatientName.family_name}/*)" '
        "will find all files under the subdirectory within '/path/to/non-dicoms/' that matches "
        "<GIVEN-NAME>_<FAMILY-NAME>. Will be interpreted as being relative to `dicoms_dir` "
        "if a relative path is provided."
    ),
)
@click.option(
    "--delete/--dont-delete",
    default=False,
    envvar="XNAT_INGEST_DELETE",
    help="Whether to delete the session directories after they have been uploaded or not",
)
@click.option(
    "--log-level",
    default="info",
    type=str,
    envvar="XNAT_INGEST_LOGLEVEL",
    help=(
        "The level of the logging printed to stdout"
    )
)
@click.option(
    "--log-file",
    default=None,
    type=LogFile,
    envvar="XNAT_INGEST_LOGFILE",
    help=(
        'Location to write the output logs to, defaults to "upload-logs" in the '
        "export directory"
    ),
)
@click.option(
    "--log-email",
    "log_emails",
    type=LogEmail,
    metavar="<address> <loglevel> <subject-preamble>",
    multiple=True,
    envvar="XNAT_INGEST_LOGEMAIL",
    help=(
        "Email(s) to send logs to. When provided in an environment variable, "
        "mail and log level are delimited by ',' and separate destinations by ';'"
    ),
)
@click.option(
    "--mail-server",
    type=MailServer,
    metavar="<host> <sender-email> <user> <password>",
    default=None,
    envvar="XNAT_INGEST_MAILSERVER",
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
def stage(
    dicoms_path: str,
    staging_dir: Path,
    assoc_files_glob: str,
    project_field: str,
    subject_field: str,
    session_field: str,
    project_id: str | None,
    delete: bool,
    log_level: str,
    log_file: Path,
    log_emails: LogEmail,
    mail_server: MailServer,
    raise_errors: bool,
):

    set_logger_handling(log_level, log_file, log_emails, mail_server)

    logger.info(
        "Loading DICOM sessions from '%s' and associated files from '%s'",
        str(dicoms_path),
        str(assoc_files_glob),
    )

    sessions = ImagingSession.construct(
        dicoms_path=dicoms_path,
        associated_files_pattern=assoc_files_glob,
        project_field=project_field,
        subject_field=subject_field,
        session_field=session_field,
        project_id=project_id,
    )

    logger.info("Staging sessions to '%s'", str(staging_dir))

    for session in tqdm(sessions, f"Staging DICOM sessions found in '{dicoms_path}'"):
        try:
            session_staging_dir = staging_dir / session.name
            if session_staging_dir.exists():
                logger.info(
                    "Skipping %s session as staging directory %s already exists",
                    session.name,
                    str(session_staging_dir),
                )
                continue
            session_staging_dir.mkdir(exist_ok=True)
            # Deidentify files and save them to the staging directory
            staged_session = session.deidentify(session_staging_dir)
            staged_session.save(session_staging_dir)
            if delete:
                session.delete()
                logger.info(
                    f"Deleted original '{session.name}' session data after successful upload"
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
