from pathlib import Path
import traceback
import logging.config
import logging.handlers
import click
from tqdm import tqdm
from .base import cli
from ..session import ImagingSession
from ..utils import logger
from .utils import DicomField, LoggerEmail, MailServer


HASH_CHUNK_SIZE = 2**20


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
    "--log-file",
    default=None,
    type=click.Path(path_type=Path),
    envvar="XNAT_INGEST_LOGFILE",
    help=(
        'Location to write the output logs to, defaults to "upload-logs" in the '
        "export directory"
    ),
)
@click.option(
    "--log-email",
    "log_emails",
    type=LoggerEmail,
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
    log_file: Path,
    log_emails: LoggerEmail,
    mail_server: MailServer,
    raise_errors: bool,
):
    # Configure the email logger
    if log_emails:
        if not mail_server:
            raise ValueError(
                "Mail server needs to be provided, either by `--mail-server` option or "
                "XNAT_INGEST_MAILSERVER environment variable if logger emails "
                "are provided: " + ", ".join(log_emails)
            )
        for log_email in log_emails:
            smtp_handler = logging.handlers.SMTPHandler(
                mailhost=mail_server.host,
                fromaddr=mail_server.sender_email,
                toaddrs=[log_email.address],
                subject=log_email.subject,
                credentials=(mail_server.user, mail_server.password),
                secure=None,
            )
            logger.addHandler(smtp_handler)
            logger.info(f"Email logger configured for {log_email}")

    # Configure the file logger
    if log_file is not None:
        log_file.parent.mkdir(exist_ok=True)
        log_file_hdle = logging.FileHandler(log_file)
        log_file_hdle.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(log_file_hdle)

    sessions = ImagingSession.constuct(
        dicoms_path=dicoms_path,
        associated_files_pattern=assoc_files_glob,
        project_field=project_field,
        subject_field=subject_field,
        session_field=session_field,
        project_id=project_id,
    )

    for session in tqdm(
        sessions, f"Staging DICOM sessions found in '{dicoms_path}'"
    ):
        try:
            session_staging_dir = staging_dir / session.name
            if session_staging_dir.exists():
                logger.info(
                    "Skipping %s session as staging directory %s already exists",
                    session.name, str(session_staging_dir)
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
