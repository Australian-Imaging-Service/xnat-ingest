from pathlib import Path
import shutil
import traceback
import tempfile
import click
from tqdm import tqdm
from fileformats.generic import File
from fileformats.medimage import DicomSeries
from arcana.core.data.set import Dataset
from arcana.xnat import Xnat
from .base import cli
from ..session import ImagingSession
from ..utils import (
    logger,
    add_exc_note,
    LogFile,
    LogEmail,
    MailServer,
    set_logger_handling,
    get_checksums,
    calculate_checksums,
)


@cli.command(
    help="""uploads all sessions found in the staging directory (as prepared by the
`stage` sub-command) to XNAT.

STAGING_DIR is the directory that the files for each session are collated to before they
are uploaded to XNAT

SERVER is address of the XNAT server to upload the scans up to. Can alternatively provided
by setting the "XNAT_INGEST_HOST" environment variable.

USER is the XNAT user to connect with, alternatively the "XNAT_INGEST_USER" env. var

PASSWORD is the password for the XNAT user, alternatively "XNAT_INGEST_PASS" env. var
""",
)
@click.argument("staging_dir", type=click.Path(path_type=Path))
@click.argument("server", type=str, envvar="XNAT_INGEST_HOST")
@click.argument("user", type=str, envvar="XNAT_INGEST_USER")
@click.argument("password", type=str, envvar="XNAT_INGEST_PASS")
@click.option(
    "--delete/--dont-delete",
    default=True,
    envvar="XNAT_INGEST_DELETE",
    help="Whether to delete the session directories after they have been uploaded or not",
)
@click.option(
    "--log-level",
    default="info",
    type=str,
    envvar="XNAT_INGEST_LOGLEVEL",
    help=("The level of the logging printed to stdout"),
)
@click.option(
    "--log-file",
    default=None,
    type=LogFile(),
    nargs=2,
    metavar="<path> <loglevel>",
    envvar="XNAT_INGEST_LOGFILE",
    help=(
        'Location to write the output logs to, defaults to "upload-logs" in the '
        "export directory"
    ),
)
@click.option(
    "--log-email",
    "log_emails",
    type=LogEmail(),
    nargs=3,
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
    type=MailServer(),
    metavar="<host> <sender-email> <user> <password>",
    default=None,
    envvar="XNAT_INGEST_MAILSERVER",
    help=(
        "the mail server to send logger emails to. When provided in an environment variable, "
        "args are delimited by ';'"
    ),
)
@click.option(
    "--always-include",
    "-i",
    default=None,
    type=click.Choice(("all", "dicom", "associated"), case_sensitive=False),
    envvar="XNAT_INGEST_ALWAYSINCLUDE",
    help=(
        "Whether to include scans in the upload regardless of whether they are "
        "specified in a column or not"
    ),
)
@click.option(
    "--raise-errors/--dont-raise-errors",
    default=False,
    type=bool,
    help="Whether to raise errors instead of logging them (typically for debugging)",
)
def upload(
    staging_dir: Path,
    server: str,
    user: str,
    password: str,
    delete: bool,
    log_level: str,
    log_file: Path,
    log_emails: LogEmail,
    mail_server: MailServer,
    always_include: str,
    raise_errors: bool,
):

    set_logger_handling(log_level, log_file, log_emails, mail_server)

    xnat_repo = Xnat(
        server=server, user=user, password=password, cache_dir=Path(tempfile.mkdtemp())
    )

    with xnat_repo.connection:
        for session_staging_dir in tqdm(
            list(staging_dir.iterdir()),
            f"Processing staged sessions found in '{str(staging_dir)}' directory",
        ):
            session = ImagingSession.load(session_staging_dir)
            try:
                if "MR" in session.modalities:
                    SessionClass = xnat_repo.connection.classes.MrSessionData
                    default_scan_modality = "MR"
                elif "PT" in session.modalities:
                    SessionClass = xnat_repo.connection.classes.PetSessionData
                    default_scan_modality = "PT"
                elif "CT" in session.modalities:
                    SessionClass = xnat_repo.connection.classes.CtSessionData
                    default_scan_modality = "CT"
                else:
                    raise RuntimeError(
                        f"Found the following unsupported modalities {session.modalities}, "
                        "in the session. Must contain one of 'MR', 'PT' or 'CT'"
                    )

                # Create corresponding session on XNAT
                xproject = xnat_repo.connection.projects[session.project_id]
                xsubject = xnat_repo.connection.classes.SubjectData(
                    label=session.subject_id, parent=xproject
                )
                try:
                    xsession = xproject.experiments[session.session_id]
                except KeyError:
                    if "MR" in session.modalities:
                        SessionClass = xnat_repo.connection.classes.MrSessionData
                    elif "PT" in session.modalities:
                        SessionClass = xnat_repo.connection.classes.PetSessionData
                    elif "CT" in session.modalities:
                        SessionClass = xnat_repo.connection.classes.CtSessionData
                    else:
                        raise RuntimeError(
                            "Found the following unsupported modalities in "
                            f"{session.name}: {session.modalities}"
                        )
                    xsession = SessionClass(label=session.session_id, parent=xsubject)
                session_path = (
                    f"{session.project_id}:{session.subject_id}:{session.session_id}"
                )

                # Access Arcana dataset associated with project
                try:
                    dataset = Dataset.load(session.project_id, xnat_repo)
                except Exception as e:
                    add_exc_note(
                        e,
                        f"Did not load dataset definition from {session.project_id} project "
                        f"on {server}. Please set one up using the Arcana command line tool "
                        "in order to check presence of required scans and associated "
                        "files (e.g. raw-data exports)",
                    )
                    raise e

                # Anonymise DICOMs and save to directory prior to upload
                if always_include:
                    logger.info(
                        f"Including {always_include} scans/files in upload from '{session.name}' to "
                        f"{session_path} regardless of whether they are explicitly specified"
                    )

                for scan_id, scan_type, resource_name, scan in tqdm(
                    session.select_resources(
                        dataset,
                        always_include=always_include,
                    ),
                    f"Uploading scans found in {session.name}",
                ):
                    if scan.metadata:
                        image_type = scan.metadata.get("ImageType")
                        if image_type and image_type[:2] == ["DERIVED", "SECONDARY"]:
                            modality = "SC"
                        else:
                            modality = scan.metadata.get(
                                "Modality", default_scan_modality
                            )
                    else:
                        modality = default_scan_modality
                    if modality == "SC":
                        ScanClass = xnat_repo.connection.classes.ScScanData
                    elif modality == "MR":
                        ScanClass = xnat_repo.connection.classes.MrScanData
                    elif modality == "PT":
                        ScanClass = xnat_repo.connection.classes.PetScanData
                    elif modality == "CT":
                        ScanClass = xnat_repo.connection.classes.CtScanData
                    else:
                        raise RuntimeError(
                            f"Unsupported image modality '{scan.modality}'"
                        )
                    xscan = ScanClass(id=scan_id, type=scan_type, parent=xsession)
                    xresource = xscan.create_resource(resource_name)
                    if isinstance(scan, File):
                        for fspath in scan.fspaths:
                            xresource.upload(str(fspath), fspath.name)
                    else:
                        xresource.upload_dir(scan.parent)
                    remote_checksums = get_checksums(xresource)
                    calc_checksums = calculate_checksums(scan)
                    if remote_checksums != calc_checksums:
                        mismatching = [
                            k
                            for k, v in remote_checksums.items()
                            if v != calc_checksums[k]
                        ]
                        raise RuntimeError(
                            "Checksums do not match after upload of "
                            f"'{session.name}:{scan_id}:{resource_name}' resource. "
                            f"Mismatching files were {mismatching}"
                        )
                    logger.info(f"Uploaded '{scan_id}' in '{session.name}'")
                logger.info(f"Successfully uploaded all files in '{session.name}'")
                # Extract DICOM metadata
                logger.info("Extracting metadata from DICOMs on XNAT..")
                xnat_repo.connection.put(
                    f"/data/experiments/{xsession.id}?pullDataFromHeaders=true"
                )
                xnat_repo.connection.put(
                    f"/data/experiments/{xsession.id}?fixScanTypes=true"
                )
                xnat_repo.connection.put(
                    f"/data/experiments/{xsession.id}?triggerPipelines=true"
                )

                msg = f"Succesfully uploaded all files in '{session.name}'"
                if delete:
                    msg += ", deleting originals..."
                logger.info(msg)
                if delete:
                    shutil.rmtree(session_staging_dir)
                    logger.info(
                        f"Deleted staging dir '{str(session_staging_dir)}' session data "
                        "after successful upload"
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
