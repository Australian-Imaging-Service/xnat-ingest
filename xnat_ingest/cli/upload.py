from pathlib import Path
import shutil
import traceback
import typing as ty
from collections import defaultdict
import tempfile
import click
from tqdm import tqdm
import boto3
from fileformats.generic import File
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

STAGED is either a directory that the files for each session are collated to before they
are uploaded to XNAT or an S3 bucket to download the files from.

SERVER is address of the XNAT server to upload the scans up to. Can alternatively provided
by setting the "XNAT_INGEST_HOST" environment variable.

USER is the XNAT user to connect with, alternatively the "XNAT_INGEST_USER" env. var

PASSWORD is the password for the XNAT user, alternatively "XNAT_INGEST_PASS" env. var
""",
)
@click.argument("staged", type=str)
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
@click.option(
    "--store-credentials",
    type=str,
    metavar="<access-key> <secret-key>",
    envvar="XNAT_INGEST_STORE_CREDENTIALS",
    default=None,
    nargs=2,
    help="Credentials to use to access of data stored in remote stores (e.g. AWS S3)",
)
@click.option(
    "--work-dir",
    type=Path,
    default=None,
    envvar="XNAT_INGEST_WORKDIR",
    help="The directory to use for temporary downloads (i.e. from s3)",
)
@click.option(
    "--use-manifest/--dont-use-manifest",
    default=None,
    envvar="XNAT_INGEST_REQUIRE_MANIFEST",
    help=(
        "Whether to use the manifest file in the staged sessions to load the "
        "directory structure. By default it is used if present and ignore if not there"
    ),
    type=bool,
)
def upload(
    staged: str,
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
    store_credentials: ty.Tuple[str, str],
    work_dir: ty.Optional[Path],
    use_manifest: bool,
):

    set_logger_handling(log_level, log_file, log_emails, mail_server)

    xnat_repo = Xnat(
        server=server, user=user, password=password, cache_dir=Path(tempfile.mkdtemp())
    )

    with xnat_repo.connection:

        def xnat_session_exists(project_id, subject_id, visit_id):
            try:
                xnat_repo.connection.projects[project_id].subjects[
                    subject_id
                ].experiments[
                    ImagingSession.make_session_id(project_id, subject_id, visit_id)
                ]
            except KeyError:
                return False
            else:
                logger.info(
                    "Skipping session '%s-%s-%s' as it already exists on XNAT",
                    project_id,
                    subject_id,
                    visit_id,
                )
                return True

        if staged.startswith("s3://"):
            # List sessions stored in s3 bucket
            s3 = boto3.resource(
                "s3",
                aws_access_key_id=store_credentials[0],
                aws_secret_access_key=store_credentials[1],
            )
            bucket_name, prefix = staged[5:].split("/", 1)
            bucket = s3.Bucket(bucket_name)
            all_objects = bucket.objects.filter(Prefix=prefix)
            session_objs = defaultdict(list)
            for obj in all_objects:
                if obj.key.endswith("/"):
                    continue
                path_parts = obj.key[len(prefix) :].split("/")
                session_ids = tuple(path_parts[:3])
                session_objs[session_ids].append((path_parts[3:], obj))

            session_objs = {
                ids: objs
                for ids, objs in session_objs.items()
                if not xnat_session_exists(*ids)
            }

            num_sessions = len(session_objs)

            if work_dir:
                tmp_download_dir = work_dir / "xnat-ingest-download"
                tmp_download_dir.mkdir(parents=True, exist_ok=True)
            else:
                tmp_download_dir = Path(tempfile.mkdtemp())

            def iter_staged_sessions():
                for ids, objs in session_objs.items():
                    # Just in case the manifest file is not included in the list of objects
                    # we recreate the project/subject/sesssion directory structure
                    session_tmp_dir = tmp_download_dir.joinpath(*ids)
                    session_tmp_dir.mkdir(parents=True, exist_ok=True)
                    for relpath, obj in tqdm(
                        objs,
                        desc=f"Downloading scans in {':'.join(ids)} session from S3 bucket",
                    ):
                        obj_path = session_tmp_dir.joinpath(*relpath)
                        obj_path.parent.mkdir(parents=True, exist_ok=True)
                        logger.debug("Downloading %s to %s", obj, obj_path)
                        with open(obj_path, "wb") as f:
                            bucket.download_fileobj(obj.key, f)
                    yield session_tmp_dir
                    shutil.rmtree(
                        session_tmp_dir
                    )  # Delete the tmp session after the upload

            logger.info("Found %d sessions in S3 bucket '%s'", num_sessions, staged)
            sessions = iter_staged_sessions()
            logger.debug("Created sessions iterator")
        else:
            sessions = []
            for project_dir in Path(staged).iterdir():
                for subject_dir in project_dir.iterdir():
                    for session_dir in subject_dir.iterdir():
                        if not xnat_session_exists(
                            project_dir.name, subject_dir.name, session_dir.name
                        ):
                            sessions.append(session_dir)
            num_sessions = len(sessions)
            logger.info(
                "Found %d sessions in staging directory '%s'", num_sessions, staged
            )

        for session_staging_dir in tqdm(
            sessions,
            total=num_sessions,
            desc=f"Processing staged sessions found in '{staged}'",
        ):
            session = ImagingSession.load(
                session_staging_dir, use_manifest=use_manifest
            )
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
                    f"{session.project_id}:{session.subject_id}:{session.visit_id}"
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
