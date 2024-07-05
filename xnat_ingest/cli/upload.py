from pathlib import Path
import shutil
import os
import datetime
import traceback
import typing as ty
from collections import defaultdict
import tempfile
from operator import itemgetter
import subprocess as sp
import click
from tqdm import tqdm
from natsort import natsorted
import xnat
import boto3
import paramiko
from fileformats.generic import File
from arcana.core.data.set import Dataset
from arcana.xnat import Xnat
from xnat.exceptions import XNATResponseError
from xnat_ingest.cli.base import cli
from xnat_ingest.session import ImagingSession
from xnat_ingest.utils import (
    logger,
    LogFile,
    LogEmail,
    MailServer,
    set_logger_handling,
    get_checksums,
    calculate_checksums,
    StoreCredentials,
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
@click.argument("staged", type=str, envvar="XNAT_INGEST_UPLOAD_STAGED")
@click.argument("server", type=str, envvar="XNAT_INGEST_UPLOAD_HOST")
@click.argument("user", type=str, envvar="XNAT_INGEST_UPLOAD_USER")
@click.option("--password", default=None, type=str, envvar="XNAT_INGEST_UPLOAD_PASS")
@click.option(
    "--log-level",
    default="info",
    type=str,
    envvar="XNAT_INGEST_UPLOAD_LOGLEVEL",
    help=("The level of the logging printed to stdout"),
)
@click.option(
    "--log-file",
    "log_files",
    default=None,
    type=LogFile.cli_type,
    nargs=2,
    metavar="<path> <loglevel>",
    multiple=True,
    envvar="XNAT_INGEST_UPLOAD_LOGFILE",
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
    envvar="XNAT_INGEST_UPLOAD_LOGEMAIL",
    help=(
        "Email(s) to send logs to. When provided in an environment variable, "
        "mail and log level are delimited by ',' and separate destinations by ';'"
    ),
)
@click.option(
    "--add-logger",
    type=str,
    multiple=True,
    default=(),
    envvar="XNAT_INGEST_UPLOAD_LOGGERS",
    help=(
        "The loggers to use for logging. By default just the 'xnat-ingest' logger is used. "
        "But additional loggers can be included (e.g. 'xnat') can be "
        "specified here"
    ),
)
@click.option(
    "--mail-server",
    type=MailServer.cli_type,
    metavar="<host> <sender-email> <user> <password>",
    default=None,
    envvar="XNAT_INGEST_UPLOAD_MAILSERVER",
    help=(
        "the mail server to send logger emails to. When provided in an environment variable, "
        "args are delimited by ';'"
    ),
)
@click.option(
    "--always-include",
    "-i",
    default=(),
    type=str,
    multiple=True,
    envvar="XNAT_INGEST_UPLOAD_ALWAYSINCLUDE",
    help=(
        "Scan types to always include in the upload, regardless of whether they are"
        "specified in a column or not. Specified using the scan types IANA mime-type or "
        'fileformats "mime-like" (see https://arcanaframework.github.io/fileformats/), '
        "e.g. 'application/json', 'medimage/dicom-series', "
        "'image/jpeg'). Use 'all' to include all file-types in the session"
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
    type=StoreCredentials.cli_type,
    metavar="<access-key> <secret-key>",
    envvar="XNAT_INGEST_UPLOAD_STORE_CREDENTIALS",
    default=None,
    nargs=2,
    help="Credentials to use to access of data stored in remote stores (e.g. AWS S3)",
)
@click.option(
    "--temp-dir",
    type=Path,
    default=None,
    envvar="XNAT_INGEST_UPLOAD_TEMPDIR",
    help="The directory to use for temporary downloads (i.e. from s3)",
)
@click.option(
    "--use-manifest/--dont-use-manifest",
    default=None,
    envvar="XNAT_INGEST_UPLOAD_REQUIRE_MANIFEST",
    help=(
        "Whether to use the manifest file in the staged sessions to load the "
        "directory structure. By default it is used if present and ignore if not there"
    ),
    type=bool,
)
@click.option(
    "--clean-up-older-than",
    type=int,
    metavar="<days>",
    envvar="XNAT_INGEST_UPLOAD_CLEANUP_OLDER_THAN",
    default=0,
    help="The number of days to keep files in the remote store for",
)
@click.option(
    "--verify-ssl/--dont-verify-ssl",
    type=bool,
    default=True,
    envvar="XNAT_INGEST_UPLOAD_VERIFY_SSL",
    help="Whether to verify the SSL certificate of the XNAT server",
)
@click.option(
    "--use-curl-jsession/--dont-use-curl-jsession",
    type=bool,
    default=False,
    envvar="XNAT_INGEST_UPLOAD_USE_CURL_JSESSION",
    help=(
        "Whether to use CURL to create a JSESSION token to authenticate with XNAT. This is "
        "used to work around a strange authentication issue when running within a Kubernetes "
        "cluster and targeting the XNAT Tomcat directly"
    ),
)
def upload(
    staged: str,
    server: str,
    user: str,
    password: str,
    log_level: str,
    log_files: ty.List[LogFile],
    log_emails: ty.List[LogEmail],
    mail_server: MailServer,
    always_include: ty.Sequence[str],
    add_logger: ty.List[str],
    raise_errors: bool,
    store_credentials: ty.Tuple[str, str],
    temp_dir: ty.Optional[Path],
    use_manifest: bool,
    clean_up_older_than: int,
    verify_ssl: bool,
    use_curl_jsession: bool,
):

    set_logger_handling(
        log_level=log_level,
        log_emails=log_emails,
        log_files=log_files,
        mail_server=mail_server,
        add_logger=add_logger,
    )
    if temp_dir:
        tempfile.tempdir = str(temp_dir)

    xnat_repo = Xnat(
        server=server,
        user=user,
        password=password,
        cache_dir=Path(tempfile.mkdtemp()),
        verify_ssl=verify_ssl,
    )

    if use_curl_jsession:
        jsession = sp.check_output(
            [
                "curl",
                "-X",
                "PUT",
                "-d",
                f"username={user}&password={password}",
                f"{server}/data/services/auth",
            ]
        ).decode("utf-8")
        xnat_repo.connection.depth = 1
        xnat_repo.connection.session = xnat.connect(
            server, user=user, jsession=jsession
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

        project_ids = set()

        if staged.startswith("s3://"):
            # List sessions stored in s3 bucket
            s3 = boto3.resource(
                "s3",
                aws_access_key_id=store_credentials.access_key,
                aws_secret_access_key=store_credentials.access_secret,
            )
            bucket_name, prefix = staged[5:].split("/", 1)
            bucket = s3.Bucket(bucket_name)
            if not prefix.endswith("/"):
                prefix += "/"
            all_objects = bucket.objects.filter(Prefix=prefix)
            session_objs = defaultdict(list)
            for obj in all_objects:
                if obj.key.endswith("/"):
                    continue  # skip directories
                path_parts = obj.key[len(prefix) :].split("/")
                session_ids = tuple(path_parts[:3])
                project_ids.add(session_ids[0])
                session_objs[session_ids].append((path_parts[3:], obj))

            for ids, objs in list(session_objs.items()):
                if xnat_session_exists(*ids):
                    logger.info(
                        "Skipping session '%s' as it already exists on XNAT", ids
                    )
                    del session_objs[ids]

            num_sessions = len(session_objs)

            if temp_dir:
                tmp_download_dir = temp_dir / "xnat-ingest-download"
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
                project_ids.add(project_dir.name)
            num_sessions = len(sessions)
            logger.info(
                "Found %d sessions in staging directory '%s'", num_sessions, staged
            )

        # Check for dataset definitions on XNAT if an always_include option is not
        # provided
        if not always_include:
            missing_datasets = set()
            for project_id in project_ids:
                try:
                    dataset = Dataset.load(project_id, xnat_repo)
                except Exception:
                    missing_datasets.add(project_id)
                else:
                    logger.debug(
                        "Found dataset definition for '%s' project", project_id
                    )
            if missing_datasets:
                raise ValueError(
                    "Either an '--always-include' option must be provided or dataset "
                    "definitions must be present on XNAT for the following projects "
                    f"({missing_datasets}) in order to upload the sessions"
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

                # Access Arcana dataset associated with project
                try:
                    dataset = Dataset.load(session.project_id, xnat_repo)
                except Exception as e:
                    logger.warning(
                        "Did not load dataset definition (%s) from %s project "
                        "on %s. Only the scan types specified in --always-include",
                        e,
                        session.project_id,
                        server,
                    )
                    dataset = None

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

                # Anonymise DICOMs and save to directory prior to upload
                if always_include:
                    logger.info(
                        f"Including {always_include} scans/files in upload from '{session.name}' to "
                        f"{session_path} regardless of whether they are explicitly specified"
                    )

                for scan_id, scan_type, resource_name, scan in tqdm(
                    natsorted(
                        session.select_resources(
                            dataset,
                            always_include=always_include,
                        ),
                        key=itemgetter(0),
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
                        if SessionClass is xnat_repo.connection.classes.PetSessionData:
                            ScanClass = xnat_repo.connection.classes.PetScanData
                        elif SessionClass is xnat_repo.connection.classes.CtSessionData:
                            ScanClass = xnat_repo.connection.classes.CtScanData
                        else:
                            ScanClass = xnat_repo.connection.classes.MrScanData
                        logger.info(
                            "Can't determine modality of %s-%s scan, defaulting to the "
                            "default for %s sessions, %s",
                            scan_id,
                            scan_type,
                            SessionClass,
                            ScanClass,
                        )
                    logger.debug("Creating scan %s in %s", scan_id, session_path)
                    xscan = ScanClass(id=scan_id, type=scan_type, parent=xsession)
                    logger.debug(
                        "Creating resource %s in %s in %s",
                        resource_name,
                        scan_id,
                        session_path,
                    )
                    xresource = xscan.create_resource(resource_name)
                    if isinstance(scan, File):
                        for fspath in scan.fspaths:
                            xresource.upload(str(fspath), fspath.name)
                    else:
                        xresource.upload_dir(scan.parent)
                    logger.debug("retrieving checksums for %s", xresource)
                    remote_checksums = get_checksums(xresource)
                    logger.debug("calculating checksums for %s", xresource)
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
                try:
                    xnat_repo.connection.put(
                        f"/data/experiments/{xsession.id}?pullDataFromHeaders=true"
                    )
                except XNATResponseError as e:
                    logger.warning(
                        f"Failed to extract metadata from DICOMs in '{session.name}': {e}"
                    )
                try:
                    xnat_repo.connection.put(
                        f"/data/experiments/{xsession.id}?fixScanTypes=true"
                    )
                except XNATResponseError as e:
                    logger.warning(f"Failed to fix scan types in '{session.name}': {e}")
                try:
                    xnat_repo.connection.put(
                        f"/data/experiments/{xsession.id}?triggerPipelines=true"
                    )
                except XNATResponseError as e:
                    logger.warning(
                        f"Failed to trigger pipelines in '{session.name}': {e}"
                    )
                logger.info(f"Succesfully uploaded all files in '{session.name}'")
            except Exception as e:
                if not raise_errors:
                    logger.error(
                        f"Skipping '{session.name}' session due to error in staging: \"{e}\""
                        f"\n{traceback.format_exc()}\n\n"
                    )
                    continue
                else:
                    raise

    if use_curl_jsession:
        xnat_repo.exit()

    if clean_up_older_than:
        logger.info(
            "Cleaning up files in %s older than %d days",
            staged,
            clean_up_older_than,
        )
        if staged.startswith("s3://"):
            remove_old_files_on_s3(remote_store=staged, threshold=clean_up_older_than)
        elif "@" in staged:
            remove_old_files_on_ssh(remote_store=staged, threshold=clean_up_older_than)
        else:
            assert False


def remove_old_files_on_s3(remote_store: str, threshold: int):
    # Parse S3 bucket and prefix from remote store
    bucket_name, prefix = remote_store[5:].split("/", 1)

    # Create S3 client
    s3_client = boto3.client("s3")

    # List objects in the bucket with the specified prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    now = datetime.datetime.now()

    # Iterate over objects and delete files older than the threshold
    for obj in response.get("Contents", []):
        last_modified = obj["LastModified"]
        age = (now - last_modified).days
        if age > threshold:
            s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])


def remove_old_files_on_ssh(remote_store: str, threshold: int):
    # Parse SSH server and directory from remote store
    server, directory = remote_store.split("@", 1)

    # Create SSH client
    ssh_client = paramiko.SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.connect(server)

    # Execute find command to list files in the directory
    stdin, stdout, stderr = ssh_client.exec_command(f"find {directory} -type f")

    now = datetime.datetime.now()

    # Iterate over files and delete files older than the threshold
    for file_path in stdout.read().decode().splitlines():
        last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        age = (now - last_modified).days
        if age > threshold:
            ssh_client.exec_command(f"rm {file_path}")

    ssh_client.close()


if __name__ == "__main__":
    upload()
