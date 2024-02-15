import subprocess as sp
import typing as ty
from pathlib import Path
import tempfile
import shutil
import click
from tqdm import tqdm
from arcana.xnat import Xnat
from ..utils import (
    logger,
    LogFile,
    LogEmail,
    MailServer,
    set_logger_handling,
)
from .base import cli
import os
import datetime
import boto3
import paramiko


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


@cli.command(
    help="""transfers data from a staging directory to an intermediate remote store,
such as an S3 bucket or a remote server accessible via SSH, before they are finally
uploaded to XNAT.

STAGING_DIR is the directory that the files for each session are collated to

REMOTE_STORE is location of the remote store to transfer the data to. This can be an
AWS S3 bucket or a remote server accessible via SSH. The format of the remote store
is determined by the prefix of the path. For example, a path starting with 's3://' is
interpreted as an S3 bucket, while a path starting with 'xxxx@xxxx:' is interpreted as
an SSH server.
""",
)
@click.argument("staging_dir", type=str)
@click.argument("remote_store", type=str, envvar="XNAT_INGEST_REMOTE_STORE")
@click.option(
    "--credentials",
    type=str,
    metavar="<access-key> <secret-key>",
    envvar="XNAT_INGEST_STORE_CREDENTIALS",
    default=None,
    nargs=2,
    help="Credentials to use to access of data stored in remote stores (e.g. AWS S3)",
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
    "--delete/--dont-delete",
    default=False,
    envvar="XNAT_INGEST_DELETE",
    help="Whether to delete the session directories after they have been uploaded or not",
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
    type=str,
    default=None,
    metavar="<host> <user> <password>",
    help="The XNAT server to upload to plus the user and password to use",
    envvar="XNAT_INGEST_XNAT_LOGIN",
)
@click.option(
    "--clean-up-older-than",
    type=int,
    metavar="<days>",
    default=0,
    help="The number of days to keep files in the remote store for",
)
def transfer(
    staging_dir: str,
    remote_store: str,
    credentials: ty.Tuple[str, str],
    log_file: ty.Tuple[str, str],
    log_level: str,
    log_emails: ty.List[ty.Tuple[str, str, str]],
    mail_server: ty.Tuple[str, str, str, str],
    delete: bool,
    raise_errors: bool,
    xnat_login: ty.Optional[ty.Tuple[str, str, str]],
    clean_up_older_than: int,
):

    staging_dir = Path(staging_dir)
    if not staging_dir.exists():
        raise ValueError(f"Staging directory '{staging_dir}' does not exist")

    set_logger_handling(log_level, log_file, log_emails, mail_server)

    if remote_store.startswith("s3://"):
        store_type = "s3"
    elif "@" in remote_store:
        store_type = "ssh"
    else:
        raise ValueError(
            f"Remote store {remote_store} is not a valid remote store. "
            "It should be an S3 bucket or an SSH server"
        )

    if xnat_login is not None:
        server, user, password = xnat_login
        xnat_repo = Xnat(
            server=server,
            user=user,
            password=password,
            cache_dir=Path(tempfile.mkdtemp()),
        )
    else:
        xnat_repo = None

    for project_dir in tqdm(
        list(staging_dir.iterdir()),
        f"Transferring projects to remote store {remote_store}",
    ):
        if project_dir.name.startswith("UNKNOWN"):
            logger.error(
                "Project %s is not recognised and will not be transferred, please "
                "rename manually and transfer again",
                project_dir.name,
            )
            continue
        if xnat_repo:
            with xnat_repo.connection:
                try:
                    xnat_repo.connection.projects[project_dir.name]
                except KeyError:
                    logger.error(
                        "Project %s does not exist on XNAT. Please rename the directory "
                        "to match the project ID on XNAT",
                        project_dir.name,
                    )
                    continue
        for subject_dir in tqdm(
            list(project_dir.iterdir()),
            f"Transferring subjects for {project_dir.name} project",
        ):
            if subject_dir.name.startswith("UNKNOWN"):
                logger.error(
                    "Subject % in project %s is not recognised and will not be "
                    "transferred, please rename manually and transfer again",
                    subject_dir.name,
                    project_dir.name,
                )
                continue
            for session_dir in tqdm(
                list(subject_dir.iterdir()),
                f"Transferring sessions for {project_dir.name}:{subject_dir.name} subject",
            ):
                if session_dir.name.startswith("UNKNOWN"):
                    logger.error(
                        "Session % in subject %s in project %s is not recognised and "
                        "will not be transferred, please rename manually and transfer again",
                        session_dir.name,
                        subject_dir.name,
                        project_dir.name,
                    )
                    continue
                remote_path = (
                    remote_store
                    + "/"
                    + project_dir.name
                    + "/"
                    + subject_dir.name
                    + "/"
                    + session_dir.name
                )
                if store_type == "s3":
                    logger.debug(
                        "Transferring %s to S3 (%s)", session_dir, remote_store
                    )
                    sp.check_call(
                        [
                            "aws",
                            "s3",
                            "sync",
                            str(session_dir),
                            remote_path,
                        ]
                    )
                elif store_type == "ssh":
                    logger.debug(
                        "Transferring %s to %s via SSH", session_dir, remote_store
                    )
                    sp.check_call(["rsync", str(session_dir), remote_path])
                else:
                    assert False
                if delete:
                    logger.info("Deleting %s after successful upload", session_dir)
                    shutil.rmtree(session_dir)

    if clean_up_older_than:
        logger.info(
            "Cleaning up files in %s older than %d days",
            remote_store,
            clean_up_older_than,
        )
        if store_type == "s3":
            remove_old_files_on_s3(
                remote_store=remote_store, threshold=clean_up_older_than
            )
        elif store_type == "ssh":
            remove_old_files_on_ssh(
                remote_store=remote_store, threshold=clean_up_older_than
            )
        else:
            assert False


if __name__ == "__main__":
    transfer()
