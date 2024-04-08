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
    StoreCredentials,
    XnatLogin,
    MailServer,
    set_logger_handling,
)
from .base import cli


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
@click.argument(
    "staging_dir", type=click.Path(path_type=Path), envvar="XNAT_INGEST_STAGE_DIR"
)
@click.argument("remote_store", type=str, envvar="XNAT_INGEST_TRANSFER_REMOTE_STORE")
@click.option(
    "--store-credentials",
    type=StoreCredentials.cli_type,
    metavar="<access-key> <secret-key>",
    envvar="XNAT_INGEST_TRANSFER_STORE_CREDENTIALS",
    default=None,
    nargs=2,
    help="Credentials to use to access of data stored in remote stores (e.g. AWS S3)",
)
@click.option(
    "--log-level",
    default="info",
    type=str,
    envvar="XNAT_INGEST_TRANSFER_LOGLEVEL",
    help=("The level of the logging printed to stdout"),
)
@click.option(
    "--log-file",
    "log_files",
    default=None,
    type=LogFile.cli_type,
    nargs=2,
    multiple=True,
    metavar="<path> <loglevel>",
    envvar="XNAT_INGEST_TRANSFER_LOGFILE",
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
    envvar="XNAT_INGEST_TRANSFER_LOGEMAIL",
    help=(
        "Email(s) to send logs to. When provided in an environment variable, "
        "mail and log level are delimited by ',' and separate destinations by ';'"
    ),
)
@click.option(
    "--mail-server",
    type=MailServer.cli_type,
    metavar="<host> <sender-email> <user> <password>",
    default=None,
    envvar="XNAT_INGEST_TRANSFER_MAILSERVER",
    help=(
        "the mail server to send logger emails to. When provided in an environment variable, "
        "args are delimited by ';'"
    ),
)
@click.option(
    "--delete/--dont-delete",
    default=False,
    envvar="XNAT_INGEST_TRANSFER_DELETE",
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
    type=XnatLogin.cli_type,
    default=None,
    metavar="<host> <user> <password>",
    help="The XNAT server to upload to plus the user and password to use",
    envvar="XNAT_INGEST_TRANSFER_XNAT_LOGIN",
)
def transfer(
    staging_dir: Path,
    remote_store: str,
    store_credentials: ty.Tuple[str, str],
    log_files: ty.List[LogFile],
    log_level: str,
    log_emails: ty.List[LogEmail],
    mail_server: MailServer,
    delete: bool,
    raise_errors: bool,
    xnat_login: ty.Optional[ty.Tuple[str, str, str]],
):

    if not staging_dir.exists():
        raise ValueError(f"Staging directory '{staging_dir}' does not exist")

    set_logger_handling(
        log_level=log_level,
        log_files=log_files,
        log_emails=log_emails,
        mail_server=mail_server,
    )

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
                            "--quiet",
                            str(session_dir),
                            remote_path,
                        ]
                    )
                elif store_type == "ssh":
                    logger.debug(
                        "Transferring %s to %s via SSH", session_dir, remote_store
                    )
                    sp.check_call(["rsync", "--quiet", str(session_dir), remote_path])
                else:
                    assert False
                if delete:
                    logger.info("Deleting %s after successful upload", session_dir)
                    shutil.rmtree(session_dir)


if __name__ == "__main__":
    transfer()
