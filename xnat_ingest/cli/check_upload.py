import typing as ty
from pathlib import Path

import click

from xnat_ingest.cli.base import base_cli
from xnat_ingest.helpers.arg_types import LoggerConfig, StoreCredentials
from xnat_ingest.helpers.logging import set_logger_handling

from ..api import check_upload


@base_cli.command(
    "check-upload",
    help="""Checks staging directory against uploaded files and logs all files that aren't uploaded

STAGED is either a directory that the files for each session are collated to before they
are uploaded to XNAT or an S3 bucket to download the files from.

SERVER is address of the XNAT server to upload the scans up to. Can alternatively provided
by setting the "XNAT_INGEST_HOST" environment variable.
""",
)
@click.argument("input_dir", type=str, envvar="XINGEST_STAGED")
@click.argument("server", type=str, envvar="XINGEST_HOST")
@click.option(
    "--user",
    type=str,
    envvar="XINGEST_USER",
    help=(
        'the XNAT user to connect with (alternatively the "XINGEST_USER" env. variable can be used.'
    ),
)
@click.option(
    "--password",
    default=None,
    type=str,
    envvar="XINGEST_PASS",
    help='the password for the XNAT user, alternatively "XINGEST_PASS" env. var',
)
@click.option(
    "--logger",
    "loggers",
    multiple=True,
    type=LoggerConfig.cli_type,
    envvar="XINGEST_LOGGERS",
    nargs=3,
    default=[],
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
    default=[],
    envvar="XINGEST_ADDITIONAL_LOGGERS",
    help=(
        "The loggers to use for logging. By default just the 'xnat-ingest' logger is used. "
        "But additional loggers can be included (e.g. 'xnat') can be "
        "specified here (XINGEST_ADDITIONAL_LOGGERS env. var)."
    ),
)
@click.option(
    "--always-include",
    "-i",
    default=[],
    type=str,
    multiple=True,
    envvar="XINGEST_ALWAYSINCLUDE",
    help=(
        "Scan types to always include in the upload, regardless of whether they are"
        "specified in a column or not. Specified using the scan types IANA mime-type or "
        'fileformats "mime-like" (see https://arcanaframework.github.io/fileformats/), '
        "e.g. 'application/json', 'medimage/dicom-series', "
        "'image/jpeg'). Use 'all' to include all file-types in the session (XINGEST_ALWAYSINCLUDE "
        "env. var)."
    ),
)
@click.option(
    "--store-credentials",
    type=StoreCredentials.cli_type,
    metavar="<access-key> <secret-key>",
    envvar="XINGEST_STORE_CREDENTIALS",
    default=None,
    nargs=2,
    help="Credentials to use to access of data stored in remote stores (e.g. AWS S3)",
)
@click.option(
    "--temp-dir",
    type=Path,
    default=None,
    envvar="XINGEST_TEMPDIR",
    help="The directory to use for temporary downloads (i.e. from s3) (XINGEST_TEMPDIR env. var)",
)
@click.option(
    "--verify-ssl/--dont-verify-ssl",
    type=bool,
    default=True,
    envvar="XINGEST_VERIFY_SSL",
    help="Whether to verify the SSL certificate of the XNAT server (XINGEST_VERIFY_SSL env. var)",
)
@click.option(
    "--use-curl-jsession/--dont-use-curl-jsession",
    type=bool,
    default=False,
    envvar="XINGEST_USE_CURL_JSESSION",
    help=(
        "Whether to use CURL to create a JSESSION token to authenticate with XNAT. This is "
        "used to work around a strange authentication issue when running within a Kubernetes "
        "cluster and targeting the XNAT Tomcat directly (XINGEST_USE_CURL_JSESSION env. var)."
    ),
)
@click.option(
    "--disable-progress/--enable-progress",
    type=bool,
    default=False,
    envvar="XINGEST_DISABLE_PROGRESS",
    help=("Disable the progress bar"),
)
def check_upload_cli(
    input_dir: str,
    server: str,
    user: str,
    password: str,
    loggers: ty.List[LoggerConfig],
    always_include: ty.Sequence[str],
    additional_loggers: ty.List[str],
    store_credentials: StoreCredentials,
    temp_dir: ty.Optional[Path],
    verify_ssl: bool,
    use_curl_jsession: bool,
    disable_progress: bool,
) -> None:

    set_logger_handling(
        logger_configs=loggers,
        additional_loggers=additional_loggers,
        clean_format=True,
    )

    check_upload(
        input_dir=input_dir,
        server=server,
        user=user,
        password=password,
        store_credentials=store_credentials,
        always_include=always_include,
        temp_dir=temp_dir,
        verify_ssl=verify_ssl,
        use_curl_jsession=use_curl_jsession,
        disable_progress=disable_progress,
    )
