import datetime
import logging
import subprocess as sp
import tempfile
import time
import typing as ty
from pathlib import Path

import botocore.exceptions
import click
import requests.exceptions
import xnat
from frametree.xnat import Xnat

from xnat.exceptions import XNATResponseError

from xnat_ingest.cli.base import base_cli

from ..api import upload
from ..helpers.arg_types import LoggerConfig, StoreCredentials, UploadMethod
from ..helpers.logging import logger, set_logger_handling


@base_cli.command(
    name="upload",
    help="""uploads all sessions found in the staging directory (as prepared by the
`stage` sub-command) to XNAT.

STAGED is either a directory that the files for each session are collated to before they
are uploaded to XNAT or an S3 bucket to download the files from.

SERVER is address of the XNAT server to upload the scans up to. Can alternatively provided
by setting the "XNAT_INGEST_HOST" environment variable.
""",
)
@click.argument("staged", type=str, envvar="XINGEST_STAGED")
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
    "--raise-errors/--dont-raise-errors",
    default=False,
    type=bool,
    help="Whether to raise errors instead of logging them (typically for debugging) ",
)
@click.option(
    "--store-credentials",
    type=StoreCredentials.cli_type,
    metavar="<access-key> <secret-key>",
    envvar="XINGEST_STORE_CREDENTIALS",
    default=None,
    nargs=2,
    help=(
        "Credentials to use to access of data stored in remote stores (e.g. AWS S3) "
        "(XINGEST_STORE_CREDENTIALS env. var)"
    ),
)
@click.option(
    "--temp-dir",
    type=Path,
    default=None,
    envvar="XINGEST_TEMPDIR",
    help="The directory to use for temporary downloads (i.e. from s3)",
)
@click.option(
    "--require-manifest/--dont-require-manifest",
    default=None,
    envvar="XINGEST_REQUIRE_MANIFEST",
    help=("Whether to require manifest files in the staged resources or not"),
    type=bool,
)
@click.option(
    "--verify-ssl/--dont-verify-ssl",
    type=bool,
    default=True,
    envvar="XINGEST_VERIFY_SSL",
    help="Whether to verify the SSL certificate of the XNAT server",
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
    "--method",
    "methods",
    type=UploadMethod.cli_type,
    multiple=True,
    nargs=2,
    metavar="<method> <datatype>",
    default=[],
    envvar="XINGEST_METHODS",
    help=(
        "The methods to use to upload the file types to XNAT with. Passed through to XNATPy and controls "
        "whether directories are tarred and/or gzipped before they are uploaded, by default "
        "'tgz_file' is used (XINGEST_METHODS env. var)."
    ),
)
@click.option(
    "--wait-period",
    type=int,
    default=0,
    envvar="XINGEST_WAIT_PERIOD",
    help=(
        "The number of seconds to wait since the last file modification in sessions "
        "in the S3 bucket or source file-system directory before uploading them to "
        "avoid uploading partial sessions (XINGEST_WAIT_PERIOD env. var)."
    ),
)
@click.option(
    "--loop",
    type=int,
    default=-1,
    envvar="XINGEST_LOOP",
    help="Run the staging process continuously every LOOP seconds (XINGEST_LOOP env. var). ",
)
@click.option(
    "--num-files-per-batch",
    type=int,
    default=0,
    envvar="XINGEST_NUM_FILES_PER_BATCH",
    help=(
        "When uploading files to XNAT, the number of files to upload in each batch. "
        "The number of files that are uploaded in a single batch can be limited to "
        "avoid overloading the building of the catalog file. If <= 0 (the default), "
        "then all files are uploaded in a single batch (XINGEST_NUM_FILES_PER_BATCH env. var)."
    ),
)
@click.option(
    "--check-checksums/--dont-check-checksums",
    type=bool,
    default=True,
    envvar="XINGEST_CHECK_CHECKSUMS",
    help=(
        "Whether to check the checksums of the files in the staged resources against the "
        "checksums of both the checksums saved in the manifests and verify after upload "
        "to verify that they were uploaded correctly (if checksums are enabled site-wide on "
        "XNAT, i.e. `enableChecksums` is set to `true` in the XNAT configuration)"
    ),
)
@click.option(
    "--dry-run/--no-dry-run",
    type=bool,
    default=False,
    envvar="XINGEST_DRY_RUN",
    help=(
        "List the sessions that will be uploaded instead of the actually uploading them"
    ),
)
def upload_cli(
    staged: str,
    server: str,
    user: str,
    password: str,
    loggers: ty.List[LoggerConfig],
    additional_loggers: ty.List[str],
    always_include: ty.Sequence[str],
    raise_errors: bool,
    store_credentials: StoreCredentials,
    temp_dir: ty.Optional[Path],
    require_manifest: bool,
    verify_ssl: bool,
    use_curl_jsession: bool,
    methods: ty.Sequence[UploadMethod],
    wait_period: int,
    loop: int,
    num_files_per_batch: int,
    check_checksums: bool,
    dry_run: bool,
) -> None:

    if raise_errors and loop >= 0:
        raise ValueError(
            "Cannot use --raise-errors and --loop together as the loop will "
            "continue to run even if an error occurs"
        )

    set_logger_handling(
        logger_configs=loggers,
        additional_loggers=additional_loggers,
    )

    # Set the directory to create temporary files/directories in away from system default
    if temp_dir:
        tempfile.tempdir = str(temp_dir)

    # Open ONE XNAT connection for the whole `--loop` lifetime.
    #
    # Previously upload() created a fresh Xnat() and called xnat.connect()
    # on every iteration. Each connect rebuilds Python classes from the
    # server's XSD schema (xnat.build_model / xnat.convert_xsd) and those
    # generated objects accumulate in xnatpy's module-level state, producing
    # a perfectly linear RSS growth of ~330-380 MiB/hour in production
    # deployments. Holding one connection open across iterations eliminates
    # the regeneration entirely (verified: 0 MiB/iter growth).
    #
    # If the held connection drops mid-loop, the except-block below closes
    # it cleanly and the next iteration re-creates one. So this keeps the
    # existing loop-resilience properties while removing the leak.
    def _open_repo() -> "Xnat":
        repo = Xnat(
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
            repo.connection.depth = 1
            repo.connection.session = xnat.connect(
                server, user=user, jsession=jsession, logger=logging.getLogger("xnat")
            )
        repo.connection.__enter__()
        return repo

    def _close_repo(repo: "Xnat") -> None:
        try:
            repo.connection.__exit__(None, None, None)
        except Exception:  # noqa: BLE001 — best-effort cleanup of a possibly dead session
            pass

    def _is_auth_failure(error_messages: ty.List[str]) -> bool:
        """Detect whether any error returned by upload() is an XNAT auth
        failure (HTTP 401/403).

        These are CONNECTION-level failures, not per-session data problems:
        XNAT expires the held connection's server-side session (JSESSIONID)
        after its configured timeout, after which every call on the held
        connection returns 401 and never recovers on its own. upload()
        catches these as per-session "skip" errors and returns them as
        strings, so they never reach the transient-error handler below — the
        daemon would otherwise 401 every session forever until manually
        restarted. Detecting the signature here lets us force a reconnect.
        """
        signatures = ("status 401", "status 403", "Unauthorized", "Forbidden")
        return any(
            any(sig in msg for sig in signatures) for msg in error_messages

        )
#     # Loop the upload process if loop is set to a positive value, otherwise just run it once
#     while True:
#         start_time = datetime.datetime.now()
#         try:
#             errors = upload(
#                 input_dir=staged,
#                 server=server,
#                 user=user,
#                 password=password,
#                 always_include=always_include,
#                 raise_errors=raise_errors,
#                 store_credentials=store_credentials,
#                 require_manifest=require_manifest,
#                 use_curl_jsession=use_curl_jsession,
#                 verify_ssl=verify_ssl,
#                 methods=methods,
#                 wait_period=wait_period,
#                 num_files_per_batch=num_files_per_batch,
#                 check_checksums=check_checksums,
#                 dry_run=dry_run,
#                 s3_cache_dir=(
#                     Path(temp_dir) / "s3_cache"
#                     if temp_dir is not None
#                     else tempfile.mkdtemp()
#                 ),
#             )
#             if errors:
#                 logger.error(
#                     f"Upload completed with {len(errors)} errors:\n\n{''.join(errors)}"
#                 )
#             else:
#                 logger.info("Upload completed successfully without errors")
#         except (
#             requests.exceptions.ConnectionError,
#             requests.exceptions.ConnectTimeout,
#             requests.exceptions.ReadTimeout,
#             XNATResponseError,
#             botocore.exceptions.EndpointConnectionError,
#             botocore.exceptions.ConnectTimeoutError,
#             botocore.exceptions.ReadTimeoutError,
#         ) as e:
#             # Transient network or XNAT-side error. In loop mode, log and continue
#             # instead of crashing the process (which causes container restarts in
#             # Kubernetes deployments). In one-shot mode, re-raise so callers see
#             # the failure.
#             if loop < 0:
#                 raise
#             logger.error(
#                 "Transient error connecting to XNAT or S3 store: %s. "
#                 "Will retry on next loop iteration.",
#                 e,
#             )
#         if loop < 0:
#             break
#         end_time = datetime.datetime.now()
#         elapsed_seconds = (end_time - start_time).total_seconds()
#         sleep_time = loop - elapsed_seconds
#         logger.info(
#             "Stage took %s seconds, waiting another %s seconds before running "
#             "again (loop every %s seconds)",
#             elapsed_seconds,
#             sleep_time,
#             loop,
          # )
    xnat_repo = _open_repo()
    try:
        # Loop the upload process if loop is set to a positive value, otherwise just run it once
        while True:
            start_time = datetime.datetime.now()
            # A prior reconnect attempt may have failed and left us without a
            # connection. Re-establish before doing any work this iteration.
            if xnat_repo is None:
                try:
                    xnat_repo = _open_repo()
                except Exception as e:  # noqa: BLE001
                    logger.error(
                        "XNAT connection is down and reconnect failed: %s. "
                        "Retrying after the loop interval.",
                        e,
                    )
                    if loop < 0:
                        raise
                    time.sleep(loop)
                    continue
            try:
                errors = upload(
                    input_dir=staged,
                    xnat_repo=xnat_repo,
                    always_include=always_include,
                    raise_errors=raise_errors,
                    store_credentials=store_credentials,
                    require_manifest=require_manifest,
                    use_curl_jsession=use_curl_jsession,
                    verify_ssl=verify_ssl,
                    methods=methods,
                    wait_period=wait_period,
                    num_files_per_batch=num_files_per_batch,
                    check_checksums=check_checksums,
                    dry_run=dry_run,
                    s3_cache_dir=(
                        Path(temp_dir) / "s3_cache"
                        if temp_dir is not None
                        else tempfile.mkdtemp()
                    ),
                )
                if errors:
                    logger.error(
                        f"Upload completed with {len(errors)} errors:\n\n{''.join(errors)}"
                    )
                    # If the errors are XNAT auth failures, the held session has
                    # expired — reconnect so the next iteration uses a fresh
                    # session. Reactive ONLY: we must not reconnect on every
                    # iteration, because each xnat.connect() rebuilds the xnatpy
                    # schema model and reintroduces the ~330-380 MiB/h memory leak
                    # the single held connection was added to avoid.
                    if loop >= 0 and _is_auth_failure(errors):
                        logger.warning(
                            "Detected XNAT authentication failure (expired session). "
                            "Re-establishing the XNAT connection before the next iteration."
                        )
                        _close_repo(xnat_repo)
                        try:
                            xnat_repo = _open_repo()
                        except Exception as reconnect_err:  # noqa: BLE001
                            logger.error(
                                "Failed to re-establish XNAT connection: %s. "
                                "Will retry on the next loop iteration.",
                                reconnect_err,
                            )
                            xnat_repo = None
                else:
                    logger.info("Upload completed successfully without errors")
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ReadTimeout,
                XNATResponseError,
                botocore.exceptions.EndpointConnectionError,
                botocore.exceptions.ConnectTimeoutError,
                botocore.exceptions.ReadTimeoutError,
            ) as e:
                # Transient network or XNAT-side error. The held connection
                # may now be dead; close it and re-open on the next
                # iteration so a server bounce doesn't poison subsequent
                # uploads.
                if loop < 0:
                    raise
                logger.error(
                    "Transient error connecting to XNAT or S3 store: %s. "
                    "Closing and re-opening XNAT connection before next "
                    "loop iteration.",
                    e,
                )
                _close_repo(xnat_repo)
                xnat_repo = _open_repo()
            if loop < 0:
                break
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
    finally:
        _close_repo(xnat_repo)
