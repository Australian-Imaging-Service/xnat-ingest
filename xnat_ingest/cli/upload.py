from pathlib import Path
import traceback
import typing as ty
import tempfile
import time
import datetime
import subprocess as sp
import click
from tqdm import tqdm
import xnat
from fileformats.generic import File
from frametree.core.frameset import FrameSet
from frametree.xnat import Xnat
from xnat.exceptions import XNATResponseError
from xnat_ingest.cli.base import cli
from xnat_ingest.session import ImagingSession
from xnat_ingest.resource import ImagingResource
from xnat_ingest.utils import (
    logger,
    LoggerConfig,
    set_logger_handling,
    StoreCredentials,
)
from xnat_ingest.upload_helpers import (
    get_xnat_session,
    get_xnat_resource,
    get_xnat_checksums,
    calculate_checksums,
    iterate_s3_sessions,
    remove_old_files_on_s3,
    remove_old_files_on_ssh,
    dir_older_than,
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
@click.argument("staged", type=str, envvar="XINGEST_STAGED")
@click.argument("server", type=str, envvar="XINGEST_HOST")
@click.argument("user", type=str, envvar="XINGEST_USER")
@click.option("--password", default=None, type=str, envvar="XINGEST_PASS")
@click.option(
    "--logger",
    "loggers",
    multiple=True,
    type=LoggerConfig.cli_type,
    envvar="XINGEST_LOGGERS",
    nargs=3,
    default=(),
    metavar="<logtype> <loglevel> <location>",
    help=("Setup handles to capture logs that are generated"),
)
@click.option(
    "--additional-logger",
    "additional_loggers",
    type=str,
    multiple=True,
    default=(),
    envvar="XINGEST_ADDITIONALLOGGERS",
    help=(
        "The loggers to use for logging. By default just the 'xnat-ingest' logger is used. "
        "But additional loggers can be included (e.g. 'xnat') can be "
        "specified here"
    ),
)
@click.option(
    "--always-include",
    "-i",
    default=(),
    type=str,
    multiple=True,
    envvar="XINGEST_ALWAYSINCLUDE",
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
    "--clean-up-older-than",
    type=int,
    metavar="<days>",
    envvar="XINGEST_CLEANUP_OLDER_THAN",
    default=0,
    help="The number of days to keep files in the remote store for",
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
        "cluster and targeting the XNAT Tomcat directly"
    ),
)
@click.option(
    "--method",
    type=click.Choice(["per_file", "tar_memory", "tgz_memory", "tar_file", "tgz_file"]),
    default="tgz_file",
    envvar="XINGEST_METHOD",
    help=(
        "The method to use to upload the files to XNAT. Passed through to XNATPy and controls "
        "whether directories are tarred and/or gzipped before they are uploaded, by default "
        "'tgz_file' is used"
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
        "avoid uploading partial sessions"
    ),
)
@click.option(
    "--loop",
    type=int,
    default=None,
    envvar="XINGEST_LOOP",
    help="Run the staging process continuously every LOOP seconds",
)
def upload(
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
    clean_up_older_than: int,
    verify_ssl: bool,
    use_curl_jsession: bool,
    method: str,
    wait_period: int,
    loop: int | None,
) -> None:

    if raise_errors and loop:
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

    xnat_repo = Xnat(
        server=server,
        user=user,
        password=password,
        cache_dir=Path(tempfile.mkdtemp()),
        verify_ssl=verify_ssl,
    )

    def do_upload() -> None:
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
                server, user=user, jsession=jsession, logger="xnat"
            )

        with xnat_repo.connection:

            num_sessions: int
            sessions: ty.Iterable[Path]
            if staged.startswith("s3://"):
                sessions = iterate_s3_sessions(
                    staged, store_credentials, temp_dir, wait_period=wait_period
                )
                # bit of a hack: number of sessions is the first item in the iterator
                num_sessions = next(sessions)  # type: ignore[assignment]
            else:
                sessions = []
                for session_dir in Path(staged).iterdir():
                    if dir_older_than(session_dir, wait_period):
                        sessions.append(session_dir)
                    else:
                        logger.info(
                            "Skipping '%s' session as it has been modified recently",
                            session_dir,
                        )
                num_sessions = len(sessions)
                logger.info(
                    "Found %d sessions in staging directory to stage'%s'",
                    num_sessions,
                    staged,
                )

            framesets: dict[str, FrameSet] = {}

            for session_staging_dir in tqdm(
                sessions,
                total=num_sessions,
                desc=f"Processing staged sessions found in '{staged}'",
            ):

                session = ImagingSession.load(
                    session_staging_dir,
                    require_manifest=require_manifest,
                )
                try:
                    # Create corresponding session on XNAT
                    logger.debug(
                        "Creating XNAT session for '%s' in project '%s'",
                    )
                    xproject = xnat_repo.connection.projects[session.project_id]

                    # Access Arcana frameset associated with project
                    try:
                        frameset = framesets[session.project_id]
                    except KeyError:
                        try:
                            frameset = FrameSet.load(session.project_id, xnat_repo)
                        except Exception as e:
                            if not always_include:
                                logger.error(
                                    "Did not load frameset definition (%s) from %s project "
                                    "on %s. Either '--always-include' flag must be used or "
                                    "the frameset must be defined on XNAT using the `frametree` "
                                    "command line tool (see https://arcanaframework.github.io/frametree/).",
                                    e,
                                    session.project_id,
                                    xnat_repo.server,
                                )
                                continue
                            else:
                                frameset = None
                        framesets[session.project_id] = frameset

                    xsession = get_xnat_session(session, xproject)

                    # Anonymise DICOMs and save to directory prior to upload
                    if always_include:
                        logger.info(
                            f"Including {always_include} scans/files in upload from '{session.name}' to "
                            f"{session.path} regardless of whether they are explicitly specified"
                        )

                    for resource in tqdm(
                        sorted(
                            session.select_resources(
                                frameset, always_include=always_include
                            )
                        ),
                        f"Uploading resources found in {session.name}",
                    ):
                        xresource = get_xnat_resource(resource, xsession)
                        if xresource is None:
                            logger.info(
                                "Skipping '%s' resource as it is already uploaded",
                                resource.path,
                            )
                            continue  # skipping as resource already exists
                        else:
                            logger.debug(
                                "Uploading '%s' resource to '%s'",
                                resource.path,
                                xresource,
                            )
                        if isinstance(resource.fileset, File):
                            for fspath in resource.fileset.fspaths:
                                logger.debug(
                                    "Uploading '%s' to '%s' in %s",
                                    fspath,
                                    fspath.name,
                                    xresource,
                                )
                                xresource.upload(str(fspath), fspath.name)
                        else:
                            # Temporarily move the manifest file out of the way so it
                            # doesn't get uploaded
                            manifest_file = (
                                resource.fileset.parent / ImagingResource.MANIFEST_FNAME
                            )
                            moved_manifest_file = (
                                resource.fileset.parent.parent
                                / ImagingResource.MANIFEST_FNAME
                            )
                            if manifest_file.exists():
                                manifest_file.rename(moved_manifest_file)
                            # Upload the contents of the resource to XNAT
                            logger.debug(
                                "Uploading directory '%s' to %s with '%s' method",
                                resource.fileset.parent,
                                xresource,
                                method,
                            )
                            xresource.upload_dir(resource.fileset.parent, method=method)
                            # Move the manifest file back again
                            if moved_manifest_file.exists():
                                moved_manifest_file.rename(manifest_file)
                        logger.debug("retrieving checksums for %s", xresource)
                        remote_checksums = get_xnat_checksums(xresource)
                        if any(remote_checksums.values()):
                            logger.debug("calculating checksums for %s", xresource)
                            calc_checksums = calculate_checksums(resource.fileset)
                            if remote_checksums != calc_checksums:
                                extra_keys = set(remote_checksums) - set(calc_checksums)
                                missing_keys = set(calc_checksums) - set(
                                    remote_checksums
                                )
                                mismatching = [
                                    k
                                    for k, v in calc_checksums.items()
                                    if v != remote_checksums[k]
                                ]
                                raise RuntimeError(
                                    "Checksums do not match after upload of "
                                    f"'{resource.path}' resource.\n"
                                    f"Extra keys were {extra_keys}\n"
                                    f"Missing keys were {missing_keys}\n"
                                    f"Mismatching files were {mismatching}\n"
                                    f"Remote checksums were {remote_checksums}\n"
                                    f"Calculated checksums were {calc_checksums}\n"
                                )
                        else:
                            logger.debug(
                                "Remote checksums were not calculted for %s "
                                "(requires `enableChecksums` to be set site-wide), "
                                "assuming upload was successful",
                                xresource,
                            )

                        logger.info(f"Uploaded '{resource.path}' in '{session.name}'")
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
                        logger.warning(
                            f"Failed to fix scan types in '{session.name}': {e}"
                        )
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
                            f"Skipping '{session.name}' session due to error uploading: \"{e}\""
                            f"\n{traceback.format_exc()}\n\n"
                        )
                        continue
                    else:
                        raise

        if use_curl_jsession:
            xnat_repo.connection.exit()

        if clean_up_older_than:
            logger.info(
                "Cleaning up files in %s older than %d days",
                staged,
                clean_up_older_than,
            )
            if staged.startswith("s3://"):
                remove_old_files_on_s3(
                    remote_store=staged, threshold=clean_up_older_than
                )
            elif "@" in staged:
                remove_old_files_on_ssh(
                    remote_store=staged, threshold=clean_up_older_than
                )
            else:
                assert False

    if loop is not None:
        while True:
            start_time = datetime.datetime.now()
            try:
                do_upload()
            except Exception as e:
                logger.error(
                    f'Error attempting to prepare upload of sessions: "{e}"'
                    f"\n{traceback.format_exc()}\n\n"
                )
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
    else:
        try:
            do_upload()
        except Exception as e:
            if not raise_errors:
                logger.error(
                    f'Error attempting to prepare upload of sessions: "{e}"'
                    f"\n{traceback.format_exc()}\n\n"
                )
            else:
                raise
        logger.info("Upload completed successfully")


if __name__ == "__main__":
    upload()
