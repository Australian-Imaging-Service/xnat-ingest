import datetime
import logging
import math
import shutil
import subprocess as sp
import tempfile
import time
import traceback
import typing as ty
from pathlib import Path

import click
import xnat
from fileformats.generic import File, FileSet
from frametree.core.frameset import FrameSet
from frametree.xnat import Xnat
from tqdm import tqdm
from xnat.exceptions import XNATResponseError

from xnat_ingest.cli.base import cli
from xnat_ingest.session import ImagingSession
from xnat_ingest.upload_helpers import (
    LocalSessionListing,
    SessionListing,
    calculate_checksums,
    dir_older_than,
    get_xnat_checksums,
    get_xnat_resource,
    get_xnat_session,
    iterate_s3_sessions,
)
from xnat_ingest.utils import (
    LoggerConfig,
    StoreCredentials,
    UploadMethod,
    logger,
    set_logger_handling,
)


@cli.command(
    "check-upload",
    help="""Checks staging directory against uploaded files and logs all files that aren't uploaded

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
        'the XNAT user to connect with (alternatively the "XNAT_INGEST_USER" env. variable can be used.'
    ),
)
@click.option(
    "--password",
    default=None,
    type=str,
    envvar="XINGEST_PASS",
    help='the password for the XNAT user, alternatively "XNAT_INGEST_PASS" env. var',
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
    help=("Setup handles to capture logs that are generated"),
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
        "specified here"
    ),
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
def check_upload(
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
    verify_ssl: bool,
    use_curl_jsession: bool,
) -> None:

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
            server, user=user, jsession=jsession, logger=logging.getLogger("xnat")
        )

    with xnat_repo.connection:

        num_sessions: int
        sessions: ty.Iterable[SessionListing]
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
                    sessions.append(LocalSessionListing(session_dir))
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

        for session_listing in tqdm(
            sessions,
            total=num_sessions,
            desc=f"Processing staged sessions found in '{staged}'",
        ):

            try:

                if session_listing.all_uploaded(xnat_repo.connection):
                    logger.info(
                        "Skipping upload of '%s' as all the resources already exist on XNAT",
                        session_listing.name,
                    )
                    continue  # skip as session already exists

                session = ImagingSession.load(
                    session_listing.cache_path,
                    require_manifest=require_manifest,
                )
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

                # Get the XNAT session object (creates it if it does not exist)
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
                        # Upload the contents of the resource to XNAT
                        upload_method = get_method(type(resource.fileset))
                        # Get the directory containing the files to upload
                        # and create a temporary upload directory alongside it
                        # to hardlink files to upload in each batch into
                        dir_to_upload = resource.fileset.parent
                        upload_dir = dir_to_upload.parent / (
                            "." + dir_to_upload.name + "-upload"
                        )
                        # Split the files to upload into batches and hardlink them into
                        # separate directories so we can use upload_dir
                        files_to_upload = list(resource.fileset.fspaths)
                        num_files = len(files_to_upload)
                        batch_size = (
                            num_files_per_batch
                            if num_files_per_batch > 0
                            else num_files
                        )
                        num_batches = math.ceil(num_files / batch_size)
                        logger.debug(
                            "Uploading %s files to '%s' in %s in %s batches of %s files using '%s' method",
                            num_files,
                            resource.path,
                            xresource,
                            num_batches,
                            batch_size,
                            upload_method,
                        )
                        for i in range(num_batches):
                            # Create a temporary directory to upload the batch from
                            if upload_dir.exists():
                                shutil.rmtree(upload_dir)
                            upload_dir.mkdir()
                            for fspath in files_to_upload[
                                i * batch_size : (i + 1) * batch_size
                            ]:
                                dest = upload_dir / fspath.relative_to(dir_to_upload)
                                dest.hardlink_to(fspath)
                            logger.debug(
                                "Uploading batch %s of %s of '%s' to %s with '%s' method",
                                i,
                                num_batches,
                                upload_dir,
                                xresource,
                                upload_method,
                            )
                            xresource.upload_dir(upload_dir, method=upload_method)
                            shutil.rmtree(upload_dir)
                    logger.debug("retrieving checksums for %s", xresource)
                    remote_checksums = get_xnat_checksums(xresource)
                    if any(remote_checksums.values()):
                        logger.debug("calculating checksums for %s", xresource)
                        calc_checksums = calculate_checksums(resource.fileset)
                        if remote_checksums != calc_checksums:
                            extra_keys = set(remote_checksums) - set(calc_checksums)
                            missing_keys = set(calc_checksums) - set(remote_checksums)
                            intersect_keys = set(calc_checksums) & set(remote_checksums)
                            mismatching = [
                                k for k, v in intersect_keys if v != remote_checksums[k]
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
                        f"Skipping upload of '{session_listing.name}' due to error: \"{e}\""
                        f"\n{traceback.format_exc()}\n\n"
                    )
                    continue
                else:
                    raise

    if use_curl_jsession:
        xnat_repo.connection.exit()
