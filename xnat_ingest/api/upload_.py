import logging
import math
import shutil
import subprocess as sp
import tempfile
import traceback
import typing as ty
from pathlib import Path

import xnat
from fileformats.generic import File, FileSet
from frametree.core.frameset import FrameSet
from frametree.xnat import Xnat
from tqdm import tqdm
from xnat.exceptions import XNATResponseError

from xnat_ingest.helpers.remotes import (
    LocalSessionListing,
    SessionListing,
    calculate_checksums,
    dir_older_than,
    get_xnat_checksums,
    get_xnat_resource,
    get_xnat_session,
    iterate_s3_sessions,
)

from ..helpers.arg_types import StoreCredentials, UploadMethod
from ..helpers.logging import logger
from ..model.session import ImagingSession


def upload(
    input_dir: Path,
    server: str,
    user: str,
    password: str,
    always_include: ty.Sequence[str | FileSet],
    store_credentials: StoreCredentials | None = None,
    require_manifest: bool = True,
    verify_ssl: bool = True,
    methods: ty.Sequence[UploadMethod] = (),
    wait_period: int = 0,
    num_files_per_batch: int = 0,
    check_checksums: bool = True,
    use_curl_jsession: bool = False,
    s3_cache_dir: ty.Optional[Path] = None,
    raise_errors: bool = False,
    dry_run: bool = False,
) -> list[str]:
    """Upload sorted sessions in the given staging directory to XNAT

    Parameters
    ----------
    input_dir: Path
        The directory containing the sessions to upload. Each session should be in a separate subdirectory.
    xnat_repo: Xnat
        The XNAT repository to upload to
    always_include: Sequence[str]
        A sequence of scan types or file paths to always include in the upload regardless of whether they are
        explicitly specified in the frameset definition
    raise_errors: bool
        Whether to raise errors that occur during upload or to log them and continue with the next session
    store_credentials: StoreCredentials
        Whether to store credentials for accessing staging directories that require authentication (e.g. S3)
    require_manifest: bool
        Whether to require a manifest file in each session directory that specifies the resources to upload
        and their checksums
    methods: Sequence[UploadMethod]
        The upload method to use for each datatype (e.g. 'tgz_file' or 'directory')
    wait_period: int
        The minimum age in seconds of session directories to upload (only applicable for local staging directories)
    num_files_per_batch: int
        The number of files to upload in each batch when uploading resources with the 'directory' method
        (if 0, all files will be uploaded in a single batch)
    check_checksums: bool
        Whether to check checksums of uploaded resources against the checksums specified in the manifest file and
        the checksums of the files in the staged resources (if available) to verify that they were
    dry_run: bool
         Whether to list the sessions that would be uploaded instead of actually uploading them
    """

    errors = []

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
        if input_dir.startswith("s3://"):
            if s3_cache_dir is None:
                s3_cache_dir = Path(tempfile.mkdtemp())
                logger.info(
                    f"Using temporary directory '{s3_cache_dir}' to cache S3 files during upload"
                )
            sessions = iterate_s3_sessions(
                input_dir, store_credentials, s3_cache_dir, wait_period=wait_period
            )
            # bit of a hack: number of sessions is the first item in the iterator
            num_sessions = next(sessions)  # type: ignore[assignment]
        else:
            sessions = []
            for session_dir in Path(input_dir).iterdir():
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
                input_dir,
            )

        framesets: dict[str, FrameSet] = {}

        for session_listing in tqdm(
            sessions,
            total=num_sessions,
            desc=f"Processing staged sessions found in '{input_dir}'",
        ):

            if dry_run:
                logger.info(
                    "Would attempt to upload '%s' if not dry run",
                    session_listing.name,
                )
                continue

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
                    check_checksums=check_checksums,
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
                        upload_method = UploadMethod.select_method(
                            methods, type(resource.fileset)
                        )
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
                    if check_checksums:
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
                                intersect_keys = set(calc_checksums) & set(
                                    remote_checksums
                                )
                                mismatching = [
                                    k
                                    for k, v in intersect_keys
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
                    else:
                        logger.debug(
                            "Not checking checksums for '%s' resource as checksum "
                            "checking is disabled",
                            resource.path,
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
                    msg = [
                        f"Skipping upload of '{session_listing.name}' due to error: \"{e}\""
                        f"\n{traceback.format_exc()}\n\n"
                    ]
                    logger.error("".join(msg))
                    errors.extend(msg)
                    continue
                else:
                    raise

        if use_curl_jsession:
            xnat_repo.connection.exit()
        return errors
