import math
import shutil
import tempfile
import traceback
import typing as ty
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from fileformats.generic import File, FileSet
from fileformats.medimage import DicomCollection
from frametree.core.frameset import FrameSet
from frametree.xnat import Xnat
from tqdm import tqdm
from xnat.exceptions import XNATResponseError

from xnat_ingest.helpers.remotes import (
    LocalSessionListing,
    SessionListing,
    SessionOnlyListing,
    calculate_checksums,
    dir_older_than,
    get_xnat_checksums,
    get_xnat_resource,
    get_xnat_session,
    iterate_s3_sessions,
    list_session_dirs,
)

from ..helpers.arg_types import StoreCredentials, UploadMethod
from ..helpers.logging import logger
from ..model.resource import ImagingResource
from ..model.session import ImagingSession


def has_scan_dicom(resources: ty.Iterable[ImagingResource]) -> bool:
    """Whether resources include DICOM files attached to an imaging scan."""
    return any(
        resource.scan is not None and isinstance(resource.fileset, DicomCollection)
        for resource in resources
    )


def upload(
    input_dir: str,
    xnat_repo: Xnat,
    always_include: ty.Sequence[str | FileSet] = (),
    store_credentials: StoreCredentials | None = None,
    require_manifest: bool = True,
    methods: ty.Sequence[UploadMethod] = (),
    wait_period: int = 0,
    num_files_per_batch: int = 0,
    check_checksums: bool = True,
    s3_cache_dir: ty.Optional[Path] = None,
    raise_errors: bool = False,
    dry_run: bool = False,
    max_workers: ty.Optional[int] = None,
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
    max_workers: int, optional
        The number of threads to use to upload resources within a session concurrently.
        Different resources map to different scans/catalogs on XNAT so are safe to
        upload in parallel; a failure uploading one resource doesn't stop the others
        from being attempted. If None, defaults to
        `concurrent.futures.ThreadPoolExecutor`'s default.
    """

    errors = []

    # Ensure input_path is a string so we can check for s3://
    input_dir = str(input_dir)

    # Note that this context manager doesn't do anything if the connection is
    # already open, so it's safe to use even if the connection is already open
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
            for session_dir in list_session_dirs(input_dir):
                if dir_older_than(session_dir, wait_period):
                    if "." in session_dir.name:
                        sessions.append(LocalSessionListing(session_dir))
                    else:
                        sessions.append(SessionOnlyListing(session_dir))
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

                if isinstance(session_listing, SessionOnlyListing):
                    xsession = session_listing.find_xnat_session(xnat_repo.connection)
                    if xsession is None:
                        raise RuntimeError(
                            f"No XNAT session found with label '{session_listing.session_id}'. "
                            "Ensure the session exists on XNAT before uploading session-only resources."
                        )
                    for resource_name in session_listing.resource_paths:
                        resource = ImagingResource.load(
                            session_listing.cache_path / resource_name,
                            require_manifest=require_manifest,
                            check_checksums=check_checksums,
                        )
                        uri = f"{xsession.uri}/resources/{resource.name}"
                        xnat_repo.connection.put(uri)
                        xnat_repo.connection.clearcache()
                        xresource = xnat_repo.connection.create_object(uri)
                        xresource.upload_dir(
                            session_listing.cache_path / resource.name,
                            method=UploadMethod.select_method(
                                methods, type(resource.fileset)
                            ),
                        )
                        logger.info(
                            "Uploaded '%s' to session '%s'",
                            resource.name,
                            session_listing.session_id,
                        )
                    logger.info(
                        "Successfully uploaded all resources to '%s'",
                        session_listing.session_id,
                    )
                    continue

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

                selected_resources = sorted(
                    session.select_resources(frameset, always_include=always_include)
                )
                session_has_dicom = has_scan_dicom(selected_resources)

                # Resolve which resources need uploading sequentially -- this can
                # create new scans/resources on XNAT and mutates xsession's/xscan's
                # shared caches, so isn't safe to do concurrently. The actual upload
                # of each resource's files is independent (different resources map to
                # different scan/resource catalogs on XNAT) so is safe to fan out.
                to_upload: list[tuple[ImagingResource, ty.Any]] = []
                for resource in selected_resources:
                    xresource = get_xnat_resource(resource, xsession)
                    if xresource is None:
                        logger.info(
                            "Skipping '%s' resource as it is already uploaded",
                            resource.path,
                        )
                        continue  # skipping as resource already exists
                    to_upload.append((resource, xresource))

                def _upload_resource(
                    resource: ImagingResource, xresource: ty.Any
                ) -> None:
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
                                "Remote checksums were not calculated for %s "
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

                resource_errors: list[tuple[ImagingResource, BaseException]] = []
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(_upload_resource, resource, xresource): resource
                        for resource, xresource in to_upload
                    }
                    for future in tqdm(
                        as_completed(futures),
                        total=len(futures),
                        desc=f"Uploading resources found in {session.name}",
                    ):
                        resource = futures[future]
                        try:
                            future.result()
                        except Exception as e:
                            logger.error(
                                "Failed to upload '%s' resource in '%s': %s\n%s",
                                resource.path,
                                session.name,
                                e,
                                traceback.format_exc(),
                            )
                            resource_errors.append((resource, e))

                if resource_errors:
                    msg = (
                        f"{len(resource_errors)} of {len(to_upload)} resource(s) "
                        f"failed to upload in '{session.name}': "
                        + ", ".join(r.path for r, _ in resource_errors)
                    )
                    errors.append(msg)
                    if raise_errors:
                        raise RuntimeError(msg) from resource_errors[0][1]
                    logger.error(msg)
                else:
                    logger.info(f"Successfully uploaded all files in '{session.name}'")
                # Extract DICOM metadata
                if session_has_dicom:
                    logger.info("Extracting metadata from DICOMs on XNAT..")
                    try:
                        xnat_repo.connection.put(
                            f"/data/experiments/{xsession.id}?pullDataFromHeaders=true"
                        )
                    except XNATResponseError as e:
                        logger.warning(
                            f"Failed to extract metadata: {e}\nResponse: "
                            f"{e.response.text if hasattr(e, 'response') else 'N/A'}"
                        )
                    try:
                        xnat_repo.connection.put(
                            f"/data/experiments/{xsession.id}?fixScanTypes=true"
                        )
                    except XNATResponseError as e:
                        logger.warning(
                            f"Failed to fix scan types in '{session.name}': {e}\nResponse: "
                            f"{e.response.text if hasattr(e, 'response') else 'N/A'}"
                        )
                else:
                    logger.info(
                        "Skipping DICOM header pull / scan-type fixing for '%s' as it "
                        "contains no DICOM data",
                        session.name,
                    )
                try:
                    xnat_repo.connection.put(
                        f"/data/experiments/{xsession.id}?triggerPipelines=true"
                    )
                except XNATResponseError as e:
                    logger.warning(
                        f"Failed to trigger pipelines in '{session.name}': {e}\nResponse: "
                        f"{e.response.text if hasattr(e, 'response') else 'N/A'}"
                    )
                logger.info(f"Successfully uploaded all files in '{session.name}'")
            except Exception as e:
                if not raise_errors:
                    msg = [
                        (
                            f"Skipping upload of '{session_listing.name}' due to error: \"{e}\""
                            f"\n{traceback.format_exc()}\n\n"
                        )
                    ]
                    logger.error("".join(msg))
                    errors.extend(msg)
                    continue
                else:
                    raise

    if errors:
        logger.error("Upload completed with %s errors", len(errors))
    else:
        logger.info("Upload completed successfully")
    return errors
