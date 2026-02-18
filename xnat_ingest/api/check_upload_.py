import logging
import pprint
import subprocess as sp
import tempfile
import typing as ty
from pathlib import Path

import tqdm
import xnat
from fileformats.core import FileSet, from_mime, to_mime
from fileformats.medimage import DicomSeries
from frametree.xnat import Xnat
from xnat.exceptions import XNATResponseError

from xnat_ingest.helpers.arg_types import LoggerConfig, StoreCredentials
from xnat_ingest.helpers.logging import logger, set_logger_handling
from xnat_ingest.helpers.remotes import (
    LocalSessionListing,
    SessionListing,
    get_xnat_checksums,
    iterate_s3_sessions,
)


def check_upload(
    input_dir: str,
    server: str,
    user: str,
    password: str,
    store_credentials: StoreCredentials,
    always_include: ty.Sequence[str | ty.Type[FileSet]] = (DicomSeries,),
    temp_dir: Path | None = None,
    verify_ssl: bool = True,
    use_curl_jsession: bool = False,
    disable_progress: bool = False,
) -> None:
    """Checks the staged sessions against the XNAT server to check for any issues before upload.

    This is intended to be used as a pre-upload check to identify any issues with the staged sessions before
    attempting to upload them to XNAT. It checks that the project, session, scan and resource exist on XNAT
    and that the checksums of the files in the resource match those on XNAT. If any issues are found, they
    are logged and a summary of the issues is logged at the end. If the `--raise-errors` flag is set, then
    any issues will raise an exception instead of being logged.

    Parameters
    ----------
    input_dir: str
        The directory containing the staged sessions to check.
    server: str
        The URL of the XNAT server to check against.
    user: str
        The username to use when connecting to the XNAT server.
    password: str
        The password to use when connecting to the XNAT server.
    loggers: list[LoggerConfig]
        The logger configurations to use for logging the output of the function.
    always_include: list[str]
        A list of MIME types to always include in the check, even if they aren't defined in
        the frameset on XNAT. This can be used to include additional file formats that aren't
        defined in the frameset, or to include all file formats by using "all".
    """

    xnat_repo = Xnat(
        server=server,
        user=user,
        password=password,
        cache_dir=Path(temp_dir) if temp_dir else Path(tempfile.mkdtemp()),
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

    included_formats = set()

    for mime_like in always_include:
        if mime_like == "all":
            included_formats.add(FileSet)
        else:
            fileformat = from_mime(mime_like)  # type: ignore[assignment]
            if not issubclass(fileformat, FileSet):
                raise ValueError(
                    f"{mime_like!r} does not correspond to a file format ({fileformat})"
                )
            included_formats.add(fileformat)

    with xnat_repo.connection:

        num_sessions: int
        sessions: ty.Iterable[SessionListing]
        if str(input_dir).startswith("s3://"):
            sessions = iterate_s3_sessions(
                input_dir, store_credentials, temp_dir, wait_period=0
            )
            # bit of a hack: number of sessions is the first item in the iterator
            num_sessions = next(sessions)  # type: ignore[assignment]
        else:
            sessions = []
            for session_dir in Path(input_dir).iterdir():
                sessions.append(LocalSessionListing(session_dir))
            num_sessions = len(sessions)
            logger.info(
                "Found %d sessions in staging directory to check '%s'",
                num_sessions,
                input_dir,
            )

        # framesets: dict[str, FrameSet] = {}

        num_issues = 0

        for session_listing in tqdm(
            sessions,
            total=num_sessions,
            desc=f"Processing staged sessions found in '{input_dir}'",
            disable=disable_progress,
        ):

            try:
                xproject = xnat_repo.connection.projects[session_listing.project_id]
            except KeyError:
                logger.error(
                    "MISSING PROJECT - %s (%s)",
                    session_listing.project_id,
                    session_listing.name,
                )
                continue

            # # Access Arcana frameset associated with project
            # try:
            #     frameset = framesets[session_listing.project_id]
            # except KeyError:
            #     try:
            #         frameset = FrameSet.load(session_listing.project_id, xnat_repo)
            #     except Exception as e:
            #         if not always_include:
            #             logger.error(
            #                 "Did not load frameset definition (%s) from %s project "
            #                 "on %s. Either '--always-include' flag must be used or "
            #                 "the frameset must be defined on XNAT using the `frametree` "
            #                 "command line tool (see https://arcanaframework.github.io/frametree/).",
            #                 e,
            #                 session_listing.project_id,
            #                 xnat_repo.server,
            #             )
            #             continue
            #         else:
            #             frameset = None
            #     framesets[session_listing.project_id] = frameset

            # Get the XNAT session object (creates it if it does not exist)
            session_desc = f"{session_listing.session_id} in {xproject.id}"
            try:
                xsession = xproject.experiments[session_listing.session_id]
            except KeyError:
                logger.error("MISSING SESSION - %s", session_desc)
                continue

            logger.info("CHECKING %s", session_listing.name)

            for resource_path, manifests in session_listing.resource_manifests.items():
                scan_path, resource_name = resource_path.split("/", 1)
                scan_id, _ = scan_path.split("-", 1)
                datatype = from_mime(manifests["datatype"])
                checksums = manifests["checksums"]
                if not any(issubclass(datatype, r) for r in included_formats):
                    logger.debug(
                        "Skipping checking %s in %s as it's datatype (%s) isn't in the included formats %s",
                        resource_path,
                        session_desc,
                        manifests["datatype"],
                        sorted(to_mime(f) for f in included_formats),
                    )
                    continue
                try:
                    xscan = xsession.scans[scan_id]
                except KeyError:
                    logger.error(
                        "MISSING SCAN - '%s' was not found in %s",
                        scan_path,
                        session_desc,
                    )
                    continue
                # Ensure that catalog is rebuilt if the file counts are 0
                try:
                    num_files = len(xscan.files)
                except XNATResponseError as e:
                    if e.status_code == 404:
                        logger.warning(
                            "ARCHIVED RESOURCE - attempting to access %s in %s resulted in a 404 error "
                            "looks like the session might be corrupted",
                            scan_path,
                            session_desc,
                            str(e),
                        )
                    else:
                        logger.error(
                            "POSSIBLY CORRUPT SESSION - attempting to access %s in %s resulted in a %s error "
                            "looks like the session might be corrupted",
                            scan_path,
                            session_desc,
                            str(e),
                        )
                    continue
                if not num_files:
                    # Force the rebuild of the catalog if no files are found to check they
                    # aren't there in the background already
                    xnat_repo.connection.post(
                        "/data/services/refresh/catalog?"
                        "options=populateStats,append,delete,checksum"
                        f"&resource=/archive/experiments/{xsession.id}/scans/{xscan.id}"
                    )
                    if not xscan.files:
                        logger.error(
                            "EMPTY SCAN - '%s' in %s is empty. Please delete on XNAT "
                            "to overwrite\n",
                            scan_path,
                            session_desc,
                        )
                        num_issues += 1
                        continue
                try:
                    xresource = xscan.resources[resource_name]
                except KeyError:
                    logger.error(
                        "MISSING RESOURCE - '%s' was not found in %s in %s",
                        resource_name,
                        scan_path,
                        session_desc,
                    )
                    num_issues += 1
                    continue
                xchecksums = get_xnat_checksums(xresource)
                if not any(xchecksums.values()):
                    logger.debug(
                        "Skipping checksum check for '%s' resource in '%s' in %s as "
                        "no checksums found on XNAT",
                        resource_name,
                        scan_path,
                        session_desc,
                    )
                elif sorted(checksums.values()) != sorted(xchecksums.values()):
                    fnames = set(checksums)
                    xfnames = set(xchecksums)
                    missing = xfnames - fnames
                    extra = fnames - xfnames
                    differing = {
                        k: (xchecksums[k], checksums[k])
                        for k in fnames & xfnames
                        if xchecksums[k] != checksums[k]
                    }
                    logger.error(
                        "CHECKSUM FAIL - '%s' resource in '%s' in %s already exists "
                        "on XNAT with different files/checksums. Please delete on XNAT to "
                        "overwrite:\n",
                        resource_name,
                        scan_path,
                        session_desc,
                    )
                    logger.debug(
                        "Checksum differences are:\n"
                        "    missing: %s\n"
                        "    extra: %s\n"
                        "    differing checksums:\n%s",
                        list(missing),
                        list(extra),
                        pprint.pformat(differing),
                    )
                    num_issues += 1

    if use_curl_jsession:
        xnat_repo.connection.exit()
