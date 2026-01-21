import logging
import pprint
import subprocess as sp
import tempfile
import typing as ty
from pathlib import Path

import click
import xnat
from fileformats.core import FileSet, from_mime

# from frametree.core.frameset import FrameSet
from frametree.xnat import Xnat
from tqdm import tqdm

from xnat_ingest.cli.base import cli
from xnat_ingest.upload_helpers import (
    LocalSessionListing,
    SessionListing,
    get_xnat_checksums,
    get_xnat_session,
    iterate_s3_sessions,
)
from xnat_ingest.utils import (
    LoggerConfig,
    StoreCredentials,
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
        "'image/jpeg'). Use 'all' to include all file-types in the session"
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
    help="The directory to use for temporary downloads (i.e. from s3)",
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
    always_include: ty.Sequence[str],
    additional_loggers: ty.List[str],
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
        if staged.startswith("s3://"):
            sessions = iterate_s3_sessions(
                staged, store_credentials, temp_dir, wait_period=0
            )
            # bit of a hack: number of sessions is the first item in the iterator
            num_sessions = next(sessions)  # type: ignore[assignment]
        else:
            sessions = []
            for session_dir in Path(staged).iterdir():
                sessions.append(LocalSessionListing(session_dir))
            num_sessions = len(sessions)
            logger.info(
                "Found %d sessions in staging directory to stage'%s'",
                num_sessions,
                staged,
            )

        # framesets: dict[str, FrameSet] = {}

        num_issues = 0

        for session_listing in tqdm(
            sessions,
            total=num_sessions,
            desc=f"Processing staged sessions found in '{staged}'",
        ):

            xproject = xnat_repo.connection.projects[session_listing.project_id]

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
            xsession = get_xnat_session(session_listing, xproject)

            session_desc = f"{session_listing.name} in {xproject.id}"

            for resource_path, manifests in session_listing.resource_manifests.items():
                scan_path, resource_name = resource_path.split("/", 1)
                scan_id, _ = scan_path.split("-", 1)
                datatype = from_mime(manifests["datatype"])
                checksums = manifests["checksums"]
                if not any(issubclass(datatype, r) for r in included_formats):
                    logger.debug(
                        "Skipping checking %s in %s as it isn't in the included formats %s",
                        resource_path,
                        session_desc,
                        included_formats,
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
                else:
                    # Ensure that catalog is rebuilt if the file counts are 0
                    if not xscan.files:
                        # Force the rebuild of the catalog if no files are found to check they aren't there in
                        # the background already
                        xscan.resources[resource_name].xnat_session.post(
                            "/data/services/refresh/catalog?options=populateStats,append,delete,checksum&"
                            f"resource=/archive/experiments/{xsession.id}/scans/{xscan.id}"
                        )
                        if not xscan.files:
                            logger.error(
                                "EMPTY SCAN - '%s' in %s is empty. Please delete on XNAT to overwrite\n",
                                scan_path,
                                session_desc,
                            )
                            num_issues += 1
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
                    else:
                        xchecksums = get_xnat_checksums(xresource)
                        if not any(xchecksums.values()):
                            logger.debug(
                                "Skipping checksum check for '%s' resource in '%s' in %s as no checksums found on XNAT",
                                resource_name,
                                scan_path,
                                session_desc,
                            )
                        elif checksums != xchecksums:
                            difference = {
                                k: (v, checksums[k])
                                for k, v in xchecksums.items()
                                if v != checksums[k]
                            }
                            logger.error(
                                "CHECKSUM FAIL - '%s' resource in '%s' in %s already exists on XNAT with "
                                "different checksums. Please delete on XNAT to overwrite:\n%s",
                                resource_name,
                                scan_path,
                                session_desc,
                                pprint.pformat(difference),
                            )
                            num_issues += 1

    if use_curl_jsession:
        xnat_repo.connection.exit()
