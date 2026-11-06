"""Helper functions and classes for listing staged sessions, locally and in S3
buckets, and for working with other remote storage locations such as SSH servers"""

import abc
import datetime
import hashlib
import io
import json
import os
import shutil
import tempfile
import threading
import typing as ty
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import attrs
import boto3.resources.base
import paramiko
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from fileformats.application import Json
from fileformats.core import FileSet
from tqdm import tqdm

from accessionflow.exceptions import (
    SessionIdentityRefused,
)
from accessionflow.model.accession import Accession
from accessionflow.model.resource import Resource

from .arg_types import StoreCredentials
from .logging import logger
from .metadata import Metadata

DEFAULT_MAX_WORKERS = 10


def read_grouping_uid(metadata: ty.Mapping[str, ty.Any]) -> str | None:
    """Read the format-independent identity saved by Accession."""
    if not isinstance(metadata, ty.Mapping):
        raise SessionIdentityRefused(
            "Cannot verify session identity: malformed metadata"
        )
    value = metadata.get(Accession.UID_METADATA_KEY)
    if value is None or value == "":
        return None
    if not isinstance(value, str) or not value.strip():
        raise SessionIdentityRefused(
            "Cannot verify session identity: ambiguous grouping UID"
        )
    return value


class SessionListing(metaclass=abc.ABCMeta):
    # Every subclass supplies this, as a field or a property, and `ids` below
    # has always relied on it. Declared so that reliance is part of the
    # interface rather than an assumption each caller has to make afresh.
    name: str

    @property
    @abc.abstractmethod
    def cache_path(self) -> Path:
        pass

    @property
    @abc.abstractmethod
    def resource_paths(self) -> set[str]:
        pass

    @property
    def resource_manifests(self) -> dict[str, dict[str, ty.Any]]:
        """The staged manifests keyed by resource path.

        A manifest holds more than checksums, so the values are deliberately
        untyped: "checksums" maps file name to digest, other keys do not.

        Read from the staging directory. A listing that stages elsewhere
        overrides this. With no manifest present this raises, which
        all_uploaded() treats as "cannot check".
        """
        manifests = {}
        for relpath in sorted(self.resource_paths):
            resource_dir = self.cache_path / relpath
            if resource_dir.is_dir():
                manifest = Json(Resource.manifest_fpath(resource_dir))
                manifests[relpath] = manifest.contents
        return manifests

    @property
    def expected_uid(self) -> str | None:
        """The UID that a session on XNAT must hold, or None if it isn't to be checked.

        The subject of the session is still checked when this is None."""
        return None if self.skip_uid_check else self.grouping_uid

    @property
    def grouping_uid(self) -> str | None:
        try:
            metadata = json.loads((self.cache_path / Metadata.FNAME).read_text())
        except FileNotFoundError:
            return None  # Legacy staging directories need not carry metadata.
        return read_grouping_uid(metadata)

    @property
    def ids(self):
        if "." in self.name:
            ids = self.name.split(".")
        else:
            # For backwards compatibility
            ids = self.name.split("-")[:3]
        return ids

    @property
    def project_id(self) -> str:
        return self.ids[0]

    @property
    def subject_id(self) -> str:
        return self.ids[1]

    @property
    def session_id(self) -> str:
        return self.ids[2]

    @property
    def is_session_only(self) -> bool:
        return False


@attrs.define
class LocalSessionListing(SessionListing):
    fspath: Path
    skip_uid_check: bool = attrs.field(default=False, kw_only=True)

    @property
    def cache_path(self) -> Path:
        return self.fspath

    @property
    def resource_paths(self) -> set[str]:
        # Hidden entries are not resources, as the loaders treat them: an upload
        # batch folder (".<resource>-upload-*") that a killed uploader left.
        paths = {
            str(p.relative_to(self.fspath))
            for p in self.fspath.glob("*/*")
            if not p.name.startswith(".") and not p.parent.name.startswith(".")
        }
        # session resources: top-level dirs with no "." in name
        for item in self.fspath.iterdir():
            if item.is_dir() and "." not in item.name:
                paths.add(item.name)
        paths -= {p for p in paths if Path(p).name == Metadata.FNAME}
        return paths

    @property
    def name(self) -> str:  # type: ignore[override]  # base declares a plain attr
        return self.fspath.name

    @property
    def session_id(self) -> str:
        return self.ids[2]


@attrs.define
class SessionOnlyListing(SessionListing):
    """A staging directory named by session label only (no project.subject.visit structure).

    Used when uploading resources directly to an existing XNAT session identified only by
    its label. The label must be globally unique across all projects accessible to the upload
    account — an error is raised if multiple sessions match.
    """

    fspath: Path
    skip_uid_check: bool = attrs.field(default=False, kw_only=True)

    @property
    def cache_path(self) -> Path:
        return self.fspath

    @property
    def name(self) -> str:  # type: ignore[override]  # base declares a plain attr
        return self.fspath.name

    @property
    def session_id(self) -> str:
        return self.fspath.name

    @property
    def is_session_only(self) -> bool:
        return True

    @property
    def resource_paths(self) -> set[str]:
        # FIXME: This doesn't look right. It looks like it is picking out the
        # scan level not the session level.
        return {
            item.name
            for item in self.fspath.iterdir()
            if item.is_dir() and "." not in item.name
        }

    # all_uploaded is deliberately NOT defined here. Only the session lookup
    # (find_xnat_session and find_xnat_session_uri) differs between the modes;
    # a second copy of the completeness rule is how
    # this class kept a label comparison after the base class stopped using one.


@attrs.define
class S3SessionListing(SessionListing):
    name: str
    bucket: ty.Any
    objects: ty.List[ty.Tuple[ty.List[str], ty.Any]]
    _cache_path: Path
    max_workers: int = DEFAULT_MAX_WORKERS
    skip_uid_check: bool = attrs.field(default=False, kw_only=True)
    _downloaded: bool = attrs.field(default=False, init=False)
    _download_lock: threading.Lock = attrs.field(factory=threading.Lock, init=False)

    @property
    def cache_path(self) -> Path:
        """Download the session once and reuse it for the life of the listing.

        api/upload_api.py reads this four times for a single upload. Downloading
        inside a property meant each read re-fetched the whole session, so a
        1.2 GB session was pulled from S3 four times over. The download is
        guarded by a lock as well as the flag, because the property is reachable
        from more than one thread and a bare flag would let two downloads write
        the same paths concurrently, each opening them with mode "wb".
        """
        with self._download_lock:
            if self._downloaded:
                return self._cache_path
            self._download_objects()
            self._downloaded = True
        return self._cache_path

    def _download_objects(self) -> None:
        logger.info(
            "Downloading session '%s' from S3 bucket with %d workers",
            self.name,
            self.max_workers,
        )
        transfer_config = TransferConfig(max_concurrency=1, use_threads=False)

        def _download(item: ty.Tuple[ty.List[str], ty.Any]) -> None:
            relpath, obj = item
            obj_path = self._cache_path.joinpath(*relpath)
            obj_path.parent.mkdir(parents=True, exist_ok=True)
            logger.debug("Downloading %s to %s", obj, obj_path)
            with open(obj_path, "wb") as f:
                self.bucket.download_fileobj(obj.key, f, Config=transfer_config)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            list(
                tqdm(
                    executor.map(_download, self.objects),
                    total=len(self.objects),
                    desc=f"Downloading scans in '{self.name}' session from S3 bucket",
                )
            )

    @property
    def grouping_uid(self) -> str | None:
        metadata_objects = [
            obj for parts, obj in self.objects if parts == [Metadata.FNAME]
        ]
        if not metadata_objects:
            return None
        if len(metadata_objects) != 1:
            raise SessionIdentityRefused(
                "Cannot verify session identity: duplicate metadata"
            )
        with io.BytesIO() as stream:
            self.bucket.download_fileobj(metadata_objects[0].key, stream)
            return read_grouping_uid(json.loads(stream.getvalue()))

    @property
    def resource_paths(self) -> set[str]:
        paths = set()
        for path_parts, _ in self.objects:
            if not path_parts:
                continue
            first = path_parts[0]
            if first.startswith(".") or (
                len(path_parts) >= 2 and path_parts[1].startswith(".")
            ):
                continue  # hidden: an upload batch folder copied with the session
            if "." in first:
                # scan resource: <scan_id>.<scan_type>/<resource_name>
                if len(path_parts) >= 2:
                    paths.add(f"{first}/{path_parts[1]}")
            else:
                # session resource: <resource_name> (no dot in dir name)
                paths.add(first)
        paths -= {p for p in paths if Path(p).name == Metadata.FNAME}
        return paths

    @property
    def resource_manifests(self) -> dict[str, dict[str, str]]:
        manifests = {}
        manifest_fnames_by_relpath: dict[str, str] = {}
        for path_parts, obj in self.objects:
            if not path_parts:
                # An object sitting directly at the session root. resource_paths
                # skips these; without the same guard this raised IndexError on
                # every pass, which the caller reads as "cannot check".
                continue
            fname = path_parts[-1]
            if fname not in (
                Resource.MANIFEST_FNAME,
                Resource.OLD_MANIFEST_FNAME,
            ):
                continue
            relpath = "/".join(path_parts[:-1])
            # Prefer the current manifest filename over the legacy one if both are
            # present in the same resource directory
            if (
                relpath in manifest_fnames_by_relpath
                and fname == Resource.OLD_MANIFEST_FNAME
            ):
                continue
            manifest_fnames_by_relpath[relpath] = fname
            manifest_path = self._cache_path / relpath / fname
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            with open(manifest_path, "wb") as f:
                self.bucket.download_fileobj(obj.key, f)
            manifests[relpath] = Json(manifest_path).contents
        return manifests


@attrs.define
class S3SessionOnlyListing(S3SessionListing):
    """An S3 staging folder named only by its existing XNAT session label."""

    @property
    def session_id(self) -> str:
        return self.name

    @property
    def is_session_only(self) -> bool:
        return True


def iterate_s3_sessions(
    bucket_path: str,
    store_credentials: StoreCredentials,
    temp_dir: Path | None,
    wait_period: int,
    max_workers: int | None = None,
    skip_uid_check: bool = False,
    listing_class: type["S3SessionListing"] | None = None,
    session_only_listing_class: type["S3SessionListing"] | None = None,
) -> ty.Iterator[SessionListing]:
    """Iterate over sessions stored in an S3 bucket

    Parameters
    ----------
    bucket_path : str
        the path to the S3 bucket
    store_credentials : StoreCredentials
        the credentials to access the S3 bucket
    temp_dir : Path, optional
        the temporary directory to download the sessions to, by default None
    wait_period : int
        the number of seconds after the last write before considering a session complete
    max_workers : int, optional
        maximum number of S3 objects to download concurrently. Defaults to 10,
        matching botocore's default connection-pool size.
    listing_class : type[S3SessionListing], optional
        the class to list sessions named 'project.subject.session' with, by default
        S3SessionListing. Store plugins pass a subclass that can find the
        session in the store
    session_only_listing_class : type[S3SessionListing], optional
        the class to list sessions named by session label only with, by default
        S3SessionOnlyListing
    """
    if max_workers is None:
        max_workers = DEFAULT_MAX_WORKERS
    elif max_workers < 1:
        raise ValueError("max_workers must be greater than zero")

    # List sessions stored in s3 bucket
    s3: boto3.resources.base.ServiceResource = boto3.resource(
        "s3",
        aws_access_key_id=store_credentials.access_key,
        aws_secret_access_key=store_credentials.access_secret,
        config=Config(max_pool_connections=max_workers),
    )
    bucket_name, prefix = bucket_path[5:].split("/", 1)
    bucket = s3.Bucket(bucket_name)
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    all_objects = bucket.objects.filter(Prefix=prefix)
    session_objs = defaultdict(list)
    for obj in all_objects:
        if obj.key.endswith("/"):
            continue  # skip directories
        path_parts = obj.key[len(prefix) :].split("/")
        session_name = path_parts[0]
        if session_name.startswith("__") and session_name.endswith("__"):
            continue  # skip internal directories
        session_objs[session_name].append((path_parts[1:], obj))

    num_sessions = len(session_objs)
    # Bit of a hack to allow the caller to know how many sessions are in the bucket
    # we yield the number of sessions as the first item in the iterator
    yield num_sessions  # type: ignore[misc]

    if temp_dir:
        tmp_download_dir = Path(temp_dir) / "accessionflow-download"
        tmp_download_dir.mkdir(parents=True, exist_ok=True)
    else:
        tmp_download_dir = Path(tempfile.mkdtemp())

    for session_name, objs in session_objs.items():
        # Just in case the manifest file is not included in the list of objects
        # we recreate the project/subject/session directory structure
        session_tmp_dir = tmp_download_dir / session_name
        session_tmp_dir.mkdir(parents=True, exist_ok=True)
        try:
            # Check to see if the session is still being updated
            last_modified = None
            for _, obj in objs:
                if last_modified is None or obj.last_modified > last_modified:
                    last_modified = obj.last_modified
            assert last_modified is not None
            if (
                datetime.datetime.now(datetime.timezone.utc) - last_modified
            ) >= datetime.timedelta(seconds=wait_period):
                Listing = (
                    (listing_class or S3SessionListing)
                    if "." in session_name
                    else (session_only_listing_class or S3SessionOnlyListing)
                )
                yield Listing(
                    name=session_name,
                    objects=session_objs[session_name],
                    bucket=bucket,
                    cache_path=session_tmp_dir,
                    max_workers=max_workers,
                    skip_uid_check=skip_uid_check,
                )
            else:
                logger.info(
                    "Skipping session '%s' as it was last modified less than %d "
                    "seconds ago and waiting until it is complete",
                    session_name,
                    wait_period,
                )
        finally:
            # Delete the tmp session after the upload, also when the caller
            # stops early (e.g. on an error).
            shutil.rmtree(session_tmp_dir, ignore_errors=True)

    logger.info("Found %d sessions in S3 bucket '%s'", num_sessions, bucket_path)
    logger.debug("Created sessions iterator")


def remove_old_files_on_s3(remote_store: str, threshold: int) -> None:
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


def remove_old_files_on_ssh(remote_store: str, threshold: int) -> None:
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


def calculate_checksums(
    scan: FileSet, max_workers: ty.Optional[int] = None
) -> ty.Dict[str, str]:
    """
    Calculates the MD5 digests associated with the files in a fileset.

    Parameters
    ----------
    scan : FileSet
        the file-set to calculate the checksums for
    max_workers : int, optional
        the number of threads to use to hash the files concurrently. If None,
        defaults to `concurrent.futures.ThreadPoolExecutor`'s default.

    Returns
    -------
    dict[str, str]
        the calculated checksums
    """

    return scan.hash_files(
        crypto=hashlib.md5,
        chunk_len=HASH_CHUNK_SIZE,
        relative_to=scan.parent,
        max_workers=max_workers,
    )


HASH_CHUNK_SIZE = 2**20


def dir_older_than(path: Path, period: int) -> bool:
    """
    Get the most recent modification time of a directory and its contents.

    Parameters
    ----------
    path : Path
        the directory to get the modification time of
    period : int
        the number of seconds after the last modification time to check against

    Returns
    -------
    bool
        whether the directory is older than the specified period
    """
    mtimes = [path.stat().st_mtime]
    for root, _, files in os.walk(path):
        for file in files:
            mtimes.append((Path(root) / file).stat().st_mtime)
    last_modified = datetime.datetime.fromtimestamp(max(mtimes))
    return (datetime.datetime.now() - last_modified) >= datetime.timedelta(
        seconds=period
    )


def upload_file_to_s3(file_path: Path, bucket: str, s3_key: str) -> None:

    s3_client = boto3.client("s3")
    s3_client.upload_file(str(file_path), bucket, s3_key)


def list_session_dirs(sorted_dir: Path) -> list[Path]:
    """List the session directories in the sorted directory, excluding any directories that start with '__'.

    Includes both dotted dirs (PROJ.SUBJ.VISIT) and no-dot dirs (session label only).
    """
    return [
        p
        for p in Path(sorted_dir).iterdir()
        if p.is_dir() and not p.name.startswith("__") and not p.name.endswith("__")
    ]
