import datetime
import hashlib
import os
import pprint
import shutil
import tempfile
import typing as ty
from collections import defaultdict
from pathlib import Path

import boto3.resources.base
import paramiko
from fileformats.core import FileSet
from tqdm import tqdm

from xnat_ingest.utils import StoreCredentials, logger

from .resource import ImagingResource
from .session import ImagingSession


def iterate_s3_sessions(
    bucket_path: str,
    store_credentials: StoreCredentials,
    temp_dir: Path | None,
    wait_period: int,
) -> ty.Iterator[Path]:
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
    """
    # List sessions stored in s3 bucket
    s3: boto3.resources.base.ServiceResource = boto3.resource(
        "s3",
        aws_access_key_id=store_credentials.access_key,
        aws_secret_access_key=store_credentials.access_secret,
    )
    bucket_name, prefix = bucket_path[5:].split("/", 1)
    bucket = s3.Bucket(bucket_name)
    if not prefix.endswith("/"):
        prefix += "/"
    all_objects = bucket.objects.filter(Prefix=prefix)
    session_objs = defaultdict(list)
    for obj in all_objects:
        if obj.key.endswith("/"):
            continue  # skip directories
        path_parts = obj.key[len(prefix) :].split("/")
        session_name = path_parts[0]
        session_objs[session_name].append((path_parts[1:], obj))

    num_sessions = len(session_objs)
    # Bit of a hack to allow the caller to know how many sessions are in the bucket
    # we yield the number of sessions as the first item in the iterator
    yield num_sessions  # type: ignore[misc]

    if temp_dir:
        tmp_download_dir = temp_dir / "xnat-ingest-download"
        tmp_download_dir.mkdir(parents=True, exist_ok=True)
    else:
        tmp_download_dir = Path(tempfile.mkdtemp())

    for session_name, objs in session_objs.items():
        # Just in case the manifest file is not included in the list of objects
        # we recreate the project/subject/sesssion directory structure
        session_tmp_dir = tmp_download_dir / session_name
        session_tmp_dir.mkdir(parents=True, exist_ok=True)
        # Check to see if the session is still being updated
        last_modified = None
        for _, obj in objs:
            if last_modified is None or obj.last_modified > last_modified:
                last_modified = obj.last_modified
        assert last_modified is not None
        if (
            datetime.datetime.now(datetime.timezone.utc) - last_modified
        ) >= datetime.timedelta(seconds=wait_period):
            logger.info("Downloading session '%s' from S3 bucket", session_name)
            for relpath, obj in tqdm(
                objs,
                desc=f"Downloading scans in '{session_name}' session from S3 bucket",
            ):
                if last_modified is None or obj.last_modified > last_modified:
                    last_modified = obj.last_modified
                obj_path = session_tmp_dir.joinpath(*relpath)
                obj_path.parent.mkdir(parents=True, exist_ok=True)
                logger.debug("Downloading %s to %s", obj, obj_path)
                with open(obj_path, "wb") as f:
                    bucket.download_fileobj(obj.key, f)
            yield session_tmp_dir
        else:
            logger.info(
                "Skipping session '%s' as it was last modified less than %d seconds ago "
                "and waiting until it is complete",
                session_name,
                wait_period,
            )
        shutil.rmtree(session_tmp_dir)  # Delete the tmp session after the upload

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


def get_xnat_session(session: ImagingSession, xproject: ty.Any) -> ty.Any:
    """Get the XNAT session object for the given session

    Parameters
    ----------
    session : ImagingSession
        the session to upload
    xnat_repo : Xnat
        the XNAT repository to upload to

    Returns
    -------
    xsession : ty.Any
        the XNAT session object
    """
    xclasses = xproject.xnat_session.classes

    xsubject = xclasses.SubjectData(label=session.subject_id, parent=xproject)
    try:
        xsession = xproject.experiments[session.session_id]
    except KeyError:
        if "MR" in session.modalities:
            SessionClass = xclasses.MrSessionData
        elif "PT" in session.modalities:
            SessionClass = xclasses.PetSessionData
        elif "CT" in session.modalities:
            SessionClass = xclasses.CtSessionData
        else:
            raise RuntimeError(
                "Found the following unsupported modalities in "
                f"{session.name}: {session.modalities}"
            )
        xsession = SessionClass(label=session.session_id, parent=xsubject)
    return xsession


def get_xnat_resource(resource: ImagingResource, xsession: ty.Any) -> ty.Any:
    """Get the XNAT resource object for the given resource

    Parameters
    ----------
    resource : ImagingResource
        the resource to upload
    xsession : ty.Any
        the XNAT session object

    Returns
    -------
    xresource : ty.Any
        the XNAT resource object
    """
    xclasses = xsession.xnat_session.classes
    try:
        xscan = xsession.scans[resource.scan.id]
    except KeyError:
        if isinstance(xsession, xclasses.MrSessionData):
            default_scan_modality = "MR"
        elif isinstance(xsession, xclasses.PetSessionData):
            default_scan_modality = "PT"
        else:
            default_scan_modality = "CT"
        if resource.metadata:
            image_type = resource.metadata.get("ImageType")
            if image_type and image_type[:2] == [
                "DERIVED",
                "SECONDARY",
            ]:
                modality = "SC"
                resource_name = "secondary"
            else:
                modality = resource.metadata.get("Modality", default_scan_modality)
        else:
            modality = default_scan_modality
        if modality == "SC":
            ScanClass = xclasses.ScScanData
        elif modality == "MR":
            ScanClass = xclasses.MrScanData
        elif modality == "PT":
            ScanClass = xclasses.PetScanData
        elif modality == "CT":
            ScanClass = xclasses.CtScanData
        else:
            SessionClass = type(xsession)
            if SessionClass is xclasses.PetSessionData:
                ScanClass = xclasses.PetScanData
            elif SessionClass is xclasses.CtSessionData:
                ScanClass = xclasses.CtScanData
            else:
                ScanClass = xclasses.MrScanData
            logger.info(
                "Can't determine modality of %s-%s scan, defaulting to the "
                "default for %s sessions, %s",
                resource.scan.id,
                resource.scan.type,
                SessionClass,
                ScanClass,
            )
        logger.debug(
            "Creating scan %s in %s", resource.scan.id, resource.scan.session.path
        )
        xscan = ScanClass(
            id=resource.scan.id,
            type=resource.scan.type,
            parent=xsession,
        )
    try:
        xresource = xscan.resources[resource.name]
    except KeyError:
        pass
    else:
        checksums = get_xnat_checksums(xresource)
        if checksums == resource.checksums:
            logger.info(
                "Skipping '%s' resource in '%s' as it " "already exists on XNAT",
                resource.name,
                resource.scan.path,
            )
        else:
            difference = {
                k: (v, resource.checksums[k])
                for k, v in checksums.items()
                if v != resource.checksums[k]
            }
            logger.error(
                "'%s' resource in '%s' already exists on XNAT with "
                "different checksums. Please delete on XNAT to overwrite:\n%s",
                resource.name,
                resource.scan.path,
                pprint.pformat(difference),
            )
        # Ensure that catalog is rebuilt if the file counts are 0
        if not xscan.files:
            xresource.xnat_session.post(
                "/data/services/refresh/catalog?options=populateStats,append,delete,checksum&"
                f"resource=/archive/experiments/{xsession.id}/scans/{xscan.id}"
            )
            if not xscan.files:
                logger.error(
                    "'%s' resource in '%s' already exists on XNAT with "
                    "and is empty. Please delete on XNAT to overwrite\n",
                    resource.name,
                    resource.scan.path,
                )
        return None
    logger.debug(
        "Creating resource %s in %s",
        resource.name,
        resource.scan.path,
    )
    xresource = xscan.create_resource(resource.name)
    return xresource


def get_xnat_checksums(xresource: ty.Any) -> dict[str, str]:
    """
    Downloads the MD5 digests associated with the files in a resource.

    Parameters
    ----------
    xresource : xnat.classes.Resource
        XNAT resource to retrieve the checksums from

    Returns
    -------
    dict[str, str]
        the checksums calculated by XNAT
    """
    result = xresource.xnat_session.get(xresource.uri + "/files")
    if result.status_code != 200:
        raise RuntimeError(
            "Could not download metadata for resource {}. Files "
            "may have been uploaded but cannot check checksums".format(xresource.id)
        )
    return dict((r["Name"], r["digest"]) for r in result.json()["ResultSet"]["Result"])


def calculate_checksums(scan: FileSet) -> ty.Dict[str, str]:
    """
    Calculates the MD5 digests associated with the files in a fileset.

    Parameters
    ----------
    scan : FileSet
        the file-set to calculate the checksums for

    Returns
    -------
    dict[str, str]
        the calculated checksums
    """
    checksums = {}
    for fspath in scan.fspaths:
        try:
            hsh = hashlib.md5()
            with open(fspath, "rb") as f:
                for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b""):
                    hsh.update(chunk)
            checksum = hsh.hexdigest()
        except OSError:
            raise RuntimeError(f"Could not create digest of '{fspath}' ")
        checksums[str(fspath.relative_to(scan.parent))] = checksum
    return checksums


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
    return (datetime.datetime.now() - last_modified) >= datetime.timedelta(
        seconds=period
    )
