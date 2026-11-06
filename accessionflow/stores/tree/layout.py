"""How staged accessions are found, and where their resources go in a tree store.

An accession is deposited in a folder per label, in level order, under the root of
the store, with a folder per acquisition (named as in the staging directory,
'<id>.<description>') holding a folder per resource:

    <root>/<label1>/<label2>/.../<acq-id>.<acq-desc>/<resource>/<files...>
                                                              /__MANIFEST__.json
    <root>/<label1>/<label2>/.../<session-resource>/<files...>
    <root>/<label1>/<label2>/.../__FIELDS__.json

The resources' manifests (the checksums of their files) are kept, and are written
after the files they list, so a resource with a manifest is a complete one, on
every kind of file system. The accession's fields are kept too, but not its
metadata.
"""

import hashlib
import json
import tempfile
import typing as ty
from pathlib import Path

import attrs
import fsspec
from fsspec.implementations.local import LocalFileSystem

from accessionflow.helpers.arg_types import StoreCredentials
from accessionflow.helpers.logging import logger
from accessionflow.helpers.remotes import (
    LocalSessionListing,
    SessionListing,
    dir_older_than,
    iterate_s3_sessions,
    list_session_dirs,
)
from accessionflow.model.accession import Accession
from accessionflow.model.resource import Resource

STORE_TYPE = "tree"
FIELDS_FNAME = "__FIELDS__.json"


def open_store(
    dest: str, storage_options: ty.Mapping[str, ty.Any] | None = None
) -> tuple[fsspec.AbstractFileSystem, str]:
    """The file system of the store at `dest` (a path or an fsspec URL, e.g.
    's3://bucket/archive'), and the store's root path within it"""
    fs, root = fsspec.core.url_to_fs(str(dest), **dict(storage_options or {}))
    return fs, root.rstrip("/")


def staged_sessions(
    input_dir: str | Path,
    wait_period: int = 0,
    store_credentials: StoreCredentials | None = None,
    temp_dir: Path | None = None,
    s3_max_workers: int | None = None,
) -> ty.Iterator[SessionListing]:
    """The labelled accessions in a local staging directory, or an S3 bucket, that
    haven't been modified for `wait_period` seconds"""
    if str(input_dir).startswith("s3://"):
        if store_credentials is None:
            raise ValueError(
                f"Credentials are needed to read staged accessions from '{input_dir}'"
            )
        sessions = iterate_s3_sessions(
            str(input_dir),
            store_credentials,
            temp_dir,
            wait_period=wait_period,
            max_workers=s3_max_workers,
        )
        next(sessions)  # the number of sessions comes first
        yield from sessions
        return
    for session_dir in list_session_dirs(Path(input_dir)):
        if not dir_older_than(session_dir, wait_period):
            logger.info(
                "Skipping '%s' as it was modified less than %d seconds ago",
                session_dir.name,
                wait_period,
            )
            continue
        yield LocalSessionListing(session_dir)


def load_labelled(listing: SessionListing, require_manifest: bool) -> Accession:
    """Load a staged accession, checking it was labelled for a tree store"""
    accession = Accession.load(listing.cache_path, require_manifest=require_manifest)
    if not accession.labels:
        raise ValueError(f"'{listing.name}' hasn't been labelled")
    if accession.store_type not in (None, STORE_TYPE):
        raise ValueError(
            f"'{listing.name}' was labelled for a '{accession.store_type}' data store, "
            "not a tree. Label it with 'label tree' to deposit it in a tree"
        )
    return accession


@attrs.define
class StagedResource:
    """A resource of a staged accession, and where it goes in the store"""

    name: str
    staged_dir: Path
    store_dir: str
    #: file path (relative to the resource directory) -> MD5 checksum
    checksums: dict[str, str]
    datatype: str | None = None

    @property
    def manifest(self) -> dict[str, ty.Any]:
        return {"datatype": self.datatype, "checksums": self.checksums}


def staged_resources(
    accession: Accession, staged_dir: Path, root: str
) -> ty.Iterator[StagedResource]:
    """The resources of a staged accession, with their checksums and where they go in
    the store whose root is `root`"""
    store_dir = accession_dir(accession, root)
    resource_dirs = [
        (resource, f"{scan.id}.{scan.type or ''}/{resource.name}")
        for scan in accession.scans.values()
        for resource in scan.resources.values()
    ] + [(resource, resource.name) for resource in accession.session_resources.values()]
    for resource, relpath in resource_dirs:
        resource_dir = staged_dir / relpath
        manifest_fpath = Resource.manifest_fpath(resource_dir)
        if manifest_fpath.exists():
            manifest = json.loads(manifest_fpath.read_text())
            checksums = manifest["checksums"]
            datatype = manifest.get("datatype")
        else:
            # Staged without a manifest (--dont-require-manifest), so work the
            # checksums out from the files
            checksums = {
                str(p.relative_to(resource_dir).as_posix()): md5(p)
                for p in sorted(resource_dir.rglob("*"))
                if p.is_file() and not _is_side_car(p.name)
            }
            datatype = None
        yield StagedResource(
            name=relpath,
            staged_dir=resource_dir,
            store_dir=f"{store_dir}/{relpath}",
            checksums=checksums,
            datatype=datatype,
        )


def accession_dir(accession: Accession, root: str) -> str:
    return "/".join([root, *accession.labels.values()])


def md5(path: Path) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(2**20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def remote_md5(fs: fsspec.AbstractFileSystem, path: str) -> str:
    digest = hashlib.md5()
    with fs.open(path, "rb") as f:
        for chunk in iter(lambda: f.read(2**20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_store_manifest(
    fs: fsspec.AbstractFileSystem, store_dir: str
) -> dict[str, ty.Any] | None:
    """The manifest of a resource in the store, or None if it has none (it hasn't
    been deposited, or depositing it didn't finish) or it can't be read"""
    try:
        manifest = json.loads(fs.cat_file(f"{store_dir}/{Resource.MANIFEST_FNAME}"))
    except FileNotFoundError:
        return None
    except (ValueError, UnicodeDecodeError):
        logger.warning("The manifest of '%s' can't be read", store_dir)
        return None
    return manifest if isinstance(manifest, dict) else None


def write_atomically(fs: fsspec.AbstractFileSystem, path: str, data: bytes) -> None:
    """Write a small file in one call, so that it is either absent or complete"""
    if isinstance(fs, LocalFileSystem):
        # A local write can be a rename, which is atomic
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=target.parent, prefix=f".{target.name}.", delete=False
        ) as tmp:
            tmp.write(data)
        Path(tmp.name).replace(target)
    else:
        # Object stores publish an object only once it has been written in full
        fs.pipe_file(path, data)


def _is_side_car(fname: str) -> bool:
    return fname in (
        Resource.MANIFEST_FNAME,
        Resource.OLD_MANIFEST_FNAME,
        Resource.MANIFEST_TMP_FNAME,
        "__METADATA__.json",
    ) or fname.endswith(".tmp")
