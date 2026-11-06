import json
import traceback
import typing as ty
from pathlib import Path

import fsspec

from accessionflow.helpers.arg_types import StoreCredentials
from accessionflow.helpers.logging import logger
from accessionflow.model.resource import Resource

from ..layout import (
    FIELDS_FNAME,
    StagedResource,
    accession_dir,
    load_labelled,
    open_store,
    read_store_manifest,
    staged_resources,
    staged_sessions,
    write_atomically,
)


def store(
    input_dir: str | Path,
    dest: str,
    storage_options: ty.Mapping[str, ty.Any] | None = None,
    wait_period: int = 0,
    require_manifest: bool = True,
    overwrite: bool = False,
    raise_errors: bool = False,
    store_credentials: StoreCredentials | None = None,
    temp_dir: Path | None = None,
    s3_max_workers: int | None = None,
) -> list[str]:
    """Deposits the labelled accessions in a staging directory in a tree store: a tree
    of nested label folders on a local path, a mount, or a remote file system such as
    S3 or SFTP (see `accessionflow.stores.tree.layout` for the layout).

    Depositing again is cheap: a resource whose manifest in the store matches the
    staged one is skipped, and one left without a manifest by an interrupted deposit
    is deposited again.

    Parameters
    ----------
    input_dir : str or Path
        the staging directory holding the labelled accessions (the output of 'label
        tree', or of 'deidentify' after it), or an S3 bucket ('s3://...') to download
        them from
    dest : str
        the root of the store, a path or an fsspec URL, e.g. '/mnt/archive',
        's3://bucket/archive' or 'sftp://host/archive'
    storage_options : Mapping[str, Any], optional
        options for the fsspec file system of the store, e.g. credentials
    wait_period : int
        the number of seconds since an accession was last modified before it is
        deposited, so one that is still being written isn't
    require_manifest : bool
        whether the staged resources must have manifests. If False, the checksums of
        resources without one are worked out from their files
    overwrite : bool
        whether to replace a resource that is already in the store with different
        files. If False, it is reported as an error and left as it is
    raise_errors : bool
        whether to raise errors instead of logging them and continuing with the next
        accession
    store_credentials : StoreCredentials, optional
        the credentials of the S3 bucket the accessions are staged in, if they are
    temp_dir : Path, optional
        the directory to download accessions staged in S3 to
    s3_max_workers : int, optional
        the number of files to download from an S3 staging bucket at once

    Returns
    -------
    list[str]
        the errors encountered, one per accession that couldn't be deposited
    """
    fs, root = open_store(dest, storage_options)
    errors: list[str] = []
    n_deposited = 0
    for listing in staged_sessions(
        input_dir,
        wait_period=wait_period,
        store_credentials=store_credentials,
        temp_dir=temp_dir,
        s3_max_workers=s3_max_workers,
    ):
        try:
            accession = load_labelled(listing, require_manifest)
            changed = 0
            for resource in staged_resources(accession, listing.cache_path, root):
                if _deposit_resource(fs, resource, overwrite):
                    changed += 1
            if accession.fields:
                fields = json.dumps(dict(accession.fields), default=str, indent=4)
                fields_path = f"{accession_dir(accession, root)}/{FIELDS_FNAME}"
                try:
                    unchanged = fs.cat_file(fields_path).decode() == fields
                except FileNotFoundError:
                    unchanged = False
                if not unchanged:
                    write_atomically(fs, fields_path, fields.encode())
            if changed:
                n_deposited += 1
                logger.info(
                    "Deposited %d resource(s) of '%s' in '%s'",
                    changed,
                    listing.name,
                    accession_dir(accession, root),
                )
            else:
                logger.info("'%s' is already in the store", listing.name)
        except Exception as e:
            if raise_errors:
                raise
            msg = f"Could not deposit '{listing.name}': {e}"
            logger.error(msg)
            logger.debug(traceback.format_exc())
            errors.append(msg)
    if errors:
        logger.error("Storing completed with %d errors", len(errors))
    else:
        logger.info("Storing completed, %d accession(s) deposited", n_deposited)
    return errors


def _deposit_resource(
    fs: fsspec.AbstractFileSystem, resource: StagedResource, overwrite: bool
) -> bool:
    """Copy a staged resource into the store, unless it is already there.

    The manifest is removed before the files are changed and written after them, so
    a resource with a manifest in the store is always complete.

    Returns
    -------
    bool
        whether the resource was deposited, rather than already being in the store
    """
    manifest_path = f"{resource.store_dir}/{Resource.MANIFEST_FNAME}"
    existing = read_store_manifest(fs, resource.store_dir)
    if existing is not None:
        if existing.get("checksums") == resource.checksums:
            return False
        if not overwrite:
            raise FileExistsError(
                f"'{resource.store_dir}' is already in the store with different files. "
                "Deposit with overwrite set (--overwrite) to replace it"
            )
        logger.warning("Replacing '%s' in the store", resource.store_dir)
        fs.rm_file(manifest_path)
    elif fs.exists(resource.store_dir):
        logger.warning(
            "'%s' is in the store without a manifest, so depositing it didn't finish. "
            "Depositing it again",
            resource.store_dir,
        )
    fs.makedirs(resource.store_dir, exist_ok=True)
    for relpath in resource.checksums:
        target = f"{resource.store_dir}/{relpath}"
        if "/" in relpath:
            fs.makedirs(target.rsplit("/", 1)[0], exist_ok=True)
        fs.put_file(str(resource.staged_dir / relpath), target)
    # Files of an earlier deposit that the resource no longer has
    prefix = resource.store_dir.rstrip("/") + "/"
    for path in fs.find(resource.store_dir):
        if path.startswith(prefix) and path[len(prefix) :] not in resource.checksums:
            fs.rm_file(path)
    write_atomically(
        fs, manifest_path, json.dumps(resource.manifest, indent=4).encode()
    )
    return True
