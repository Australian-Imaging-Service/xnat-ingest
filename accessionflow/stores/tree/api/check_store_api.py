import traceback
import typing as ty
from pathlib import Path

from accessionflow.helpers.arg_types import StoreCredentials
from accessionflow.helpers.logging import logger

from ..layout import (
    load_labelled,
    open_store,
    read_store_manifest,
    remote_md5,
    staged_resources,
    staged_sessions,
)


def check_store(
    input_dir: str | Path,
    dest: str,
    storage_options: ty.Mapping[str, ty.Any] | None = None,
    checksums: bool = True,
    require_manifest: bool = True,
    raise_errors: bool = False,
    store_credentials: StoreCredentials | None = None,
    temp_dir: Path | None = None,
    s3_max_workers: int | None = None,
) -> list[str]:
    """Checks what has been deposited in a tree store against the staging directory it
    was deposited from, and logs every resource that is missing, incomplete or
    different.

    Parameters
    ----------
    input_dir : str or Path
        the staging directory the accessions were deposited from, or an S3 bucket
    dest : str
        the root of the store, a path or an fsspec URL, e.g. 's3://bucket/archive'
    storage_options : Mapping[str, Any], optional
        options for the fsspec file system of the store, e.g. credentials
    checksums : bool
        whether to check the files' checksums, which reads every file back from the
        store. If False, only their sizes are compared, which is much cheaper
    require_manifest : bool
        whether the staged resources must have manifests
    raise_errors : bool
        whether to raise errors instead of logging them
    store_credentials : StoreCredentials, optional
        the credentials of the S3 bucket the accessions are staged in, if they are
    temp_dir : Path, optional
        the directory to download accessions staged in S3 to
    s3_max_workers : int, optional
        the number of files to download from an S3 staging bucket at once

    Returns
    -------
    list[str]
        a description of each problem found
    """
    fs, root = open_store(dest, storage_options)
    problems: list[str] = []
    for listing in staged_sessions(
        input_dir,
        store_credentials=store_credentials,
        temp_dir=temp_dir,
        s3_max_workers=s3_max_workers,
    ):
        try:
            accession = load_labelled(listing, require_manifest)
            for resource in staged_resources(accession, listing.cache_path, root):
                stored = read_store_manifest(fs, resource.store_dir)
                if stored is None:
                    problems.append(
                        f"'{resource.store_dir}' is missing from the store, or "
                        "depositing it didn't finish"
                    )
                    continue
                if stored.get("checksums") != resource.checksums:
                    problems.append(
                        f"'{resource.store_dir}' in the store holds different files to "
                        f"'{resource.staged_dir}'"
                    )
                    continue
                for relpath, checksum in resource.checksums.items():
                    path = f"{resource.store_dir}/{relpath}"
                    if not fs.exists(path):
                        problems.append(f"'{path}' is missing from the store")
                    elif checksums:
                        if remote_md5(fs, path) != checksum:
                            problems.append(f"'{path}' in the store is corrupted")
                    elif (
                        fs.size(path) != (resource.staged_dir / relpath).stat().st_size
                    ):
                        problems.append(f"'{path}' in the store has the wrong size")
        except Exception as e:
            if raise_errors:
                raise
            msg = f"Could not check '{listing.name}': {e}"
            logger.debug(traceback.format_exc())
            problems.append(msg)
    for problem in problems:
        logger.error(problem)
    if not problems:
        logger.info("Everything staged is in the store")
    return problems
