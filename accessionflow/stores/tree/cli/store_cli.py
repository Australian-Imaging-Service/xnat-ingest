import time
import typing as ty
from pathlib import Path

import click

from accessionflow.cli.store_cli import store_group
from accessionflow.helpers.arg_types import LoggerConfig, StoreCredentials
from accessionflow.helpers.logging import logger, set_logger_handling
from accessionflow.stores.tree.api.store_api import store

from .options import common_options, storage_options


@store_group.command(
    name="tree",
    help="""Deposits the labelled accessions in a staging directory (as prepared by
'label tree') in a tree data store: a tree of nested label folders, with a folder per
acquisition and per resource, on a local path, a mount, or a remote file system such as
S3 or SFTP.

STAGED is the staging directory, or an S3 bucket to download the accessions from.

DEST is the root of the store, a path or an fsspec URL, e.g. '/mnt/archive',
's3://bucket/archive' or 'sftp://host/archive'. Remote file systems need their fsspec
package installed, e.g. 'accessionflow[s3]' for S3 or 'accessionflow[ssh]' for SFTP.

The resources' manifests and the accessions' fields are deposited with them, but not
their metadata. A resource already in the store with the same files is skipped, so
depositing again only copies what has changed.
""",
)
@common_options
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    envvar="ACFLOW_OVERWRITE",
    help=(
        "Whether to replace a resource that is already in the store with different "
        "files. If not, it is reported as an error (ACFLOW_OVERWRITE env. var)"
    ),
)
@click.option(
    "--wait-period",
    type=int,
    default=0,
    envvar="ACFLOW_WAIT_PERIOD",
    help=(
        "The number of seconds since an accession was last modified before it is "
        "deposited (ACFLOW_WAIT_PERIOD env. var)"
    ),
)
@click.option(
    "--loop",
    type=int,
    default=-1,
    envvar="ACFLOW_LOOP",
    help="Run continuously, every LOOP seconds (ACFLOW_LOOP env. var)",
)
def store_cmd(
    staged: str,
    dest: str,
    storage_option_pairs: ty.Sequence[tuple[str, str]],
    require_manifest: bool,
    store_credentials: StoreCredentials | None,
    temp_dir: Path | None,
    s3_max_workers: int | None,
    loggers: ty.List[LoggerConfig],
    additional_loggers: ty.List[str],
    raise_errors: bool,
    overwrite: bool,
    wait_period: int,
    loop: int,
) -> None:
    if raise_errors and loop >= 0:
        raise ValueError(
            "Cannot use --raise-errors and --loop together as the loop will "
            "continue to run even if an error occurs"
        )
    set_logger_handling(logger_configs=loggers, additional_loggers=additional_loggers)
    options = storage_options(storage_option_pairs)
    while True:
        start_time = time.monotonic()
        store(
            input_dir=staged,
            dest=dest,
            storage_options=options,
            wait_period=wait_period,
            require_manifest=require_manifest,
            overwrite=overwrite,
            raise_errors=raise_errors,
            store_credentials=store_credentials,
            temp_dir=temp_dir,
            s3_max_workers=s3_max_workers,
        )
        if loop < 0:
            break
        elapsed_seconds = time.monotonic() - start_time
        sleep_time = max(loop - elapsed_seconds, 0)
        logger.info(
            "Storing took %s seconds, waiting another %s seconds before running "
            "again (loop every %s seconds)",
            elapsed_seconds,
            sleep_time,
            loop,
        )
        time.sleep(sleep_time)
