import sys
import typing as ty
from pathlib import Path

import click

from accessionflow.cli.store_cli import check_store_group
from accessionflow.helpers.arg_types import LoggerConfig, StoreCredentials
from accessionflow.helpers.logging import set_logger_handling
from accessionflow.stores.tree.api.check_store_api import check_store

from .options import common_options, storage_options


@check_store_group.command(
    name="tree",
    help="""Checks what has been deposited in a tree data store against the staging
directory it was deposited from, and logs every resource that is missing, incomplete or
different. Exits with a non-zero status if there are any.

STAGED is the staging directory, or an S3 bucket to download the accessions from.

DEST is the root of the store, a path or an fsspec URL, e.g. 's3://bucket/archive'.
""",
)
@common_options
@click.option(
    "--checksums/--sizes-only",
    default=True,
    envvar="ACFLOW_CHECK_CHECKSUMS",
    help=(
        "Whether to check the files' checksums, which reads every file back from the "
        "store, or only compare their sizes, which is much cheaper "
        "(ACFLOW_CHECK_CHECKSUMS env. var)"
    ),
)
def check_store_cmd(
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
    checksums: bool,
) -> None:
    set_logger_handling(logger_configs=loggers, additional_loggers=additional_loggers)
    problems = check_store(
        input_dir=staged,
        dest=dest,
        storage_options=storage_options(storage_option_pairs),
        checksums=checksums,
        require_manifest=require_manifest,
        raise_errors=raise_errors,
        store_credentials=store_credentials,
        temp_dir=temp_dir,
        s3_max_workers=s3_max_workers,
    )
    if problems:
        sys.exit(1)
