import os
import shutil
import traceback
import typing as ty
from pathlib import Path

from tqdm import tqdm

from accessionflow.helpers.arg_types import IDSpec
from accessionflow.helpers.logging import logger
from accessionflow.helpers.remotes import LocalSessionListing, list_session_dirs
from accessionflow.model.accession import Accession

from .collate_api import BUILD_NAME_DEFAULT


def select_fields(
    input_dir: Path,
    output_dir: Path,
    extract: ty.Mapping[str, IDSpec],
    raise_errors: bool = False,
) -> list[str]:
    """Extract session metadata into XNAT fields and write staged sessions."""
    if not extract:
        raise ValueError("At least one field must be specified")

    listings = [LocalSessionListing(p) for p in list_session_dirs(input_dir)]
    output_dir.mkdir(parents=True, exist_ok=True)
    errors: list[str] = []

    for listing in tqdm(listings, desc=f"Extracting fields from '{input_dir}'"):
        try:
            session = Accession.load(
                listing.cache_path,
                check_checksums=True,
            )
            extracted = session.fields.extract(session.metadata, extract)
            logger.info(
                "Extracted XNAT fields %s for '%s'",
                sorted(extracted),
                listing.name,
            )

            final_dir = output_dir / session.staging_dirname()
            if final_dir.exists():
                session.save(output_dir)
            else:
                build_dir = output_dir / BUILD_NAME_DEFAULT
                work_dir = build_dir / f"fields_{listing.name}"
                if work_dir.exists():
                    shutil.rmtree(work_dir)
                work_dir.mkdir(parents=True)
                try:
                    _, built_dir = session.save(work_dir)
                    os.replace(built_dir, final_dir)
                finally:
                    shutil.rmtree(work_dir, ignore_errors=True)
                    try:
                        build_dir.rmdir()
                    except OSError:
                        pass

        except Exception as e:
            if raise_errors:
                raise
            message = f"Error extracting fields for session '{listing.name}': {e}"
            logger.error(message)
            logger.debug(traceback.format_exc())
            errors.append(message)

    if errors:
        logger.error("Field extraction completed with %d errors", len(errors))
    else:
        logger.info("Field extraction completed successfully")
    return errors
