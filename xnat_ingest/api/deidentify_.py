import traceback
from pathlib import Path

from fileformats.core import FileSet
from tqdm import tqdm

from xnat_ingest.helpers.remotes import LocalSessionListing

from ..helpers.logging import logger
from ..model.session import ImagingSession


def deidentify(
    input_dir: Path,
    output_dir: Path,
    avoid_clashes: bool = False,
    raise_errors: bool = False,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.copy,
    require_manifest: bool = True,
    delete: bool = False,
) -> list[str]:

    sessions: list[LocalSessionListing] = [
        LocalSessionListing(p) for p in Path(input_dir).iterdir()
    ]
    num_sessions = len(sessions)
    logger.info(
        "Found %d sessions in staging directory to stage'%s'",
        num_sessions,
        input_dir,
    )

    errors: list[str] = []

    for session_listing in tqdm(
        sessions,
        total=num_sessions,
        desc=f"Processing staged sessions found in '{input_dir}'",
    ):

        try:
            session = ImagingSession.load(
                session_listing.cache_path,
                require_manifest=require_manifest,
                check_checksums=False,
            )
            deidentified_session = session.deidentify(
                output_dir,
                copy_mode=copy_mode,
                avoid_clashes=avoid_clashes,
            )
            deidentified_session.save(output_dir / session_listing.name)
        except Exception as e:
            if raise_errors:
                raise
            logger.error(
                "Error deidentifying session '%s': %s",
                session_listing.session_dir,
                str(e),
            )
            logger.debug(traceback.format_exc())
            errors.append(str(e))
    return errors
