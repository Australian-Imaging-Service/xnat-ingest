import traceback
import typing as ty
from pathlib import Path

from fileformats.core import FileSet
from tqdm import tqdm

from xnat_ingest.helpers.remotes import LocalSessionListing

from ..helpers.arg_types import AssociatedFiles
from ..helpers.logging import logger
from ..model.session import ImagingSession


def associate(
    input_dir: Path,
    output_dir: Path,
    associated_files: ty.List[AssociatedFiles],
    spaces_to_underscores: bool = False,
    avoid_clashes: bool = False,
    raise_errors: bool = False,
    require_manifest: bool = True,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.copy,
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
            session.associate_files(
                associated_files,
                spaces_to_underscores=spaces_to_underscores,
                avoid_clashes=avoid_clashes,
            )
            session.save(output_dir / session_listing.name, copy_mode=copy_mode)
        except Exception as e:
            if raise_errors:
                raise
            logger.error(
                "Error associating files for session '%s': %s",
                session_listing.session_dir,
                str(e),
            )
            logger.debug(traceback.format_exc())
            errors.append(str(e))
    return errors
