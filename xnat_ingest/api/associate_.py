import traceback
import typing as ty
from pathlib import Path

from fileformats.core import FileSet
from tqdm import tqdm

from xnat_ingest.helpers.remotes import LocalSessionListing

from ..helpers.arg_types import AssociatedFiles
from ..helpers.logging import logger
from ..model.session import ImagingSession
from .sort_ import list_session_dirs


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

    session_dirs = list_session_dirs(input_dir)
    sessions: list[LocalSessionListing] = [LocalSessionListing(p) for p in session_dirs]

    # Check __metadata__/ for sessions whose directories have been
    # removed. Create metadata-only sessions from the YAML files so associated files can still be discovered.
    metadata_dir = input_dir / ImagingSession.METADATA_DIR
    metadata_sessions: list[ImagingSession] = []
    if metadata_dir.is_dir():
        existing_names = {p.name for p in session_dirs}
        for yaml_path in sorted(metadata_dir.glob("*.yaml")):
            session_name = yaml_path.stem
            if session_name not in existing_names:
                try:
                    metadata_sessions.append(
                        ImagingSession.from_metadata_yaml(yaml_path)
                    )
                    logger.info(
                        "Created metadata-only session '%s' from '%s'",
                        session_name,
                        yaml_path,
                    )
                except Exception as e:
                    logger.warning(
                        "Failed to load metadata session from '%s': %s",
                        yaml_path,
                        e,
                    )

    num_sessions = len(sessions) + len(metadata_sessions)
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
            associated = session.associate_files(
                associated_files,
                spaces_to_underscores=spaces_to_underscores,
                avoid_clashes=avoid_clashes,
            )
            session.save(output_dir, copy_mode=copy_mode)
        except Exception as e:
            if raise_errors:
                raise
            logger.error(
                "Error associating files for session '%s': %s",
                session_listing.name,
                str(e),
            )
            logger.debug(traceback.format_exc())
            errors.append(str(e))
        else:
            if delete:
                associated.unlink()

    for session in tqdm(
        metadata_sessions,
        total=len(metadata_sessions),
        desc="Processing metadata-only sessions",
    ):
        try:
            associated = session.associate_files(
                associated_files,
                spaces_to_underscores=spaces_to_underscores,
                avoid_clashes=avoid_clashes,
            )
            session.save(output_dir, copy_mode=copy_mode)
        except Exception as e:
            if raise_errors:
                raise
            logger.error(
                "Error associating files for metadata-only session '%s': %s",
                session.name,
                str(e),
            )
            logger.debug(traceback.format_exc())
            errors.append(str(e))
        else:
            if delete:
                for fileset in associated:
                    fileset.unlink()

    return errors
