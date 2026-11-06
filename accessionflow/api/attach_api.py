import traceback
import typing as ty
from pathlib import Path

from fileformats.core import FileSet
from tqdm import tqdm

from accessionflow.helpers.arg_types import AttachedFiles, OnResourceClash
from accessionflow.helpers.logging import logger
from accessionflow.helpers.remotes import LocalSessionListing, list_session_dirs
from accessionflow.model.accession import Accession


def attach(
    input_dir: Path,
    output_dir: Path,
    datatype: ty.Type[FileSet],
    glob: str,
    identity_pattern: str,
    spaces_to_underscores: bool = False,
    on_resource_clash: OnResourceClash = "error",
    raise_errors: bool = False,
    require_manifest: bool = True,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.copy,
    unlink_source: str | None = None,
) -> list[str]:

    session_dirs = list_session_dirs(input_dir)
    sessions: list[LocalSessionListing] = [LocalSessionListing(p) for p in session_dirs]

    errors: list[str] = []
    # Check __metadata__/ for sessions whose directories have been
    # removed. Create metadata-only sessions from the YAML files so attached files can still be discovered.
    metadata_dir = input_dir / Accession.METADATA_DIR
    metadata_sessions: list[Accession] = []
    if metadata_dir.is_dir():
        existing_names = {p.name for p in session_dirs}
        for yaml_path in sorted(metadata_dir.glob("*.yaml")):
            session_name = yaml_path.stem
            if session_name not in existing_names:
                try:
                    metadata_sessions.append(Accession.from_metadata_yaml(yaml_path))
                    logger.info(
                        "Created metadata-only session '%s' from '%s'",
                        session_name,
                        yaml_path,
                    )
                except Exception as e:
                    logger.error(
                        "Failed to load metadata session from '%s': %s",
                        yaml_path,
                        e,
                    )
                    errors.append(str(e))

    num_sessions = len(sessions) + len(metadata_sessions)
    logger.info(
        "Found %d sessions in staging directory to stage'%s'",
        num_sessions,
        input_dir,
    )

    for session_listing in tqdm(
        sessions,
        total=num_sessions,
        desc=f"Processing staged sessions found in '{input_dir}'",
    ):
        try:
            session = Accession.load(
                session_listing.cache_path,
                require_manifest=require_manifest,
                check_checksums=False,
            )
            attached = session.attach_files(
                [AttachedFiles(datatype, glob, identity_pattern)],
                spaces_to_underscores=spaces_to_underscores,
                on_resource_clash=on_resource_clash,
            )
            session.save(output_dir, copy_mode=copy_mode)
        except Exception as e:
            if raise_errors:
                raise
            logger.error(
                "Error attaching files for session '%s': %s",
                session_listing.name,
                str(e),
            )
            logger.debug(traceback.format_exc())
            errors.append(str(e))
        else:
            if unlink_source is not None:
                # 'all' and 'keep-metadata' are equivalent here: the attached files
                # live at an arbitrary external location, not a directory tree
                # accessionflow owns, so only the individual matched files are ever
                # removed
                for fileset in attached:
                    fileset.unlink()

    for session in tqdm(
        metadata_sessions,
        total=len(metadata_sessions),
        desc="Processing metadata-only sessions",
    ):
        try:
            attached = session.attach_files(
                [AttachedFiles(datatype, glob, identity_pattern)],
                spaces_to_underscores=spaces_to_underscores,
                on_resource_clash=on_resource_clash,
            )
            session.save(output_dir, copy_mode=copy_mode)
        except Exception as e:
            if raise_errors:
                raise
            logger.error(
                "Error attaching files for metadata-only session '%s': %s",
                session.name,
                str(e),
            )
            logger.debug(traceback.format_exc())
            errors.append(str(e))
        else:
            if unlink_source is not None:
                for fileset in attached:
                    fileset.unlink()
    if errors:
        logger.error("Attachment completed with %s errors", len(errors))
    else:
        logger.info("Attachment completed successfully without errors")
    return errors
