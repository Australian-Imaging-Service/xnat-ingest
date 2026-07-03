import shutil
import traceback
from pathlib import Path

from fileformats.core import FileSet
from tqdm import tqdm

from ..helpers.logging import logger
from ..helpers.remotes import LocalSessionListing, list_session_dirs
from ..model.session import ImagingSession


def assign(
    input_dir: Path,
    output_dir: Path,
    project_field: str,
    subject_field: str,
    visit_field: str,
    session_field: str | None = None,
    project_id: str | None = None,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.hardlink_or_copy,
    delete: bool = False,
    raise_errors: bool = False,
) -> list[str]:
    """Sorts the input files into sessions and stages them into the staging directory.

    Parameters
    ----------
    input_dir: Path
        List of paths to search for input files. Can be local paths or S3 paths.
    output_dir: Path
        Path to the staging directory where the sorted sessions will be saved. This should be a local path.
    project_field: str
        Field name to use for extracting the project ID from the input files.
    subject_field: str
        Field name to use for extracting the subject ID from the input files.
    visit_field: str
        Field name to use for extracting the visit ID from the input files.
    session_field: str | None
        Field name to use for extracting the session ID from the input files. If None, the
        session ID will be generated from the subject and visit IDs.
    project_id: str | None
        If provided, this project ID will be used for all sessions instead of extracting it from the input files.
    copy_mode: FileSet.CopyMode
        The copy mode to use when saving the sessions. This determines whether files are copied, moved or symlinked when
        saving the sessions to the staging directory.
    delete: bool
        If True, the input files will be deleted after staging. If False, the input files will be left in place.
    raise_errors: bool
        If True, any errors encountered during staging will raise an exception. If False, errors will be logged and the
        staging process will continue for the remaining sessions.
    """

    sessions: list[LocalSessionListing] = [
        LocalSessionListing(d) for d in list_session_dirs(input_dir)
    ]
    num_sessions = len(sessions)
    logger.info(
        "Found %d sessions in staging directory to stage'%s'",
        num_sessions,
        input_dir,
    )

    # Ensure the output and reid directories exist
    output_dir.mkdir(parents=True, exist_ok=True)

    errors: list[str] = []

    for session_listing in tqdm(
        sessions,
        total=num_sessions,
        desc=f"Processing staged sessions found in '{input_dir}'",
    ):

        try:
            session = ImagingSession.load(
                session_listing.cache_path,
            )

            session.assign(
                project_field=project_field,
                subject_field=subject_field,
                visit_field=visit_field,
                session_field=session_field,
                constant_project_id=project_id,
            )

            session.save(
                dest_dir=output_dir,
                copy_mode=copy_mode,
            )
        except Exception as e:
            if raise_errors:
                raise
            logger.error(
                "Error assigning session '%s': %s",
                session_listing.name,
                str(e),
            )
            logger.debug(traceback.format_exc())
            errors.append(str(e))
        else:
            if delete:
                # remove the original session directory after successful deidentification
                shutil.rmtree(session_listing.fspath)
    return errors
