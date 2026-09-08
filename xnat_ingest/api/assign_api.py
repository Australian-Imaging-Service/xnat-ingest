import json
import os
import shutil
import traceback
import typing as ty
from pathlib import Path

from fileformats.core import FileSet
from tqdm import tqdm

from ..helpers.logging import logger
from ..helpers.metadata import Metadata
from ..helpers.remotes import LocalSessionListing, list_session_dirs
from ..model.session import ImagingSession
from .group_api import BUILD_NAME_DEFAULT

INVALID_DIRNAME = "__invalid__"


def _existing_invalid_uids(invalid_dir: Path) -> set[str]:
    """Return the set of source UIDs already saved under __invalid__/.

    Each subdirectory's __METADATA__.json is checked for the ``__uid__`` key
    that ``ImagingSession.save()`` writes.  Directories whose metadata can't
    be read are silently skipped.
    """
    uids: set[str] = set()
    if not invalid_dir.is_dir():
        return uids
    for session_dir in invalid_dir.iterdir():
        if not session_dir.is_dir():
            continue
        meta_path = session_dir / Metadata.FNAME
        if not meta_path.exists():
            continue
        try:
            with open(meta_path) as f:
                meta = json.load(f)
            uid = meta.get(ImagingSession.UID_METADATA_KEY)
            if uid is not None:
                uids.add(uid)
        except (json.JSONDecodeError, OSError):
            continue
    return uids


def assign(
    input_dir: Path,
    output_dir: Path,
    project_field: str,
    subject_field: str,
    session_field: str,
    scan_field: str | None = None,
    project_id: str | None = None,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.hardlink_or_copy,
    unlink_source: str | None = None,
    raise_errors: bool = False,
    include: ty.Sequence[type[FileSet]] = (),
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
    session_field: str
        Field name to use for extracting the session ID from the input files.
    scan_field: str | None
        Field name to use for extracting a description for each scan. Scans for which the field
        can't be resolved are left without a description.
    project_id: str | None
        If provided, this project ID will be used for all sessions instead of extracting it from the input files.
        Useful for instruments that upload to a single project.
    copy_mode: FileSet.CopyMode
        The copy mode to use when saving the sessions. This determines whether files are copied, moved or symlinked when
        saving the sessions to the staging directory.
    unlink_source: str | None
        If "all", the grouped session directory is removed in its entirety after assignment. If "keep-metadata", the
        resource data is removed but the session/scan-level metadata is left behind as a lightweight skeleton. If
        None, the grouped session directory is left in place.
    raise_errors: bool
        If True, any errors encountered during staging will raise an exception. If False, errors will be logged and the
        staging process will continue for the remaining sessions.
    include: Sequence[type[FileSet]]
        Only stage resources matching at least one of these datatypes. An empty
        sequence stages all resources.

    Notes
    -----
    If a session's project/subject/session ID can't be resolved from its metadata, it
    is saved with placeholder IDs (see `ImagingSession.assign`) under an
    `INVALID_NAME_DEFAULT` ('__invalid__') subdirectory of `output_dir` instead of the
    regular output location, so it can be found and manually reprocessed rather than
    being lost.
    """

    if include and unlink_source is not None:
        raise ValueError(
            "--unlink-source cannot be used with datatype filtering because other "
            "assign pathways may still need the grouped source"
        )
    if invalid := [
        datatype for datatype in include if not issubclass(datatype, FileSet)
    ]:
        raise ValueError(
            "Included datatypes must be file formats, got "
            f"{[datatype.mime_like for datatype in invalid]}"
        )

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

    invalid_dir = output_dir / INVALID_DIRNAME
    already_invalid = _existing_invalid_uids(invalid_dir)
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

            if include and not any(
                resource.matches_datatypes(include) for resource in session.resources
            ):
                logger.info(
                    "Skipping '%s' because it has no resources matching %s",
                    session_listing.name,
                    [datatype.mime_like for datatype in include],
                )
                continue

            missing_ids = session.assign(
                project_field=project_field,
                subject_field=subject_field,
                session_field=session_field,
                constant_project_id=project_id,
                scan_field=scan_field,
            )

            if missing_ids:
                if session.uid in already_invalid:
                    logger.warning(
                        "Skipping '%s' — already saved as invalid (uid=%s). "
                        "Manually review/fix and remove from '%s' to re-process.",
                        session_listing.name,
                        session.uid,
                        invalid_dir,
                    )
                    continue
                msg = (
                    f"Could not resolve project/subject/session IDs for '{session_listing.name}', "
                    f"due to missing metadata fields {list(missing_ids)}. "
                    f"Saved to '{invalid_dir}/{session.name}' for manual review instead"
                )
                logger.error(msg)
                errors.append(msg)
                already_invalid.add(session.uid)
            dest_dir = invalid_dir if missing_ids else output_dir
            # MATERIALISE A NEW SESSION BY RENAME, NOT IN PLACE.
            #
            # save() creates the session directory and then fills it, so a run
            # that dies part-way leaves a partial session under its real name.
            # That matters more here than anywhere else in the pipeline: with
            # de-identification done in Orthanc, which is how every site is
            # configured, `upload` reads this directory DIRECTLY. Nothing
            # rewrites the partial tree, its mtimes go quiet, it satisfies the
            # settle window on both upload paths, and it is uploaded as though
            # it were whole. There is no completeness check on that path, and
            # sites run with --dont-require-manifest, which switches off the one
            # check that would have rejected a short resource.
            #
            # Only when the output does not exist yet. Re-saving over one that
            # is already there writes in place, because building elsewhere and
            # renaming in defeats the write-if-changed short-circuits in
            # ImagingResource.save and Metadata.save, both of which are
            # conditional on the destination existing. Losing them moves every
            # mtime on every pass, which is what stops a session from ever
            # settling long enough to be uploaded.
            #
            # group_api does the same thing with move_dir; this is the same
            # guarantee reached with a rename, which is atomic within a
            # directory tree on one filesystem.
            available_projects = None
            build_dir = dest_dir / BUILD_NAME_DEFAULT
            work_dir = build_dir / f"promote_{session_listing.name}"
            if work_dir.exists():
                shutil.rmtree(work_dir)
            try:
                final_dir = dest_dir / session.staging_dirname(available_projects)
                if final_dir.exists():
                    session.save(
                        dest_dir=dest_dir,
                        available_projects=available_projects,
                        copy_mode=copy_mode,
                        include=include,
                    )
                else:
                    work_dir.mkdir(parents=True)
                    _, built_dir = session.save(
                        dest_dir=work_dir,
                        available_projects=available_projects,
                        copy_mode=copy_mode,
                        include=include,
                    )
                    os.replace(built_dir, final_dir)
            finally:
                shutil.rmtree(work_dir, ignore_errors=True)
                # rmdir, not rmtree: it removes the build directory only when it
                # is empty, so a session still being built by another pass is
                # never taken out from under it. Leaving an empty __build__ in
                # the output would be harmless to the pipeline, since every
                # consumer skips the name, but it is noise in a directory
                # operators read by hand.
                try:
                    build_dir.rmdir()
                except OSError:
                    pass
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
            if unlink_source == "all":
                # remove the grouped session directory in its entirety
                shutil.rmtree(session_listing.fspath)
            elif unlink_source == "keep-metadata":
                # remove just the resource data, leaving the session/scan-level
                # metadata behind as a lightweight skeleton
                session.unlink(keep_metadata=True)
    if errors:
        logger.error(
            "Assign completed with %s errors",
            len(errors),
        )
    else:
        logger.info("Assign completed successfully")
    return errors
