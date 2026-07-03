import time
import traceback
import typing as ty
from pathlib import Path

from fileformats.core import FileSet
from tqdm import tqdm

from ..helpers.arg_types import IDSpec, CacheMetadata
from ..helpers.logging import logger
from ..model.session import ImagingSession

BUILD_NAME_DEFAULT = "__build__"
INVALID_NAME_DEFAULT = "__invalid__"


def group(
    input_paths: list[str],
    output_dir: Path,
    datatypes: list[FileSet],
    session: list[IDSpec],
    scan: list[IDSpec],
    resource: list[IDSpec],
    delete: bool = False,
    raise_errors: bool = False,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.hardlink_or_copy,
    collation_map: dict[ty.Type[FileSet], FileSet.CopyCollation] | None = None,
    wait_period: int = 0,
    avoid_clashes: bool = True,
    recursive: bool = False,
    cache_metadata: ty.Sequence[CacheMetadata] = (),
) -> list[str]:
    """Groups the input files into sessions/scans/resources and stages them into the
    staging directory. Project/subject/visit IDs and scan descriptions are not
    assigned at this point, see the 'assign' function for that.

    Parameters
    ----------
    input_paths: list[str]
        List of paths to search for input files. Can be local paths or S3 paths.
    output_dir: Path
        Path to the staging directory where the grouped sessions will be saved. This should be a local path.
    datatypes: list[MimeType]
        List of datatypes to look for in the input files. Only files with these datatypes will be considered for staging.
    session: list[FieldSpec] | None
        List of field specifications to use for extracting the session UIDs from the input files to group them into
        separate sessions
    scan: list[FieldSpec]
        List of field specifications to use for extracting the scan IDs from the input files to group them into
        scans
    delete: bool
        If True, the input files will be deleted after staging. If False, the input files will be left in place.
    raise_errors: bool
        If True, any errors encountered during staging will raise an exception. If False, errors will be logged and the
        staging process will continue for the remaining sessions.
    copy_mode: FileSet.CopyMode
        The copy mode to use when saving the sessions. This determines whether files are copied, moved or symlinked when
        saving the sessions to the staging directory.
    wait_period: int
        If provided, this is the number of seconds that must have passed since the last modification time of the session before
        it will be staged. This can be used to avoid staging sessions that are still being modified or created.
    avoid_clashes: bool
        If True, if a session with the same name already exists in the staging directory, a suffix will be added to the session
        name to avoid overwriting the existing session. If False, existing sessions with the same name will be overwritten.
    recursive: bool
        If True, the input paths will be searched recursively for files to stage. If False, only the files directly within the
        input paths will be considered for staging.
    cache_metadata: list[CacheMetadata]
        Which metadata to save for different file types
    """

    errors = []

    # Create sub-directories of the output directory for the different phases of the
    # staging process
    build_dir = output_dir / BUILD_NAME_DEFAULT
    invalid_dir = output_dir / INVALID_NAME_DEFAULT

    build_dir.mkdir(parents=True, exist_ok=True)
    invalid_dir.mkdir(parents=True, exist_ok=True)

    sessions = ImagingSession.from_paths(
        files_path=input_paths,
        datatypes=datatypes,
        session_field=session,
        scan_field=scan,
        resource_field=resource,
        recursive=recursive,
        avoid_clashes=avoid_clashes,
    )

    save_sessions_to_dir(
        sessions,
        f"Grouping files found in '{input_paths}' to {str(output_dir)}",
        wait_period=wait_period,
        build_dir=build_dir,
        copy_mode=copy_mode,
        output_dir=output_dir,
        delete=delete,
        raise_errors=raise_errors,
        collation_map=collation_map,
        cache_metadata=cache_metadata,
    )

    return errors


def group_orthanc(
    url: str,
    store_dir: Path,
    output_dir: Path,
    user: str,
    password: str,
    delete: bool = False,
    raise_errors: bool = False,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.hardlink_or_copy,
    cache_metadata: bool | Path = False,
    processed_label: str = "xnat-sorted",
) -> list[str]:
    """Groups the input files into sessions and stages them into the staging directory.

    Parameters
    ----------
    url: str
        Orthanc server to retrieve the DICOM resources from.
    output_dir: Path
        Path to the staging directory where the grouped sessions will be saved. This should be
    user: str
        Orthanc user to login with
    password: str
        Orthanc password to login with
    processed_label: str
        The label applied to the sessions in Orthanc to signify that they have already been grouped.
    session_id: list[FieldSpec] | None
        List of field specifications to use for extracting the session ID from the input files. If None, the
        session ID will be generated from the subject and visit IDs.
    scan_id: list[FieldSpec]
        List of field specifications to use for extracting the scan ID from the input files.
    delete: bool
        If True, the input files will be deleted after staging. If False, the input files will be left in place.
    raise_errors: bool
        If True, any errors encountered during staging will raise an exception. If False, errors will be logged and the
        staging process will continue for the remaining sessions.
    copy_mode: FileSet.CopyMode
        The copy mode to use when saving the sessions. This determines whether files are copied, moved or symlinked when
        saving the sessions to the staging directory.
    wait_period: int
        If provided, this is the number of seconds that must have passed since the last modification time of the session before
        it will be staged. This can be used to avoid staging sessions that are still being modified or created.
    cache_metadata: bool or Path
        Whether to save the session metadata to a JSON file in the session directory. If True, the metadata will be saved to a file
        named "METADATA.json" in the session directory. If a Path, the metadata will be saved to this path. If False, the metadata
        will not be saved.
    """

    errors = []

    # Create sub-directories of the output directory for the different phases of the
    # staging process
    build_dir = output_dir / BUILD_NAME_DEFAULT
    invalid_dir = output_dir / INVALID_NAME_DEFAULT

    build_dir.mkdir(parents=True, exist_ok=True)
    invalid_dir.mkdir(parents=True, exist_ok=True)

    sessions = ImagingSession.from_orthanc(  # noqa
        url=url,
        output_dir=output_dir,
        store_dir=store_dir,
        user=user,
        password=password,
        orthanc_label=processed_label,
    )

    # save_sessions_to_dir(
    #     sessions,
    #     f"Grouping resources found in Orthanc instance at '{url}' to {output_dir}",
    #     build_dir=build_dir,
    #     copy_mode=copy_mode,
    #     output_dir=output_dir,
    #     delete=delete,
    #     raise_errors=raise_errors,
    # )

    return errors


def save_sessions_to_dir(
    sessions: list[ImagingSession],
    msg: str,
    build_dir,
    copy_mode: FileSet.CopyMode,
    output_dir: Path,
    wait_period: int = 0,
    collation_map=None,
    delete: bool = False,
    raise_errors: bool = False,
    cache_metadata: ty.Sequence[CacheMetadata] = (),
):
    errors = []
    for session in tqdm(sessions, msg):

        if wait_period:
            last_mod = session.last_modified()
            if (time.time_ns() - last_mod) < wait_period * 1e9:
                logger.info(
                    "Skipping grouping of session '%s' as it was last modified "
                    "at %s which is less than %s seconds ago to ensure transfer is complete. ",
                    session.uid,
                    last_mod,
                    wait_period,
                )
                continue

        try:

            # We save the session into a temporary "pre-stage" directory first before
            # moving them into the final "staged" directory. This is to prevent the
            # files being transferred/deleted until the saved session is in a final state.
            _, saved_dir = session.save(
                build_dir,
                copy_mode=copy_mode,
                collation_map=collation_map,
            )
            logger.info(
                "Successfully grouped session '%s' to '%s'",
                session.uid,
                str(saved_dir),
            )
            session_output_dir = output_dir.joinpath(*session.staging_relpath)
            ImagingSession.move_dir(saved_dir, session_output_dir)
            if delete:
                session.unlink()
        except Exception as e:
            if not raise_errors:
                msg = (
                    f"Skipping '{session.uid}' session due to error in grouping: \"{e}\""
                    f"\n{traceback.format_exc()}\n\n"
                )
                logger.error(msg)
                errors.append(msg)
                continue
            else:
                raise
