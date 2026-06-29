import tempfile
import time
import traceback
import typing as ty
from pathlib import Path

from fileformats.core import FileSet
from fileformats.medimage import DicomSeries
from frametree.xnat import Xnat
from tqdm import tqdm

from ..helpers.arg_types import IDSpec, OrthancLogin, XnatLogin
from ..helpers.logging import logger
from ..model.session import ImagingSession

BUILD_NAME_DEFAULT = "__build__"
INVALID_NAME_DEFAULT = "__invalid__"


def sort(
    input_paths: list[str],
    output_dir: Path,
    datatypes: list[FileSet],
    project_id: list[IDSpec],
    subject_id: list[IDSpec],
    visit_id: list[IDSpec],
    session_uid: list[IDSpec] | None,
    scan_id: list[IDSpec],
    scan_desc: list[IDSpec],
    resource: list[IDSpec],
    session_id: list[IDSpec] | None = None,
    fixed_project_id: str | None = None,
    delete: bool = False,
    raise_errors: bool = False,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.hardlink_or_copy,
    collation_map: dict[ty.Type[FileSet], FileSet.CopyCollation] | None = None,
    wait_period: int = 0,
    avoid_clashes: bool = False,
    recursive: bool = False,
    xnat_login: XnatLogin | None = None,
    save_metadata: bool | Path = False,
    orthanc: OrthancLogin | None = None,
    orthanc_label: str = "xnat-sorted",
) -> list[str]:
    """Sorts the input files into sessions and stages them into the staging directory.

    Parameters
    ----------
    input_paths: list[str]
        List of paths to search for input files. Can be local paths or S3 paths.
    staging_dir: Path
        Path to the staging directory where the sorted sessions will be saved. This should be a local path.
    datatypes: list[MimeType]
        List of datatypes to look for in the input files. Only files with these datatypes will be considered for staging.
    project_id: list[FieldSpec]
        List of field specifications to use for extracting the project ID from the input files.
    subject_id: list[FieldSpec]
        List of field specifications to use for extracting the subject ID from the input files.
    visit_id: list[FieldSpec]
        List of field specifications to use for extracting the visit ID from the input files.
    session_id: list[FieldSpec] | None
        List of field specifications to use for extracting the session ID from the input files. If None, the
        session ID will be generated from the subject and visit IDs.
    scan_id: list[FieldSpec]
        List of field specifications to use for extracting the scan ID from the input files.
    scan_desc: list[FieldSpec]
        List of field specifications to use for extracting the scan description from the input files.
    resource: list[FieldSpec]
        List of field specifications to use for extracting the resource name from the input files.
    project_id: str | None
        If provided, this project ID will be used for all sessions instead of extracting it from the input files.
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
    xnat_login: XnatLogin
        If provided, this XNAT login information will be used to log into the XNAT server and check that the project IDs extracted
        from the input files exist on the XNAT server before staging. If not provided, the project IDs will not be checked
        against the XNAT server before staging.
    save_metadata: bool or Path
        Whether to save the session metadata to a JSON file in the session directory. If True, the metadata will be saved to a file
        named "METADATA.json" in the session directory. If a Path, the metadata will be saved to this path. If False, the metadata
        will not be saved.
    """

    errors = []

    if xnat_login:
        logger.info(
            "Logging into XNAT server '%s' as user '%s' to check project IDs",
            xnat_login.host,
            xnat_login.user,
        )
        xnat_repo = Xnat(
            server=xnat_login.host,
            user=xnat_login.user,
            password=xnat_login.password,
            cache_dir=Path(tempfile.mkdtemp()),
        )
        with xnat_repo.connection:
            project_list = [p.name for p in xnat_repo.connection.projects]
    else:
        logger.info("No XNAT login provided, will not check project IDs in XNAT")
        project_list = None

    # Create sub-directories of the output directory for the different phases of the
    # staging process
    build_dir = output_dir / BUILD_NAME_DEFAULT
    invalid_dir = output_dir / INVALID_NAME_DEFAULT

    build_dir.mkdir(parents=True, exist_ok=True)
    invalid_dir.mkdir(parents=True, exist_ok=True)
    if save_metadata:
        metadata_dir = output_dir / ImagingSession.METADATA_DIR
        metadata_dir.mkdir(parents=True, exist_ok=True)

    if orthanc:
        id_specs = (project_id, subject_id, visit_id, scan_id, scan_desc)
        if non_field_types := [i for i in id_specs if i.type != "field"]:
            raise ValueError(
                f"Only field sources can be used in Orthanc sorting: {non_field_types}"
            )
        if non_dicom := [i for i in id_specs if i.datatype != DicomSeries]:
            raise ValueError(
                f"Only DICOM types can be used in Orthanc sorting: {non_dicom}"
            )
        ImagingSession.from_orthanc(
            orthanc_url=orthanc.url,
            output_dir=output_dir,
            orthanc_storage_dir=orthanc.storage_dir,
            project_field=project_id,
            subject_field=subject_id,
            visit_field=visit_id,
            scan_id_field=scan_id,
            scan_desc_field=scan_desc,
            fixed_project_id=fixed_project_id,
            orthanc_user=orthanc.user,
            orthanc_password=orthanc.password,
            orthanc_label=orthanc_label,
            available_projects=project_list,
        )
        return errors

    sessions = ImagingSession.from_paths(
        files_path=input_paths,
        datatypes=datatypes,
        project_field=project_id,
        subject_field=subject_id,
        visit_field=visit_id,
        session_uid_field=session_uid,
        session_id_field=session_id,
        scan_id_field=scan_id,
        scan_desc_field=scan_desc,
        resource_field=resource,
        project_id=project_id,
        avoid_clashes=avoid_clashes,
        recursive=recursive,
    )

    logger.info("Staging sessions to '%s'", str(output_dir))

    for session in tqdm(sessions, f"Staging resources found in '{input_paths}'"):

        if wait_period:
            last_mod = session.last_modified()
            if (time.time_ns() - last_mod) < wait_period * 1e9:
                logger.info(
                    "Skipping staging of session '%s' as it was last modified "
                    "at %s which is less than %s seconds ago",
                    session.name,
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
                available_projects=project_list,
                copy_mode=copy_mode,
                collation_map=collation_map,
                save_metadata=save_metadata,
            )
            logger.info(
                "Successfully sorted session '%s' to '%s'",
                session.name,
                str(saved_dir),
            )
            session_output_dir = output_dir / session.name
            ImagingSession.move_dir(saved_dir, session_output_dir)
            if delete:
                session.unlink()
        except Exception as e:
            if not raise_errors:
                msg = (
                    f"Skipping '{session.name}' session due to error in sorting: \"{e}\""
                    f"\n{traceback.format_exc()}\n\n"
                )
                logger.error(msg)
                errors.append(msg)
                continue
            else:
                raise

    return errors


def sort_from_orthanc(
    input_paths: list[str],
    output_dir: Path,
    datatypes: list[FileSet],
    project_id: list[IDSpec],
    subject_id: list[IDSpec],
    visit_id: list[IDSpec],
    session_uid: list[IDSpec] | None,
    scan_id: list[IDSpec],
    scan_desc: list[IDSpec],
    resource: list[IDSpec],
    session_id: list[IDSpec] | None = None,
    fixed_project_id: str | None = None,
    delete: bool = False,
    raise_errors: bool = False,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.hardlink_or_copy,
    collation_map: dict[ty.Type[FileSet], FileSet.CopyCollation] | None = None,
    wait_period: int = 0,
    avoid_clashes: bool = False,
    recursive: bool = False,
    xnat_login: XnatLogin | None = None,
    save_metadata: bool | Path = False,
    orthanc: OrthancLogin | None = None,
    orthanc_label: str = "xnat-sorted",
) -> list[str]:
    """Sorts the input files into sessions and stages them into the staging directory.

    Parameters
    ----------
    input_paths: list[str]
        List of paths to search for input files. Can be local paths or S3 paths.
    staging_dir: Path
        Path to the staging directory where the sorted sessions will be saved. This should be a local path.
    datatypes: list[MimeType]
        List of datatypes to look for in the input files. Only files with these datatypes will be considered for staging.
    project_id: list[FieldSpec]
        List of field specifications to use for extracting the project ID from the input files.
    subject_id: list[FieldSpec]
        List of field specifications to use for extracting the subject ID from the input files.
    visit_id: list[FieldSpec]
        List of field specifications to use for extracting the visit ID from the input files.
    session_id: list[FieldSpec] | None
        List of field specifications to use for extracting the session ID from the input files. If None, the
        session ID will be generated from the subject and visit IDs.
    scan_id: list[FieldSpec]
        List of field specifications to use for extracting the scan ID from the input files.
    scan_desc: list[FieldSpec]
        List of field specifications to use for extracting the scan description from the input files.
    resource: list[FieldSpec]
        List of field specifications to use for extracting the resource name from the input files.
    project_id: str | None
        If provided, this project ID will be used for all sessions instead of extracting it from the input files.
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
    xnat_login: XnatLogin
        If provided, this XNAT login information will be used to log into the XNAT server and check that the project IDs extracted
        from the input files exist on the XNAT server before staging. If not provided, the project IDs will not be checked
        against the XNAT server before staging.
    save_metadata: bool or Path
        Whether to save the session metadata to a JSON file in the session directory. If True, the metadata will be saved to a file
        named "METADATA.json" in the session directory. If a Path, the metadata will be saved to this path. If False, the metadata
        will not be saved.
    """

    errors = []

    if xnat_login:
        logger.info(
            "Logging into XNAT server '%s' as user '%s' to check project IDs",
            xnat_login.host,
            xnat_login.user,
        )
        xnat_repo = Xnat(
            server=xnat_login.host,
            user=xnat_login.user,
            password=xnat_login.password,
            cache_dir=Path(tempfile.mkdtemp()),
        )
        with xnat_repo.connection:
            project_list = [p.name for p in xnat_repo.connection.projects]
    else:
        logger.info("No XNAT login provided, will not check project IDs in XNAT")
        project_list = None

    # Create sub-directories of the output directory for the different phases of the
    # staging process
    build_dir = output_dir / BUILD_NAME_DEFAULT
    invalid_dir = output_dir / INVALID_NAME_DEFAULT

    build_dir.mkdir(parents=True, exist_ok=True)
    invalid_dir.mkdir(parents=True, exist_ok=True)
    if save_metadata:
        metadata_dir = output_dir / ImagingSession.METADATA_DIR
        metadata_dir.mkdir(parents=True, exist_ok=True)

    if orthanc:
        id_specs = (project_id, subject_id, visit_id, scan_id, scan_desc)
        if non_field_types := [i for i in id_specs if i.type != "field"]:
            raise ValueError(
                f"Only field sources can be used in Orthanc sorting: {non_field_types}"
            )
        if non_dicom := [i for i in id_specs if i.datatype != DicomSeries]:
            raise ValueError(
                f"Only DICOM types can be used in Orthanc sorting: {non_dicom}"
            )
        ImagingSession.from_orthanc(
            orthanc_url=orthanc.url,
            output_dir=output_dir,
            orthanc_storage_dir=orthanc.storage_dir,
            project_field=project_id,
            subject_field=subject_id,
            visit_field=visit_id,
            scan_id_field=scan_id,
            scan_desc_field=scan_desc,
            fixed_project_id=fixed_project_id,
            orthanc_user=orthanc.user,
            orthanc_password=orthanc.password,
            orthanc_label=orthanc_label,
            available_projects=project_list,
        )
        return errors

    sessions = ImagingSession.from_paths(
        files_path=input_paths,
        datatypes=datatypes,
        project_field=project_id,
        subject_field=subject_id,
        visit_field=visit_id,
        session_uid_field=session_uid,
        session_id_field=session_id,
        scan_id_field=scan_id,
        scan_desc_field=scan_desc,
        resource_field=resource,
        project_id=project_id,
        avoid_clashes=avoid_clashes,
        recursive=recursive,
    )

    logger.info("Staging sessions to '%s'", str(output_dir))

    for session in tqdm(sessions, f"Staging resources found in '{input_paths}'"):

        if wait_period:
            last_mod = session.last_modified()
            if (time.time_ns() - last_mod) < wait_period * 1e9:
                logger.info(
                    "Skipping staging of session '%s' as it was last modified "
                    "at %s which is less than %s seconds ago",
                    session.name,
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
                available_projects=project_list,
                copy_mode=copy_mode,
                collation_map=collation_map,
                save_metadata=save_metadata,
            )
            logger.info(
                "Successfully sorted session '%s' to '%s'",
                session.name,
                str(saved_dir),
            )
            session_output_dir = output_dir / session.name
            ImagingSession.move_dir(saved_dir, session_output_dir)
            if delete:
                session.unlink()
        except Exception as e:
            if not raise_errors:
                msg = (
                    f"Skipping '{session.name}' session due to error in sorting: \"{e}\""
                    f"\n{traceback.format_exc()}\n\n"
                )
                logger.error(msg)
                errors.append(msg)
                continue
            else:
                raise

    return errors


def list_session_dirs(sorted_dir: Path) -> list[Path]:
    """List the session directories in the sorted directory, excluding any directories that start with '__'.

    Includes both dotted dirs (PROJ.SUBJ.VISIT) and no-dot dirs (session label only).
    """
    return [
        p
        for p in Path(sorted_dir).iterdir()
        if p.is_dir() and not p.name.startswith("__") and not p.name.endswith("__")
    ]
