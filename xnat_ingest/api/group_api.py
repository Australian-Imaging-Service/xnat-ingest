import time
import traceback
import typing as ty
from pathlib import Path

from fileformats.core import FileSet
from tqdm import tqdm

from ..helpers.arg_types import (
    IDSpec,
    MetadataTable,
    OnResourceClash,
    PathMetadataRegex,
)
from ..helpers.logging import logger
from ..model.session import ImagingSession

BUILD_NAME_DEFAULT = "__build__"


def group(
    input_paths: list[str],
    output_dir: Path,
    datatypes: list[FileSet],
    session: list[IDSpec],
    scan: list[IDSpec],
    resource: list[IDSpec],
    path_metadata_regex: ty.Sequence[PathMetadataRegex] = (),
    unlink_source: str | None = None,
    raise_errors: bool = False,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.hardlink_or_copy,
    wait_period: int = 0,
    collation_map: dict[type[FileSet], FileSet.CopyCollation] | None = None,
    conversion_map: dict[type[FileSet], type[FileSet]] | None = None,
    ignore_paths: list[str] | None = None,
    ignore_types: list[type[FileSet]] = (),
    on_resource_clash: OnResourceClash = "error",
    metadata_tables: list[MetadataTable] | None = None,
    recursive: bool = False,
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
    datatypes: list[FileSet]
        List of FileSet types to look for in the input files. Only files with these datatypes will be considered for staging.
    session: list[IDSpec] | None
        List of field specifications to use for extracting the session UIDs from the input files to group them into
        separate sessions
    scan: list[IDSpec]
        List of field specifications to use for extracting the scan IDs from the input files to group them into
        scans
    resource: list[IDSpec]
        List of field specifications to use for extracting the resource IDs from the input files to group them into
        resources
    path_metadata_regex: ty.Sequence[PathMetadataRegex]
        Regular expressions to extract "metadata" values from resource file paths as named groups. The named
        groups are used as metadata fields for the resource files, and the extracted values will be used to populate
        the corresponding metadata fields to complement the metadata read from the file headers.
    unlink_source: str | None
        If "all" or "keep-metadata", the input files will be unlinked one by one after staging (both behave the same
        here, since the source isn't a directory tree that xnat-ingest owns). If None, the input files will be left
        in place.
    raise_errors: bool
        If True, any errors encountered during staging will raise an exception. If False, errors will be logged and the
        staging process will continue for the remaining sessions.
    copy_mode: FileSet.CopyMode
        The copy mode to use when saving the sessions. This determines whether files are copied, moved or symlinked when
        saving the sessions to the staging directory.
    collation_map: dict[ty.Type[FileSet], FileSet.CopyCollation] | None
        A mapping of FileSet types to CopyCollation objects that specify how to collate files of that type when saving the
        sessions. If None, the default collation behavior for each FileSet type will be used.
    conversion_map: dict[ty.Type[FileSet], ty.Type[FileSet]] | None
        A mapping of source FileSet types to target FileSet types. When a resource matches a source type, it will be converted to the target type during save.
    ignore_paths: list[str] | None
        Regular expressions to match paths that should be ignored when grouping files into sessions. If None, no paths will be ignored.
        To ignore all paths by default, use ".*" as the value for this parameter.
    ignore_types: list[type[FileSet]] | None
        Datatypes that should be ignored when grouping files into sessions. If None, paths that aren't recognised as part of the
        requested datatypes or filtered using ignore_paths will raise an error
    wait_period: int
        If provided, this is the number of seconds that must have passed since the last modification time of the session before
        it will be staged. This can be used to avoid staging sessions that are still being modified or created.
    on_resource_clash: OnResourceClash = "avoid"
        If "avoid", if a session with the same name already exists in the staging directory, a suffix will be added to the session
        name to avoid overwriting the existing session.
        If "merge", existing sessions with the same name will be merged.
        If "error", an error will be raised if a session with the same name already exists in the staging directory.
    recursive: bool
        If True, the input paths will be searched recursively for files to stage. If False, only the files directly within the
        input paths will be considered for staging.
    metadata_tables: list[MetadataTable] | None
        Specify metadata tables to extract and join metadata from input files (XINGEST_METADATA_TABLES env. var).
        The 'path' arg specifies the location of the metadata table file.
        By default it will attempt to match the datatype to csv, tsv and openxml spreadsheet (i.e. '.xlsx') formats.
        However, the format can be explicitly specified by placing its mime-type in square brackets after the
        file path, e.g., 'path/to/file.csv[text/csv]'.
        The "row frequency" arg specifies what each row in the
        metadata table corresponds to in the data hierarchy, and can be one of 'session', 'scan', 'resource',
        'fileset', 'fileset[<mime-type>]'. If provided, the mime-type(s) explicitly specified in square brackets
        after the 'fileset' define the file-types to match (multiple mime-types can be '|'-separated, e.g.
        'fileset[image/png|image/jpeg]').
        The 'join-exprs' arg specifies the expressions used to join the metadata table to the input files.
        Each join expression should be in the format '<column-name>=<cell-value-to-match>'. The cell value to match
        can either be the name of an existing metadata field or a Python string template built from one or more existing
        metadata fields, e.g., '{PatientID}_{SessionID}'.
        A complex real-world example is shown below, where the relative path of a PNG file is extracted using the
        `--path-metadata-regex` option and then used to join the metadata table and inject the rows values based on
        a hyperlink in the 'ImagePath' column.

            group(
                ...,
                metadata_table=MetadataTable(
                    table_file='path/to/file.csv[text/csv]',
                    row_frequency='fileset[image/png]',
                    join_exprs='ImagePath="Hyperlink(\\'{relpath}\\')"
                ),
                path_metadata_regex=PathMetadataRegex(
                    pattern='.*(?<relpath>[\\w\\-]+/[\\w\\-]+.(?:png|jpg))'),
                    datatypes='image/png|image/jpg'
                )
            )
    """

    errors = []

    # Create sub-directory of the output directory to build sessions in before
    # moving them into their final location
    build_dir = output_dir / BUILD_NAME_DEFAULT

    build_dir.mkdir(parents=True, exist_ok=True)

    sessions = ImagingSession.from_paths(
        files_path=input_paths,
        datatypes=datatypes,
        session_field=session,
        scan_field=scan,
        resource_field=resource,
        recursive=recursive,
        on_resource_clash=on_resource_clash,
        ignore_paths=ignore_paths,
        ignore_types=ignore_types,
        path_metadata_regex=path_metadata_regex,
        metadata_tables=metadata_tables,
    )

    errors = save_sessions_to_dir(
        sessions,
        f"Grouping files found in '{input_paths}' to {output_dir!s}",
        wait_period=wait_period,
        build_dir=build_dir,
        copy_mode=copy_mode,
        output_dir=output_dir,
        unlink_source=unlink_source,
        raise_errors=raise_errors,
        collation_map=collation_map,
        conversion_map=conversion_map,
    )
    if errors:
        logger.error("Grouping completed with %s errors", len(errors))
    else:
        logger.info("Grouping completed successfully")
    return errors


def group_orthanc(
    url: str,
    store_dir: Path,
    output_dir: Path,
    user: str,
    password: str,
    to_process_label: str | None = None,
    processed_label: str | None = None,
    unlink_source: str | None = None,
    raise_errors: bool = False,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.hardlink_or_copy,
    wait_period: int = 0,
) -> list[str]:
    """Groups the input files into sessions and stages them into the staging directory.

    Parameters
    ----------
    url: str
        Orthanc server to retrieve the DICOM resources from.
    output_dir: Path
        Path to the staging directory where the grouped sessions will be saved. This should be the final location for the grouped sessions.
    user: str
        Orthanc user to login with
    password: str
        Orthanc password to login with
    processed_label: str | None
        The label applied to the sessions in Orthanc by this script to signify that they have already been processed.
    to_process_label: str | None
        The label externally applied to  sessions in Orthanc to signify that should be processed. If None,
        all sessions will be processed.
    session_id: list[IDSpec] | None
        List of ID specifications to use for extracting the session ID from the input files. If None, the
        session ID will be generated from the subject and visit IDs.
    scan_id: list[IDSpec]
        List of ID specifications to use for extracting the scan ID from the input files.
    unlink_source: str | None
        If "all" or "keep-metadata", the source studies in Orthanc will be unlinked after staging. Not yet
        implemented. If None, the source studies will be left in place.
    raise_errors: bool
        If True, any errors encountered during staging will raise an exception. If False, errors will be logged and the
        staging process will continue for the remaining sessions.
    copy_mode: FileSet.CopyMode
        The copy mode to use when saving the sessions. This determines whether files are copied, moved or symlinked when
        saving the sessions to the staging directory.
    wait_period: int
        If provided, this is the number of seconds that must have passed since the last modification time of the session before
        it will be staged. This can be used to avoid staging sessions that are still being modified or created.
    """

    if (
        unlink_source is not None
        or copy_mode is not FileSet.CopyMode.hardlink_or_copy
        or raise_errors is True
    ):
        raise NotImplementedError(
            "'unlink_source', copy_mode' and 'raise_errors' are not yet implemented for Orthanc grouping."
        )

    errors = []

    # Create sub-directory of the output directory to build sessions in before
    # moving them into their final location
    build_dir = output_dir / BUILD_NAME_DEFAULT

    build_dir.mkdir(parents=True, exist_ok=True)

    sessions = ImagingSession.from_orthanc(  # noqa
        url=url,
        output_dir=output_dir,
        store_dir=store_dir,
        user=user,
        password=password,
        to_process_label=to_process_label,
        processed_label=processed_label,
        wait_period=wait_period,
    )

    # Should from_orthanc() not actually move the data, just reference it in place like from_paths()
    # does? If so, we can just call save_sessions_to_dir() here.

    # errors = save_sessions_to_dir(
    #     sessions,
    #     f"Grouping resources found in Orthanc instance at '{url}' to {output_dir}",
    #     build_dir=build_dir,
    #     copy_mode=copy_mode,
    #     output_dir=output_dir,
    #     unlink_source=unlink_source,
    #     raise_errors=raise_errors,
    # )

    if errors:
        logger.error("Grouping from Orthanc completed with %s errors", len(errors))
    else:
        logger.info("Grouping from Orthanc completed successfully")
    return errors


def save_sessions_to_dir(
    sessions: list[ImagingSession],
    msg: str,
    build_dir,
    copy_mode: FileSet.CopyMode,
    output_dir: Path,
    wait_period: int = 0,
    collation_map=None,
    conversion_map: dict[type[FileSet], type[FileSet]] | None = None,
    unlink_source: str | None = None,
    raise_errors: bool = False,
) -> list[str]:
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
                conversion_map=conversion_map,
            )
            logger.info(
                "Successfully grouped session '%s' to '%s'",
                session.uid,
                str(saved_dir),
            )
            session_output_dir = output_dir.joinpath(*session.staging_relpath)
            ImagingSession.move_dir(saved_dir, session_output_dir)
            if unlink_source is not None:
                # 'all' and 'keep-metadata' are equivalent here: this session's source
                # files may live in a directory shared with other, not-yet-processed
                # sessions, so only the individual files are ever removed — never the
                # whole parent directory (unlike 'assign'/'deidentify', which clean up
                # a staged directory that xnat-ingest created and owns exclusively)
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

    if errors:
        logger.error("Grouping from Orthanc completed with %s errors", len(errors))
    else:
        logger.info("Grouping from Orthanc completed successfully")
    return errors
