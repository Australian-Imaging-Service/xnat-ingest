import tempfile
import traceback
import typing as ty
from pathlib import Path

from fileformats.core import FileSet
from fileformats.generic import File
from tqdm import tqdm

from xnat_ingest.helpers.remotes import LocalSessionListing, list_session_dirs

from ..helpers.arg_types import AssociatedFiles, OnResourceClash
from ..helpers.logging import logger
from ..model.session import ImagingSession


def prepare_samples(session: ImagingSession) -> int:
    """For resources whose fileset supports peek_header/extract_first (duck-typed),
    peek DICOM metadata into the resource and extract a sample file as a secondary
    resource on the same scan.

    This is needed for archive formats (e.g. DicomZip) where XNAT cannot read
    metadata directly from the uploaded file. A sample DICOM lets XNAT's
    pullDataFromHeaders populate scan metadata, and the peeked SOPClassUID/Modality
    lets upload create the scan with the correct datatype.

    Returns the number of samples created.
    """
    count = 0
    for scan in list(session.scans.values()):
        for resource in list(scan.resources.values()):
            fileset = resource.fileset
            peek = getattr(fileset, "peek_header", None)
            extract = getattr(fileset, "extract_first", None)
            if not callable(peek) or not callable(extract):
                continue
            # Already has a secondary resource — don't duplicate
            if "secondary" in scan.resources:
                logger.debug(
                    "Scan %s already has a 'secondary' resource, skipping sample",
                    scan.id,
                )
                continue
            # Peek header metadata and store on the resource
            header = peek()
            if not header:
                logger.warning(
                    "Could not peek header from '%s' — archive may be empty",
                    resource.path,
                )
                continue
            for key, value in header.items():
                if value is not None:
                    resource.metadata[key] = value
            logger.info(
                "Peeked metadata from '%s': Modality=%s SOPClassUID=%s",
                resource.path,
                header.get("Modality"),
                header.get("SOPClassUID"),
            )
            # Extract sample file as a secondary resource so XNAT's
            # pullDataFromHeaders can populate scan metadata from real DICOM
            scratch = Path(tempfile.mkdtemp())
            sample_path = extract(scratch)
            if sample_path is None:
                logger.warning(
                    "Could not extract sample from '%s' — archive is empty",
                    resource.path,
                )
                continue
            session.add_resource(
                scan_id=scan.id,
                scan_type=scan.type,
                resource_name="secondary",
                fileset=File(sample_path),
                metadata={"content": "SAMPLE", "format": "DICOM"},
            )
            count += 1
            logger.info(
                "Extracted sample from '%s' as secondary resource on scan %s",
                resource.path,
                scan.id,
            )
    return count


def associate(
    input_dir: Path,
    output_dir: Path,
    datatype: ty.Type[FileSet] | None = None,
    glob: str | None = None,
    identity_pattern: str | None = None,
    spaces_to_underscores: bool = False,
    on_resource_clash: OnResourceClash = "error",
    raise_errors: bool = False,
    require_manifest: bool = True,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.copy,
    unlink_source: str | None = None,
) -> list[str]:

    has_glob_pattern = (
        datatype is not None and glob is not None and identity_pattern is not None
    )

    session_dirs = list_session_dirs(input_dir)
    sessions: list[LocalSessionListing] = [LocalSessionListing(p) for p in session_dirs]

    errors: list[str] = []
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
            session = ImagingSession.load(
                session_listing.cache_path,
                require_manifest=require_manifest,
                check_checksums=False,
            )
            associated: list[FileSet] = []
            if has_glob_pattern:
                associated = session.associate_files(
                    [AssociatedFiles(datatype, glob, identity_pattern)],
                    spaces_to_underscores=spaces_to_underscores,
                    on_resource_clash=on_resource_clash,
                )
            prepare_samples(session)
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
            if unlink_source is not None:
                # 'all' and 'keep-metadata' are equivalent here: the associated files
                # live at an arbitrary external location, not a directory tree
                # xnat-ingest owns, so only the individual matched files are ever
                # removed
                for fileset in associated:
                    fileset.unlink()

    for session in tqdm(
        metadata_sessions,
        total=len(metadata_sessions),
        desc="Processing metadata-only sessions",
    ):
        try:
            associated = []
            if has_glob_pattern:
                associated = session.associate_files(
                    [AssociatedFiles(datatype, glob, identity_pattern)],
                    spaces_to_underscores=spaces_to_underscores,
                    on_resource_clash=on_resource_clash,
                )
            prepare_samples(session)
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
            if unlink_source is not None:
                for fileset in associated:
                    fileset.unlink()
    if errors:
        logger.error("Association completed with %s errors", len(errors))
    else:
        logger.info("Association completed successfully without errors")
    return errors
