import json
import traceback
import typing as ty
from pathlib import Path

from cryptography.fernet import Fernet
from fileformats.core import FileSet, from_mime
from fileformats.medimage.base import MedicalImagingData
from tqdm import tqdm

from xnat_ingest.helpers.remotes import LocalSessionListing, list_session_dirs

from ..helpers.arg_types import OnResourceClash
from ..helpers.logging import logger
from ..model.session import ImagingSession

DEFAULT_SPEC_DIR = "__default__"


def deidentify(
    input_dir: Path,
    output_dir: Path,
    spec_dir: Path,
    reid_dir: Path,
    on_resource_clash: OnResourceClash = "error",
    raise_errors: bool = False,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.copy,
    require_manifest: bool = True,
    unlink_source: str | None = None,
    reid_encrypt_key: bytes | None = None,
    max_workers: int | None = None,
) -> list[str]:
    """
    Parameters
    ----------
    max_workers : int, optional
        the number of threads handed to a resource's own deidentify implementation to
        parallelise work within that resource (e.g. the per-file loop for a DICOM
        series). Ignored by formats that don't support it.
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
    reid_dir.mkdir(parents=True, exist_ok=True)

    errors: list[str] = []

    default_spec = load_specs(spec_dir / DEFAULT_SPEC_DIR)

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
            # Get the project-specific deidentification specs for this session
            # for each file type
            specs = load_specs(spec_dir / session.project_id)
            if specs is None:
                if default_spec is None:
                    raise ValueError(
                        f"No deidentification specs found for project '{session.project_id}' "
                        "and no default specs provided."
                    )
                specs = default_spec

            deidentified_session, reid_mdata = session.deidentify(
                output_dir,
                copy_mode=copy_mode,
                on_resource_clash=on_resource_clash,
                specs=specs,
                max_workers=max_workers,
            )
            deidentified_session.save(output_dir / session_listing.name)
            reid_document = {
                "session_uid": session.uid,
                "changed_fields": reid_mdata,
            }
            # default=str handles values that aren't natively JSON-serialisable but
            # have a sensible string representation, e.g. pydicom's PersonName
            # (kept as a rich object elsewhere in metadata for .family_name/
            # .given_name access, see xnat_ingest.helpers.metadata.Metadata.save).
            reid_mdata_json = json.dumps(reid_document, indent=2, default=str).encode()
            if reid_encrypt_key is not None:
                reid_fspath = reid_dir / f"{session_listing.name}.json.enc"
                reid_fspath.write_bytes(
                    Fernet(reid_encrypt_key).encrypt(reid_mdata_json)
                )
            else:
                reid_fspath = reid_dir / f"{session_listing.name}.json"
                reid_fspath.write_bytes(reid_mdata_json)
        except Exception as e:
            if raise_errors:
                raise
            logger.error(
                "Error deidentifying session '%s': %s",
                session_listing.session_id,
                str(e),
            )
            logger.debug(traceback.format_exc())
            errors.append(str(e))
        else:
            if unlink_source == "all":
                # remove the original (assigned) session directory in its entirety
                session_listing.session_dir.rmdir()
            elif unlink_source == "keep-metadata":
                # remove just the resource data, leaving the session/scan-level
                # metadata behind as a lightweight skeleton
                session.unlink(keep_metadata=True)
    if errors:
        logger.error(
            "Deidentification completed with %d errors",
            len(errors),
        )
    else:
        logger.info("Deidentification completed successfully")

    return errors


def load_specs(spec_dir: Path) -> ty.Mapping[ty.Type[MedicalImagingData], Path] | None:
    """Loads the deidentification specifications from the given directory,
    returning a mapping of file-formats to their corresponding spec file paths.
    The spec files should be named in the format '{mime_type}.json', where
    the mime type is transformed by replacing '/' with '@' to be filesystem-friendly.
    If the spec directory does not exist, returns None

    Parameters
    ----------
    spec_dir : Path
        the directory containing the deidentification specification files

    Returns
    -------
    dict or None
        A mapping of file-format types to their corresponding spec file paths,
        or None if the spec directory does not exist.

    """
    if not spec_dir.exists():
        return None
    return {
        from_mime(p.stem.replace("@", "/")): p
        for p in spec_dir.iterdir()
        if "@" in p.name
    }
