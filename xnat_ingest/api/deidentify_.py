import os
import json
import traceback
from pathlib import Path
import typing as ty

from cryptography.fernet import Fernet
from fileformats.core import extra_implementation, from_mime
from fileformats.medimage.base import MedicalImagingData
from fileformats.medimage.dicom import DicomImage
from fileformats.core import FileSet
from tqdm import tqdm

from xnat_ingest.helpers.remotes import LocalSessionListing, list_session_dirs

from ..helpers.logging import logger
from ..model.session import ImagingSession

DEFAULT_SPEC_DIR = "__default__"


def deidentify(
    input_dir: Path,
    output_dir: Path,
    spec_dir: Path,
    reid_dir: Path,
    avoid_clashes: bool = False,
    raise_errors: bool = False,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.copy,
    require_manifest: bool = True,
    delete: bool = False,
    reid_encrypt_key: bytes | None = None,
) -> list[str]:

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
                avoid_clashes=avoid_clashes,
                specs=specs,
            )
            deidentified_session.save(output_dir / session_listing.name)
            reid_mdata_json = json.dumps(reid_mdata, indent=2).encode()
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
            if delete:
                # remove the original session directory after successful deidentification
                session_listing.session_dir.rmdir()
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
        from_mime(p.name.replace("@", "/")): p
        for p in spec_dir.iterdir()
        if "@" in p.name
    }


@extra_implementation(MedicalImagingData.deidentify)
def dicom_deidentify(
    dicom: DicomImage,
    spec: ty.Any = None,
    out_dir: os.PathLike[str] | None = None,
) -> tuple[DicomImage, ty.Mapping[str, ty.Any]]:
    raise NotImplementedError
