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

from dicom_deid.engine import DeidEngine
from dicom_deid.header_reid import build_reid_document, snapshot_from_pydicom
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
    unlink_source: str | None = None,
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
            if unlink_source == "all":
                # remove the original (assigned) session directory in its entirety
                session_listing.session_dir.rmdir()
            elif unlink_source == "keep-metadata":
                # remove just the resource data, leaving the session/scan-level
                # metadata behind as a lightweight skeleton
                session.unlink(keep_metadata=True)
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
    """
    De-identify a single DicomImage using the dicom_deid engine.
    
    Returns the de-identified DicomImage and a mapping dict of metadata for aggregation by XNAT Ingest's session-level reid logic.
    
    Parameters
    ----------
    dicom : DicomImage
        The DicomImage to de-identify.
    spec : ty.Any, optional
        Path to a project-specific deidentification specification file.
    out_dir : os.PathLike[str] | None, optional
        The output directory for the de-identified image. If none, a temporary directory will be used.
    
    Returns
    -------
    tuple[DicomImage, ty.Mapping[str, ty.Any]]
        The de-identified DicomImage and a mapping dict of metadata.
    """
    import tempfile
    import pydicom
    
    # Add value error when spec is none, since dicom_deid requires a spec to run.
    if spec is None:
        raise ValueError(
            "No deidentification spec provided to dicom_deidentify(). "
            "Ensure a project-specific recipe file exists in spec_dir for this project and is named using the mime-type convention (e.g. 'medimage@dicom-image')."
        )
    recipe_path = Path(spec)
    if not recipe_path.exists():
        raise FileNotFoundError(
            f"Recipe file not found at: {recipe_path}"
    )

    # Resolve output path
    if out_dir is None:
        out_dir = Path(tempfile.mkdtemp())
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Resolve input file path from DicomImage object
    infile = Path(dicom.fspath)
    outfile = out_dir / infile.name

    # Take pre-snapshot before de-identification for reid metadata
    original_ds = pydicom.dcmread(str(infile), stop_before_pixels=True)
    pre_snapshot = snapshot_from_pydicom(original_ds)

    # Configure deidentification
    # Claude suggested moving this outside the function to reduce overhead if processing many files with the same spec. It suggested creating a cache dict that maps spec paths to DeidEngine instances. Is this something we should consider?
    _engine = DeidEngine(
        recipe_path = Path(spec),   # Tom to add guard for if spec is None (use a default recipe or raise an error)
        capture_headers = False,  # header capture is handled by xnat-ingest's reid logic
        strip_sequences = True,
        remove_private = True,
    )

    # Run de-identification using dicom_deid
    result = _engine.process_file(infile, outfile)

    if not result.success:
        raise RuntimeError(
            f"De-identification failed for {infile.name}: {result.error}"
        )
    
    #Take post-snapshot after de-identification for reid metadata
    deid_ds = pydicom.dcmread(str(outfile), stop_before_pixels=True)
    post_snapshot = snapshot_from_pydicom(deid_ds)

    #Build re-identification mapping dict
    reid_mdata = build_reid_document(
        pre_snapshot = pre_snapshot,
        post_snapshot = post_snapshot,
        uid_keys = ["SOPInstanceUID", "StudyInstanceUID", "SeriesInstanceUID"],
        source_file = str(infile),
        format_label = "DICOM",
    )

    # Return the de-identified DicomImage and the re-identification metadata
    deid_dicom = DicomImage(outfile)
    return deid_dicom, reid_mdata
