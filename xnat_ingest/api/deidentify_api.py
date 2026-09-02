import importlib.util
import json
import shutil
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
from ..model.session import ImagingSession, Transform

DEFAULT_SPEC_DIR = "__default__"
TRANSFORMS_SUFFIX = ".transforms.py"


def _newest_mtime(path: Path) -> float:
    """The most recent mtime of anything under `path`, or 0.0 if it doesn't exist.

    Used to tell whether a session's deidentified output is still current with
    respect to its input.
    """
    if not path.exists():
        return 0.0
    mtimes = [p.stat().st_mtime for p in path.rglob("*") if p.is_file()]
    mtimes.append(path.stat().st_mtime)
    return max(mtimes)


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

    default_loaded = load_specs(spec_dir / DEFAULT_SPEC_DIR)

    for session_listing in tqdm(
        sessions,
        total=num_sessions,
        desc=f"Processing staged sessions found in '{input_dir}'",
    ):
        try:
            # Skip sessions whose output is already up to date, otherwise every
            # cycle rewrites __METADATA__.json and `upload` keeps deferring the
            # session as recently modified.
            # Compared by mtime, not just existence, so a session that is still
            # receiving scans is reprocessed rather than left partially done.
            out_session = output_dir / session_listing.name
            if _newest_mtime(out_session) >= _newest_mtime(session_listing.cache_path):
                logger.debug(
                    "Skipping '%s', its deidentified output is up to date",
                    session_listing.name,
                )
                continue

            session = ImagingSession.load(
                session_listing.cache_path,
                require_manifest=require_manifest,
                check_checksums=False,
            )
            # Get the project-specific deidentification specs for this session
            # for each file type
            loaded = load_specs(spec_dir / session.project_id)
            if loaded is None or not any(loaded):
                if default_loaded is None or not any(default_loaded):
                    raise ValueError(
                        f"No deidentification specs found for project '{session.project_id}' "
                        "and no default specs provided."
                    )
                loaded = default_loaded
            specs, transforms = loaded

            # Create a scratch directory for temporary files during deidentification
            # The scratch directory is created within the output directory to ensure that
            # it is on the same filesystem, which is important for efficient file operations
            # The scratch dir is removed after the deidentification process is complete, regardless of success or failure
            scratch_dir = output_dir / f".deid_scratch_{session_listing.name}"
            scratch_dir.mkdir(parents=True, exist_ok=True)
            try:
                deidentified_session, reid_mdata = session.deidentify(
                    scratch_dir,
                    copy_mode=copy_mode,
                    on_resource_clash=on_resource_clash,
                    specs=specs,
                    transforms=transforms,
                    max_workers=max_workers,
                )
                deidentified_session.save(output_dir)
            finally:
                if scratch_dir.exists():
                    shutil.rmtree(scratch_dir)
                # List dcm files in the output directory to ensure that the deidentification process produced output
                output_files = [
                    str(file)
                    for file in output_dir.rglob("*")
                    if file.is_file() and str(file).endswith(".dcm")
                ]
                input_files = [
                    str(file)
                    for file in input_dir.rglob("*")
                    if file.is_file() and str(file).endswith(".dcm")
                ]
                if len(output_files) != len(input_files):
                    logger.warning(
                        "Deidentification of session '%s' produced %d output files, but %d input files were found. "
                        "This may indicate that some files were not processed correctly.",
                        session_listing.session_id,
                        len(output_files),
                        len(input_files),
                    )
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


def load_specs(
    spec_dir: Path,
) -> (
    tuple[
        ty.Mapping[type[MedicalImagingData], Path],
        ty.Mapping[type[MedicalImagingData], dict[str, Transform]],
    ]
    | None
):
    """Loads the deidentification specifications from the given directory,
    returning a mapping of file-formats to their corresponding spec file paths
    and a mapping of file-formats to their transforms.

    The directory structure mirrors the MIME-like hierarchy of the file formats::

        spec_dir/
        └── <category>/          # e.g. "medimage"
            ├── <format>         # e.g. "dicom-series" (any extension or none)
            └── <format>.transforms.py   # optional transforms

    Transforms files must define a ``TRANSFORMS`` dict mapping transform
    names to callables that accept a dataset/mapping and return a value.

    If the spec directory does not exist, returns None.

    Parameters
    ----------
    spec_dir : Path
        the directory containing the deidentification specification files

    Returns
    -------
    tuple or None
        A 2-tuple of (specs, transforms) where *specs* maps file-format
        types to their corresponding spec file paths and *transforms*
        maps file-format types to their transform dicts.  Returns None if the
        spec directory does not exist.
    """
    if not spec_dir.exists():
        return None
    specs: dict[type[MedicalImagingData], Path] = {}
    transforms: dict[type[MedicalImagingData], dict[str, Transform]] = {}
    for category_dir in spec_dir.iterdir():
        if not category_dir.is_dir() or category_dir.name.startswith((".", "_")):
            continue
        for p in category_dir.iterdir():
            if p.is_dir() or p.name.startswith("."):
                continue
            if p.name.endswith(TRANSFORMS_SUFFIX):
                # e.g. medimage/dicom-series.transforms.py
                format_name = p.name[: -len(TRANSFORMS_SUFFIX)]
                filetype = _resolve_mime_type(category_dir.name, format_name)
                if filetype is None:
                    continue
                loaded = _load_transforms(p)
                if loaded:
                    transforms[filetype] = loaded
            else:
                # Spec file — use full name first, then stem (strip extension)
                filetype = _resolve_mime_type(
                    category_dir.name, p.name
                ) or _resolve_mime_type(category_dir.name, p.stem)
                if filetype is not None:
                    specs[filetype] = p
    return specs, transforms


def _resolve_mime_type(
    category: str,
    format_name: str,
) -> type[MedicalImagingData] | None:
    """Convert a category and format name to a file-format type.

    E.g. ``_resolve_mime_type("medimage", "dicom-series")`` returns
    ``DicomSeries``.  Returns None if the MIME-like string is not recognised.
    """
    mime_like = f"{category}/{format_name}"
    try:
        return from_mime(mime_like)
    except Exception:
        return None


def _load_transforms(
    transforms_path: Path,
) -> dict[str, Transform] | None:
    """Load a TRANSFORMS dict from a Python file.

    Parameters
    ----------
    transforms_path : Path
        path to a ``.transforms.py`` file that defines ``TRANSFORMS``

    Returns
    -------
    dict or None
        the transforms dict, or None if the file does not define one
    """
    spec = importlib.util.spec_from_file_location(
        f"xnat_ingest._deid_transforms.{transforms_path.stem}", transforms_path
    )
    if spec is None or spec.loader is None:
        logger.warning("Could not load transforms from '%s'", transforms_path)
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    loaded = getattr(module, "TRANSFORMS", None)
    if loaded is None:
        logger.warning(
            "Transforms file '%s' does not define TRANSFORMS",
            transforms_path,
        )
    return loaded
