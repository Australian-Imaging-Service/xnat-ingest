import hashlib
import hmac
import json
import os
import shutil
import traceback
import typing as ty
from pathlib import Path

from cryptography.fernet import Fernet
from fileformats.core import FileSet, from_mime
from fileformats.medimage.base import MedicalImagingData
from tqdm import tqdm

from accessionflow.helpers.arg_types import OnResourceClash
from accessionflow.helpers.logging import logger
from accessionflow.helpers.metadata import Metadata
from accessionflow.helpers.remotes import LocalSessionListing, list_session_dirs
from accessionflow.model.accession import Accession, recipe_formats_for

from .collate_api import BUILD_NAME_DEFAULT

DEFAULT_RECIPE_DIR = "__default__"


def _hash_uid(uid: str, key: str) -> str:
    """A plain hash could be recomputed by anyone holding the original UID, so the hash
    is keyed. Hexadecimal, so that it is safe to use in a directory name."""
    return hmac.new(key.encode(), uid.encode(), hashlib.sha256).hexdigest()


def deidentify(
    input_dir: Path,
    output_dir: Path,
    recipe_dir: Path,
    reid_dir: Path | None = None,
    on_resource_clash: OnResourceClash = "error",
    raise_errors: bool = False,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.copy,
    require_manifest: bool = True,
    unlink_source: str | None = None,
    reid_encrypt_key: bytes | None = None,
    max_workers: int | None = None,
    metadata_allowlist: ty.Collection[str] = {Accession.UID_METADATA_KEY},
    hash_uid_key: str | None = None,
) -> list[str]:
    """
    Parameters
    ----------
    reid_dir : Path, optional
        the directory to write the re-identification metadata to, one JSON file per
        session. If not given, the re-identification metadata is discarded rather than
        written to disk (use this when retaining the original<->deidentified mapping
        would itself be a security concern).
    max_workers : int, optional
        the number of threads handed to a resource's own deidentify implementation to
        parallelise work within that resource (e.g. the per-file loop for a DICOM
        series). Ignored by formats that don't support it.
    hash_uid_key : str, optional
        the secret key used to replace each session's UID (by default its DICOM
        StudyInstanceUID) with a keyed hash of it in the output. The same UID and key
        always give the same hash, so a session is still recognised when it is uploaded
        again. If not given, the original UID is kept in the output.
    """

    if hash_uid_key is not None and not hash_uid_key:
        raise ValueError("The key used to hash session UIDs must not be empty")

    if reid_encrypt_key is not None and reid_dir is None:
        logger.warning(
            "--reid-encrypt-key was provided but no --reid-dir; the re-identification "
            "metadata is being discarded, so the key has no effect"
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
    if reid_dir is not None:
        reid_dir.mkdir(parents=True, exist_ok=True)

    errors: list[str] = []
    n_skeletons = 0

    default_loaded = load_recipe_files(recipe_dir / DEFAULT_RECIPE_DIR)

    for session_listing in tqdm(
        sessions,
        total=num_sessions,
        desc=f"Processing staged sessions found in '{input_dir}'",
    ):
        try:
            with Accession.load_for_processing(
                session_listing.cache_path,
                require_manifest=require_manifest,
                check_checksums=False,
                unlink_source=unlink_source,
            ) as session:
                # keep-metadata leaves an expected skeleton after processing.
                if not session.resources:
                    if (session_listing.fspath / Metadata.FNAME).exists():
                        n_skeletons += 1
                        logger.debug(
                            "Skipping metadata-only skeleton '%s'",
                            session_listing.name,
                            extra={
                                "event": "deid_skeleton_skipped",
                                "session": session_listing.name,
                            },
                        )
                        continue
                    raise ValueError(
                        f"Session '{session_listing.name}' contains no data "
                        "files and no session metadata, so there is nothing to deidentify"
                    )
                # Recipes are selected by the accession's top-level label, e.g. the
                # XNAT project
                if not session.labels:
                    raise ValueError(
                        f"Session '{session_listing.name}' hasn't been labelled, so its "
                        "deidentification recipes can't be selected"
                    )
                top_label = next(iter(session.labels.values()))
                loaded = load_recipe_files(recipe_dir / top_label)
                if loaded is None or not any(loaded):
                    if default_loaded is None or not any(default_loaded):
                        raise ValueError(
                            "No deidentification recipe files found for "
                            f"'{top_label}' and no default recipe files provided."
                        )
                    loaded = default_loaded

                # New sessions appear by rename. Existing sessions retain their
                # earlier scans; save() publishes each changed resource only
                # after its files and sidecars are complete.
                build_dir = output_dir / BUILD_NAME_DEFAULT
                scratch_dir = build_dir / f"scratch_{session_listing.name}"
                work_dir = build_dir / f"promote_{session_listing.name}"
                for stale in (scratch_dir, work_dir):
                    if stale.exists():
                        shutil.rmtree(stale)
                scratch_dir.mkdir(parents=True)
                original_uid = session.uid
                try:
                    deidentified_session, reid_mdata = session.deidentify(
                        scratch_dir,
                        copy_mode=copy_mode,
                        on_resource_clash=on_resource_clash,
                        recipe_files=loaded,
                        max_workers=max_workers,
                    )
                    if hash_uid_key is not None:
                        deidentified_session.uid = _hash_uid(original_uid, hash_uid_key)
                    final_dir = output_dir / deidentified_session.staging_dirname()
                    if final_dir.exists():
                        deidentified_session.save(
                            output_dir,
                            metadata_allowlist=metadata_allowlist,
                        )
                    else:
                        work_dir.mkdir(parents=True)
                        _, built_dir = deidentified_session.save(
                            work_dir,
                            metadata_allowlist=metadata_allowlist,
                        )
                        os.replace(built_dir, final_dir)
                finally:
                    for temporary in (scratch_dir, work_dir):
                        if temporary.exists():
                            shutil.rmtree(temporary)
                if reid_dir is not None:
                    reid_document = {
                        "session_uid": original_uid,
                        "changed_fields": reid_mdata,
                    }
                    if hash_uid_key is not None:
                        reid_document["deidentified_session_uid"] = (
                            deidentified_session.uid
                        )
                    # PersonName and similar metadata values need string conversion.
                    reid_mdata_json = json.dumps(
                        reid_document, indent=2, default=str
                    ).encode()
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
                session_listing.name,
                str(e),
                extra={"event": "deid_failed", "session": session_listing.name},
            )
            logger.debug(traceback.format_exc())
            errors.append(str(e))
    if n_skeletons:
        # ONE line per pass, not one per skeleton: see the DEBUG line above.
        logger.info(
            "Skipped %d already-processed session(s) reduced to metadata-only "
            "skeletons",
            n_skeletons,
            extra={"event": "deid_skeletons_skipped", "count": n_skeletons},
        )
    if errors:
        logger.error(
            "Deidentification completed with %d errors",
            len(errors),
        )
    else:
        logger.info("Deidentification completed successfully")

    return errors


def load_recipe_files(
    recipe_dir: Path,
) -> ty.Mapping[type[MedicalImagingData], Path] | None:
    """Loads the deidentification recipe files from the given directory, returning a
    mapping of file-formats to their corresponding recipe file paths.

    The directory structure mirrors the MIME-like hierarchy of the file formats::

        recipe_dir/
        └── <category>/          # e.g. "medimage"
            ├── <format>         # recipe file, e.g. "dicom-series" (any extension or none)
            └── <format>.<side-car-ext>  # optional side-cars of the recipe format

    Side-cars of the recipe file (e.g. the ``.transforms.yaml`` and ``.salt`` side-cars of
    a `DeidRecipeX`) are picked up by the recipe format when the recipe is loaded,
    and are skipped here.

    If the recipe directory does not exist, returns None.

    Parameters
    ----------
    recipe_dir : Path
        the directory containing the deidentification recipe files

    Returns
    -------
    Mapping[type[MedicalImagingData], Path] or None
        maps file-format types to their corresponding recipe file paths, or None if
        the recipe directory does not exist.
    """
    if not recipe_dir.exists():
        return None
    candidates: dict[type[MedicalImagingData], list[Path]] = {}
    for category_dir in recipe_dir.iterdir():
        if not category_dir.is_dir() or category_dir.name.startswith((".", "_")):
            continue
        for p in category_dir.iterdir():
            if p.is_dir() or p.name.startswith("."):
                continue
            # Recipe file — use full name first, then stem (strip extension)
            filetype = _resolve_mime_type(
                category_dir.name, p.name
            ) or _resolve_mime_type(category_dir.name, p.stem)
            if filetype is not None:
                candidates.setdefault(filetype, []).append(p)
    recipe_files: dict[type[MedicalImagingData], Path] = {}
    for filetype, paths in candidates.items():
        if len(paths) > 1:
            # Side-cars of the recipe can also resolve to the file type by their stem
            # (e.g. "dicom-series.salt"), so only keep the ones that are valid recipes
            recipe_formats = recipe_formats_for(filetype)
            if recipe_formats is not None:
                paths = [p for p in paths if any(f.matches(p) for f in recipe_formats)]
        if len(paths) != 1:
            raise ValueError(
                f"Expected exactly one deidentification recipe file for {filetype} in "
                f"{recipe_dir}, found {[p.name for p in candidates[filetype]]}"
            )
        recipe_files[filetype] = paths[0]
    return recipe_files


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
