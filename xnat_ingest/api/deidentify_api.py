import importlib.util
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

from xnat_ingest.helpers.remotes import LocalSessionListing, list_session_dirs
from xnat_ingest.helpers.metadata import Metadata
from xnat_ingest.model.resource import ImagingResource

from .group_api import BUILD_NAME_DEFAULT
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

def _data_file_count(session_dir: Path) -> int:
    """Number of DATA files under a session directory.

    Sidecars are excluded on both sides of the comparison: they are written by
    ImagingResource.save/Metadata.save rather than carried through from the input,
    so counting them would make a complete session look short by exactly the number
    of manifests and metadata files it happens to contain.
    """
    sidecars = {
        ImagingResource.MANIFEST_FNAME,
        ImagingResource.OLD_MANIFEST_FNAME,
        Metadata.FNAME,
    }
    if not session_dir.exists():
        return 0
    return sum(
        1 for f in session_dir.rglob("*") if f.is_file() and f.name not in sidecars
    )


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
            # Scratch lives under __build__, which is invisible to every consumer:
            # list_session_dirs skips names starting with "__" and the s3 uploader
            # skips it by name. The previous ".deid_scratch_<name>" was NOT invisible,
            # because list_session_dirs excludes "__"-prefixed names and not
            # "."-prefixed ones, so on tier-1, where upload reads /data/deidentified
            # directly, a scratch directory could be listed as a session mid-write.
            #
            # Wiped before use as well as after: a tree surviving a run that died
            # would otherwise be picked up by the next one.
            #
            # A SECOND directory under __build__ is used to materialise an output
            # that does not exist yet, so that it appears under its real name by
            # rename rather than filling up in place. A crash part-way through a
            # first save would otherwise leave a partial tree under the real name;
            # once its mtimes go quiet it looks complete to both upload paths, and
            # the completeness gate below cannot catch it because the gate runs in
            # the process that just died.
            #
            # Only when there is NO existing output. Re-saving over one that is
            # already there goes straight to output_dir, because building
            # elsewhere and renaming in defeats both write-if-changed
            # short-circuits: each is conditional on the destination already
            # existing (Metadata.save compares against the file, ImagingResource
            # .save is gated on `if resource_dir.exists()`). Measured: always
            # building rewrote every file including the DICOMs on an unchanged
            # session, resetting both upload settle windows every cycle, which is
            # a worse stall than the metadata churn it was meant to replace.
            #
            # The rename costs nothing: save() writes the same bytes either way
            # and os.replace within output_dir is a rename, not a copy.
            build_dir = output_dir / BUILD_NAME_DEFAULT
            scratch_dir = build_dir / f"scratch_{session_listing.name}"
            work_dir = build_dir / f"promote_{session_listing.name}"
            for stale in (scratch_dir, work_dir):
                if stale.exists():
                    shutil.rmtree(stale)
            scratch_dir.mkdir(parents=True)
            # None until save() hands back the real path. If the run raises, the
            # finally must say "could not verify", NOT count a rebuilt path and
            # report a completeness verdict for output that was never produced.
            saved_dir = None
            try:
                deidentified_session, reid_mdata = session.deidentify(
                    scratch_dir,
                    copy_mode=copy_mode,
                    on_resource_clash=on_resource_clash,
                    specs=specs,
                    transforms=transforms,
                    max_workers=max_workers,
                )
                # Where save() WILL put it, worked out with save()'s own rule.
                # ONE binding, passed to both calls. If the exists-check and the
                # save ever disagreed on available_projects, the check would look
                # for a name without the INVALID_UNRECOGNISED_ prefix while save()
                # wrote one with it, and every cycle would take the promote branch
                # onto a path that is not where the output actually lives.
                available_projects = None
                final_dir = output_dir / deidentified_session.staging_dirname(
                    available_projects
                )
                if final_dir.exists():
                    # In place, and so NOT atomic: a crash here leaves an output
                    # part old and part new. Much narrower than the first save,
                    # because the checksum short-circuit rewrites only resources
                    # whose content actually changed, and a stale resource left by
                    # a half-finished pass is overwritten on the next one.
                    _, saved_dir = deidentified_session.save(
                        output_dir, available_projects
                    )
                else:
                    work_dir.mkdir(parents=True)
                    _, built_dir = deidentified_session.save(
                        work_dir, available_projects
                    )
                    # Appears under its real name in one step. If final_dir did
                    # somehow get created in between, os.replace raises rather
                    # than merging, the except below records it, and the source
                    # is not unlinked.
                    os.replace(built_dir, final_dir)
                    saved_dir = final_dir
            finally:
                # Both, and unconditionally: whatever is left in either is by
                # definition not part of a promoted output, and leaving it would be
                # adopted by the next run.
                shutil.rmtree(scratch_dir, ignore_errors=True)
                shutil.rmtree(work_dir, ignore_errors=True)
                # PER SESSION, not per directory. The previous form compared every
                # file under output_dir against every file under input_dir, so with
                # more than one session in flight the two numbers described different
                # sets and the comparison meant nothing.
                #
                # The result GATES the unlink below. A run that produced fewer files
                # than it read must not delete the input it read them from, which is
                # the only copy still able to repair the difference.
                # saved_dir comes from save(), NOT rebuilt from the input directory
                # name. save() derives the name from project/subject/session ids, adds
                # run_uid when set and an invalid-project prefix when the project is
                # unrecognised, so the two can differ. Rebuilding it would count an
                # empty path, make every session look incomplete, and refuse every
                # unlink forever. group_api does the same thing the same way.
                n_in = _data_file_count(session_listing.fspath)
                if saved_dir is None:
                    # The run failed before producing output. "I could not check" is
                    # not "the data is missing"; the except branch above has already
                    # recorded the real error.
                    complete = False
                    n_out = -1
                else:
                    n_out = _data_file_count(saved_dir)
                    complete = n_out == n_in
                if saved_dir is not None and not complete:
                    logger.warning(
                        "Deidentification of session '%s' produced %d output files, but %d input files were found. "
                        "This may indicate that some files were not processed correctly.",
                        session_listing.session_id,
                        n_out,
                        n_in,
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
                extra={"event": "deid_failed", "session": session_listing.name},
            )
            logger.debug(traceback.format_exc())
            errors.append(str(e))
        else:
            if saved_dir is not None and not complete:
                # RECORDED regardless of unlink_source. Incompleteness is a property of
                # the OUTPUT, not of the deletion policy, and today no site passes
                # --unlink-source at all, so making this conditional on the flag would
                # leave every incomplete run reporting success.
                msg = (
                    f"Deidentified output of session '{session_listing.session_id}' is "
                    f"incomplete ({n_out} of {n_in} files)"
                )
                if unlink_source:
                    # The input is the only copy that can still make it whole.
                    msg += "; source not unlinked so the work can be retried"
                # STRUCTURED, not just prose. Under AIS_LOG_FORMAT=json the line is
                # {"ts","level","logger","message"}, and an alert cannot reliably match
                # free text inside `message`. JsonFormatter passes `extra` through, so
                # this emits an `event` field a rule can select on, the same shape the
                # data-policy rules already use.
                logger.error(
                    msg,
                    extra={
                        "event": "deid_incomplete",
                        "session": session_listing.name,
                        "files_out": n_out,
                        "files_in": n_in,
                    },
                )
                errors.append(msg)
            elif unlink_source == "all":
                # NOTE: this whole branch is an `else:` of the try above, so an
                # exception here is NOT caught by that except and would propagate out
                # of the session loop, out of deidentify() and, under --loop, out of
                # the while loop, killing the stage for every other session. rmtree can
                # raise on EACCES or a concurrent removal, so it is guarded.
                # remove the original (assigned) session directory in its entirety.
                # rmtree, NOT rmdir: the session directory holds scan directories, so
                # rmdir() raises "Directory not empty". assign does the same job the
                # same way (assign_api.py). LocalSessionListing exposes `fspath`;
                # there is no `session_dir` attribute, so the previous form raised
                # AttributeError before it could delete anything.
                try:
                    shutil.rmtree(session_listing.fspath)
                except OSError as unlink_err:
                    msg = (
                        f"Could not unlink source of session "
                        f"'{session_listing.session_id}' after a complete run: "
                        f"{unlink_err}"
                    )
                    logger.error(
                        msg,
                        extra={
                            "event": "deid_unlink_failed",
                            "session": session_listing.name,
                        },
                    )
                    errors.append(msg)
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
