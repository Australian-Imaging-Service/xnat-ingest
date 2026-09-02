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


def _newest_mtime(path: Path) -> float:
    """The most recent mtime of anything under `path`, or 0.0 if it is absent."""
    if not path.exists():
        return 0.0
    mtimes = [f.stat().st_mtime for f in path.rglob("*") if f.is_file()]
    mtimes.append(path.stat().st_mtime)
    return max(mtimes)


def _output_is_current(output_session_dir: Path, input_session_dir: Path, n_in: int) -> bool:
    """Whether this session's output can be left alone for this cycle.

    Re-deidentifying an unchanged session is wasted work: measured at roughly 80
    seconds for a 535-instance series, which a 60-second loop cannot absorb once
    a site has more than a couple of sessions staged.

    COMPLETENESS IS CHECKED FIRST, and that is the whole point of this function.
    Comparing mtimes alone, which is what the original version of this skip did,
    treats "newer than its input" as "finished". An output that is SHORT is also
    newer than its input, so a session left half-written by a run that died would
    be skipped on this cycle and on every cycle after it, permanently, while
    sitting in the directory the upload stage reads. That is the exact failure
    this stage is supposed to prevent, reached by way of an optimisation.
    """
    if not output_session_dir.exists():
        return False
    if _data_file_count(output_session_dir) != n_in:
        return False
    if _newest_mtime(output_session_dir) < _newest_mtime(input_session_dir):
        return False
    # CONTENT, NOT JUST COUNT, and this is the load-bearing part.
    #
    # A re-save writes into the output IN PLACE, so a run that dies during one
    # can leave a file truncated or half-written under the real name. The file
    # is still there, so the count is right, and it was written after its input,
    # so the mtime is newer. Counting alone would call that finished and skip it
    # on this cycle and every cycle afterwards.
    #
    # Skipping is not merely a missed opportunity to notice: it BYPASSES the
    # repair. ImagingResource.save is what fixes a bad output, by loading what is
    # on disk, finding the checksums disagree and overwriting it. That code only
    # runs if save() runs, and this function decides whether it does. So a skip
    # that examines a corrupt output is what makes the corruption permanent.
    #
    # Verifying against the manifest costs a hash of the output, which is a small
    # fraction of the de-identification it avoids.
    return _output_verifies(output_session_dir)


def _output_verifies(output_session_dir: Path) -> bool:
    """Whether the output on disk hashes to what its own manifest says.

    Loading with check_checksums=True recomputes every file's checksum and
    compares it against the manifest written beside it, so this catches a file
    that is present but truncated or half-written, which a count cannot.
    """
    try:
        ImagingSession.load(
            output_session_dir, require_manifest=True, check_checksums=True
        )
    except Exception:
        return False
    return True


def _unlink_source(
    session_listing: LocalSessionListing,
    session: ImagingSession,
    unlink_source: str | None,
    errors: list[str],
) -> None:
    """Retire a session's input now that a complete output exists for it.

    Shared by the path that has just produced the output and the path that
    found one already there. Both need it: with --unlink-source set, a session
    whose output was produced BEFORE the flag was turned on is skipped as
    already current on every later cycle, so if only the first path unlinked,
    that backlog would sit in the input directory for ever.

    Exceptions are contained. The caller reaches this from an `else:` clause,
    where a raise is not caught by the enclosing `except` and would propagate
    out of the session loop and, under --loop, kill the stage for every other
    session.
    """
    if unlink_source == "all":
        # rmtree, NOT rmdir: the session directory holds scan directories, so
        # rmdir() raises "Directory not empty". assign does the same job the
        # same way. LocalSessionListing exposes `fspath`; there is no
        # `session_dir` attribute, so the previous form raised AttributeError
        # before it could delete anything.
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
    n_skeletons = 0

    default_loaded = load_specs(spec_dir / DEFAULT_SPEC_DIR)

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
            # AN EMPTY INPUT IS NOT A COMPLETE RUN. The completeness gate below
            # compares n_out against n_in, and 0 == 0 is vacuously True, so a
            # session directory holding no data files used to pass as complete:
            # deidentify created an output for it, reported success and exited 0,
            # and on tier-1 upload reads that directory directly. Measured: an
            # empty input produced an output containing nothing but a metadata
            # sidecar, with errors == [].
            #
            # Reported as its own event rather than folded into deid_incomplete.
            # "I was handed a session with nothing in it" is a different fault
            # from "my output is short": the first means the stage before this
            # one failed or the directory is a leftover, and only the second
            # says anything about this stage's work.
            n_in = _data_file_count(session_listing.fspath)
            if n_in == 0:
                # TWO DIFFERENT THINGS LOOK LIKE ZERO, and only one is a fault.
                #
                # `--unlink-source keep-metadata` deliberately leaves a session
                # with no data files: session.unlink removes each resource
                # directory outright and keeps the scan and session metadata, so a
                # later stage can still work out which scan a late-arriving file
                # belongs to. That skeleton is the designed steady state of that
                # mode, and under --loop it is re-examined every cycle forever.
                # Reporting it would emit an error every interval, for every
                # session ever processed, for as long as the stage runs.
                #
                # The session-level metadata file is what separates them: save()
                # writes it as its very last action, so its presence means the
                # session was fully written at least once and what is left is a
                # skeleton. A directory that never got that far is either a
                # leftover or the wreckage of a stage that died, and that IS a
                # fault worth reporting.
                #
                # This also catches a HALF-WRITTEN INPUT, which is why the test
                # is on the metadata file and not on some marker of the unlink
                # mode. assign writes its output in place, so a session it died
                # part-way through has data files but no session metadata, and
                # one that died before writing any has neither. Either way this
                # stage now declines to consume a partial upstream output rather
                # than deidentifying it and presenting it as whole.
                if (session_listing.fspath / Metadata.FNAME).exists():
                    n_skeletons += 1
                    # DEBUG, not INFO: skeletons are never removed, so under
                    # --loop this line would repeat for every processed session
                    # every interval, without bound, for the life of the
                    # deployment. One aggregate line per cycle is logged below
                    # instead. The structured event stays so a rule can still
                    # count them.
                    logger.debug(
                        "Skipping '%s': already processed and reduced to a "
                        "metadata-only skeleton",
                        session_listing.name,
                        extra={
                            "event": "deid_skeleton_skipped",
                            "session": session_listing.name,
                        },
                    )
                    continue
                msg = (
                    f"Session '{session_listing.session_id}' contains no data "
                    f"files and no session metadata, so there is nothing to "
                    f"deidentify"
                )
                logger.error(
                    msg,
                    extra={
                        "event": "deid_empty_input",
                        "session": session_listing.name,
                    },
                )
                errors.append(msg)
                continue

            # SKIP AN OUTPUT THAT IS ALREADY DONE. Supersedes the mtime-only
            # version of this check, which could skip a partial output for ever.
            #
            # The name is taken from the loaded input session rather than from
            # the input DIRECTORY name, so that a run_uid suffix or an
            # invalid-project prefix is accounted for. If the deidentified
            # session were somehow to carry different ids from its input, this
            # fails open: the guess does not exist, nothing is skipped, and the
            # session is processed as usual.
            if _output_is_current(
                output_dir / session.staging_dirname(), session_listing.fspath, n_in
            ):
                logger.debug(
                    "Skipping '%s', its deidentified output is complete and current",
                    session_listing.name,
                    extra={
                        "event": "deid_output_current",
                        "session": session_listing.name,
                    },
                )
                # AND RETIRE ITS INPUT, on the same terms as a run that just
                # produced the output. _output_is_current has already verified
                # the output is complete AND that its files hash to what its
                # manifest says, which is a stronger check than the file count
                # the post-run gate applies. Without this, turning
                # --unlink-source on would never clear sessions de-identified
                # before it was turned on: they are skipped as current on every
                # cycle, so the branch that unlinks is never reached and the
                # backlog stays for ever.
                _unlink_source(session_listing, session, unlink_source, errors)
                continue

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
                # n_in was measured BEFORE the run, above, not here. Measuring it
                # after the fact would count whatever the source holds NOW: a
                # session that gained files while this run was working would read
                # n_out < n_in and be called incomplete, when the right answer is
                # that the new files are simply not this run's to carry.
                if saved_dir is None:
                    # The run failed before producing output. "I could not check" is
                    # not "the data is missing"; the except branch above has already
                    # recorded the real error.
                    complete = False
                    n_out = -1
                else:
                    n_out = _data_file_count(saved_dir)
                    # THE SAME CHECK THE SKIP PATH USES, and it belongs here
                    # more than there. This gate is what permits the unlink, and
                    # the unlink is irreversible: the input is the only copy that
                    # could repair a bad output. It also guards the riskier
                    # moment. The skip path judges an output that has already
                    # survived a whole cycle; this one judges an output written
                    # seconds ago, by a run that may have been interrupted, into
                    # a destination that on the re-save branch is written IN
                    # PLACE and can be left truncated under its real name.
                    #
                    # Counting alone let exactly that through: a corrupt output
                    # with the right number of files passed here, its input was
                    # deleted, and the corruption was then caught on the next
                    # cycle by the skip path's checksum test, correctly and a
                    # cycle too late to repair.
                    complete = n_out == n_in and _output_verifies(saved_dir)
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
            else:
                _unlink_source(session_listing, session, unlink_source, errors)
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
