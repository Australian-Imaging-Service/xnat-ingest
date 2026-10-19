import os
import shutil
import traceback
import typing as ty
import zipfile
from pathlib import Path

from fileformats.application import Zip
from fileformats.core import FileSet
from fileformats.medimage import DicomCollection, DicomImage
from tqdm import tqdm

from ..helpers.arg_types import OnResourceClash
from ..helpers.logging import logger
from ..helpers.metadata import Metadata
from ..helpers.remotes import LocalSessionListing, list_session_dirs
from ..helpers.xnat_scan_types import xnat_resource_label_from_sop_class
from ..model.resource import ImagingResource
from ..model.session import ImagingSession
from .group_api import BUILD_NAME_DEFAULT

OutputResources = ty.Mapping[str, tuple[type[FileSet], ty.Mapping[str, ty.Any]]]


def _package_session(
    session: ImagingSession,
    source_datatype: type[FileSet],
    output_resources: OutputResources,
    on_resource_clash: OnResourceClash,
) -> ImagingSession:
    """Create a session whose matching scan resources are replaced by conversions.

    Every conversion receives the same original fileset. In particular, a sample
    can be copied directly from a deidentified DICOM series instead of being read
    back out of another conversion's zip output.
    """
    packaged = session.new_empty()
    packaged.metadata.update(dict(session.metadata))
    study_uids: set[str] = set()
    makes_dicom_sample = any(
        issubclass(target_datatype, DicomImage)
        for target_datatype, _options in output_resources.values()
    )

    for scan in session.scans.values():
        matched = False
        for resource in scan.resources.values():
            if isinstance(resource.fileset, source_datatype):
                matched = True
                if makes_dicom_sample and isinstance(resource.fileset, DicomCollection):
                    source_uid = resource.metadata.get("StudyInstanceUID")
                    source_uids = (
                        {str(uid) for uid in source_uid if uid}
                        if isinstance(source_uid, (list, tuple, set))
                        else {str(source_uid)}
                        if source_uid
                        else set()
                    )
                    if len(source_uids) != 1:
                        raise ValueError(
                            f"DICOM series in scan {scan.id!r} must contain exactly "
                            "one StudyInstanceUID before packaging"
                        )
                    study_uids.update(source_uids)
                for label, (target_datatype, options) in output_resources.items():
                    logger.info(
                        "Converting %s/%s resource from %s to %s as '%s'",
                        scan.id,
                        resource.name,
                        resource.fileset.mime_like,
                        target_datatype.mime_like,
                        label,
                    )
                    converted = target_datatype.convert(
                        resource.fileset, **dict(options)
                    )
                    if isinstance(converted, DicomImage):
                        sop_class_uid = converted.metadata.get("SOPClassUID")
                        study_uid = converted.metadata.get("StudyInstanceUID")
                        if not sop_class_uid or not study_uid:
                            raise ValueError(
                                f"DICOM sample for scan {scan.id!r} is missing "
                                "SOPClassUID or StudyInstanceUID"
                            )
                        study_uids.add(str(study_uid))
                        expected_label = xnat_resource_label_from_sop_class(
                            str(sop_class_uid)
                        )
                        if label not in ("auto", expected_label):
                            raise ValueError(
                                f"DICOM sample for scan {scan.id!r} must use XNAT "
                                f"resource label {expected_label!r}, not {label!r}; "
                                "use the 'auto' output label to select it per scan"
                            )
                        output_label = expected_label
                    elif label == "auto":
                        raise ValueError(
                            "The 'auto' output label requires a DICOM image target"
                        )
                    else:
                        output_label = label
                    packaged.add_resource(
                        scan.id,
                        scan.type,
                        output_label,
                        converted,
                        associated=scan.associated,
                        on_clash=on_resource_clash,
                        metadata=dict(resource.metadata),
                    )
            else:
                packaged.add_resource(
                    scan.id,
                    scan.type,
                    resource.name,
                    resource.fileset,
                    associated=scan.associated,
                    on_clash=on_resource_clash,
                    metadata=dict(resource.metadata),
                )
        if scan.id in packaged.scans:
            packaged.scans[scan.id].metadata.update(dict(scan.metadata))
        elif not matched:
            # A scan with no resources is unusual but still carries meaningful
            # scan-level metadata and should survive this structural operation.
            packaged_scan = scan.new_empty()
            packaged_scan.associated = scan.associated
            packaged_scan.session = packaged
            packaged_scan.metadata.update(dict(scan.metadata))
            packaged.scans[scan.id] = packaged_scan

    if len(study_uids) > 1:
        raise ValueError(
            "A packaged XNAT session contains DICOM samples from multiple "
            f"StudyInstanceUIDs: {sorted(study_uids)}"
        )

    # Packaging is deliberately a scan-resource operation. Session resources do
    # not have an XNAT scan under which named derived outputs could be attached,
    # so they pass through unchanged.
    for resource in session.session_resources.values():
        packaged_resource = ImagingResource(resource.name, resource.fileset)
        packaged_resource.metadata.update(dict(resource.metadata))
        packaged.session_resources[resource.name] = packaged_resource

    return packaged


FileState = tuple[int, int, int]


def _refuse_member_loss(
    existing: ImagingSession, packaged: ImagingSession, shared: set[tuple[str, str]]
) -> None:
    """A zip about to replace a packaged one must hold every member of it.

    A session staged with only part of a series (a file drop that brings more
    images of a series already packaged) gives a zip with fewer members. Writing
    it over the packaged one would lose the images that are not in this input,
    and the upload stage cannot merge zips. The session fails instead, its
    input and output both stay, and the error says why."""
    for scan_id, label in sorted(shared):
        old = existing.scans[scan_id].resources[label].fileset
        new = packaged.scans[scan_id].resources[label].fileset
        if not (isinstance(old, Zip) and isinstance(new, Zip)):
            continue
        with zipfile.ZipFile(old.fspath) as z:
            old_members = set(z.namelist())
        with zipfile.ZipFile(new.fspath) as z:
            new_members = set(z.namelist())
        missing = old_members - new_members
        if missing:
            raise ValueError(
                f"The new {label!r} of scan {scan_id!r} lacks {len(missing)} "
                f"member(s) of the packaged copy, for example "
                f"{sorted(missing)[:3]}: this input holds only part of the "
                "series, and a packaged zip cannot be merged. Both copies are "
                "kept."
            )


def _file_states(session_dir: Path) -> dict[Path, FileState]:
    """The data files of a staged session, as paths inside it, each with its
    size, mtime and inode. Sidecars and hidden entries are not data."""
    sidecars = {
        ImagingResource.MANIFEST_FNAME,
        ImagingResource.OLD_MANIFEST_FNAME,
        Metadata.FNAME,
    }
    states: dict[Path, FileState] = {}
    for f in session_dir.rglob("*"):
        rel = f.relative_to(session_dir)
        if f.name in sidecars or any(part.startswith(".") for part in rel.parts):
            continue
        if not f.is_file():
            continue
        st = f.stat()
        states[rel] = (st.st_size, st.st_mtime_ns, st.st_ino)
    return states


def _session_files(session: ImagingSession, session_dir: Path) -> set[Path]:
    """The data files a loaded session refers to, as paths inside its directory."""
    return {
        Path(path).relative_to(session_dir)
        for resource in session.resources
        for path in resource.fileset.all_file_paths()
    }


def _input_drift(
    session_dir: Path, read_files: set[Path], read_states: dict[Path, FileState]
) -> tuple[set[Path], set[Path]]:
    """Files the loaded session did not carry, and files whose size, mtime or
    inode changed (or that went) since the session was read."""
    now = _file_states(session_dir)
    unknown = set(now) - read_files
    changed = {p for p, st in now.items() if read_states.get(p) != st}
    changed |= set(read_states) - set(now)
    return unknown, changed


def package(
    input_dir: Path,
    output_dir: Path,
    source_datatype: type[FileSet],
    output_resources: OutputResources,
    on_resource_clash: OnResourceClash = "error",
    raise_errors: bool = False,
    require_manifest: bool = True,
    copy_mode: FileSet.CopyMode = FileSet.CopyMode.hardlink_or_copy,
    unlink_source: str | None = None,
) -> list[str]:
    """Replace matching scan resources with multiple named converted resources.

    Conversions are materialised only after a staged session has been loaded, so
    this stage can be placed after deidentification. All output conversions are
    independent and receive the original source fileset.
    """
    if not issubclass(source_datatype, FileSet):
        raise TypeError("source_datatype must be a FileSet type")
    if not output_resources:
        raise ValueError("At least one output resource must be specified")
    for label, (datatype, _options) in output_resources.items():
        if not label:
            raise ValueError("Output resource labels cannot be empty")
        if not issubclass(datatype, FileSet):
            raise TypeError(f"Output datatype for {label!r} must be a FileSet type")

    listings = [LocalSessionListing(p) for p in list_session_dirs(input_dir)]
    output_dir.mkdir(parents=True, exist_ok=True)
    errors: list[str] = []

    for listing in tqdm(
        listings,
        desc=f"Packaging staged sessions found in '{input_dir}'",
    ):
        try:
            session = ImagingSession.load(
                listing.cache_path,
                require_manifest=require_manifest,
                check_checksums=True,
            )
            # What this cycle read: the state of every data file in the
            # directory, and the files the loaded session refers to. The unlink
            # at the end compares against both.
            read_states = _file_states(listing.cache_path)
            read_files = _session_files(session, listing.cache_path)
            if not any(
                isinstance(resource.fileset, source_datatype)
                for scan in session.scans.values()
                for resource in scan.resources.values()
            ):
                # Nothing to convert. The session still has to reach the
                # upload stage, which reads this stage's output: it passes
                # through as it is.
                logger.info(
                    "Passing '%s' through unchanged: it has no %s scan resources",
                    listing.name,
                    source_datatype.mime_like,
                )
                packaged = session
            else:
                packaged = _package_session(
                    session,
                    source_datatype=source_datatype,
                    output_resources=output_resources,
                    on_resource_clash=on_resource_clash,
                )

            final_dir = output_dir / packaged.staging_dirname()
            build_dir = output_dir / BUILD_NAME_DEFAULT
            work_dir = build_dir / f"package_{listing.name}"
            if work_dir.exists():
                shutil.rmtree(work_dir)
            try:
                if final_dir.exists():
                    existing = ImagingSession.load(
                        final_dir,
                        require_manifest=require_manifest,
                        check_checksums=True,
                    )
                    expected_paths = {
                        (scan.id, resource.name)
                        for scan in packaged.scans.values()
                        for resource in scan.resources.values()
                    }
                    existing_paths = {
                        (scan.id, resource.name)
                        for scan in existing.scans.values()
                        for resource in scan.resources.values()
                    }
                    # A scan the input does not carry was packaged by an
                    # earlier run (a series that arrived late is staged as a
                    # session of its own): it stays. A scan the input carries
                    # must have exactly the expected resources, or the layout
                    # changed and the old output must go first.
                    earlier_scans = {
                        scan_id
                        for scan_id, _ in existing_paths
                        if scan_id not in packaged.scans
                    }
                    if earlier_scans:
                        logger.info(
                            "Keeping the %d earlier packaged scan(s) %s of '%s' "
                            "beside the new ones",
                            len(earlier_scans),
                            sorted(earlier_scans),
                            listing.name,
                        )
                    stale_paths = {
                        p
                        for p in existing_paths - expected_paths
                        if p[0] not in earlier_scans
                    }
                    if stale_paths:
                        raise ValueError(
                            f"Existing packaged session {final_dir} contains "
                            f"obsolete scan resources {sorted(stale_paths)}; "
                            "write the new layout to a fresh output directory"
                        )
                    _refuse_member_loss(
                        existing, packaged, expected_paths & existing_paths
                    )
                    packaged.save(output_dir, copy_mode=copy_mode)
                else:
                    work_dir.mkdir(parents=True)
                    _, built_dir = packaged.save(work_dir, copy_mode=copy_mode)
                    os.replace(built_dir, final_dir)
            finally:
                shutil.rmtree(work_dir, ignore_errors=True)
                try:
                    build_dir.rmdir()
                except OSError:
                    pass
        except Exception as e:
            if raise_errors:
                raise
            msg = f"Error packaging session '{listing.name}': {e}"
            logger.error(msg)
            logger.debug(traceback.format_exc())
            errors.append(msg)
        else:
            unknown: set[Path] = set()
            changed: set[Path] = set()
            if unlink_source:
                unknown, changed = _input_drift(
                    listing.cache_path, read_files, read_states
                )
            if unknown or changed:
                # The directory holds data this run did not package: a file the
                # loaded session did not carry (the deidentify stage merges a
                # late series into a session it already wrote, and its manifest
                # can lag behind the files), or a file written again since it
                # was read. None of it is in the output yet: keep the input, the
                # next cycle packages it again.
                logger.warning(
                    "Not unlinking '%s': %d file(s) it did not carry when it was "
                    "loaded %s and %d file(s) changed since %s. It is packaged "
                    "again on a later cycle.",
                    listing.name,
                    len(unknown),
                    sorted(str(p) for p in unknown)[:5],
                    len(changed),
                    sorted(str(p) for p in changed)[:5],
                    extra={"event": "package_input_changed", "session": listing.name},
                )
            elif unlink_source == "all":
                shutil.rmtree(listing.cache_path)
            elif unlink_source == "keep-metadata":
                session.unlink(keep_metadata=True)

    if errors:
        logger.error("Packaging completed with %d errors", len(errors))
    else:
        logger.info("Packaging completed successfully")
    return errors
