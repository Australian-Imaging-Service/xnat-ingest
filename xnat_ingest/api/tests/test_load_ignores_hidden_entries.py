"""An uploader that is killed inside a batch leaves its batch folder next to the
resource: ".<resource>-upload-<random>". Loading the session read that folder as
a resource, raised "Manifest file not found", and the uploader skipped the whole
session on every pass. The session is not lost, but it never reaches XNAT until
an operator removes the folder.

The loaders now ignore hidden entries. The folder stays: only a run that owns a
batch may remove it, and a run cannot tell an orphan from a batch of another
live worker (resources split by modality share one folder)."""

from pathlib import Path

import pytest
from fileformats.medimage import DicomSeries
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (
    get_image as get_pet_image,
)

from xnat_ingest.api.group_api import group
from xnat_ingest.helpers.arg_types import IDSpec
from xnat_ingest.helpers.remotes import list_session_dirs
from xnat_ingest.model.session import ImagingSession


@pytest.fixture(scope="module")
def session_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A grouped session on disk, with manifests, as the pipeline writes it."""
    dicom_dir = tmp_path_factory.mktemp("dicom")
    get_pet_image(out_dir=dicom_dir)
    grouped_dir = tmp_path_factory.mktemp("grouped")
    errors = group(
        input_paths=[str(dicom_dir)],
        output_dir=grouped_dir,
        datatypes=[DicomSeries],
        session=[IDSpec("StudyInstanceUID", "medimage/dicom-collection")],
        scan=[IDSpec("SeriesNumber", "medimage/dicom-collection")],
        resource=[IDSpec("ImageType[2:]", "medimage/dicom-collection")],
    )
    assert errors == []
    return next(iter(list_session_dirs(grouped_dir)))


def _resource_dir(session_dir: Path) -> Path:
    scan_dir = next(d for d in session_dir.iterdir() if d.is_dir())
    return next(d for d in scan_dir.iterdir() if d.is_dir())


def test_load_ignores_a_batch_folder_left_in_a_scan(session_dir: Path) -> None:
    before = ImagingSession.load(session_dir, require_manifest=True)
    resource_dir = _resource_dir(session_dir)
    orphan = resource_dir.parent / f".{resource_dir.name}-upload-orphan"
    orphan.mkdir()
    for fspath in resource_dir.iterdir():
        (orphan / fspath.name).hardlink_to(fspath)

    session = ImagingSession.load(session_dir, require_manifest=True)

    scan = next(iter(session.scans.values()))
    assert set(scan.resources) == {resource_dir.name}
    assert len(session.scans) == len(before.scans)
    assert orphan.is_dir(), "the orphan is kept for an operator"


def test_load_ignores_a_batch_folder_left_in_a_session(session_dir: Path) -> None:
    before = ImagingSession.load(session_dir, require_manifest=True)
    orphan = session_dir / ".notes-upload-orphan"
    orphan.mkdir()
    (orphan / "notes.txt").write_text("from a killed uploader")

    session = ImagingSession.load(session_dir, require_manifest=True)

    assert len(session.scans) == len(before.scans)
    assert session.session_resources == {}
    assert orphan.is_dir()
