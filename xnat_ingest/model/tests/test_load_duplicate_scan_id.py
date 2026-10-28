"""A staged session with two scan directories of one scan ID (the same
SeriesNumber with two descriptions) loaded as one scan: the second directory
replaced the first in the scans dict, and the next stage saved only the survivor
while unlinking the whole input. Loading such a session is an error now."""

import shutil
from pathlib import Path

import pytest
from fileformats.medimage import DicomSeries
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (
    get_image as get_pet_image,
)

from xnat_ingest.api.group_api import group
from xnat_ingest.exceptions import ImagingSessionParseError
from xnat_ingest.helpers.arg_types import IDSpec
from xnat_ingest.helpers.remotes import list_session_dirs
from xnat_ingest.model.session import ImagingSession


@pytest.fixture
def session_dir(tmp_path: Path) -> Path:
    dicom_dir = tmp_path / "dicom"
    get_pet_image(out_dir=dicom_dir)
    grouped_dir = tmp_path / "grouped"
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


def test_load_refuses_two_scan_directories_with_one_scan_id(session_dir: Path) -> None:
    scan_dir = next(d for d in session_dir.iterdir() if d.is_dir())
    scan_id = scan_dir.name.split(".")[0]
    twin = session_dir / f"{scan_id}.Other"
    shutil.copytree(scan_dir, twin)

    with pytest.raises(ImagingSessionParseError) as excinfo:
        ImagingSession.load(session_dir, require_manifest=True)

    assert f"have the scan ID '{scan_id}'" in excinfo.value.msg
    assert "'Other'" in excinfo.value.msg

    # Without the twin the session loads as before.
    shutil.rmtree(twin)
    session = ImagingSession.load(session_dir, require_manifest=True)
    assert list(session.scans) == [scan_id]
