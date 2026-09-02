"""Probe: is real DICOM de-identification reproducible across runs?

The in-place re-save path relies on ImagingResource.save's checksum
short-circuit, which compares the EXISTING output against FRESHLY
de-identified content. If the deid engine emits different bytes each run
(remapped UIDs, date handling), the checksums never match, every cycle
rewrites every file, and the settle window on both upload paths is reset
forever. Generic File.sample() takes the plain-copy path and cannot show this.
"""

import hashlib
from pathlib import Path

import pytest
from fileformats.medimage import DicomSeries
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (
    get_image as get_pet_image,
)

import xnat_ingest.specs as _specs_pkg
from xnat_ingest.api.deidentify_api import deidentify
from xnat_ingest.model.scan import ImagingScan
from xnat_ingest.model.session import ImagingSession

SHIPPED = Path(_specs_pkg.__path__[0])
NAME = "PROJ.SUBJ.SESS"


@pytest.fixture(scope="module")
def dicom_dir(tmp_path_factory):
    d = tmp_path_factory.mktemp("dicom")
    get_pet_image(out_dir=d)
    return d


def _state(d: Path):
    out = {}
    for p in sorted(d.rglob("*")):
        if p.is_file():
            out[str(p.relative_to(d))] = (
                p.stat().st_mtime_ns,
                hashlib.md5(p.read_bytes()).hexdigest(),
            )
    return out


def test_dicom_deid_is_reproducible_and_does_not_churn(dicom_dir: Path, tmp_path: Path):
    inp, out, reid = tmp_path / "in", tmp_path / "out", tmp_path / "reid"
    for d in (inp, out, reid):
        d.mkdir()

    series = DicomSeries(sorted(p for p in dicom_dir.iterdir() if p.is_file()))
    ImagingSession(
        uid=NAME,
        project_id="PROJ",
        subject_id="SUBJ",
        session_id="SESS",
        scans=[ImagingScan(id="1", type="PET", resources={"DICOM": series})],
    ).save(inp)

    assert (
        deidentify(input_dir=inp, output_dir=out, spec_dir=SHIPPED, reid_dir=reid) == []
    )
    first = _state(out / NAME)
    assert first, "no output produced"

    assert (
        deidentify(input_dir=inp, output_dir=out, spec_dir=SHIPPED, reid_dir=reid) == []
    )
    second = _state(out / NAME)

    content = sorted(k for k in first if k in second and first[k][1] != second[k][1])
    mtime = sorted(k for k in first if k in second and first[k][0] != second[k][0])
    print(f"\nDICOM files in output: {len(first)}")
    print(f"CONTENT differs across cycles: {len(content)}")
    for k in content[:5]:
        print(f"  NONDETERMINISTIC {k}")
    print(f"MTIME advanced on cycle 2: {len(mtime)}")
    for k in mtime[:5]:
        print(f"  CHURN {k}")
    assert not content, f"deid is NOT reproducible: {len(content)} file(s) differ"
    assert not mtime, f"{len(mtime)} file(s) rewritten on an unchanged session"
