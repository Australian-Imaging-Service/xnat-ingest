"""Probe: does a second --loop cycle rewrite the output's data files?

If mtimes advance on an unchanged session, the settle window on both upload
paths can never be satisfied and the session is never uploaded.
"""

from pathlib import Path

from fileformats.generic import File

import xnat_ingest.specs as _specs_pkg
from xnat_ingest.api.deidentify_api import deidentify
from xnat_ingest.model.scan import ImagingScan
from xnat_ingest.model.session import ImagingSession

SHIPPED = Path(_specs_pkg.__path__[0])
NAME = "PROJ.SUBJ.SESS"


def _mtimes(d: Path):
    return {
        str(p.relative_to(d)): p.stat().st_mtime_ns
        for p in sorted(d.rglob("*"))
        if p.is_file()
    }


def test_second_cycle_does_not_touch_output_mtimes(tmp_path: Path):
    inp, out, reid = tmp_path / "in", tmp_path / "out", tmp_path / "reid"
    for d in (inp, out, reid):
        d.mkdir()
    ImagingSession(
        uid=NAME,
        project_id="PROJ",
        subject_id="SUBJ",
        session_id="SESS",
        scans=[ImagingScan(id="1", type="T", resources={"RES": File.sample(seed=1)})],
    ).save(inp)

    assert (
        deidentify(input_dir=inp, output_dir=out, spec_dir=SHIPPED, reid_dir=reid) == []
    )
    first = _mtimes(out / NAME)
    assert first, "no output produced"

    assert (
        deidentify(input_dir=inp, output_dir=out, spec_dir=SHIPPED, reid_dir=reid) == []
    )
    second = _mtimes(out / NAME)

    changed = sorted(k for k in first if k in second and first[k] != second[k])
    print(f"\nfiles in output: {len(first)}")
    print(f"files whose mtime ADVANCED on cycle 2: {len(changed)}")
    for k in changed:
        print(f"  CHURN {k}")
    assert (
        not changed
    ), f"{len(changed)} file(s) rewritten on an unchanged session: {changed}"
