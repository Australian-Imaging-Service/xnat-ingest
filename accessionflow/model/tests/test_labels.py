"""Accessions labelled with levels named by the data store, rather than fixed
project/subject/session IDs"""

import json
from pathlib import Path

import pytest
from fileformats.core import FileSet
from fileformats.generic import File

from accessionflow.exceptions import AccessionParseError
from accessionflow.helpers.metadata import Metadata
from accessionflow.model.accession import Accession
from accessionflow.model.acquisition import Acquisition
from accessionflow.model.resource import Resource


def _accession(tmp_path: Path, **metadata: str) -> Accession:
    seed = tmp_path / "data.txt"
    seed.write_text("data")
    accession = Accession(
        uid="1.2.3",
        scans=[Acquisition("1", "t1", resources={"RAW": Resource("RAW", File(seed))})],
    )
    for key, value in metadata.items():
        accession.metadata[key] = value
    return accession


@pytest.mark.parametrize(
    "label_fields,expected",
    [
        ({"site": "SiteID", "subject": "PatientID"}, ("SYD", "P01")),
        (
            {
                "site": "SiteID",
                "cohort": "Cohort",
                "subject": "PatientID",
                "visit": "Visit",
            },
            ("SYD", "A", "P01", "V2"),
        ),
    ],
)
def test_label_with_named_levels(
    tmp_path: Path, label_fields: dict[str, str], expected: tuple[str, ...]
) -> None:
    accession = _accession(
        tmp_path, SiteID="SYD", Cohort="A", PatientID="P01", Visit="V2"
    )
    assert accession.label(label_fields, store_type="tree") == {}
    assert list(accession.labels) == list(label_fields)
    assert tuple(accession.labels.values()) == expected
    assert accession.name == ".".join(expected)
    assert accession.staging_dirname() == ".".join(expected)

    _, saved_dir = accession.save(tmp_path / "out", copy_mode=FileSet.CopyMode.copy)
    assert saved_dir.name == ".".join(expected)
    saved = json.loads((saved_dir / Metadata.FNAME).read_text())
    assert saved[Accession.LABELS_METADATA_KEY] == dict(zip(label_fields, expected))
    assert saved[Accession.STORE_TYPE_METADATA_KEY] == "tree"

    reloaded = Accession.load(saved_dir)
    assert reloaded.labels == dict(zip(label_fields, expected))
    assert list(reloaded.labels) == list(label_fields)  # level order is kept
    assert reloaded.store_type == "tree"
    assert reloaded.run_uid is None


def test_run_uid_follows_any_number_of_levels(tmp_path: Path) -> None:
    accession = _accession(tmp_path, A="a", B="b", C="c", D="d")
    accession.label({"l1": "A", "l2": "B", "l3": "C", "l4": "D"})
    accession.run_uid = "abc123"
    assert accession.staging_dirname() == "a.b.c.d.abc123"
    _, saved_dir = accession.save(tmp_path / "out", copy_mode=FileSet.CopyMode.copy)
    reloaded = Accession.load(saved_dir)
    assert reloaded.labels == {"l1": "a", "l2": "b", "l3": "c", "l4": "d"}
    assert reloaded.run_uid == "abc123"


def test_legacy_staging_dir_is_read_as_xnat_levels(tmp_path: Path) -> None:
    """Accessions staged before the labels' level names were saved are XNAT's"""
    accession = _accession(tmp_path, StudyID="PROJ", PatientID="SUBJ", Acc="SESS")
    accession.label({"project": "StudyID", "subject": "PatientID", "session": "Acc"})
    _, saved_dir = accession.save(tmp_path / "out", copy_mode=FileSet.CopyMode.copy)
    metadata_fpath = saved_dir / Metadata.FNAME
    metadata = json.loads(metadata_fpath.read_text())
    del metadata[Accession.LABELS_METADATA_KEY]
    metadata_fpath.write_text(json.dumps(metadata))

    reloaded = Accession.load(saved_dir)
    assert reloaded.labels == {"project": "PROJ", "subject": "SUBJ", "session": "SESS"}
    assert reloaded.store_type is None


def test_staging_dir_with_wrong_number_of_labels_is_refused(tmp_path: Path) -> None:
    accession = _accession(tmp_path, A="a", B="b")
    accession.label({"l1": "A", "l2": "B"})
    _, saved_dir = accession.save(tmp_path / "out", copy_mode=FileSet.CopyMode.copy)
    renamed = saved_dir.rename(saved_dir.parent / "a.b.c.d")
    with pytest.raises(AccessionParseError, match="labels"):
        Accession.load(renamed)


def test_constant_labels(tmp_path: Path) -> None:
    accession = _accession(tmp_path, PatientID="P01")
    accession.label(
        {"site": "SiteID", "subject": "PatientID"}, constant_labels={"site": "SYD"}
    )
    assert accession.labels == {"site": "SYD", "subject": "P01"}
    with pytest.raises(ValueError, match="not label levels"):
        accession.label({"subject": "PatientID"}, constant_labels={"site": "SYD"})


def test_unresolved_label_is_invalid(tmp_path: Path) -> None:
    accession = _accession(tmp_path, PatientID="P01")
    missing = accession.label({"site": "SiteID", "subject": "PatientID"})
    assert set(missing) == {"SiteID"}
    assert accession.labels["site"].startswith("INVALID_MISSING_")
    assert accession.invalid_ids
