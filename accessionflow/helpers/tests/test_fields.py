from pathlib import Path
from unittest.mock import patch

from fileformats.generic import File
from fileformats.testing import MyFormat

from accessionflow.api.fields_api import fields
from accessionflow.helpers.arg_types import IDSpec
from accessionflow.helpers.fields import Fields
from accessionflow.model.accession import Accession
from accessionflow.model.acquisition import Acquisition


def _staged_session(input_dir: Path) -> tuple[Accession, Path]:
    source_path = input_dir.parent / "source.my"
    source_path.write_text("deidentified source")
    report_path = input_dir.parent / "report.txt"
    report_path.write_text("session report")

    session = Accession(
        uid="uid",
        project_id="PROJECT",
        subject_id="SUBJECT",
        session_id="SESSION",
        scans=[
            Acquisition(
                id="1",
                type="CT",
                resources={"SOURCE": MyFormat(source_path)},
            )
        ],
    )
    session.metadata["session-field"] = "preserved"
    session.scans["1"].metadata["scan-field"] = "preserved"
    session.scans["1"].resources["SOURCE"].metadata["resource-field"] = "preserved"
    session.add_session_resource("REPORT", File(report_path))
    _, session_dir = session.save(input_dir)
    return session, session_dir


def test_fields_writes_fields_file(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    _staged_session(input_dir)

    errors = fields(
        input_dir=input_dir,
        output_dir=output_dir,
        extract={"preserved_field": IDSpec("session-field")},
    )

    assert errors == []
    session_dir = output_dir / "PROJECT.SUBJECT.SESSION"
    assert (session_dir / Fields.FNAME).exists()
    assert dict(Accession.load(session_dir).fields) == {"preserved_field": "preserved"}


def test_fields_preserves_resources_and_metadata(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    _session, source_dir = _staged_session(input_dir)

    errors = fields(
        input_dir=input_dir,
        output_dir=output_dir,
        extract={"preserved_field": IDSpec("session-field")},
    )

    assert errors == []
    assert source_dir.exists(), "field extraction removed its input without being asked"
    out = Accession.load(output_dir / "PROJECT.SUBJECT.SESSION")
    scan = out.scans["1"]
    assert scan.type == "CT"
    assert set(scan.resources) == {"SOURCE"}
    assert scan.resources["SOURCE"].fileset.fspath.read_text() == (
        "deidentified source"
    )
    assert out.metadata["session-field"] == "preserved"
    assert scan.metadata["scan-field"] == "preserved"
    assert scan.resources["SOURCE"].metadata["resource-field"] == "preserved"
    assert out.session_resources["REPORT"].fileset.fspath.read_text() == (
        "session report"
    )
    assert dict(Accession.load(source_dir).fields) == {}


def test_fields_preserves_existing_fields(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    session, _source_dir = _staged_session(input_dir)
    session.fields["existing_field"] = "from-upstream"
    session.save(input_dir)

    errors = fields(
        input_dir=input_dir,
        output_dir=output_dir,
        extract={"preserved_field": IDSpec("session-field")},
    )

    assert errors == []
    out = Accession.load(output_dir / "PROJECT.SUBJECT.SESSION")
    assert dict(out.fields) == {
        "existing_field": "from-upstream",
        "preserved_field": "preserved",
    }


def test_fields_skips_unresolvable_mapping(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    _staged_session(input_dir)

    errors = fields(
        input_dir=input_dir,
        output_dir=output_dir,
        extract={
            "preserved_field": IDSpec("session-field"),
            "missing_field": IDSpec("missing-key"),
        },
    )

    assert errors == []
    out = Accession.load(output_dir / "PROJECT.SUBJECT.SESSION")
    assert dict(out.fields) == {"preserved_field": "preserved"}


def test_fields_does_not_finalize_output_when_extraction_fails(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    _session, source_dir = _staged_session(input_dir)

    with patch.object(Fields, "extract", side_effect=RuntimeError("extraction failed")):
        errors = fields(
            input_dir=input_dir,
            output_dir=output_dir,
            extract={"preserved_field": IDSpec("session-field")},
        )

    assert len(errors) == 1
    assert "extraction failed" in errors[0]
    assert not (output_dir / "PROJECT.SUBJECT.SESSION").exists()
    assert source_dir.exists()
