import typing as ty
from pathlib import Path

import pytest

from xnat_ingest.helpers.arg_types import FieldMapping, IDSpec
from xnat_ingest.helpers.fields import Fields
from xnat_ingest.model.session import ImagingSession


def test_fields_extract() -> None:
    metadata = {
        "ManufacturerModelName": "Prisma fit",
        "MagneticFieldStrength": 3.0,
        "Modality": ["MR", "MR", "PT"],  # differs across scans: most common wins
        "StudyDate": "20240102",
    }
    fields = Fields({}, None)
    extracted = fields.extract(
        metadata,
        {
            "scanner_model": IDSpec("ManufacturerModelName"),
            "field_strength": IDSpec("MagneticFieldStrength"),
            "modality": IDSpec("Modality"),
            "date": IDSpec("{StudyDate:%Y-%m-%d}"),
            "missing": IDSpec("NotThere"),
            "bad_index": IDSpec("ManufacturerModelName[99]"),
        },
    )
    expected = {
        "scanner_model": "Prisma fit",  # not escaped to Prisma_fit
        "field_strength": "3.0",
        "modality": "MR",
        "date": "2024-01-02",
    }
    assert extracted == expected
    assert dict(fields) == expected


def test_fields_extract_keeps_existing_values() -> None:
    fields = Fields({"carried": "from-upstream"}, None)
    fields.extract({"A": "1"}, {"added": IDSpec("A")})
    assert dict(fields) == {"carried": "from-upstream", "added": "1"}


def test_fields_extract_warns_on_unresolvable_field(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # 'xnat-ingest' loggers may not propagate to caplog's root handler
    with caplog.at_level("WARNING", logger="xnat-ingest"):
        Fields({}, None).extract({}, {"absent": IDSpec("NoSuchKey")})
    assert "absent" in caplog.text


class _ExplodingOwner:
    """An owner whose ``load_metadata`` must never be called by ``Fields``"""

    def load_metadata(self) -> ty.NoReturn:
        raise AssertionError("Fields must not read from its owner")


def test_fields_do_not_read_from_owner(tmp_path: Path) -> None:
    # Metadata would lazily collate the owner's metadata here, which for a session
    # would copy every scan's metadata into __FIELDS__.json
    fields = Fields({"a": "1"}, _ExplodingOwner())
    assert dict(fields) == {"a": "1"}
    assert "missing" not in fields
    assert fields.get("missing") is None
    fields.save(tmp_path)
    assert Fields.load(tmp_path, _ExplodingOwner())["a"] == "1"


def test_fields_save_only_writes_on_change(tmp_path: Path) -> None:
    fields = Fields({"a": "1"}, None)
    fields.save(tmp_path)
    mtime = (tmp_path / Fields.FNAME).stat().st_mtime_ns
    fields.save(tmp_path)
    assert (tmp_path / Fields.FNAME).stat().st_mtime_ns == mtime
    fields["a"] = "2"
    fields.save(tmp_path)
    assert Fields.load(tmp_path, None)["a"] == "2"


def test_fields_fname_differs_from_metadata() -> None:
    from xnat_ingest.helpers.metadata import Metadata

    assert Fields.FNAME == "__FIELDS__.json"
    assert Fields.FNAME != Metadata.FNAME


def test_session_fields_round_trip(tmp_path: Path) -> None:
    session = ImagingSession(
        uid="1.2.3", project_id="PROJ", subject_id="SUBJ", session_id="SESS"
    )
    session.fields["scanner_model"] = "Prisma"
    saved, session_dir = session.save(tmp_path)
    assert (session_dir / Fields.FNAME).exists()
    assert dict(saved.fields) == {"scanner_model": "Prisma"}
    assert dict(ImagingSession.load(session_dir).fields) == {"scanner_model": "Prisma"}


def test_session_without_fields_writes_no_file(tmp_path: Path) -> None:
    session = ImagingSession(
        uid="1.2.3", project_id="PROJ", subject_id="SUBJ", session_id="SESS"
    )
    _, session_dir = session.save(tmp_path)
    assert not (session_dir / Fields.FNAME).exists()
    assert dict(ImagingSession.load(session_dir).fields) == {}


def test_session_fields_are_separate_from_metadata(tmp_path: Path) -> None:
    session = ImagingSession(
        uid="1.2.3", project_id="PROJ", subject_id="SUBJ", session_id="SESS"
    )
    session.metadata["Modality"] = "MR"
    session.fields["scanner_model"] = "Prisma"
    _, session_dir = session.save(tmp_path)
    reloaded = ImagingSession.load(session_dir)
    assert "scanner_model" not in reloaded.metadata
    assert "Modality" not in reloaded.fields


def test_field_mapping_to_dict() -> None:
    specs = FieldMapping.to_dict(
        [
            FieldMapping("scanner_model", "ManufacturerModelName"),
            FieldMapping("date", "{StudyDate:%Y-%m-%d}"),
        ]
    )
    assert all(isinstance(s, IDSpec) for s in specs.values())
    assert specs["date"].specifier == "{StudyDate:%Y-%m-%d}"


def test_field_mapping_rejects_duplicates() -> None:
    with pytest.raises(ValueError, match="Duplicate"):
        FieldMapping.to_dict([FieldMapping("a", "X"), FieldMapping("a", "Y")])


def test_field_mapping_envvar_split() -> None:
    cli_type = FieldMapping.cli_type
    entries = cli_type.split_envvar_value(
        "scanner_model ManufacturerModelName;date {StudyDate:%Y-%m-%d}"
    )
    assert entries == [
        "scanner_model",
        "ManufacturerModelName",
        "date",
        "{StudyDate:%Y-%m-%d}",
    ]
    assert [
        cli_type.convert(entries[:2], None, None),
        cli_type.convert(entries[2:], None, None),
    ] == [
        FieldMapping("scanner_model", "ManufacturerModelName"),
        FieldMapping("date", "{StudyDate:%Y-%m-%d}"),
    ]
