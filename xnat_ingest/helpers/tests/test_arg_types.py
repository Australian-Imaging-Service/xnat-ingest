from datetime import datetime
from pathlib import Path

import click
import pytest
from click.testing import CliRunner
from fileformats.core import FileSet
from fileformats.image.raster import Jpeg, Png
from fileformats.text import Csv, Plain, Tsv

from xnat_ingest.exceptions import ImagingSessionParseError
from xnat_ingest.helpers.arg_types import (
    IDSpec,
    JoinExpr,
    MetadataTable,
    parse_join_exprs,
    row_frequency_converter,
    table_file_converter,
)
from xnat_ingest.model.resource import ImagingResource
from xnat_ingest.model.scan import ImagingScan
from xnat_ingest.model.session import ImagingSession

# ── existing plain-field / slice syntax, unaffected by the new format-string mode ──


def test_plain_field() -> None:
    assert IDSpec("SeriesNumber").get_value({"SeriesNumber": "7"}) == "7"


def test_open_ended_slice() -> None:
    value = IDSpec("ImageType[2:]").get_value(
        {"ImageType": ["ORIGINAL", "PRIMARY", "FOO", "BAR"]}
    )
    assert value == "FOO_BAR"


def test_negative_index() -> None:
    assert IDSpec("ImageType[-1]").get_value({"ImageType": ["A", "B", "C"]}) == "C"


def test_plain_field_missing_raises_without_missing_ids() -> None:
    with pytest.raises(ImagingSessionParseError):
        IDSpec("SeriesNumber").get_value({})


def test_plain_field_missing_uses_placeholder() -> None:
    missing: dict[str, str] = {}
    value = IDSpec("SeriesNumber").get_value({}, missing_ids=missing)
    assert value.startswith("INVALID_MISSING_SERIESNUMBER_")
    assert missing["SeriesNumber"] == value


# ── new format-string mode ──


def test_compound_specifier_combines_fields() -> None:
    spec = IDSpec("{PatientID}_{AccessionNumber}")
    value = spec.get_value({"PatientID": "subj-01", "AccessionNumber": "42"})
    # '-' is kept (permitted in XNAT IDs); other punctuation would collapse to '_'
    assert value == "subj-01_42"


def test_date_format_spec_against_live_datetime() -> None:
    spec = IDSpec("{PatientID}_{AcquisitionDate:%Y%m%d}")
    value = spec.get_value(
        {"PatientID": "subj01", "AcquisitionDate": datetime(2026, 3, 4)}
    )
    assert value == "subj01_20260304"


def test_date_format_spec_against_json_roundtripped_string() -> None:
    """Once metadata has round-tripped through JSON (e.g. reloaded by 'assign' from
    the '__METADATA__.json' 'group' wrote), a date field is a plain string rather
    than a real datetime - the dateutil fallback should still make %-style formatting
    work in that case"""
    spec = IDSpec("{PatientID}_{AcquisitionDate:%Y%m%d}")
    value = spec.get_value({"PatientID": "subj01", "AcquisitionDate": "20260304"})
    assert value == "subj01_20260304"


def test_non_date_string_with_percent_spec_raises() -> None:
    """A field that genuinely isn't date-like shouldn't silently produce garbage"""
    spec = IDSpec("{SeriesDescription:%Y%m%d}")
    with pytest.raises(ValueError):
        spec.get_value({"SeriesDescription": "AC CT 3.0 SWB HD_FoV"})


def test_missing_field_in_compound_specifier_uses_placeholder() -> None:
    missing: dict[str, str] = {}
    spec = IDSpec("{PatientID}_{AccessionNumber}")
    value = spec.get_value({"PatientID": "subj01"}, missing_ids=missing)
    assert value.startswith("subj01_INVALID_MISSING_ACCESSIONNUMBER_")
    assert missing["AccessionNumber"] in value


def test_missing_field_in_compound_specifier_raises_without_missing_ids() -> None:
    spec = IDSpec("{PatientID}_{AccessionNumber}")
    with pytest.raises(ImagingSessionParseError):
        spec.get_value({"PatientID": "subj01"})


def test_missing_date_field_with_percent_spec_uses_placeholder() -> None:
    """A missing field with a strftime-style spec shouldn't itself crash - the
    placeholder needs to tolerate being substituted into a '%...' format spec"""
    missing: dict[str, str] = {}
    spec = IDSpec("{PatientID}_{AcquisitionDate:%Y%m%d}")
    value = spec.get_value({"PatientID": "subj01"}, missing_ids=missing)
    assert value.startswith("subj01_INVALID_MISSING_ACQUISITIONDATE_")


def test_unreferenced_non_identifier_key_is_harmless() -> None:
    """A metadata dict containing a key that isn't a valid identifier (e.g. DICOM's
    all-digit fallback name for a private/unnamed tag) shouldn't break a compound
    specifier that doesn't reference it"""
    spec = IDSpec("{PatientID}")
    value = spec.get_value({"PatientID": "subj01", "00100010": "private tag value"})
    assert value == "subj01"


def test_directly_referencing_all_digit_key_raises_clearly() -> None:
    """Python's format-string syntax always treats an all-digit field name as a
    positional index rather than a keyword lookup, so this can't be supported - but
    it should fail with a clear, catchable error rather than a raw IndexError"""
    spec = IDSpec("{00100010}")
    with pytest.raises(ImagingSessionParseError):
        spec.get_value({"00100010": "private tag value"}, missing_ids={})


# ══════════════════════════════════════════════════════════════════════════════
# --metadata-table feature
# ══════════════════════════════════════════════════════════════════════════════


@pytest.fixture
def csv_table(tmp_path: Path) -> Csv:
    path = tmp_path / "meta.csv"
    path.write_text(
        "PatientID,Session,Age,Comment\n"
        "subj01,MR01,42,first\n"
        "subj01,MR02,42,second\n"
        "subj02,MR01,55,third\n"
    )
    return Csv(path)


@pytest.fixture
def tsv_table(tmp_path: Path) -> Tsv:
    path = tmp_path / "meta.tsv"
    path.write_text("PatientID\tComment\nsubj01\thello\nsubj02\tworld\n")
    return Tsv(path)


def _session(**metadata: object) -> ImagingSession:
    session = ImagingSession(uid="sess-uid")
    session.metadata.update(metadata)
    return session


def _scan(**metadata: object) -> ImagingScan:
    scan = ImagingScan(id="1", type="t1")
    scan.metadata.update(metadata)
    return scan


def _resource(tmp_path: Path, **metadata: object) -> ImagingResource:
    fspath = tmp_path / "datafile.txt"
    fspath.write_text("some data")
    resource = ImagingResource(name="DICOM", fileset=Plain(fspath))
    resource.metadata.update(metadata)
    return resource


# ── parse_join_exprs ────────────────────────────────────────────────────────


def test_parse_join_exprs_single_string() -> None:
    parsed = parse_join_exprs("PatientID=DicomPatientID")
    assert parsed == [JoinExpr("PatientID", "DicomPatientID")]


def test_parse_join_exprs_comma_separated() -> None:
    parsed = parse_join_exprs("A=x,B=y")
    assert parsed == [JoinExpr("A", "x"), JoinExpr("B", "y")]


def test_parse_join_exprs_strips_whitespace() -> None:
    assert parse_join_exprs(" A = x ") == [JoinExpr("A", "x")]


def test_parse_join_exprs_escaped_comma_kept_in_value() -> None:
    """A '\\,' is an escaped literal comma, not a separator between expressions"""
    parsed = parse_join_exprs(r"A=x\,y,B=z")
    assert parsed == [JoinExpr("A", "x,y"), JoinExpr("B", "z")]


def test_parse_join_exprs_value_may_hold_equals_sign() -> None:
    assert parse_join_exprs("A=a=b") == [JoinExpr("A", "a=b")]


def test_parse_join_exprs_accepts_list_of_strings() -> None:
    assert parse_join_exprs(["A=x", "B=y"]) == [
        JoinExpr("A", "x"),
        JoinExpr("B", "y"),
    ]


def test_parse_join_exprs_passes_through_joinexpr_instances() -> None:
    expr = JoinExpr("A", "x")
    assert parse_join_exprs([expr]) == [expr]


def test_parse_join_exprs_invalid_without_equals_raises() -> None:
    with pytest.raises(ValueError, match="Invalid join expression"):
        parse_join_exprs("no-equals-sign")


# ── JoinExpr.value ─────────────────────────────────────────────────────────


def test_joinexpr_value_plain_field_lookup() -> None:
    assert JoinExpr("Col", "PatientID").value({"PatientID": "subj01"}) == "subj01"


def test_joinexpr_value_coerces_non_string_to_str() -> None:
    assert JoinExpr("Col", "Age").value({"Age": 42}) == "42"


def test_joinexpr_value_template_combines_fields() -> None:
    value = JoinExpr("Col", "{PatientID}_{Session}").value(
        {"PatientID": "subj01", "Session": "MR02"}
    )
    assert value == "subj01_MR02"


def test_joinexpr_value_missing_plain_field_raises_keyerror() -> None:
    with pytest.raises(KeyError):
        JoinExpr("Col", "PatientID").value({})


def test_joinexpr_value_missing_template_field_raises_keyerror() -> None:
    with pytest.raises(KeyError):
        JoinExpr("Col", "{PatientID}").value({})


# ── table_file_converter ───────────────────────────────────────────────────


def test_table_file_converter_passes_through_fileset(csv_table: Csv) -> None:
    assert table_file_converter(csv_table) is csv_table


def test_table_file_converter_detects_csv(csv_table: Csv) -> None:
    converted = table_file_converter(str(csv_table.fspath))
    assert isinstance(converted, Csv)


def test_table_file_converter_detects_tsv(tsv_table: Tsv) -> None:
    assert isinstance(table_file_converter(str(tsv_table.fspath)), Tsv)


def test_table_file_converter_accepts_path_object(csv_table: Csv) -> None:
    assert isinstance(table_file_converter(csv_table.fspath), Csv)


def test_table_file_converter_explicit_mime_suffix(tmp_path: Path) -> None:
    """A trailing '[<mime-type>]' names the format explicitly (as in the CLI docs,
    'path/to/file.csv[text/csv]')"""
    path = tmp_path / "table.csv"
    path.write_text("A,B\n1,2\n")
    converted = table_file_converter(f"{path}[text/csv]")
    assert isinstance(converted, Csv)
    assert converted.fspath == path


def test_table_file_converter_explicit_mime_suffix_tolerates_whitespace(
    tmp_path: Path,
) -> None:
    path = tmp_path / "table.tsv"
    path.write_text("A\tB\n1\t2\n")
    converted = table_file_converter(f"  {path}[text/tab-separated-values]  ")
    assert isinstance(converted, Tsv)


def test_table_file_converter_rejects_other_types() -> None:
    with pytest.raises(TypeError):
        table_file_converter(123)  # type: ignore[arg-type]


# ── row_frequency_converter ────────────────────────────────────────────────


@pytest.mark.parametrize("freq", ["session", "scan", "resource"])
def test_row_frequency_converter_hierarchy_levels(freq: str) -> None:
    assert row_frequency_converter(freq) == freq


def test_row_frequency_converter_plain_fileset_becomes_fileset_class() -> None:
    assert row_frequency_converter("fileset") is FileSet


def test_row_frequency_converter_is_case_insensitive() -> None:
    assert row_frequency_converter("SeSsIoN") == "session"
    assert row_frequency_converter("FileSet") is FileSet


def test_row_frequency_converter_single_mime() -> None:
    assert row_frequency_converter("fileset[image/png]") is Png


def test_row_frequency_converter_union_mime_pipe_separated() -> None:
    # a '|'-union of FileSet types, usable directly with isinstance()
    assert row_frequency_converter("fileset[image/png|image/jpeg]") == Png | Jpeg


def test_row_frequency_converter_invalid_level_raises() -> None:
    with pytest.raises(ValueError, match="Invalid frequency"):
        row_frequency_converter("subject")


def test_row_frequency_converter_unrecognised_mime_raises_valueerror() -> None:
    with pytest.raises(ValueError, match="Could not recognise mime type"):
        row_frequency_converter("fileset[not/a-real-mime]")


def test_row_frequency_converter_accepts_iterable_of_types() -> None:
    assert row_frequency_converter([Png, Jpeg]) == Png | Jpeg


def test_row_frequency_converter_accepts_iterable_of_mime_strings() -> None:
    assert row_frequency_converter(["image/png", "image/jpeg"]) == Png | Jpeg


def test_row_frequency_converter_single_element_iterable_returns_bare_type() -> None:
    assert row_frequency_converter([Png]) is Png


def test_row_frequency_converter_reconverts_resolved_values() -> None:
    """attrs re-runs the converter on an already-converted value"""
    assert row_frequency_converter(Png) is Png
    assert row_frequency_converter(Png | Jpeg) == Png | Jpeg


def test_row_frequency_converter_iterable_with_bad_entry_raises() -> None:
    with pytest.raises(TypeError):
        row_frequency_converter([Png, 42])  # type: ignore[list-item]


# ── MetadataTable construction ─────────────────────────────────────────────


def test_metadata_table_built_from_cli_style_strings(csv_table: Csv) -> None:
    table = MetadataTable(str(csv_table.fspath), "session", "PatientID=PatientID")
    assert isinstance(table.table_file, Csv)
    assert table.row_frequency == "session"
    assert table.join_exprs == [JoinExpr("PatientID", "PatientID")]


def test_metadata_table_requires_at_least_one_join_expr(csv_table: Csv) -> None:
    with pytest.raises(ValueError, match="At least one join expression"):
        MetadataTable(csv_table, "session", [])


def test_metadata_table_equality_ignores_loaded_table_cache(csv_table: Csv) -> None:
    a = MetadataTable(csv_table, "session", "PatientID=PatientID")
    b = MetadataTable(csv_table, "session", "PatientID=PatientID")
    _ = a.table  # populate the lazy cache on one of them only
    assert a == b


# ── MetadataTable.table / _load_table ──────────────────────────────────────


def test_table_loads_csv_column_oriented(csv_table: Csv) -> None:
    table = MetadataTable(csv_table, "session", "PatientID=PatientID")
    assert dict(table.table) == {
        "PatientID": ["subj01", "subj01", "subj02"],
        "Session": ["MR01", "MR02", "MR01"],
        "Age": ["42", "42", "55"],
        "Comment": ["first", "second", "third"],
    }


def test_table_loads_tsv(tsv_table: Tsv) -> None:
    table = MetadataTable(tsv_table, "session", "PatientID=PatientID")
    assert dict(table.table) == {
        "PatientID": ["subj01", "subj02"],
        "Comment": ["hello", "world"],
    }


def test_table_handles_utf8_bom(tmp_path: Path) -> None:
    path = tmp_path / "bom.csv"
    path.write_bytes("PatientID,Comment\nsubj01,x\n".encode("utf-8-sig"))
    table = MetadataTable(Csv(path), "session", "PatientID=PatientID")
    assert list(table.table) == ["PatientID", "Comment"]


def test_table_is_cached_after_first_access(csv_table: Csv) -> None:
    table = MetadataTable(csv_table, "session", "PatientID=PatientID")
    assert table.table is table.table


# ── MetadataTable.inject ───────────────────────────────────────────────────


def test_inject_session_injects_whole_matched_row(csv_table: Csv) -> None:
    table = MetadataTable(
        csv_table, "session", ["PatientID=PatientID", "Session=Session"]
    )
    session = _session(PatientID="subj01", Session="MR02")
    table.inject(session)
    # every column of the matched row is injected, not just the join columns
    assert session.metadata["Age"] == "42"
    assert session.metadata["Comment"] == "second"


def test_inject_no_matching_row_is_a_noop(csv_table: Csv) -> None:
    table = MetadataTable(csv_table, "session", "PatientID=PatientID")
    session = _session(PatientID="nobody")
    table.inject(session)
    assert "Comment" not in session.metadata


def test_inject_multiple_matching_rows_raises(csv_table: Csv) -> None:
    table = MetadataTable(csv_table, "session", "PatientID=PatientID")
    session = _session(PatientID="subj01")  # matches two rows
    with pytest.raises(ValueError, match="Multiple rows"):
        table.inject(session)


def test_inject_missing_join_field_on_target_is_a_noop(csv_table: Csv) -> None:
    table = MetadataTable(csv_table, "session", "PatientID=PatientID")
    session = _session(SomethingElse="x")
    table.inject(session)
    assert "Comment" not in session.metadata


def test_inject_unknown_join_column_raises(csv_table: Csv) -> None:
    table = MetadataTable(csv_table, "session", "NoSuchColumn=PatientID")
    session = _session(PatientID="subj01")
    with pytest.raises(ValueError, match="not found in metadata table"):
        table.inject(session)


def test_inject_wrong_frequency_target_is_a_noop(
    csv_table: Csv, tmp_path: Path
) -> None:
    table = MetadataTable(csv_table, "session", "PatientID=PatientID")
    scan = _scan(PatientID="subj02", Session="MR01")
    table.inject(scan)
    assert "Comment" not in scan.metadata


def test_inject_scan_frequency(csv_table: Csv) -> None:
    table = MetadataTable(csv_table, "scan", ["PatientID=PatientID", "Session=Session"])
    scan = _scan(PatientID="subj02", Session="MR01")
    table.inject(scan)
    assert scan.metadata["Comment"] == "third"


def test_inject_resource_frequency(csv_table: Csv, tmp_path: Path) -> None:
    table = MetadataTable(
        csv_table, "resource", ["PatientID=PatientID", "Session=Session"]
    )
    resource = _resource(tmp_path, PatientID="subj01", Session="MR01")
    table.inject(resource)
    assert resource.metadata["Comment"] == "first"


def test_inject_fileset_mime_frequency_against_raw_fileset(tmp_path: Path) -> None:
    csv_path = tmp_path / "files.csv"
    csv_path.write_text("Name,Note\nhello.txt,greeting\nother.txt,ignored\n")
    data = tmp_path / "hello.txt"
    data.write_text("hi")
    fileset = Plain(data)
    fileset.metadata["Name"] = "hello.txt"

    table = MetadataTable(Csv(csv_path), "fileset[text/plain]", "Name=Name")
    assert table.row_frequency is Plain
    table.inject(fileset)
    assert fileset.metadata["Note"] == "greeting"


def test_inject_fileset_union_mime_frequency_matches_any_member(tmp_path: Path) -> None:
    lookup = tmp_path / "lookup.csv"
    lookup.write_text("Name,Note\ndata.tsv,matched\n")
    data = tmp_path / "data.tsv"
    data.write_text("a\tb\n1\t2\n")
    fileset = Tsv(data)
    fileset.metadata["Name"] = "data.tsv"

    table = MetadataTable(
        Csv(lookup), "fileset[text/csv|text/tab-separated-values]", "Name=Name"
    )
    assert table.row_frequency == Csv | Tsv
    table.inject(fileset)
    assert fileset.metadata["Note"] == "matched"

    # a fileset that is neither member is left untouched
    other = tmp_path / "note.txt"
    other.write_text("hi")
    other_fs = Plain(other)
    other_fs.metadata["Name"] = "note.txt"
    table.inject(other_fs)
    assert "Note" not in other_fs.metadata


def test_inject_plain_fileset_frequency_matches_any_fileset(tmp_path: Path) -> None:
    csv_path = tmp_path / "files.csv"
    csv_path.write_text("Name,Note\nhello.txt,greeting\n")
    data = tmp_path / "hello.txt"
    data.write_text("hi")
    fileset = Plain(data)
    fileset.metadata["Name"] = "hello.txt"

    table = MetadataTable(Csv(csv_path), "fileset", "Name=Name")
    assert table.row_frequency is FileSet
    table.inject(fileset)
    assert fileset.metadata["Note"] == "greeting"


def test_inject_and_combines_multiple_join_exprs(csv_table: Csv) -> None:
    table = MetadataTable(
        csv_table, "session", ["PatientID=PatientID", "Session=Session"]
    )
    session = _session(PatientID="subj01", Session="MR01")
    table.inject(session)
    assert session.metadata["Comment"] == "first"


def test_inject_template_join_expr(csv_table: Csv, tmp_path: Path) -> None:
    lookup = tmp_path / "lookup.csv"
    lookup.write_text("Key,Label\nsubj01/MR02,the-label\n")
    table = MetadataTable(Csv(lookup), "session", "Key={PatientID}/{Session}")
    session = _session(PatientID="subj01", Session="MR02")
    table.inject(session)
    assert session.metadata["Label"] == "the-label"


# ── MetadataTable.inject_list ──────────────────────────────────────────────


def test_inject_list_none_is_a_noop() -> None:
    MetadataTable.inject_list(None, [_session(PatientID="subj01")])


def test_inject_list_empty_is_a_noop() -> None:
    MetadataTable.inject_list([], [_session(PatientID="subj01")])


def test_inject_list_applies_every_table_to_every_target(
    csv_table: Csv, tmp_path: Path
) -> None:
    weights = tmp_path / "weights.csv"
    weights.write_text("PatientID,Weight\nsubj01,70\nsubj02,85\n")
    table_a = MetadataTable(
        csv_table, "session", ["PatientID=PatientID", "Session=Session"]
    )
    table_b = MetadataTable(Csv(weights), "session", "PatientID=PatientID")
    sessions = [
        _session(PatientID="subj01", Session="MR01"),
        _session(PatientID="subj02", Session="MR01"),
    ]
    MetadataTable.inject_list([table_a, table_b], sessions)
    # table_a contributes Comment, table_b contributes Weight, to each session
    assert (sessions[0].metadata["Comment"], sessions[0].metadata["Weight"]) == (
        "first",
        "70",
    )
    assert (sessions[1].metadata["Comment"], sessions[1].metadata["Weight"]) == (
        "third",
        "85",
    )


# ── CLI wiring (click param type + env var) ────────────────────────────────


def _metadata_table_cli() -> click.Command:
    @click.command()
    @click.option(
        "--metadata-table",
        "metadata_tables",
        type=MetadataTable.cli_type,
        multiple=True,
        nargs=3,
        default=(),
        envvar="XINGEST_METADATA_TABLES",
    )
    def cmd(metadata_tables: tuple[MetadataTable, ...]) -> None:
        for mt in metadata_tables:
            click.echo(
                f"{type(mt.table_file).__name__}|{mt.row_frequency}|"
                + ";".join(f"{e.column_name}={e.value_expr}" for e in mt.join_exprs)
            )

    return cmd


def test_metadata_table_cli_option(csv_table: Csv) -> None:
    result = CliRunner().invoke(
        _metadata_table_cli(),
        [
            "--metadata-table",
            str(csv_table.fspath),
            "session",
            "PatientID=PatientID",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert result.output.strip() == "Csv|session|PatientID=PatientID"


def test_metadata_table_cli_option_repeatable(csv_table: Csv, tsv_table: Tsv) -> None:
    result = CliRunner().invoke(
        _metadata_table_cli(),
        [
            "--metadata-table",
            str(csv_table.fspath),
            "scan",
            "A=B",
            "--metadata-table",
            str(tsv_table.fspath),
            "resource",
            "C=D",
        ],
        catch_exceptions=False,
    )
    assert result.output.splitlines() == [
        "Csv|scan|A=B",
        "Tsv|resource|C=D",
    ]


def test_metadata_table_env_var_single(
    monkeypatch: pytest.MonkeyPatch, csv_table: Csv
) -> None:
    monkeypatch.setenv(
        "XINGEST_METADATA_TABLES",
        f"{csv_table.fspath} session PatientID=PatientID",
    )
    result = CliRunner().invoke(_metadata_table_cli(), [], catch_exceptions=False)
    assert result.output.strip() == "Csv|session|PatientID=PatientID"


def test_metadata_table_env_var_multiple_semicolon_separated(
    monkeypatch: pytest.MonkeyPatch, csv_table: Csv, tsv_table: Tsv
) -> None:
    monkeypatch.setenv(
        "XINGEST_METADATA_TABLES",
        f"{csv_table.fspath} scan A=B;{tsv_table.fspath} resource C=D",
    )
    result = CliRunner().invoke(_metadata_table_cli(), [], catch_exceptions=False)
    assert result.output.splitlines() == ["Csv|scan|A=B", "Tsv|resource|C=D"]
