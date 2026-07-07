from datetime import datetime

import pytest

from xnat_ingest.exceptions import ImagingSessionParseError
from xnat_ingest.helpers.arg_types import IDSpec


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
    assert value.startswith("INVALID_NOTFOUND_SERIESNUMBER_")
    assert missing["SeriesNumber"] == value


# ── new format-string mode ──


def test_compound_specifier_combines_fields() -> None:
    spec = IDSpec("{PatientID}_{AccessionNumber}")
    value = spec.get_value({"PatientID": "subj-01", "AccessionNumber": "42"})
    assert value == "subj_01_42"


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
    assert value.startswith("subj01_INVALID_NOTFOUND_ACCESSIONNUMBER_")
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
    assert value.startswith("subj01_INVALID_NOTFOUND_ACQUISITIONDATE_")


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
