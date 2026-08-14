from pathlib import Path
from unittest.mock import MagicMock

import pytest

from xnat_ingest.helpers.remotes import SessionOnlyListing, list_session_dirs
from xnat_ingest.helpers.xnat_scan_types import (
    SOP_CLASS_UIDS_BY_SCAN_TYPE,
    XNAT_SCAN_TYPE_PRECEDENCE,
    xnat_scan_type_from_sop_class,
)


def test_list_session_dirs_includes_no_dot_dirs(tmp_path: Path) -> None:
    (tmp_path / "PROJ.SUBJ.VISIT").mkdir()
    (tmp_path / "P000065").mkdir()
    (tmp_path / "__build__").mkdir()
    names = {d.name for d in list_session_dirs(tmp_path)}
    assert "PROJ.SUBJ.VISIT" in names
    assert "P000065" in names
    assert "__build__" not in names


def test_session_only_listing_resource_paths(tmp_path: Path) -> None:
    session_dir = tmp_path / "P000065"
    (session_dir / "my-report").mkdir(parents=True)
    (session_dir / "another-resource").mkdir()
    listing = SessionOnlyListing(session_dir)
    assert listing.resource_paths == {"my-report", "another-resource"}


def test_find_xnat_session_raises_on_multiple_matches(tmp_path: Path) -> None:
    listing = SessionOnlyListing(tmp_path / "P000065")
    connection = MagicMock()
    connection.experiments.values.return_value = [
        MagicMock(label="P000065"),
        MagicMock(label="P000065"),
    ]
    with pytest.raises(RuntimeError, match="Multiple XNAT sessions"):
        listing.find_xnat_session(connection)


@pytest.mark.parametrize(
    ("sop_class_uid", "expected"),
    [
        ("1.2.840.10008.5.1.4.1.1.4", "mrScanData"),
        ("1.2.840.10008.5.1.4.1.1.88.22", "srScanData"),
        ("1.2.840.10008.5.1.4.1.1.66", "otherDicomScanData"),
        ("1.2.840.10008.5.1.4.1.1.104.1", "otherDicomScanData"),
        ("1.2.840.10008.5.1.4.1.1.104.4", "objScanData"),
        (None, "otherDicomScanData"),
    ],
)
def test_xnat_scan_type_from_sop_class(
    sop_class_uid: str | None, expected: str
) -> None:
    assert xnat_scan_type_from_sop_class(sop_class_uid) == expected


def test_xnat_scan_type_from_sop_class_uses_xnat_precedence() -> None:
    assert (
        xnat_scan_type_from_sop_class(
            [
                "1.2.840.10008.5.1.4.1.1.88.22",
                "1.2.840.10008.5.1.4.1.1.4",
            ]
        )
        == "mrScanData"
    )


def test_all_xnat_sop_mappings_are_classified() -> None:
    for scan_type, sop_class_uids in SOP_CLASS_UIDS_BY_SCAN_TYPE.items():
        expected = (
            scan_type
            if scan_type in XNAT_SCAN_TYPE_PRECEDENCE
            else "otherDicomScanData"
        )
        for sop_class_uid in sop_class_uids:
            assert xnat_scan_type_from_sop_class(sop_class_uid) == expected
