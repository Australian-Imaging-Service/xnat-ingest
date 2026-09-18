from pathlib import Path
from unittest.mock import MagicMock

import pytest

from xnat_ingest.helpers.remotes import SessionOnlyListing, list_session_dirs
from xnat_ingest.helpers.xnat_scan_types import (
    PRIMARY_SOP_CLASS_UIDS,
    SCAN_TYPE_BY_SOP_CLASS_UID,
    SOP_CLASS_UIDS_BY_SCAN_TYPE,
    XNAT_SCAN_TYPE_PRECEDENCE,
    xnat_resource_label_from_sop_class,
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


# The SOP classes XNAT considers primary, and therefore catalogues under "DICOM",
# with the dose-report classes that motivated this alongside them
@pytest.mark.parametrize(
    "sop_class_uid,expected,description",
    [
        ("1.2.840.10008.5.1.4.1.1.88.11", "DICOM", "Basic Text SR"),
        ("1.2.840.10008.5.1.4.1.1.88.22", "DICOM", "Enhanced SR"),
        ("1.2.840.10008.5.1.4.1.1.88.33", "secondary", "Comprehensive SR"),
        ("1.2.840.10008.5.1.4.1.1.88.67", "secondary", "X-Ray Radiation Dose SR"),
        ("1.2.840.10008.5.1.4.1.1.88.68", "secondary", "Radiopharmaceutical Dose SR"),
        ("1.2.840.10008.5.1.4.1.1.128", "DICOM", "PET image"),
        ("1.2.840.10008.5.1.4.1.1.4", "DICOM", "MR image"),
        ("1.2.840.10008.5.1.4.1.1.7", "secondary", "Secondary capture"),
    ],
)
def test_xnat_resource_label_matches_xnats_primary_sop_list(
    sop_class_uid: str, expected: str, description: str
) -> None:
    """The resource a scan's DICOM is catalogued under comes from the SOP class alone,
    as XNAT's CatalogBuilder does via primary-sops.txt"""
    assert xnat_resource_label_from_sop_class(sop_class_uid) == expected, description


def test_xnat_resource_label_ignores_image_type() -> None:
    """A DERIVED/SECONDARY image whose SOP class is primary is still catalogued under
    'DICOM' -- ImageType is not an input to XNAT's decision, which is what made
    pre-creating a 'secondary' resource for dose reports go wrong"""
    enhanced_sr = "1.2.840.10008.5.1.4.1.1.88.22"
    assert xnat_resource_label_from_sop_class(enhanced_sr) == "DICOM"


def test_xnat_resource_label_for_mixed_scan() -> None:
    """XNAT catalogues per file, so a scan holding any primary SOP class gets a DICOM
    catalog"""
    assert (
        xnat_resource_label_from_sop_class(
            ["1.2.840.10008.5.1.4.1.1.88.67", "1.2.840.10008.5.1.4.1.1.128"]
        )
        == "DICOM"
    )
    assert (
        xnat_resource_label_from_sop_class(
            ["1.2.840.10008.5.1.4.1.1.88.67", "1.2.840.10008.5.1.4.1.1.88.33"]
        )
        == "secondary"
    )


def test_xnat_resource_label_without_sop_class() -> None:
    assert xnat_resource_label_from_sop_class(None) == "secondary"
    assert xnat_resource_label_from_sop_class([]) == "secondary"


def test_xnat_resource_label_when_site_doesnt_separate_secondary() -> None:
    """With 'separateSecondaryDicomOnArchive' off, everything is catalogued as DICOM"""
    assert (
        xnat_resource_label_from_sop_class(
            "1.2.840.10008.5.1.4.1.1.88.67", separate_secondary=False
        )
        == "DICOM"
    )


def test_primary_sop_list_matches_xnats() -> None:
    """Pinned so that the list can't drift from XNAT's primary-sops.txt unnoticed"""
    assert len(PRIMARY_SOP_CLASS_UIDS) == 47


def test_the_two_xnat_lists_disagree_as_xnats_own_do() -> None:
    """XNAT's primary-sops.txt and series-scans.properties aren't in step with each
    other, which is why e.g. Enhanced SR is catalogued as primary 'DICOM' while other
    SR classes that map to the same srScanData type are catalogued as 'secondary'.
    Pinned so that the divergence is visible rather than surprising, and so that
    updating one list prompts a look at the other.
    """
    # Breast Tomosynthesis is on the primary list but has no scan type of its own
    assert sorted(
        uid for uid in PRIMARY_SOP_CLASS_UIDS if uid not in SCAN_TYPE_BY_SOP_CLASS_UID
    ) == ["1.2.840.10008.5.1.4.1.1.13.1.3"]
    # and 19 classes that do have a scan type aren't considered primary
    assert (
        len(
            [
                uid
                for uid in SCAN_TYPE_BY_SOP_CLASS_UID
                if uid not in PRIMARY_SOP_CLASS_UIDS
            ]
        )
        == 19
    )
