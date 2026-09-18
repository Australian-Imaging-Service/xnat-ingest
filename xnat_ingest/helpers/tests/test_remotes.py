import typing as ty
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fileformats.generic import File
from fileformats.medimage import DicomSeries
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

from xnat_ingest.helpers.remotes import (
    SessionOnlyListing,
    get_xnat_resource,
    list_session_dirs,
    split_resource_by_modality,
)
from xnat_ingest.helpers.xnat_scan_types import (
    PRIMARY_SOP_CLASS_UIDS,
    SCAN_TYPE_BY_SOP_CLASS_UID,
    SOP_CLASS_UIDS_BY_SCAN_TYPE,
    XNAT_SCAN_TYPE_PRECEDENCE,
    group_by_modality,
    xnat_resource_label_from_sop_class,
    xnat_scan_type_from_sop_class,
)
from xnat_ingest.model.resource import ImagingResource
from xnat_ingest.model.scan import ImagingScan


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


# -- group_by_modality --------------------------------------------------------------


def test_group_by_modality_no_split_needed() -> None:
    """A single, or uniformly-modality'd, series is not split"""
    assert group_by_modality([("a.dcm", "PT"), ("b.dcm", "PT")]) is None
    assert group_by_modality([("a.dcm", "PT")]) is None


def test_group_by_modality_splits_a_mixed_series() -> None:
    """Mirrors the case that motivated this: a PT series with an embedded dose-report
    SR object comes out as two groups, one per modality"""
    groups = group_by_modality(
        [
            ("a.dcm", "PT"),
            ("b.dcm", "PT"),
            ("c.dcm", "SR"),
        ]
    )
    assert groups == {"PT": ["a.dcm", "b.dcm"], "SR": ["c.dcm"]}


def test_group_by_modality_missing_modality_is_other() -> None:
    """A file with no Modality tag is grouped as 'OT', the DICOM defined term for it,
    rather than being dropped or crashing"""
    groups = group_by_modality([("a.dcm", "PT"), ("b.dcm", None)])
    assert groups == {"PT": ["a.dcm"], "OT": ["b.dcm"]}


def test_group_by_modality_empty() -> None:
    assert group_by_modality([]) is None


# -- split_resource_by_modality -------------------------------------------------------

_PET_IMAGE_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.128"
_DOSE_REPORT_SR_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.88.67"


def _write_dicom(
    path: Path, sop_class_uid: str, modality: str, series_number: str = "6"
) -> None:
    """A minimal, valid-enough DICOM file for exercising fileformats' own header
    reading, with just the tags `split_resource_by_modality` and
    `xnat_resource_label_from_sop_class` care about"""
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = sop_class_uid
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(str(path), {}, file_meta=meta, preamble=b"\0" * 128)
    ds.SOPClassUID = sop_class_uid
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.StudyInstanceUID = "1.2.3"
    ds.SeriesInstanceUID = "1.2.3.4"
    ds.SeriesNumber = series_number
    ds.Modality = modality
    ds.PatientID = "test"
    ds.is_little_endian = True
    ds.is_implicit_VR = False
    ds.save_as(str(path))


def test_split_resource_by_modality_splits_a_mixed_series(tmp_path: Path) -> None:
    """The case that motivated this: a PET series carrying an embedded dose-report SR
    object, which XNAT itself would split into '6-PT' and '6-SR' scans when it rebuilds
    the session from the DICOM headers"""
    _write_dicom(tmp_path / "pt1.dcm", _PET_IMAGE_SOP_CLASS, "PT")
    _write_dicom(tmp_path / "pt2.dcm", _PET_IMAGE_SOP_CLASS, "PT")
    _write_dicom(tmp_path / "sr1.dcm", _DOSE_REPORT_SR_SOP_CLASS, "SR")
    series = DicomSeries(list(tmp_path.iterdir()))
    scan = ImagingScan(id="6", type="chest_pt")
    resource = ImagingResource(name="DICOM", fileset=series, scan=scan)

    parts = split_resource_by_modality(resource)

    by_scan_id = {part.scan.id: part for part in parts}
    assert set(by_scan_id) == {"6-PT", "6-SR"}
    assert len(by_scan_id["6-PT"].fileset.fspaths) == 2
    assert len(by_scan_id["6-SR"].fileset.fspaths) == 1
    # each part's own metadata reflects only the files in that part, not the mix in
    # the series it was split from -- this is what lets the existing SOP-class-based
    # "DICOM"/"secondary" labelling keep working unmodified on the split-out parts
    assert by_scan_id["6-PT"].metadata.get("SOPClassUID") == _PET_IMAGE_SOP_CLASS
    assert by_scan_id["6-SR"].metadata.get("SOPClassUID") == _DOSE_REPORT_SR_SOP_CLASS
    assert all(part.name == "DICOM" for part in parts)
    assert all(part.scan.type == "chest_pt" for part in parts)


def test_split_resource_by_modality_no_split_for_uniform_series(
    tmp_path: Path,
) -> None:
    """A series with a single modality is returned unchanged, not wrapped/copied"""
    _write_dicom(tmp_path / "pt1.dcm", _PET_IMAGE_SOP_CLASS, "PT")
    _write_dicom(tmp_path / "pt2.dcm", _PET_IMAGE_SOP_CLASS, "PT")
    series = DicomSeries(list(tmp_path.iterdir()))
    scan = ImagingScan(id="6", type="chest_pt")
    resource = ImagingResource(name="DICOM", fileset=series, scan=scan)

    assert split_resource_by_modality(resource) == [resource]


def test_split_resource_by_modality_ignores_session_level_resources() -> None:
    """A resource with no scan (a session-level resource) is never split, without even
    looking at its fileset -- there is no scan id to suffix a modality onto"""

    class _SessionResource:
        scan = None

    resource: ty.Any = _SessionResource()
    assert split_resource_by_modality(resource) == [resource]


def test_split_resource_by_modality_ignores_non_dicom_resources(tmp_path: Path) -> None:
    """Only DICOM scan resources can be split -- there is no 'modality' to split a
    converted NIfTI, a brain mask or any other non-DICOM resource by"""
    a_file = tmp_path / "notes.txt"
    a_file.write_text("not DICOM")
    scan = ImagingScan(id="6", type="chest_pt")
    resource = ImagingResource(name="NOTES", fileset=File(a_file), scan=scan)

    assert split_resource_by_modality(resource) == [resource]


# -- get_xnat_resource: SOP-class relabelling must not reach non-DICOM resources -----


def test_get_xnat_resource_ignores_sop_class_on_non_dicom_resources() -> None:
    """THE REGRESSION: a Siemens raw-data resource (listmode/countrate) embeds a copy
    of a DICOM header for provenance, so it has a perfectly readable SOPClassUID, but
    it is a `BinaryFile`, not a `DicomCollection` -- XNAT's own catalog builder never
    parses it as DICOM and never applies the "DICOM"/"secondary" categorisation to it.

    Deciding the resource name from the metadata key alone, without also requiring the
    fileset to be a genuine DicomCollection, renamed both such resources on the same
    scan to "secondary", colliding two different resources into one and uploading it
    from two threads at once.
    """

    class _FakeScan:
        id = "602"
        type = "602"
        path = "proj:subj:sess:602-602"

    class _FakeVendorResource:
        # deliberately not a DicomCollection, but its metadata still carries a
        # SOPClassUID -- read from the embedded header -- like the real vendor
        # raw-data classes do
        fileset = object()
        scan = _FakeScan()
        checksums: dict[str, str] = {"a.ptd": "digest"}

        def __init__(self, name: str) -> None:
            self.name = name
            self.metadata = {"SOPClassUID": "1.3.12.2.1107.5.9.1", "Modality": "PT"}

    xscan = MagicMock()
    xscan.resources = {}
    xscan.files = ["something"]  # non-empty: skip the catalog-refresh branch
    xsession = MagicMock()
    xsession.scans = {"602": xscan}

    for name in ("COUNTRATE", "LISTMODE"):
        get_xnat_resource(_FakeVendorResource(name), xsession)

    # each is created under its own name -- neither collided into "secondary"
    created_as = [call.args[0] for call in xscan.create_resource.call_args_list]
    assert created_as == ["COUNTRATE", "LISTMODE"]
