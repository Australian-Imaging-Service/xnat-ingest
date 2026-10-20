"""Missing descriptive/number tags must not discard an otherwise usable study."""

import pytest
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, MRImageStorage
from test_from_orthanc import FakeOrthanc, _patch_requests, _publication_kwargs

from xnat_ingest.model.session import ImagingSession


def make_fake(tmp_path, number, description, series_uid="1.2.3.4"):
    store = tmp_path / "store"
    store.mkdir()
    fake = FakeOrthanc(store)
    meta = FileMetaDataset()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    meta.MediaStorageSOPClassUID = MRImageStorage
    meta.MediaStorageSOPInstanceUID = "1.2.3.4.5"
    path = tmp_path / "synthetic.dcm"
    ds = FileDataset(str(path), {}, file_meta=meta, preamble=b"\0" * 128)
    ds.SOPClassUID = MRImageStorage
    ds.SOPInstanceUID = "1.2.3.4.5"
    ds.StudyInstanceUID = "1.2.3"
    ds.SeriesInstanceUID = series_uid
    ds.Modality = "MR"
    ds.PatientID = "synthetic"
    ds.save_as(path, enforce_file_format=True)
    tags = {"SeriesInstanceUID": series_uid, "Modality": "MR"}
    if number is not None:
        tags["SeriesNumber"] = number
    if description is not None:
        tags["SeriesDescription"] = description
    fake.add_study(
        "study",
        path,
        {"StudyInstanceUID": "1.2.3", "AccessionNumber": "ACC"},
        {"PatientID": "synthetic"},
        tags,
        labels=["ready"],
    )
    return fake, store


@pytest.mark.parametrize(
    "number,description",
    [("1", None), ("", "MR"), (None, None), ("   ", " "), (0, "MR")],
)
def test_optional_series_tags_do_not_block_study(tmp_path, number, description):
    fake, store = make_fake(tmp_path, number, description)
    with _patch_requests(fake):
        sessions = ImagingSession.from_orthanc(
            **_publication_kwargs(tmp_path / "staged", store)
        )
    assert len(sessions) == 1
    scan = next(iter(sessions[0].scans.values()))
    assert scan.id and scan.type.strip()
    if number in (None, "", "   "):
        assert scan.id.startswith("series_")
    else:
        assert scan.id == str(number)
    assert fake.labelled == [("study", "done")]
    for resource in sessions[0].resources:
        resource.check_checksums()


def test_fallback_id_is_stable_for_a_repeated_series(tmp_path):
    ids = []
    for name in ["first", "second"]:
        root = tmp_path / name
        root.mkdir()
        fake, store = make_fake(root, "", None)
        with _patch_requests(fake):
            sessions = ImagingSession.from_orthanc(
                **_publication_kwargs(root / "staged", store)
            )
        ids.append(next(iter(sessions[0].scans)))
    assert ids[0] == ids[1]


def test_two_unnumbered_series_remain_separate(tmp_path):
    fake, store = make_fake(tmp_path, "", None)
    other = tmp_path / "other"
    other.mkdir()
    second, _ = make_fake(other, "", None, series_uid="1.2.3.5")
    fake.add_study(
        "second",
        other / "synthetic.dcm",
        {"StudyInstanceUID": "1.2.3"},
        {"PatientID": "synthetic"},
        second.series["study-series1"]["tags"],
    )
    fake.studies["study"]["series"].extend(fake.studies.pop("second")["series"])
    with _patch_requests(fake):
        sessions = ImagingSession.from_orthanc(
            **_publication_kwargs(tmp_path / "staged", store)
        )
    assert len(sessions) == 1
    assert len(sessions[0].scans) == 2
    assert all(scan.id.startswith("series_") for scan in sessions[0].scans.values())
    assert fake.labelled == [("study", "done")]


def test_missing_uid_uses_stable_orthanc_series_identity(tmp_path):
    fake, store = make_fake(tmp_path, "", None)
    del fake.series["study-series1"]["tags"]["SeriesInstanceUID"]
    with _patch_requests(fake):
        sessions = ImagingSession.from_orthanc(
            **_publication_kwargs(tmp_path / "staged", store)
        )
    assert len(sessions[0].scans) == 1
    assert next(iter(sessions[0].scans)).startswith("series_")


def test_normal_series_labels_stay_unchanged(tmp_path):
    fake, store = make_fake(tmp_path, "2", "T1")
    with _patch_requests(fake):
        sessions = ImagingSession.from_orthanc(
            **_publication_kwargs(tmp_path / "staged", store)
        )
    assert sessions[0].scans["2"].type == "T1"
