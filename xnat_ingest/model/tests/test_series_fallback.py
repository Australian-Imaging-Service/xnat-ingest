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
def test_optional_series_tags_do_not_block_study(tmp_path, number, description, caplog):
    fake, store = make_fake(tmp_path, number, description)
    with _patch_requests(fake):
        sessions = ImagingSession.from_orthanc(
            **_publication_kwargs(tmp_path / "staged", store)
        )
    assert len(sessions) == 1
    scan = next(iter(sessions[0].scans.values()))
    assert scan.id and scan.type.strip()
    if number in (None, "", "   "):
        assert scan.id == "1_2_3_4"
    else:
        assert scan.id == str(number)
    if number in (None, "", "   ") or not description or not str(description).strip():
        events = [
            r
            for r in caplog.records
            if getattr(r, "event", "") == "series_metadata_fallback"
        ]
        assert len(events) == 1
        assert events[0].orthanc_series_id == "study-series1"
    # Reconciliation can also add a snapshot label beside the processed label.
    assert fake.labelled.count(("study", "done")) == 1
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
    assert set(sessions[0].scans) == {"1_2_3_4", "1_2_3_5"}
    assert {s.type for s in sessions[0].scans.values()} == {
        "Series_1_2_3_4",
        "Series_1_2_3_5",
    }
    assert fake.labelled.count(("study", "done")) == 1
    # FrameTree keys its resource entries by type/resource, not by scan ID.
    # Calling every unnamed type "unknown" would collide during column selection.
    from xnat_ingest.model.store import ImagingSessionMockStore

    store = ImagingSessionMockStore(sessions[0])
    store.populate_row(store.row)
    for scan in sessions[0].scans.values():
        scan.type = "unknown"
    with pytest.raises(KeyError, match="multiple entries"):
        store.populate_row(store.row)


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


@pytest.mark.parametrize("description", [None, "", " "])
def test_missing_description_survives_following_assign_stage(tmp_path, description):
    fake, store = make_fake(tmp_path, "", description)
    with _patch_requests(fake):
        session = ImagingSession.from_orthanc(
            **_publication_kwargs(tmp_path / "staged", store)
        )[0]
    missing = session.assign(
        project_field="Unused",
        constant_project_id="proj",
        subject_field="PatientID",
        session_field="StudyInstanceUID",
        scan_field="SeriesDescription",
    )
    assert missing == {}
    assert session.scans["1_2_3_4"].type == "Series_1_2_3_4"
