import pytest
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (
    get_image as get_pet_image,
)
from fileformats.medimage import DicomSeries
from xnat_ingest.session import ImagingSession

# PATIENT_ID = "patient-id"
# STUDY_ID = "study-id"
# ACCESSION_NUMBER = "accession-number"


@pytest.fixture
def dicom_series(scope="module") -> ImagingSession:
    return DicomSeries(get_pet_image().iterdir())


def test_mrtrix_dicom_metadata(dicom_series):
    keys = [
        "AccessionNumber",
        "PatientID",
        "PatientName",
        "StudyID",
        "StudyInstanceUID",
        "SOPInstanceUID",
    ]
    dicom_series.select_metadata(keys)

    assert sorted(dicom_series.metadata) == sorted(keys + ['SpecificCharacterSet'])
    assert dicom_series["PatientName"] == "GivenName^FamilyName"
    assert dicom_series["AccessionNumber"] == "987654321"
    assert dicom_series["PatientID"] == 'Session Label'
    assert dicom_series["StudyID"] == "PROJECT_ID"
    assert not isinstance(dicom_series["StudyInstanceUID"], list)
    assert isinstance(dicom_series["SOPInstanceUID"], list)
    assert len(dicom_series["SOPInstanceUID"]) == len(list(dicom_series.contents))
