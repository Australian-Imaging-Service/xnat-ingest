import pytest
from fileformats.medimage import DicomSeries
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import \
    get_image as get_pet_image  # type: ignore[import-untyped]

# PATIENT_ID = "patient-id"
# STUDY_ID = "study-id"
# ACCESSION_NUMBER = "accession-number"


@pytest.fixture
def dicom_series(scope="module") -> DicomSeries:
    return DicomSeries(
        get_pet_image(first_name="GivenName", last_name="FamilyName").iterdir()
    )


# @pytest.mark.xfail(
#     condition=(platform.system() == "Linux"), reason="Not working on ubuntu"
# )
def test_mrtrix_dicom_metadata(dicom_series: DicomSeries):
    keys = [
        "AccessionNumber",
        "PatientID",
        "PatientName",
        "StudyID",
        "StudyInstanceUID",
        "SOPInstanceUID",
    ]
    dicom_series = DicomSeries(dicom_series, specific_tags=keys)

    assert not (set(keys + ["SpecificCharacterSet"]) - set(dicom_series.metadata))
    assert dicom_series.metadata["PatientName"] == "FamilyName^GivenName"
    assert dicom_series.metadata["AccessionNumber"] == "987654321"
    assert dicom_series.metadata["PatientID"] == "Session Label"
    assert dicom_series.metadata["StudyID"] == "PROJECT_ID"
    assert not isinstance(dicom_series.metadata["StudyInstanceUID"], list)
    assert isinstance(dicom_series.metadata["SOPInstanceUID"], list)
    assert len(dicom_series.metadata["SOPInstanceUID"]) == len(
        list(dicom_series.contents)
    )
    )
