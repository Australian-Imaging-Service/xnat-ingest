import tempfile
from pathlib import Path
from fileformats.medimage import DicomSeries
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (  # type: ignore[import-untyped]
    get_image as get_pet_image,
)


tmp_path = Path(tempfile.mkdtemp())

series = DicomSeries(
    get_pet_image(
        tmp_path,
        first_name="first",
        last_name="last",
        StudyInstanceUID="StudyInstanceUID",
        PatientID="PatientID",
        AccessionNumber="AccessionNumber",
        StudyID="xnat_project",
    ).iterdir()
)

print(series.metadata["StudyID"])
