import timeit
from fileformats.medimage import DicomSeries
from medimages4tests.dummy.dicom.mri.dwi.siemens.skyra.syngo_d13c import get_image

METADATA_KEYS = [
    "SeriesNumber",
    "SeriesDescription",
    "StudyInstanceUID",
    "StudyID",
    "PatientID",
    "AccessionNumber",
]

series = DicomSeries(get_image().iterdir(), specific_tags=METADATA_KEYS)

timeit.timeit(lambda: series.metadata)
