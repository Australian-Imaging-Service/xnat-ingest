import timeit
from fileformats.medimage import DicomSeries
from medimages4tests.dummy.dicom.mri.dwi.siemens.skyra.syngo_d13c import get_image
import xnat_ingest.dicom  # noqa

METADATA_KEYS = [
    "SeriesNumber",
    "SeriesDescription",
    "StudyInstanceUID",
    "StudyID",
    "PatientID",
    "AccessionNumber"
]

series = DicomSeries(get_image().iterdir())

timeit.timeit(lambda: series.select_metadata(METADATA_KEYS))
