"""Support for labelling sessions for, and uploading them to, XNAT repositories"""

from accessionflow.model.repository import LabelLevel, Repository

REPOSITORY = Repository(
    name="xnat",
    title="XNAT",
    labels=(
        LabelLevel("project", default="StudyComments", description="XNAT project ID"),
        LabelLevel("subject", default="PatientID", description="XNAT subject ID"),
        LabelLevel("session", default="AccessionNumber", description="XNAT session ID"),
    ),
    # XNAT calls an acquisition a scan, and the description of it the scan type
    acquisition=LabelLevel(
        "scan", default="SeriesDescription", description="XNAT scan type"
    ),
    # A scan can hold several resources, e.g. the DICOM files and a NIfTI conversion
    multiple_resources=True,
)

__all__ = ["REPOSITORY"]
