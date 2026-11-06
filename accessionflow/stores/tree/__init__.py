"""Support for depositing accessions in a tree of nested label folders, on a local
path, a mount, or a remote file system such as S3 or SFTP (through fsspec)"""

from accessionflow.model.store import LabelLevel, Store

STORE = Store(
    name="tree",
    title="tree",
    # The levels of the tree are named when labelling, with 'label tree --level'
    labels=None,
    acquisition=LabelLevel(
        "acquisition", default="SeriesDescription", description="description"
    ),
    # An acquisition can hold several resources, e.g. the DICOM files and a NIfTI
    # conversion, each in its own folder
    multiple_resources=True,
)

__all__ = ["STORE"]
