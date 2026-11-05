from .attach_cli import attach_cmd
from .base import cli
from .check_upload_cli import check_upload_cmd
from .collate_cli import collate_cmd, collate_orthanc_cmd
from .deidentify_cli import deidentify_cmd
from .fields_cli import fields_cmd
from .label_cli import label_cmd
from .package_cli import package_cmd
from .upload_cli import upload_cmd

__all__ = [
    "label_cmd",
    "attach_cmd",
    "check_upload_cmd",
    "deidentify_cmd",
    "cli",
    "upload_cmd",
    "collate_cmd",
    "collate_orthanc_cmd",
    "package_cmd",
    "fields_cmd",
]
