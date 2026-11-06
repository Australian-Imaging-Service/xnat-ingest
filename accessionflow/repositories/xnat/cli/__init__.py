"""The 'label xnat', 'upload xnat' and 'check-upload xnat' subcommands, which are
registered with their stages when this package is imported"""

from .check_upload_cli import check_upload_cmd
from .label_cli import label_cmd
from .upload_cli import upload_cmd

__all__ = ["label_cmd", "upload_cmd", "check_upload_cmd"]
