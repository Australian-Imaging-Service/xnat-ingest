"""Upload resource archives without holding their contents in memory."""

import tarfile
import tempfile
import typing as ty
from pathlib import Path

from xnat.exceptions import XNATValueError


def upload_resource_directory(
    resource: ty.Any,
    directory: Path | str,
    method: str | None = "tgz_file",
    overwrite: bool = False,
    **kwargs: ty.Any,
) -> None:
    """Use a file-backed archive for xnatpy's two file upload methods.

    xnatpy spools up to 256 MiB in memory for each ``tar_file`` or ``tgz_file``
    upload. Concurrent uploads can exhaust memory before those buffers spill
    to disk. Keep the same archive layout and upload options, but write to a
    temporary file from the start. Other methods retain xnatpy's behaviour.
    """
    method = method or "tgz_file"
    if method not in ("tar_file", "tgz_file"):
        resource.upload_dir(directory, overwrite=overwrite, method=method, **kwargs)
        return

    directory = Path(directory)
    if not directory.is_dir():
        raise XNATValueError(
            f"The argument directory {directory} is not a path to a valid directory"
        )

    mode = "w:gz" if method == "tgz_file" else "w"
    remote_name = "upload.tar.gz" if method == "tgz_file" else "upload.tar"
    with tempfile.TemporaryFile(mode="w+b") as archive:
        with tarfile.open(mode=mode, fileobj=archive) as tar:
            tar.add(directory, "")
        archive_size = archive.tell()
        archive.seek(0)
        resource.upload_data(
            archive,
            remote_name,
            overwrite=overwrite,
            extract=True,
            verbose=True,
            upload_size=archive_size,
            **kwargs,
        )
