import os
from viewer.utils.constants import DIRECTORY, BINARY, JCAMPDX, UNKNOWN
import magic


def determine_jcampdx_type(entry_path):
    try:
        # This is a simplistic binary check. You might need a more robust solution.
        with open(entry_path, "rb") as file:
            if b"\0" in file.read(
                1024
            ):  # Read the first 1KB to check for a binary file
                return BINARY
            else:
                return JCAMPDX  # Default to JCAMPDX if not clearly binary
    except Exception as e:
        return UNKNOWN  # In case of an error, such as permission issues


def determine_file_type_magic(entry_path):
    if os.path.isdir(entry_path):
        return DIRECTORY

    mime = magic.Magic(mime=True)
    file_type = mime.from_file(entry_path)

    if file_type == "text/plain":
        return determine_jcampdx_type(entry_path)
    else:
        return file_type.split("/")[-1]
