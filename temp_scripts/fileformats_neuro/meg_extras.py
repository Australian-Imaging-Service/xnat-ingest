import typing as ty
import xml.etree.ElementTree as ET
from pathlib import Path

import mne.io

from fileformats.core import extra_implementation, FileSet
from .meg import Ctf, Kit
from .eeg_extras import _info_to_metadata


@extra_implementation(FileSet.read_metadata)
def ctf_read_metadata(ctf: Ctf) -> dict[str, ty.Any]:
    raw = mne.io.read_raw_ctf(ctf.fspath, preload=False, verbose=False)
    return {
        **_info_to_metadata(raw.info),
        **_parse_infods(Path(ctf.fspath)),
    }


# elif ext in [".sqd", ".con"]:  # KIT/RIKEN main data files
#                 # For KIT format, we need to find the marker file (.mrk) in the same directory


@extra_implementation(FileSet.read_metadata)
def kit_read_metadata(kit: Kit) -> dict[str, ty.Any]:
    mrk_path = kit._find_kit_mrk_file()
    return mne.io.read_raw_kit(kit, mrk=mrk_path, verbose=False)


def _parse_infods(ds_path: Path) -> dict[str, ty.Any]:
    """
    Parse the .infods XML sidecar in a CTF .ds directory for metadata that
    MNE does not surface via raw.info (subject name, operator, study description).
    Returns an empty dict if no .infods file is present.
    """
    infods_files = list(ds_path.glob("*.infods"))
    if not infods_files:
        return {}
    tree = ET.parse(infods_files[0])
    root = tree.getroot()

    def find(tag: str) -> str | None:
        el = root.find(f".//{tag}")
        return el.text.strip() if el is not None and el.text else None

    return {
        "subject_id": find("SUBJECTID"),
        "subject_name": find("SUBJECTNAME"),
        "operator": find("OPERATOR"),
        "institution": find("INSTITUTION"),
        "study_description": find("STUDYDESCRIPTION"),
        "run_description": find("RUNDESCRIPTION"),
        "acquisition_datetime": find("ACQUISITIONDATETIME"),
        "date": find("DATE"),
    }
