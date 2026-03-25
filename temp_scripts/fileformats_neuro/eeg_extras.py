import configparser
import typing as ty

import mne.io

from fileformats.core import extra_implementation, FileSet
from .eeg import BrainVision, Edf, EdfPlus, Fif, FifGz


@extra_implementation(FileSet.read_metadata)
def fif_read_metadata(fif: Fif) -> dict[str, ty.Any]:
    raw = mne.io.read_raw_fif(fif.fspath, preload=False, verbose=False)
    return _info_to_metadata(raw.info)


@extra_implementation(FileSet.read_metadata)
def fif_gz_read_metadata(fif: FifGz) -> dict[str, ty.Any]:
    raw = mne.io.read_raw_fif(fif.fspath, preload=False, verbose=False)
    return _info_to_metadata(raw.info)


@extra_implementation(FileSet.read_metadata)
def edf_read_metadata(edf: Edf) -> dict[str, ty.Any]:
    raw = mne.io.read_raw_edf(edf.fspath, preload=False, verbose=False)
    return {
        **_info_to_metadata(raw.info),
        **_parse_edf_header(edf.fspath),
    }


@extra_implementation(FileSet.read_metadata)
def edf_plus_read_metadata(edf: EdfPlus) -> dict[str, ty.Any]:
    raw = mne.io.read_raw_edf(edf.fspath, preload=False, verbose=False)
    return {
        **_info_to_metadata(raw.info),
        **_parse_edf_header(edf.fspath),
    }


@extra_implementation(FileSet.read_metadata)
def brain_vision_read_metadata(bv: BrainVision) -> dict[str, ty.Any]:
    raw = mne.io.read_raw_brainvision(bv.header_file, preload=False, verbose=False)
    return {
        **_info_to_metadata(raw.info),
        **_parse_vhdr(bv.header_file),
    }


def _info_to_metadata(info: mne.Info) -> dict[str, ty.Any]:
    """Extract study-sorting fields from an MNE Info object."""
    subj = info.get("subject_info") or {}
    dev = info.get("device_info") or {}
    return {
        # Subject
        "subject_id": subj.get("id"),
        "subject_first_name": subj.get("first_name"),
        "subject_last_name": subj.get("last_name"),
        "subject_birthday": subj.get("birthday"),
        "subject_sex": subj.get("sex"),
        "subject_hand": subj.get("hand"),
        # Study
        "proj_name": info.get("proj_name"),
        "proj_id": info.get("proj_id"),
        "experimenter": info.get("experimenter"),
        "description": info.get("description"),
        # Recording
        "meas_date": info.get("meas_date"),
        "utc_offset": info.get("utc_offset"),
        # Acquisition
        "sfreq": info.get("sfreq"),
        "nchan": info.get("nchan"),
        "highpass": info.get("highpass"),
        "lowpass": info.get("lowpass"),
        # Device
        "device_type": dev.get("type"),
        "device_model": dev.get("model"),
        "device_serial": dev.get("serial"),
        "device_site": dev.get("site"),
    }


def _parse_edf_header(path: str) -> dict[str, ty.Any]:
    """
    Parse EDF/EDF+ header bytes directly for patient and recording fields
    that MNE may not surface via raw.info.

    Header layout (all ASCII, fixed-width):
      [0:8]    version
      [8:88]   local patient identification  "code sex birthdate name"
      [88:168] local recording identification "Startdate date id technician equipment"
      [168:176] start date DD.MM.YY
      [176:184] start time HH.MM.SS
      [192:236] reserved — contains "EDF+C" or "EDF+D" for EDF+
    """
    with open(path, "rb") as f:
        header = f.read(256)

    lpi = header[8:88].decode("ascii", errors="replace").strip()
    lri = header[88:168].decode("ascii", errors="replace").strip()
    start_date = header[168:176].decode("ascii", errors="replace").strip()
    start_time = header[176:184].decode("ascii", errors="replace").strip()
    reserved = header[192:236].decode("ascii", errors="replace").strip()

    lpi_parts = lpi.split()
    lri_parts = lri.split()

    return {
        "edf_patient_code": lpi_parts[0] if len(lpi_parts) > 0 else None,
        "edf_patient_sex": lpi_parts[1] if len(lpi_parts) > 1 else None,
        "edf_patient_birthdate": lpi_parts[2] if len(lpi_parts) > 2 else None,
        "edf_patient_name": " ".join(lpi_parts[3:]) if len(lpi_parts) > 3 else None,
        "edf_recording_startdate": lri_parts[1] if len(lri_parts) > 1 else None,
        "edf_recording_id": lri_parts[2] if len(lri_parts) > 2 else None,
        "edf_technician": lri_parts[3] if len(lri_parts) > 3 else None,
        "edf_equipment": lri_parts[4] if len(lri_parts) > 4 else None,
        "edf_start_date": start_date,
        "edf_start_time": start_time,
        "edf_subtype": reserved if reserved.startswith("EDF+") else None,
    }


def _parse_vhdr(path: str) -> dict[str, ty.Any]:
    """
    Parse a BrainVision .vhdr file (INI format) for fields not exposed by MNE.
    Extracts acquisition settings and the free-text Comment section.
    """
    parser = configparser.RawConfigParser()
    # vhdr files start with a magic line before the first INI section — skip it
    with open(path, encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    ini_lines = [line for line in lines if not line.startswith("Brain Vision")]
    parser.read_string("".join(ini_lines))

    def get(section, key, fallback=None):
        try:
            return parser.get(section, key)
        except (configparser.NoSectionError, configparser.NoOptionError):
            return fallback

    # [Common Infos] uses inconsistent casing across BrainVision versions
    common = next((s for s in parser.sections() if s.lower() == "common infos"), None)

    comment_lines = []
    if parser.has_section("Comment"):
        comment_lines = [v for _, v in parser.items("Comment") if v.strip()]

    return {
        "bv_data_format": get(common, "DataFormat") if common else None,
        "bv_data_orientation": get(common, "DataOrientation") if common else None,
        "bv_n_channels": get(common, "NumberOfChannels") if common else None,
        "bv_sampling_interval_us": get(common, "SamplingInterval") if common else None,
        "bv_binary_format": get("Binary Infos", "BinaryFormat"),
        "bv_comment": "\n".join(comment_lines) if comment_lines else None,
    }
