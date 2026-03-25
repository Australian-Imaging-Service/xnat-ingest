"""
MEG file formats for XNAT Ingest workflows.
Defines and validates common MEG file formats (FIF, CTF, KIT) for use in XNAT Ingest pipelines.

Authors:
- Miao Cao

Email:
- miaocao@swin.edu.au
"""

from fileformats.core import validated_property
from fileformats.core.mixin import WithAdjacentFiles, WithMagicNumber
from fileformats.core.exceptions import FormatMismatchError
from fileformats.generic import Directory, File, BinaryFile, UnicodeFile
from fileformats.application import Xml

from .eeg import Biosig


# ------------------------------
# Base MEG Type (Abstract Class)
# ------------------------------
class Meg(Biosig):
    """
    Base class for MEG data formats
    All specific MEG formats inherit from this class with unified validation logic
    """


class CtfMeg4(WithMagicNumber, BinaryFile, Meg):
    """
    CTF MEG4 binary data file (.meg4) — raw sensor data in CTF's proprietary format.
    The resource file (.res4) in the same .ds directory describes the channel layout.
    """

    ext = ".meg4"
    # First 8 bytes: "MEG41CP\0" (CTF MEG4 format version identifier)
    magic_number = b"MEG41CP\x00"


class CtfRes4(WithMagicNumber, BinaryFile, Meg):
    """
    CTF resource file (.res4) — binary header describing channel layout, sampling
    rate, sensor positions, and filter settings for the accompanying .meg4 data.
    """

    ext = ".res4"
    # First 8 bytes: "MEG41RS\0" (CTF resource file version identifier)
    magic_number = b"MEG41RS\x00"


class CtfInfo(Xml, Meg):
    """
    CTF dataset info file (.infods) — XML file containing dataset-level metadata
    such as subject info, acquisition date, and operator notes.
    """

    ext = ".infods"


class Ctf(Directory, Meg):
    """
    CTF format MEG (directory-based, proprietary format for CTF MEG devices)
    Core files include *.meg4/*.res4/*.infods under .ds directory
    """

    ext = ".ds"

    content_types = (CtfMeg4, CtfRes4, CtfInfo)


class KitMark(BinaryFile):
    """Marker"""

    ext = ".mrk"


class KitHeadPosition(UnicodeFile):
    ext = ".elp"


class KitSensorInfo(File):
    ext = ".hsj"


class Kit(WithAdjacentFiles, Meg, BinaryFile):
    """
    KIT/RIKEN (Ricon) MEG format (directory-based)
    Required files:
    - Main data file (.sqd or .con)
    - Marker file (.mrk)
    Optional files: .elp (head position), .hsj (sensor info)
    """

    ext = ".sqd"
    alternate_exts = (".con",)

    marker_generic_names = ("marker.mrk", "markers.mrk", "kit.mrk")

    # meg_chs = mne.pick_types(raw.info, meg=True, eeg=False)

    @validated_property
    def mark_file(self) -> KitMark:
        """
        Helper method: Find corresponding .mrk marker file for KIT/RIKEN data
        Looks for same prefix with .mrk extension within the same directory
        """
        try:
            return self.select_by_ext(KitMark)
        except FormatMismatchError:
            for cand in self.marker_generic_names:
                mrk_path = self.parent / cand
                if mrk_path.exists():
                    return mrk_path

            raise FormatMismatchError(
                f"No .mrk marker file found for KIT MEG data {self}\n"
            )

    @property
    def head_position_file(self) -> KitHeadPosition | None:
        try:
            return self.select_by_ext(KitHeadPosition)
        except FormatMismatchError as e:
            if e.args[0].startswith("No matching files"):
                return None
            raise

    @property
    def sensor_info_file(self) -> KitSensorInfo | None:
        try:
            return self.select_by_ext(KitSensorInfo)
        except FormatMismatchError as e:
            if e.args[0].startswith("No matching files"):
                return None
            raise
