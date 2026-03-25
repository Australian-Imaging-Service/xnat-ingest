"""
EEG file formats for XNAT Ingest workflows.
Defines and validates common EEG file formats (FIF, EDF, BrainVision) for use in XNAT Ingest pipelines.

Authors:
- Miao Cao

Email:
- miaocao@swin.edu.au
"""

from fileformats.core import validated_property, mtime_cached_property
from fileformats.core.exceptions import FormatMismatchError
from fileformats.generic import FileSet, BinaryFile, UnicodeFile
from fileformats.core.mixin import WithMagicNumber, WithAdjacentFiles
from fileformats.application import Gzip


class Biosig(FileSet):
    """Base class for biophysical time-series recordings"""

    pass


class Eeg(Biosig):
    """Base class for all Electroencephalography recordings"""

    pass


# ------------------------------
# Implementation of Specific EEG Formats
# ------------------------------
class Fif(WithMagicNumber, BinaryFile, Biosig):
    """
    MNE FIF format (standard format for NeuroMag/MEGIN MEG/EEG devices)
    Most commonly used binary format, supports compression (.fif.gz)
    """

    ext = ".fif"
    # FIF file magic number (hex identifier, from MNE official documentation)
    magic_number = b"\x46\x49\x46\x32"  # "FIF2"


class FifGz(Gzip[Fif], Fif):
    """Gzip-compressed MNE FIF format"""

    ext = ".fif.gz"


class Edf(WithMagicNumber, BinaryFile, Eeg):
    """
    EDF format EEG (European Data Format) — binary file with fixed-width ASCII
    header followed by binary signal data.
    """

    extensions = ".edf"
    # First 8 bytes are "0       " (version field)
    magic_number = b"\x30\x20\x20\x20\x20\x20\x20\x20"
    mime_type = "application/x-eeg-edf"

    @mtime_cached_property
    def header(self) -> str:
        with open(self, "rb") as f:
            header = f.read(256)
        return header.decode("ascii", errors="replace")

    @validated_property
    def local_recording_identification(self) -> list[str]:
        parts = self.header[88:168].split()
        if len(parts) != 5:
            raise FormatMismatchError(
                'Unrecognised "local recording identification" string, '
                f"should have 5 parts, found: {parts}"
            )
        return parts

    @validated_property
    def local_patient_identification(self) -> list[str]:
        parts = self.header[8:88].split()
        if len(parts) != 4:
            raise FormatMismatchError(
                'Unrecognised "local patient identification" string, '
                f"should have 4 parts, found: {parts}"
            )
        return parts

    @property
    def _edf_type(self) -> str:
        return self.header[192:236].strip()

    @validated_property
    def edf_type(self) -> str:
        if self._edf_type != "":
            raise FormatMismatchError(
                'EDF type field ("reserved") should be blank for plain EDF '
                "(i.e. not EDF+)"
            )
        return ""


class EdfPlus(Edf):
    """
    EDF+ format — extension of EDF supporting discontinuous recordings and
    additional annotation channels. Distinguished by "EDF+C" or "EDF+D" in
    the reserved header field.
    """

    extensions = ".edf+"

    valid_edf_types = ["EDF+C", "EDF+D"]

    @validated_property
    def edf_type(self) -> str:
        if self._edf_type not in self.valid_edf_types:
            raise FormatMismatchError(
                f'EDF type field ("reserved") should be in {self.valid_edf_types} '
                f"for EDF+, found: {self._edf_type!r}"
            )
        return self._edf_type


class BrainVisionHeader(WithMagicNumber, UnicodeFile, Biosig):
    """
    BrainVision header file (.vhdr) — plain-text INI file describing channel
    configuration, sampling rate, amplifier settings, and references to the
    data and marker files.
    """

    ext = ".vhdr"
    # First 12 bytes of "Brain Vision Data Exchange Header File Version 1.0\r\n"
    magic_number = b"Brain Vision"
    mime_type = "application/x-ephys-brainvision-header"


class BrainVisionMarker(WithMagicNumber, UnicodeFile, Biosig):
    """
    BrainVision marker file (.vmrk) — plain-text INI file containing event
    markers and annotations time-stamped to samples in the data file.
    """

    ext = ".vmrk"
    # First 12 bytes of "Brain Vision Data Exchange Marker File, Version 1.0\r\n"
    magic_number = b"Brain Vision"
    mime_type = "application/x-ephys-brainvision-marker"


class BrainVision(WithAdjacentFiles, BinaryFile, Biosig):
    """
    BrainVision binary data file (.eeg) — raw multiplexed sample data,
    format described by the accompanying .vhdr header file. No magic number.
    """

    ext = ".eeg"
    mime_type = "application/x-ephys-brainvision"

    @validated_property
    def header_file(self) -> BrainVisionHeader:
        return BrainVisionHeader(self.select_by_ext(BrainVisionHeader))

    @validated_property
    def marker_file(self) -> BrainVisionMarker:
        return BrainVisionMarker(self.select_by_ext(BrainVisionMarker))
