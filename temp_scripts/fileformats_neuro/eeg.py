"""
EEG file formats for XNAT Ingest workflows.
Defines and validates common EEG file formats (FIF, EDF, BrainVision) for use in XNAT Ingest pipelines.

Authors:
- Miao Cao

Email:
- miaocao@swin.edu.au
"""

import os

import mne  # MNE library for professional EEG file validation

from fileformats.core import FileFormat, DirectoryFormat
from fileformats.core.mixin import WithMagicNumber

# ------------------------------
# Base EEG Type (Abstract Class, not instantiated directly)
# ------------------------------
class EegFormat(FileFormat):
    """
    Base class for EEG data formats
    All specific EEG formats inherit from this class with unified validation logic
    """
    # Generic MIME type (non-official, custom)
    mime_type = "application/x-eeg"

    def validate(self):
        """
        Core validation logic:
        1. Execute parent class validation (extension/magic number) first
        2. Use MNE library to verify if file is valid EEG data
        """
        super().validate()  # Execute FileFormat base validation
        
        # Skip validation for directory types (handled separately)
        if isinstance(self, DirectoryFormat):
            return
        
        # Attempt to read file with MNE to validate EEG legitimacy
        try:
            # Select MNE read function based on file extension
            ext = os.path.splitext(self.path)[1].lower()
            if ext in ['.fif', '.fif.gz']:
                raw = mne.io.read_raw_fif(self.path, verbose=False)
            elif ext in ['.edf', '.edf+']:
                raw = mne.io.read_raw_edf(self.path, verbose=False)
            elif ext in ['.vhdr']:  # BrainVision main file
                raw = mne.io.read_raw_brainvision(self.path, verbose=False)
            else:
                raise ValueError(f"Unsupported EEG extension: {ext}")
            
            # Verify presence of EEG channels
            eeg_chs = mne.pick_types(raw.info, eeg=True, meg=False)
            if len(eeg_chs) == 0:
                raise ValueError(f"File {self.path} contains no EEG channels, not valid EEG data")
            
        except Exception as e:
            raise ValueError(
                f"EEG file validation failed {self.path}:{str(e)}"
            ) from e

# ------------------------------
# Implementation of Specific EEG Formats
# ------------------------------
class EegFif(WithMagicNumber, EegFormat):
    """
    MNE FIF format EEG (standard format for NeuroMag/MEGIN devices)
    Most commonly used binary EEG format, supports compression (.fif.gz)
    """
    # File extensions (supports regular and compressed versions)
    extensions = (".fif", ".fif.gz")
    # FIF file magic number (hex identifier, from MNE official documentation)
    magic_number = b"\x46\x49\x46\x32"  # "FIF2"
    # Custom MIME type (distinguishes between different EEG subformats)
    mime_type = "application/x-eeg-fif"

class EegEdf(WithMagicNumber, EegFormat):
    """
    EDF/EDF+ format EEG (European Data Format, generic text+binary hybrid format)
    Supports EDF (basic version) and EDF+ (extended version)
    """
    extensions = (".edf", ".edf+")
    # EDF file magic number (first 8 bytes are "0       ")
    magic_number = b"\x30\x20\x20\x20\x20\x20\x20\x20"
    mime_type = "application/x-eeg-edf"

class EegBv(DirectoryFormat, EegFormat):
    """
    BrainVision format EEG (directory-based, contains 3 files: .vhdr/.vmrk/.eeg)
    Note: This class inherits from DirectoryFormat to validate entire directory integrity
    """
    extensions = (".bv_dir",)  # Custom directory extension (optional)
    mime_type = "application/x-eeg-brainvision"

    def validate(self):
        """
        Override validation logic: Check if directory contains complete BrainVision file set
        """
        super().validate()  # Execute directory base validation
        
        # List all files in directory
        files = os.listdir(self.path)
        # Extract prefixes (BrainVision three files share the same prefix)
        prefixes = set([os.path.splitext(f)[0] for f in files])
        
        # Validate if each prefix contains complete three files
        valid_prefix = None
        for prefix in prefixes:
            required_files = [f"{prefix}.vhdr", f"{prefix}.vmrk", f"{prefix}.eeg"]
            if all(os.path.exists(os.path.join(self.path, f)) for f in required_files):
                valid_prefix = prefix
                break
        
        if not valid_prefix:
            raise ValueError(
                f"BrainVision EEG directory {self.path} is incomplete:"
                "Missing one or more of .vhdr/.vmrk/.eeg files"
            )
        
        # Validate main file (.vhdr) with MNE
        vhdr_path = os.path.join(self.path, f"{valid_prefix}.vhdr")
        try:
            raw = mne.io.read_raw_brainvision(vhdr_path, verbose=False)
            eeg_chs = mne.pick_types(raw.info, eeg=True, meg=False)
            if len(eeg_chs) == 0:
                raise ValueError("No EEG channels")
        except Exception as e:
            raise ValueError(f"BrainVision EEG validation failed:{str(e)}") from e