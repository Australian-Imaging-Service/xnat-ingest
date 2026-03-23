import os
from typing import Optional

import mne

from fileformats.core import FileFormat, DirectoryFormat
from fileformats.core.mixin import WithMagicNumber
# ------------------------------
# Base MEG Type (Abstract Class)
# ------------------------------
class MegFormat(FileFormat):
    """
    Base class for MEG data formats
    All specific MEG formats inherit from this class with unified validation logic
    """
    mime_type = "application/x-meg"

    def validate(self):
        """
        Core validation: Verify MEG data legitimacy with MNE
        """
        super().validate()
        
        if isinstance(self, DirectoryFormat):
            return
        
        try:
            ext = os.path.splitext(self.path)[1].lower()
            if ext in ['.fif', '.fif.gz']:
                raw = mne.io.read_raw_fif(self.path, verbose=False)
            elif ext in ['.ds']:  # CTF format directory (special handling)
                raw = mne.io.read_raw_ctf(self.path, verbose=False)
            elif ext in ['.sqd', '.con']:  # KIT/RIKEN main data files
                # For KIT format, we need to find the marker file (.mrk) in the same directory
                mrk_path = self._find_kit_mrk_file()
                raw = mne.io.read_raw_kit(
                    self.path,
                    mrk=mrk_path,
                    verbose=False
                )
            else:
                raise ValueError(f"Unsupported MEG extension: {ext}")
            
            # Verify presence of MEG channels
            meg_chs = mne.pick_types(raw.info, meg=True, eeg=False)
            if len(meg_chs) == 0:
                raise ValueError(f"File {self.path} contains no MEG channels, not valid MEG data")
            
        except Exception as e:
            raise ValueError(
                f"MEG file validation failed {self.path}: {str(e)}"
            ) from e

    def _find_kit_mrk_file(self) -> Optional[str]:
        """
        Helper method: Find corresponding .mrk marker file for KIT/RIKEN data
        Looks for same prefix with .mrk extension in the same directory
        """
        if not self.path.endswith(('.sqd', '.con')):
            return None
        
        dir_path = os.path.dirname(self.path)
        file_prefix = os.path.splitext(os.path.basename(self.path))[0]
        mrk_candidates = [
            os.path.join(dir_path, f"{file_prefix}.mrk"),
            os.path.join(dir_path, "marker.mrk"),  # Common default name
            os.path.join(dir_path, "markers.mrk")
        ]
        
        for mrk_path in mrk_candidates:
            if os.path.exists(mrk_path):
                return mrk_path
        
        raise FileNotFoundError(
            f"No .mrk marker file found for KIT MEG data {self.path}\n"
            f"Searched for: {', '.join(mrk_candidates)}"
        )

# ------------------------------
# Implementation of Specific MEG Formats
# ------------------------------
class MegFif(WithMagicNumber, MegFormat):
    """
    MNE FIF format MEG (standard format for NeuroMag/MEGIN devices)
    Shares magic number with EEG FIF format but distinguished by channel type
    """
    extensions = (".fif", ".fif.gz")
    magic_number = b"\x46\x49\x46\x32"  # "FIF2"
    mime_type = "application/x-meg-fif"

class MegCtf(DirectoryFormat, MegFormat):
    """
    CTF format MEG (directory-based, proprietary format for CTF MEG devices)
    Core files include *.meg4/*.info under .ds directory
    """
    extensions = (".ds",)  # Default directory extension for CTF format
    mime_type = "application/x-meg-ctf"

    def validate(self):
        """
        Override validation: Check CTF directory integrity + MEG channel legitimacy
        """
        super().validate()
        
        # Check core files in CTF directory
        required_suffixes = (".meg4", ".info")
        has_required = False
        for file in os.listdir(self.path):
            if any(file.endswith(suf) for suf in required_suffixes):
                has_required = True
                break
        
        if not has_required:
            raise ValueError(
                f"CTF MEG directory {self.path} is incomplete: "
                "Missing core .meg4 or .info files"
            )
        
        # Validate CTF directory with MNE
        try:
            raw = mne.io.read_raw_ctf(self.path, verbose=False)
            meg_chs = mne.pick_types(raw.info, meg=True, eeg=False)
            if len(meg_chs) == 0:
                raise ValueError("No MEG channels")
        except Exception as e:
            raise ValueError(f"CTF MEG validation failed: {str(e)}") from e

class MegKit(DirectoryFormat, MegFormat):
    """
    KIT/RIKEN (Ricon) MEG format (directory-based)
    Required files: 
    - Main data file (.sqd or .con)
    - Marker file (.mrk) 
    Optional files: .elp (head position), .hsj (sensor info)
    """
    extensions = (".kit_dir",)  # Custom directory extension for KIT format
    mime_type = "application/x-meg-kit"

    def validate(self):
        """
        Override validation: Check KIT/RIKEN directory integrity + MEG channel legitimacy
        Follows MNE's read_raw_kit requirements (https://mne.tools/stable/generated/mne.io.read_raw_kit.html)
        """
        super().validate()  # Execute directory base validation
        
        # Step 1: Find main data file (.sqd or .con) in directory
        main_file_path = None
        for file in os.listdir(self.path):
            if file.lower().endswith(('.sqd', '.con')):
                main_file_path = os.path.join(self.path, file)
                break
        
        if not main_file_path:
            raise ValueError(
                f"KIT MEG directory {self.path} is incomplete: "
                "Missing main data file (.sqd or .con)"
            )
        
        # Step 2: Find marker file (.mrk) (required for MNE read_raw_kit)
        mrk_path = None
        file_prefix = os.path.splitext(os.path.basename(main_file_path))[0]
        # First try matching prefix (e.g., data.sqd → data.mrk)
        candidate_mrk = os.path.join(self.path, f"{file_prefix}.mrk")
        if os.path.exists(candidate_mrk):
            mrk_path = candidate_mrk
        # Fallback to common marker filenames
        else:
            for mrk_candidate in ["marker.mrk", "markers.mrk", "kit.mrk"]:
                mrk_candidate_path = os.path.join(self.path, mrk_candidate)
                if os.path.exists(mrk_candidate_path):
                    mrk_path = mrk_candidate_path
                    break
        
        if not mrk_path:
            raise ValueError(
                f"KIT MEG directory {self.path} is incomplete: "
                f"No .mrk marker file found (searched for {file_prefix}.mrk, marker.mrk, etc.)"
            )
        
        # Step 3: Find optional files (provide warnings if missing but don't fail)
        optional_files = {
            "head position (.elp)": os.path.join(self.path, f"{file_prefix}.elp"),
            "sensor info (.hsj)": os.path.join(self.path, f"{file_prefix}.hsj")
        }
        for file_desc, file_path in optional_files.items():
            if not os.path.exists(file_path):
                print(f"⚠️  Warning: Missing optional {file_desc} for KIT MEG data (path: {file_path})")
        
        # Step 4: Validate KIT data with MNE read_raw_kit
        try:
            # Read KIT data (supports both .sqd and .con)
            raw = mne.io.read_raw_kit(
                main_file_path,
                mrk=mrk_path,
                elp=optional_files["head position (.elp)"] if os.path.exists(optional_files["head position (.elp)"]) else None,
                hsp=optional_files["sensor info (.hsj)"] if os.path.exists(optional_files["sensor info (.hsj)"]) else None,
                verbose=False
            )
            
            # Verify presence of MEG channels
            meg_chs = mne.pick_types(raw.info, meg=True, eeg=False)
            if len(meg_chs) == 0:
                raise ValueError("No MEG channels found in KIT data")
            
        except Exception as e:
            raise ValueError(f"KIT MEG validation failed: {str(e)}") from e
        
        print(f"✅ KIT MEG validation passed (main file: {main_file_path}, marker file: {mrk_path})")