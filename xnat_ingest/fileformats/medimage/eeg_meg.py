"""EEG/MEG data format definitions for fileformats integration"""
from fileformats.core import FileSet, DataType
from fileformats.medimage import MedicalImage


class EEGData(MedicalImage, FileSet):
    """Base class for EEG (Electroencephalography) data files
    
    Supports common EEG formats:
    - .cnt (Neuroscan)
    - .edf/.bdf (European Data Format)
    - .vhdr/.vmrk/.eeg (BrainVision)
    - .set/.fdt (EEGLAB)
    """
    primary_ext = ".eeg"
    mime = "application/x-eeg"
    alternative_exts = [".cnt", ".edf", ".bdf", ".vhdr", ".set"]

    @classmethod
    def match(cls, path):
        """Match EEG files by extension or header"""
        if super().match(path):
            return True
        # Additional heuristic matching for EEG files
        if path.suffix.lower() in [".vmrk", ".fdt"]:
            return True
        return False


class MEGData(MedicalImage, FileSet):
    """Base class for MEG (Magnetoencephalography) data files
    
    Supports common MEG formats:
    - .fif (MEGIN/Elekta/Neuromag)
    - .sqd (CTF)
    - .con (4D Neuroimaging)
    """
    primary_ext = ".meg"
    mime = "application/x-meg"
    alternative_exts = [".fif", ".sqd", ".con"]

    @classmethod
    def match(cls, path):
        """Match MEG files by extension"""
        if super().match(path):
            return True
        # Check for compressed FIF files
        if path.suffix.lower() == ".fif.gz":
            return True
        return False


# Vendor-specific subtypes
class NeuroscanEEGData(EEGData):
    """Neuroscan CNT format EEG data"""
    primary_ext = ".cnt"
    mime = "application/x-neuroscan-eeg"


class BrainVisionEEGData(EEGData):
    """BrainVision VHDR/VMRK/EEG format"""
    primary_ext = ".vhdr"
    mime = "application/x-brainvision-eeg"


class EeglabEEGData(EEGData):
    """EEGLAB SET/FDT format"""
    primary_ext = ".set"
    mime = "application/x-eeglab-eeg"


class NeuromagMEGData(MEGData):
    """MEGIN/Neuromag/Elekta FIF format MEG data"""
    primary_ext = ".fif"
    mime = "application/x-neuromag-meg"


class CtfMEGData(MEGData):
    """CTF SQD format MEG data"""
    primary_ext = ".sqd"
    mime = "application/x-ctf-meg"
