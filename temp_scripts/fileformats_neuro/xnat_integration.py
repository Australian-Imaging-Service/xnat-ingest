"""
Register EEG/MEG data types to XNAT Repo to support XNAT Ingest workflows
"""
import os
import mne
from arcana.xnat import XnatRepo
from .eeg import EegFif, EegEdf, EegBv
from .meg import MegFif, MegCtf, MegKit

# ------------------------------
# Register EEG Formats to XNAT
# ------------------------------
# 1. FIF format EEG
XnatRepo.register_format(
    format_cls=EegFif,
    xnat_resource="EEG_FIF",  # Resource name for storing this format in XNAT
    xnat_file_ext=".fif",     # File extension used in XNAT
    # Optional: Map XNAT metadata fields (extracted from EEG file)
    metadata_mapping={
        "subject_id": lambda f: mne.io.read_raw_fif(f.path, verbose=False).info["subject_info"]["id"],
        "recording_date": lambda f: mne.io.read_raw_fif(f.path, verbose=False).info["meas_date"].isoformat()
    }
)

# 2. EDF format EEG
XnatRepo.register_format(
    format_cls=EegEdf,
    xnat_resource="EEG_EDF",
    xnat_file_ext=".edf"
)

# 3. BrainVision format EEG (directory-based)
XnatRepo.register_format(
    format_cls=EegBv,
    xnat_resource="EEG_BRAINVISION",
    xnat_file_ext=".bv_dir",
    # Directory-based formats need to specify XNAT zip compression option
    zip_directory=True  # Compress directory to zip for storage in XNAT
)


# ------------------------------
# Register MEG Formats to XNAT
# ------------------------------
# 1. MEGIN FIF format MEG
XnatRepo.register_format(
    format_cls=MegFif,
    xnat_resource="MEG_FIF",
    xnat_file_ext=".fif"
)

# 2. CTF format MEG (directory-based)
XnatRepo.register_format(
    format_cls=MegCtf,
    xnat_resource="MEG_CTF",
    xnat_file_ext=".ds",
    zip_directory=True
)

# 3. KIT/RIKEN (Ricoh) format MEG (directory-based)
XnatRepo.register_format(
    format_cls=MegKit,
    xnat_resource="MEG_KIT",
    xnat_file_ext=".kit_dir",
    zip_directory=True,  # Compress KIT directory to zip for XNAT storage
    # Optional: Metadata mapping for KIT format
    metadata_mapping={
        "recording_duration": lambda f: mne.io.read_raw_kit(
            [os.path.join(f.path, fn) for fn in os.listdir(f.path) if fn.endswith(('.sqd', '.con'))][0],
            mrk=f._find_kit_mrk_file(),
            verbose=False
        ).times[-1],
        "meg_channel_count": lambda f: len(mne.pick_types(
            mne.io.read_raw_kit(
                [os.path.join(f.path, fn) for fn in os.listdir(f.path) if fn.endswith(('.sqd', '.con'))][0],
                mrk=f._find_kit_mrk_file(),
                verbose=False
            ).info,
            meg=True, eeg=False
        ))
    }
)