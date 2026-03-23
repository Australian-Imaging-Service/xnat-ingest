"""
Arcana Fileformats extension package: Neuroimaging (EEG/MEG) data types.

Defines and validates EEG/MEG file formats for XNAT Ingest workflows.

Authors:
- Miao Cao

Email:
- miaocao@swin.edu.au
"""

# Export core data types for external use
from .eeg import (
    EegFif,        # MNE FIF format EEG
    EegEdf,        # EDF/EDF+ format EEG
    EegBv          # BrainVision format EEG
)
from .meg import (
    MegFif,        # MNE FIF format MEG
    MegCtf,        # CTF format MEG
    MegKit         # KIT/RIKEN (Ricon) format MEG ← NEW
)

# Version information
__version__ = "0.1.0"
