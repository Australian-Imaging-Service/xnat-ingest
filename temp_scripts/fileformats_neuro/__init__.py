"""
Fileformats extension package: biosignal (EEG/MEG) data types.

Defines and validates EEG/MEG file formats for XNAT Ingest workflows.

Authors:
- Miao Cao

Email:
- miaocao@swin.edu.au
"""

from .eeg import (
    Biosig,
    Eeg,
    Fif,
    FifGz,
    Edf,
    EdfPlus,
    BrainVisionHeader,
    BrainVisionMarker,
    BrainVision,
)
from .meg import (
    Meg,
    CtfMeg4,
    CtfRes4,
    CtfInfo,
    Ctf,
    KitMark,
    KitHeadPosition,
    KitSensorInfo,
    Kit,
)

__version__ = "0.1.0"
