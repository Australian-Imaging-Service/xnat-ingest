"""
Pytest tests for EEG/MEG file format validation and metadata reading.

Test data is downloaded via MNE's dataset utilities and cached for the session.

Authors:
- Miao Cao

Email:
- miaocao@swin.edu.au
"""

from pathlib import Path

import mne
import mne.datasets
import pytest

from fileformats_neuro.eeg import (
    BrainVision,
    BrainVisionHeader,
    BrainVisionMarker,
    Edf,
    Fif,
)
from fileformats_neuro.eeg_extras import (
    brain_vision_read_metadata,
    edf_read_metadata,
    fif_read_metadata,
)
from fileformats_neuro.meg import Ctf, CtfInfo, CtfMeg4, CtfRes4, Kit
from fileformats_neuro.meg_extras import ctf_read_metadata, kit_read_metadata


# ------------------------------
# Session-scoped fixtures
# ------------------------------


@pytest.fixture(scope="session")
def sample_data_path() -> Path:
    return Path(mne.datasets.sample.data_path())


@pytest.fixture(scope="session")
def testing_data_path() -> Path:
    return Path(mne.datasets.testing.data_path())


@pytest.fixture(scope="session")
def fif_path(sample_data_path) -> Path:
    return sample_data_path / "MEG" / "sample" / "sample_audvis_raw.fif"


@pytest.fixture(scope="session")
def edf_path() -> Path:
    return Path(mne.datasets.eegbci.load_data(subject=1, runs=[1])[0])


@pytest.fixture(scope="session")
def bv_vhdr_path(testing_data_path) -> Path:
    return testing_data_path / "BrainVision" / "test.vhdr"


@pytest.fixture(scope="session")
def ctf_ds_path(testing_data_path) -> Path:
    return testing_data_path / "CTF" / "testdata_ctf.ds"


@pytest.fixture(scope="session")
def kit_sqd_path(testing_data_path) -> Path:
    return testing_data_path / "KIT" / "test.sqd"


# ------------------------------
# EEG: FIF
# ------------------------------


def test_fif_instantiate(fif_path):
    Fif(fif_path)


def test_fif_read_metadata(fif_path):
    metadata = fif_read_metadata(Fif(fif_path))
    assert isinstance(metadata, dict)
    assert metadata["sfreq"] is not None


# ------------------------------
# EEG: EDF
# ------------------------------


def test_edf_instantiate(edf_path):
    Edf(edf_path)


def test_edf_read_metadata(edf_path):
    metadata = edf_read_metadata(Edf(edf_path))
    assert isinstance(metadata, dict)
    assert metadata["sfreq"] is not None
    assert "edf_patient_code" in metadata


# ------------------------------
# EEG: BrainVision
# ------------------------------


def test_brainvision_header_instantiate(bv_vhdr_path):
    BrainVisionHeader(bv_vhdr_path)


def test_brainvision_marker_instantiate(bv_vhdr_path):
    BrainVisionMarker(bv_vhdr_path.with_suffix(".vmrk"))


def test_brainvision_data_instantiate(bv_vhdr_path):
    BrainVision(bv_vhdr_path.with_suffix(".eeg"))


def test_brainvision_read_metadata(bv_vhdr_path):
    metadata = brain_vision_read_metadata(BrainVision(bv_vhdr_path.with_suffix(".eeg")))
    assert isinstance(metadata, dict)
    assert metadata["sfreq"] is not None
    assert "bv_n_channels" in metadata


# ------------------------------
# MEG: CTF
# ------------------------------


def test_ctf_instantiate(ctf_ds_path):
    Ctf(ctf_ds_path)


def test_ctf_meg4_instantiate(ctf_ds_path):
    meg4_files = list(ctf_ds_path.glob("*.meg4"))
    assert meg4_files, "No .meg4 file found in CTF .ds directory"
    CtfMeg4(meg4_files[0])


def test_ctf_res4_instantiate(ctf_ds_path):
    res4_files = list(ctf_ds_path.glob("*.res4"))
    assert res4_files, "No .res4 file found in CTF .ds directory"
    CtfRes4(res4_files[0])


def test_ctf_infods_instantiate(ctf_ds_path):
    infods_files = list(ctf_ds_path.glob("*.infods"))
    assert infods_files, "No .infods file found in CTF .ds directory"
    CtfInfo(infods_files[0])


def test_ctf_read_metadata(ctf_ds_path):
    metadata = ctf_read_metadata(Ctf(ctf_ds_path))
    assert isinstance(metadata, dict)
    assert metadata["sfreq"] is not None


# ------------------------------
# MEG: KIT
# ------------------------------


def test_kit_instantiate(kit_sqd_path):
    Kit(kit_sqd_path)


def test_kit_read_metadata(kit_sqd_path):
    metadata = kit_read_metadata(Kit(kit_sqd_path))
    assert isinstance(metadata, dict)
    assert metadata["sfreq"] is not None
