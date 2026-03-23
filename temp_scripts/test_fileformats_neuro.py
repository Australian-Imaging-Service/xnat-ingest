"""
Test validation and XNAT integration of EEG/MEG data types
Uses sample data (MNE auto-downloaded) for testing.

Authors:
- Miao Cao

Email:
- miaocao@swin.edu.au
"""
import os
import mne

from fileformats_neuro import (
    EegFif, EegEdf, EegBv,
    MegFif, MegCtf, MegKit
)

from fileformats_neuro.xnat_integration import *

# ------------------------------
# Prepare Test Data (MNE Sample Data)
# ------------------------------
def download_test_data():
    """Download MNE sample EEG/MEG data (including KIT/RIKEN sample data)"""
    # EEG FIF data
    eeg_fif_path = mne.datasets.sample.data_path() / "MEG" / "sample" / "sample_audvis_eeg.fif"
    # MEG FIF data
    meg_fif_path = mne.datasets.sample.data_path() / "MEG" / "sample" / "sample_audvis_raw.fif"
    # EDF sample data (built-in to MNE)
    edf_path = mne.datasets.eegbci.load_data(1, 1)[0]
    # BrainVision sample data
    bv_dir = mne.datasets.brainvision.data_path()
    # CTF MEG sample data
    meg_ctf_path = mne.datasets.ctf.data_path()
    # KIT/RIKEN MEG sample data (MNE sample KIT data)
    meg_kit_path = mne.datasets.kit.data_path()
    
    return {
        "eeg_fif": str(eeg_fif_path),
        "meg_fif": str(meg_fif_path),
        "eeg_edf": str(edf_path),
        "eeg_bv" : str(bv_dir),
        "meg_ctf": meg_ctf_path,
        "meg_kit": meg_kit_path,        # KIT MEG data path
    }

# ------------------------------
# Execute Tests
# ------------------------------
if __name__ == "__main__":
    # Download test data
    test_data = download_test_data()
    print("Test data paths:", test_data)
    
    # 1. Test EEG formats
    print("\n=== Testing EEG Format Validation ===")
    # Test FIF format
    eeg_fif = EegFif(test_data["eeg_fif"])
    eeg_fif.validate()
    print(" EEG FIF format validation passed")
    
    # Test EDF format
    eeg_edf = EegEdf(test_data["eeg_edf"])
    eeg_edf.validate()
    print(" EEG EDF format validation passed")
    
    # Test BrainVision format
    eeg_bv = EegBv(test_data["eeg_bv"])
    eeg_bv.validate()
    print(" EEG BrainVision format validation passed")
    
    # 2. Test MEG formats
    print("\n=== Testing MEG Format Validation ===")
    # Test FIF format
    meg_fif = MegFif(test_data["meg_fif"])
    meg_fif.validate()
    print(" MEG FIF format validation passed")
    
    # Test CTF format
    meg_ctf = MegCtf(test_data["meg_ctf"])
    meg_ctf.validate()
    print(" MEG CTF format validation passed")
    
    # Test KIT/RIKEN format
    meg_kit = MegKit(test_data["meg_kit"])
    meg_kit.validate()
    print(" MEG KIT/RIKEN (Ricon) format validation passed")
    
    # 3. Test XNAT registration (verify no errors)
    print("\n=== Testing XNAT Format Registration ===")
    # Check if formats are registered
    registered_formats = XnatRepo.registered_formats()
    assert "EEG_FIF" in [f["xnat_resource"] for f in registered_formats]
    assert "MEG_FIF" in [f["xnat_resource"] for f in registered_formats]
    assert "MEG_KIT" in [f["xnat_resource"] for f in registered_formats]
    print(" XNAT format registration validation passed")
    
    print("\n All tests passed! EEG/MEG data types (including KIT/RIKEN) are ready for XNAT Ingest workflow")