from pathlib import Path
import pytest
from fileformats.core import from_mime
from fileformats.medimage import (
    DicomSeries,
    Vnd_Siemens_BiographVisionVr20b_PetCountRate,
    Vnd_Siemens_BiographVisionVr20b_PetListMode,
    Vnd_Siemens_BiographVisionVr20b_PetSinogram,
)
from arcana.core.data.set import Dataset
from arcana.common import DirTree
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (
    get_image as get_pet_image,
)
from medimages4tests.dummy.dicom.ct.ac.siemens.biograph_vision.vr20b import (
    get_image as get_ac_image,
)
from medimages4tests.dummy.dicom.pet.topogram.siemens.biograph_vision.vr20b import (
    get_image as get_topogram_image,
)
from medimages4tests.dummy.dicom.pet.statistics.siemens.biograph_vision.vr20b import (
    get_image as get_statistics_image,
)
from medimages4tests.dummy.raw.pet.siemens.biograph_vision.vr20b import (
    get_files as get_raw_data_files,
)
from xnat_ingest.session import ImagingSession, DummySpace


@pytest.fixture
def imaging_session() -> ImagingSession:
    first_name = "GivenName"
    last_name = "FamilyName"
    PatientName = f"{first_name}^{last_name}"
    return ImagingSession(
        project_id="PROJECTID",
        subject_id="SUBJECTID",
        session_id="SESSIONID",
        dicoms=[
            DicomSeries(d.iterdir())
            for d in (
                get_pet_image(PatientName=PatientName),
                get_ac_image(PatientName=PatientName),
                get_topogram_image(PatientName=PatientName),
                get_statistics_image(PatientName=PatientName),
            )
        ],
        non_dicoms_pattern="**/{PatientName.given_name}_{PatientName.family_name}*.ptd",
        non_dicom_fspaths=get_raw_data_files(
            first_name=first_name, last_name=last_name
        ),
    )


@pytest.fixture
def dataset(tmp_path: Path) -> Dataset:
    """For use in tests, this method creates a test dataset from the provided
    blueprint

    Parameters
    ----------
    store: DataStore
        the store to make the dataset within
    dataset_id : str
        the ID of the project/directory within the store to create the dataset
    name : str, optional
        the name to give the dataset. If provided the dataset is also saved in the
        datastore
    source_data : Path, optional
        path to a directory containing source data to use instead of the dummy
        data
    **kwargs
        passed through to create_dataset
    """
    dataset_path = tmp_path / "a-dataset"
    store = DirTree()
    dataset = store.create_dataset(
        id=dataset_path,
        leaves=[],
        hierarchy=[],
        space=DummySpace,
    )
    for col_name, col_type, col_pattern in [
        ("pet", "medimage/dicom-series", "PET SWB 8MIN"),
        ("topogram", "medimage/dicom-series", "Topogram.*"),
        ("atten_corr", "medimage/dicom-series", "AC CT.*"),
        (
            "listmode",
            "medimage/vnd.siemens.biograph-vision-vr20b.pet-list-mode",
            ".*PET_LISTMODE.*",
        ),
        (
            "sinogram",
            "medimage/vnd.siemens.biograph-vision-vr20b.pet-sinogram",
            ".*PET_EM_SINO.*",
        ),
        (
            "countrate",
            "medimage/vnd.siemens.biograph-vision-vr20b.pet-count-rate",
            ".*PET_COUNTRATE.*",
        ),
    ]:
        dataset.add_source(col_name, from_mime(col_type), col_pattern, is_regex=True)
    return dataset


def test_session_select_resources(
    imaging_session: ImagingSession, dataset: Dataset, tmp_path: Path
):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()

    staged_session = imaging_session.deidentify(staging_dir)

    resources = list(staged_session.select_resources(dataset))

    assert len(resources) == 6
    ids, descs, scans = zip(*resources)
    assert sorted(ids) == ["1", "2", "4", "countrate", "listmode", "sinogram"]
    assert sorted(descs) == [
        "AC CT 3.0  SWB HD_FoV",
        "PET SWB 8MIN",
        "PT.PET_U_FDG_SWB_LM_(Adult).602.PET_COUNTRATE.2023.08.25.15.50.51.083000.2.0.52858872",
        "PT.PET_U_FDG_SWB_LM_(Adult).602.PET_LISTMODE.2023.08.25.15.50.51.080000.2.0.52858858",
        "PT.PET_U_FDG_SWB_LM_(Adult).603.PET_EM_SINO.2023.08.25.15.50.51.30.118000.2.0.54764616",
        "Topogram 0.6 Tr60",
    ]
    assert set(type(s) for s in scans) == set(
        [
            DicomSeries,
            Vnd_Siemens_BiographVisionVr20b_PetListMode,
            Vnd_Siemens_BiographVisionVr20b_PetCountRate,
            Vnd_Siemens_BiographVisionVr20b_PetSinogram,
        ]
    )
