from pathlib import Path
import pytest
from fileformats.core import from_mime
from fileformats.medimage import (
    DicomSeries,
    Vnd_Siemens_Biograph128Vision_Vr20b_PetCountRate,
    Vnd_Siemens_Biograph128Vision_Vr20b_PetListMode,
    Vnd_Siemens_Biograph128Vision_Vr20b_PetSinogram,
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
from xnat_ingest.session import ImagingSession, ImagingScan, DummySpace
from xnat_ingest.utils import AssociatedFiles


FIRST_NAME = "GivenName"
LAST_NAME = "FamilyName"


@pytest.fixture
def imaging_session() -> ImagingSession:
    PatientName = f"{FIRST_NAME}^{LAST_NAME}"
    dicoms = [
        DicomSeries(d.iterdir())
        for d in (
            get_pet_image(PatientName=PatientName),
            get_ac_image(PatientName=PatientName),
            get_topogram_image(PatientName=PatientName),
            get_statistics_image(PatientName=PatientName),
        )
    ]
    scans = [
        ImagingScan(
            id=str(d["SeriesNumber"]),
            type=str(d["SeriesDescription"]),
            resources={"DICOM": d}) for d in dicoms]
    return ImagingSession(
        project_id="PROJECTID",
        subject_id="SUBJECTID",
        session_id="SESSIONID",
        scans=scans,
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
            "medimage/vnd.siemens.biograph128-vision.vr20b.pet-list-mode",
            ".*/LISTMODE",
        ),
        (
            "sinogram",
            "medimage/vnd.siemens.biograph128-vision.vr20b.pet-sinogram",
            ".*/EM_SINO",
        ),
        (
            "countrate",
            "medimage/vnd.siemens.biograph128-vision.vr20b.pet-count-rate",
            ".*/COUNTRATE",
        ),
    ]:
        dataset.add_source(col_name, from_mime(col_type), col_pattern, is_regex=True)
    return dataset


def test_session_select_resources(
    imaging_session: ImagingSession, dataset: Dataset, tmp_path: Path
):

    assoc_dir = tmp_path / "assoc"
    assoc_dir.mkdir()

    for fspath in get_raw_data_files(
        first_name=FIRST_NAME, last_name=LAST_NAME
    ):
        fspath.rename(assoc_dir / fspath.name)

    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()

    staged_session = imaging_session.stage(
        staging_dir,
        associated_files=AssociatedFiles(
            str(assoc_dir) + "/{PatientName.given_name}_{PatientName.family_name}*.ptd",
            r".*/[^\.]+.[^\.]+.[^\.]+.(?P<id>\d+)\.[A-Z]+_(?P<resource>[^\.]+).*"
        ),
    )

    resources = list(staged_session.select_resources(dataset))

    assert len(resources) == 6
    ids, descs, resource_names, scans = zip(*resources)
    assert set(ids) == set(("1", "2", "4", "602", "603"))
    assert set(descs) == set(
        [
            "AC CT 3.0  SWB HD_FoV",
            "PET SWB 8MIN",
            "Topogram 0.6 Tr60",
            "602",
            "603",
        ]
    )
    assert set(resource_names) == set(
        ("DICOM", "LISTMODE", "COUNTRATE", "EM_SINO")
    )
    assert set(type(s) for s in scans) == set(
        [
            DicomSeries,
            Vnd_Siemens_Biograph128Vision_Vr20b_PetListMode,
            Vnd_Siemens_Biograph128Vision_Vr20b_PetCountRate,
            Vnd_Siemens_Biograph128Vision_Vr20b_PetSinogram,
        ]
    )


def test_session_save_roundtrip(tmp_path: Path, imaging_session: ImagingSession):

    save_dir = tmp_path / imaging_session.project_id / imaging_session.subject_id / imaging_session.session_id
    save_dir.mkdir(parents=True)

    saved = imaging_session.save(save_dir)
    reloaded = ImagingSession.load(save_dir)

    assert reloaded is not saved
    assert reloaded == saved

    reloaded.save(save_dir)
    rereloaded = ImagingSession.load(save_dir)

    assert rereloaded == saved

    loaded_no_manifest = ImagingSession.load(save_dir, ignore_manifest=True)

    for scan in loaded_no_manifest.scans.values():
        for key, resource in list(scan.resources.items()):
            if key == "DICOM":
                scan.resources[key] = DicomSeries(resource)
    assert loaded_no_manifest == saved
