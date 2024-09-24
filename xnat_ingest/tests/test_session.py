from pathlib import Path
import pytest
from fileformats.core import from_mime, FileSet
from fileformats.medimage import (
    DicomSeries,
    Vnd_Siemens_Biograph128Vision_Vr20b_PetRawData,
    Vnd_Siemens_Biograph128Vision_Vr20b_PetCountRate,
    Vnd_Siemens_Biograph128Vision_Vr20b_PetListMode,
)
from frametree.core.frameset import FrameSet  # type: ignore[import-untyped]
from frametree.common import FileSystem  # type: ignore[import-untyped]
from medimages4tests.dummy.dicom.base import default_dicom_dir  # type: ignore[import-untyped]
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (  # type: ignore[import-untyped]
    get_image as get_pet_image,
    __file__ as pet_src_file,
)
from medimages4tests.dummy.dicom.ct.ac.siemens.biograph_vision.vr20b import (  # type: ignore[import-untyped]
    get_image as get_ac_image,
    __file__ as ac_src_file,
)
from medimages4tests.dummy.dicom.pet.topogram.siemens.biograph_vision.vr20b import (  # type: ignore[import-untyped]
    get_image as get_topogram_image,
    __file__ as topogram_src_file,
)
from medimages4tests.dummy.dicom.pet.statistics.siemens.biograph_vision.vr20b import (  # type: ignore[import-untyped]
    get_image as get_statistics_image,
    __file__ as statistics_src_file,
)
from xnat_ingest.session import ImagingSession, ImagingScan, DummyAxes
from xnat_ingest.utils import AssociatedFiles
from conftest import get_raw_data_files

FIRST_NAME = "Given Name"
LAST_NAME = "FamilyName"


@pytest.fixture
def imaging_session() -> ImagingSession:
    dicoms = [
        DicomSeries(d.iterdir())
        for d in (
            get_pet_image(
                out_dir=default_dicom_dir(pet_src_file).with_suffix(".with-spaces"),
                first_name=FIRST_NAME,
                last_name=LAST_NAME,
            ),
            get_ac_image(
                out_dir=default_dicom_dir(ac_src_file).with_suffix(".with-spaces"),
                first_name=FIRST_NAME,
                last_name=LAST_NAME,
            ),
            get_topogram_image(
                out_dir=default_dicom_dir(topogram_src_file).with_suffix(
                    ".with-spaces"
                ),
                first_name=FIRST_NAME,
                last_name=LAST_NAME,
            ),
            get_statistics_image(
                out_dir=default_dicom_dir(statistics_src_file).with_suffix(
                    ".with-spaces"
                ),
                first_name=FIRST_NAME,
                last_name=LAST_NAME,
            ),
        )
    ]
    scans = [
        ImagingScan(
            id=str(d.metadata["SeriesNumber"]),
            type=str(d.metadata["SeriesDescription"]),
            resources={"DICOM": d},
        )
        for d in dicoms
    ]
    return ImagingSession(
        project_id="PROJECTID",
        subject_id="SUBJECTID",
        visit_id="SESSIONID",
        scans=scans,
    )


@pytest.fixture
def dataset(tmp_path: Path) -> FrameSet:
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
    store = FileSystem()
    dataset = store.create_dataset(
        id=dataset_path,
        leaves=[],
        hierarchy=[],
        axes=DummyAxes,
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
        # (
        #     "sinogram",
        #     "medimage/vnd.siemens.biograph128-vision.vr20b.pet-sinogram",
        #     ".*/EM_SINO",
        # ),
        (
            "countrate",
            "medimage/vnd.siemens.biograph128-vision.vr20b.pet-count-rate",
            ".*/COUNTRATE",
        ),
    ]:
        dataset.add_source(col_name, from_mime(col_type), col_pattern, is_regex=True)
    return dataset


# @pytest.mark.xfail(
#     condition=platform.system() == "Linux", reason="Not working on ubuntu"
# )
def test_session_select_resources(
    imaging_session: ImagingSession, dataset: FrameSet, tmp_path: Path
):

    assoc_dir = tmp_path / "assoc"
    assoc_dir.mkdir()

    get_raw_data_files(
        out_dir=assoc_dir, first_name=FIRST_NAME.replace(" ", "_"), last_name=LAST_NAME
    )

    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()

    staged_session = imaging_session.stage(
        staging_dir,
        associated_file_groups=[
            AssociatedFiles(
                Vnd_Siemens_Biograph128Vision_Vr20b_PetRawData,
                str(assoc_dir)
                + "/{PatientName.family_name}_{PatientName.given_name}*.ptd",
                r".*/[^\.]+.[^\.]+.[^\.]+.(?P<id>\d+)\.[A-Z]+_(?P<resource>[^\.]+).*",
            )
        ],
        spaces_to_underscores=True,
    )

    resources = list(staged_session.select_resources(dataset))

    assert len(resources) == 5  # 6
    ids, descs, resource_names, scans = zip(*resources)
    assert set(ids) == set(("1", "2", "4", "602"))  # , "603"))
    assert set(descs) == set(
        [
            "AC CT 30  SWB HD_FoV",
            "PET SWB 8MIN",
            "Topogram 06 Tr60",
            "602",
            # "603",
        ]
    )
    assert set(resource_names) == set(("DICOM", "LISTMODE", "COUNTRATE"))  # , "EM_SINO"
    assert set(type(s) for s in scans) == set(
        [
            DicomSeries,
            Vnd_Siemens_Biograph128Vision_Vr20b_PetListMode,
            Vnd_Siemens_Biograph128Vision_Vr20b_PetCountRate,
            # Vnd_Siemens_Biograph128Vision_Vr20b_PetSinogram,
        ]
    )


def test_session_save_roundtrip(tmp_path: Path, imaging_session: ImagingSession):

    # Save imaging sessions to a temporary directory
    saved = imaging_session.save(tmp_path)
    assert saved is not imaging_session

    # Calculate where the session should have been saved to
    session_dir = tmp_path.joinpath(*imaging_session.staging_relpath)
    reloaded = ImagingSession.load(session_dir)

    # Check that reloaded session matches saved session, should match the original just
    # the paths should be different
    assert reloaded == saved

    # Save again to the same location (files shouldn't be overwritten)
    reloaded.save(tmp_path)
    rereloaded = ImagingSession.load(session_dir)
    assert rereloaded == saved

    # Load from saved directory, this time only using directory structure instead of
    # manifest. Should be the same with the exception of the detected fileformats
    loaded_no_manifest = ImagingSession.load(session_dir, use_manifest=False)
    for scan in loaded_no_manifest.scans.values():
        for key, resource in list(scan.resources.items()):
            if key == "DICOM":
                assert isinstance(resource, FileSet)
                scan.resources[key] = DicomSeries(resource)
    assert loaded_no_manifest == saved
