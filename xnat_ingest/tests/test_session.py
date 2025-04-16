from pathlib import Path
import pytest
import typing as ty
from fileformats.core import from_mime
from fileformats.medimage import (
    DicomSeries,
    Vnd_Siemens_Biograph128Vision_Vr20b_PetRawData,
    Vnd_Siemens_Biograph128Vision_Vr20b_PetCountRate,
    Vnd_Siemens_Biograph128Vision_Vr20b_PetListMode,
)
from frametree.core.frameset import FrameSet  # type: ignore[import-untyped]
from frametree.common import FileSystem  # type: ignore[import-untyped]
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (  # type: ignore[import-untyped]
    get_image as get_pet_image,
)
from medimages4tests.dummy.dicom.ct.ac.siemens.biograph_vision.vr20b import (  # type: ignore[import-untyped]
    get_image as get_ac_image,
)
from medimages4tests.dummy.dicom.pet.topogram.siemens.biograph_vision.vr20b import (  # type: ignore[import-untyped]
    get_image as get_topogram_image,
)
from medimages4tests.dummy.dicom.pet.statistics.siemens.biograph_vision.vr20b import (  # type: ignore[import-untyped]
    get_image as get_statistics_image,
)
from xnat_ingest.session import ImagingSession, ImagingScan
from xnat_ingest.store import DummyAxes
from xnat_ingest.utils import AssociatedFiles
from conftest import get_raw_data_files

FIRST_NAME = "Given Name"
LAST_NAME = "FamilyName"

DICOM_COLUMNS: ty.List[ty.Tuple[str, str, str]] = [
    ("pet", "medimage/dicom-series", "PET SWB 8MIN"),
    ("topogram", "medimage/dicom-series", "Topogram.*"),
    ("atten_corr", "medimage/dicom-series", "AC CT.*"),
]

RAW_COLUMNS: ty.List[ty.Tuple[str, str, str]] = [
    (
        "listmode",
        "medimage/vnd.siemens.biograph128-vision.vr20b.pet-list-mode",
        ".*/PET_LISTMODE",
    ),
    # (
    #     "sinogram",
    #     "medimage/vnd.siemens.biograph128-vision.vr20b.pet-sinogram",
    #     ".*/PET_EM_SINO",
    # ),
    (
        "countrate",
        "medimage/vnd.siemens.biograph128-vision.vr20b.pet-count-rate",
        ".*/PET_COUNTRATE",
    ),
]


@pytest.fixture
def imaging_session() -> ImagingSession:
    dicoms = [
        DicomSeries(d.iterdir())
        for d in (
            get_pet_image(
                first_name=FIRST_NAME,
                last_name=LAST_NAME,
            ),
            get_ac_image(
                first_name=FIRST_NAME,
                last_name=LAST_NAME,
            ),
            get_topogram_image(
                first_name=FIRST_NAME,
                last_name=LAST_NAME,
            ),
            get_statistics_image(
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
    for col_name, col_type, col_pattern in DICOM_COLUMNS + RAW_COLUMNS:
        dataset.add_source(col_name, from_mime(col_type), col_pattern, is_regex=True)
    return dataset


@pytest.fixture
def raw_frameset(tmp_path: Path) -> FrameSet:
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
    for col_name, col_type, col_pattern in RAW_COLUMNS:
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

    imaging_session.associate_files(
        patterns=[
            AssociatedFiles(
                Vnd_Siemens_Biograph128Vision_Vr20b_PetRawData,
                str(assoc_dir)
                + "/{PatientName.family_name}_{PatientName.given_name}*.ptd",
                r".*/[^\.]+.[^\.]+.[^\.]+.(?P<id>\d+)\.(?P<resource>[^\.]+).*",
            )
        ],
        spaces_to_underscores=True,
    )

    saved_session, saved_dir = imaging_session.save(staging_dir)

    resources_iter = saved_session.select_resources(dataset)
    resources = list(resources_iter)

    assert len(resources) == 5  # 6
    assert set([r.scan.id for r in resources]) == set(
        ("1", "2", "4", "602")
    )  # , "603"))
    assert set([r.scan.type for r in resources]) == set(
        [
            "AC CT 30  SWB HD_FoV",
            "PET SWB 8MIN",
            "Topogram 06 Tr60",
            "602",
            # "603",
        ]
    )
    assert set([r.name for r in resources]) == set(
        ("DICOM", "PET_LISTMODE", "PET_COUNTRATE")
    )  # , "PET_EM_SINO"
    assert set([r.datatype for r in resources]) == set(
        [
            DicomSeries,
            Vnd_Siemens_Biograph128Vision_Vr20b_PetListMode,
            Vnd_Siemens_Biograph128Vision_Vr20b_PetCountRate,
            # Vnd_Siemens_Biograph128Vision_Vr20b_PetSinogram,
        ]
    )


def test_session_save_roundtrip(tmp_path: Path, imaging_session: ImagingSession):

    # Save imaging sessions to a temporary directory
    saved, _ = imaging_session.save(tmp_path)
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

    # # Load from saved directory, this time only using directory structure instead of
    # # manifest. Should be the same with the exception of the detected fileformats
    # loaded_no_manifest = ImagingSession.load(session_dir, require_manifest=False)
    # for scan in loaded_no_manifest.scans.values():
    #     for key, resource in list(scan.resources.items()):
    #         if key == "DICOM":
    #             assert isinstance(resource, FileSet)
    #             scan.resources[key] = DicomSeries(resource)
    # assert loaded_no_manifest == saved


def test_stage_raw_data_directly(raw_frameset: FrameSet, tmp_path: Path):

    raw_data_dir = tmp_path / "raw"
    raw_data_dir.mkdir()

    num_sessions = 2

    for i in range(num_sessions):
        sess_dir = raw_data_dir / str(i)
        sess_dir.mkdir()
        get_raw_data_files(
            out_dir=sess_dir,
            first_name=FIRST_NAME + str(i),
            last_name=LAST_NAME + str(i),
            StudyID=f"Study{i}",
            PatientID=f"Patient{i}",
            AccessionNumber=f"AccessionNumber{i}",
            StudyInstanceUID=f"StudyInstanceUID{i}",
        )

    imaging_sessions = ImagingSession.from_paths(
        f"{raw_data_dir}/**/*.ptd",
        datatypes=[
            Vnd_Siemens_Biograph128Vision_Vr20b_PetListMode,
            Vnd_Siemens_Biograph128Vision_Vr20b_PetCountRate,
        ],
    )

    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()

    staged_sessions = []

    for imaging_session in imaging_sessions:
        staged_sessions.append(
            imaging_session.save(
                staging_dir,
            )[0]
        )

    for staged_session in staged_sessions:
        resources = list(staged_session.select_resources(raw_frameset))

        assert len(resources) == 2
        assert set([r.scan.id for r in resources]) == set(["602"])
        assert set([r.scan.type for r in resources]) == set(["PET Raw Data"])
        assert set(r.name for r in resources) == set(("PET_LISTMODE", "PET_COUNTRATE"))
        assert set(type(r.fileset) for r in resources) == set(
            [
                Vnd_Siemens_Biograph128Vision_Vr20b_PetListMode,
                Vnd_Siemens_Biograph128Vision_Vr20b_PetCountRate,
            ]
        )
