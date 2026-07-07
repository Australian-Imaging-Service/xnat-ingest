import logging
import typing as ty
from pathlib import Path

import pytest
import yaml
from fileformats.core import from_mime
from fileformats.generic import File
from fileformats.medimage import DicomSeries
from fileformats.vendor.siemens.medimage import (
    SyngoMi_Vr20b_CountRate,
    SyngoMi_Vr20b_ListMode,
    SyngoMi_Vr20b_RawData,
)
from frametree.common import FileSystem  # type: ignore[import-untyped]
from frametree.core.frameset import FrameSet  # type: ignore[import-untyped]
from medimages4tests.dummy.dicom.ct.ac.siemens.biograph_vision.vr20b import (
    get_image as get_ac_image,  # type: ignore[import-untyped]
)
from medimages4tests.dummy.dicom.pet.statistics.siemens.biograph_vision.vr20b import (
    get_image as get_statistics_image,  # type: ignore[import-untyped]
)
from medimages4tests.dummy.dicom.pet.topogram.siemens.biograph_vision.vr20b import (
    get_image as get_topogram_image,  # type: ignore[import-untyped]
)
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (
    get_image as get_pet_image,  # type: ignore[import-untyped]
)

from conftest import get_raw_data_files
from xnat_ingest.helpers.arg_types import AssociatedFiles, IDSpec, PathMetadataRegex
from xnat_ingest.helpers.metadata import Metadata
from xnat_ingest.model.session import ImagingScan, ImagingSession
from xnat_ingest.model.store import DummyAxes

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
        "medimage/vnd.siemens.syngo-mi.vr20b.list-mode",
        ".*/PET_LISTMODE",
    ),
    # (
    #     "sinogram",
    #     "medimage/vnd.siemens.syngo-mi.vr20b.sinogram",
    #     ".*/PET_EM_SINO",
    # ),
    (
        "countrate",
        "medimage/vnd.siemens.syngo-mi.vr20b.count-rate",
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
        uid="12345",
        project_id="PROJECTID",
        subject_id="SUBJECTID",
        session_id="SESSIONID",
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
) -> None:

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
                SyngoMi_Vr20b_RawData,
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
            SyngoMi_Vr20b_ListMode,
            SyngoMi_Vr20b_CountRate,
            # SyngoMi_Vr20b_Sinogram,
        ]
    )


def test_session_save_roundtrip(
    tmp_path: Path, imaging_session: ImagingSession
) -> None:

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


def test_unlink_keep_metadata(tmp_path: Path, imaging_session: ImagingSession) -> None:
    """unlink(keep_metadata=True) should remove resource directories in their
    entirety, while leaving the scan/session-level metadata behind so the session
    can still be reloaded (e.g. by 'associate' to work out which scan a
    late-arriving file belongs to) without its underlying data"""

    # Force each scan's metadata to be read from its resources before saving, as
    # 'assign' would do in production when resolving a scan description from
    # metadata — otherwise the lazily-populated Metadata objects are still empty
    # at save time and nothing meaningful ends up in '__METADATA__.json'
    for scan in imaging_session.scans.values():
        assert "SeriesDescription" in scan.metadata

    saved, session_dir = imaging_session.save(tmp_path)

    # Sanity check: resource directories exist with data before unlinking, and are
    # direct children of their scan's own directory
    resource_dirs = [
        resource.fileset.parent
        for scan in saved.scans.values()
        for resource in scan.resources.values()
    ]
    scan_dirs = {resource_dir.parent for resource_dir in resource_dirs}
    assert resource_dirs
    for resource_dir in resource_dirs:
        assert resource_dir.exists()
        assert any(resource_dir.iterdir())

    saved.unlink(keep_metadata=True)

    # Resource directories should be gone entirely, scan/session metadata should remain
    for resource_dir in resource_dirs:
        assert not resource_dir.exists()
    for scan_dir in scan_dirs:
        assert (scan_dir / Metadata.FNAME).exists()
    assert (session_dir / Metadata.FNAME).exists()

    # The skeleton should still be loadable, with scan-level metadata intact but no
    # resources
    reloaded = ImagingSession.load(session_dir)
    assert reloaded.uid == saved.uid
    assert reloaded.project_id == saved.project_id
    for scan_id, scan in reloaded.scans.items():
        assert scan.resources == {}
        assert (
            scan.metadata["SeriesDescription"]
            == imaging_session.scans[scan_id].metadata["SeriesDescription"]
        )


def test_stage_raw_data_directly(raw_frameset: FrameSet, tmp_path: Path) -> None:

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
            SyngoMi_Vr20b_ListMode,
            SyngoMi_Vr20b_CountRate,
        ],
        session_field=[IDSpec("StudyInstanceUID")],
        scan_field=[IDSpec("SeriesNumber")],
        resource_field=[IDSpec("ImageType[2:]")],
    )

    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()

    staged_sessions = []

    for imaging_session in imaging_sessions:
        imaging_session.assign(
            project_field="StudyID",
            subject_field="PatientID",
            session_field="StudyInstanceUID",
            scan_field="SeriesDescription",
        )
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
                SyngoMi_Vr20b_ListMode,
                SyngoMi_Vr20b_CountRate,
            ]
        )


def test_path_metadata_regex_extracts_named_groups(tmp_path: Path) -> None:
    raw_data_dir = tmp_path / "raw" / "cohort-A"
    raw_data_dir.mkdir(parents=True)
    get_pet_image(out_dir=raw_data_dir)

    sessions = ImagingSession.from_paths(
        f"{raw_data_dir}/**/*",
        datatypes=[DicomSeries],
        session_field=[IDSpec("StudyInstanceUID")],
        scan_field=[IDSpec("SeriesNumber")],
        resource_field=[IDSpec("ImageType[2:]")],
        path_metadata_regex=[
            PathMetadataRegex(r".*/(?P<cohort>[^/]+)$", DicomSeries),
        ],
    )

    assert len(sessions) == 1
    scan = next(iter(sessions[0].scans.values()))
    resource = next(iter(scan.resources.values()))
    assert resource.metadata["cohort"] == "cohort-A"


def test_path_metadata_regex_no_match_raises(tmp_path: Path) -> None:
    raw_data_dir = tmp_path / "raw" / "cohort-A"
    raw_data_dir.mkdir(parents=True)
    get_pet_image(out_dir=raw_data_dir)

    with pytest.raises(ValueError, match="Could not extract metadata"):
        ImagingSession.from_paths(
            f"{raw_data_dir}/**/*",
            datatypes=[DicomSeries],
            session_field=[IDSpec("StudyInstanceUID")],
            scan_field=[IDSpec("SeriesNumber")],
            resource_field=[IDSpec("ImageType[2:]")],
            path_metadata_regex=[
                PathMetadataRegex(r"^/nonexistent/(?P<cohort>.+)$", DicomSeries),
            ],
        )


CLASH_SCAN_ID = "1"
CLASH_SCAN_TYPE = "a-type"
CLASH_RESOURCE_NAME = "FILE"


def test_clash_duplicate(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:

    logger = logging.getLogger("xnat-ingest")
    logger.setLevel(logging.DEBUG)

    file1 = File.sample(seed=1)
    file1_cpy = file1.copy(tmp_path / "file1")

    session = ImagingSession(
        uid="12345",
        project_id="PROJECTID",
        subject_id="SUBJECTID",
        session_id="SESSIONID",
        scans=[
            ImagingScan(
                id=CLASH_SCAN_ID,
                type=CLASH_SCAN_TYPE,
                resources={CLASH_RESOURCE_NAME: file1},
            )
        ],
    )

    session.add_resource(
        scan_id=CLASH_SCAN_ID,
        scan_type=CLASH_SCAN_TYPE,
        resource_name=CLASH_RESOURCE_NAME,
        fileset=file1_cpy,
    )
    assert "as it is identical to a resource that is already present" in caplog.text


def test_clash_overwrite(caplog: pytest.LogCaptureFixture) -> None:

    logger = logging.getLogger("xnat-ingest")
    logger.setLevel(logging.DEBUG)

    file1 = File.sample(seed=1)
    file2 = File.sample(seed=2)

    session = ImagingSession(
        uid="12345",
        project_id="PROJECTID",
        subject_id="SUBJECTID",
        session_id="SESSIONID",
        scans=[
            ImagingScan(
                id=CLASH_SCAN_ID,
                type=CLASH_SCAN_TYPE,
                resources={CLASH_RESOURCE_NAME: file1},
            )
        ],
    )

    with pytest.raises(KeyError) as exc:
        session.add_resource(
            scan_id=CLASH_SCAN_ID,
            scan_type=CLASH_SCAN_TYPE,
            resource_name=CLASH_RESOURCE_NAME,
            fileset=file2,
        )

    assert "Clash between resource names" in str(exc.value)

    session.add_resource(
        scan_id=CLASH_SCAN_ID,
        scan_type=CLASH_SCAN_TYPE,
        resource_name=CLASH_RESOURCE_NAME,
        fileset=file2,
        overwrite=True,
    )
    assert "Overwriting existing resource" in caplog.text


def test_clash_avoid(caplog: pytest.LogCaptureFixture) -> None:

    logger = logging.getLogger("xnat-ingest")
    logger.setLevel(logging.DEBUG)

    file1 = File.sample(seed=1)
    file2 = File.sample(seed=2)

    session = ImagingSession(
        uid="12345",
        project_id="PROJECTID",
        subject_id="SUBJECTID",
        session_id="SESSIONID",
        scans=[
            ImagingScan(
                id=CLASH_SCAN_ID,
                type=CLASH_SCAN_TYPE,
                resources={CLASH_RESOURCE_NAME: file1},
            )
        ],
    )

    session.add_resource(
        scan_id=CLASH_SCAN_ID,
        scan_type=CLASH_SCAN_TYPE,
        resource_name=CLASH_RESOURCE_NAME,
        fileset=file2,
        avoid_clashes=True,
    )
    assert "to avoid clash with existing resources" in caplog.text
    assert sorted(session.scans[CLASH_SCAN_ID].resources) == [
        CLASH_RESOURCE_NAME,
        CLASH_RESOURCE_NAME + "__2",
    ]


def test_from_metadata_yaml(tmp_path: Path) -> None:
    metadata = {
        ImagingSession.UID_METADATA_KEY: "12345",
        "PatientName": "FamilyName_GivenName",
        "PatientID": "PID001",
        "StudyDate": "20230101",
    }
    yaml_path = tmp_path / "PROJ.SUBJ.VIS.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(metadata, f)

    session = ImagingSession.from_metadata_yaml(yaml_path)

    assert session.project_id == "PROJ"
    assert session.subject_id == "SUBJ"
    assert session.session_id == "VIS"
    assert session.scans == {}
    assert dict(session.metadata) == metadata


def test_associate_files_metadata_only(tmp_path: Path) -> None:
    metadata = {
        ImagingSession.UID_METADATA_KEY: "12345",
        "PatientName": "FamilyName_Given_Name",
        "PatientID": "PID001",
    }
    yaml_path = tmp_path / "PROJ.SUBJ.VIS.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(metadata, f)

    session = ImagingSession.from_metadata_yaml(yaml_path)

    # Verify metadata-only session
    assert session.scans == {}
    assert len(session.primary_parents) == 0

    # Generate dummy .ptd files
    assoc_dir = tmp_path / "assoc"
    assoc_dir.mkdir()
    get_raw_data_files(
        out_dir=assoc_dir,
        first_name="Given_Name",
        last_name="FamilyName",
    )

    session.associate_files(
        patterns=[
            AssociatedFiles(
                SyngoMi_Vr20b_RawData,
                str(assoc_dir) + "/{PatientName}*.ptd",
                r".*/[^\.]+\.[^\.]+\.[^\.]+\.(?P<id>\d+)\.(?P<resource>[^\.]+).*",
            )
        ],
        spaces_to_underscores=False,
    )

    # Scans should now have been populated from the associated files
    assert len(session.scans) > 0
    assert "602" in session.scans
    assert set(session.scans["602"].resources.keys()) == {
        "PET_LISTMODE",
        "PET_COUNTRATE",
    }


def test_session_resource_save_roundtrip(tmp_path: Path) -> None:
    """Session-level resources (no-dot dirs) survive a save/load roundtrip."""
    pdf = File.sample(seed=42)

    session = ImagingSession(
        uid="12345",
        project_id="PROJ",
        subject_id="SUBJ",
        session_id="VIS",
        scans=[],
    )
    session.add_session_resource("radiology-doc-report", pdf)

    saved, _ = session.save(tmp_path)
    assert "radiology-doc-report" in saved.session_resources

    session_dir = tmp_path.joinpath(*session.staging_relpath)
    reloaded = ImagingSession.load(session_dir)

    assert "radiology-doc-report" in reloaded.session_resources
    assert (
        reloaded.session_resources["radiology-doc-report"].checksums
        == saved.session_resources["radiology-doc-report"].checksums
    )


def test_id_escape(tmp_path: Path) -> None:
    raw_data_dir = tmp_path / "raw"
    raw_data_dir.mkdir()
    get_raw_data_files(
        out_dir=raw_data_dir,
        first_name="GivenName",
        last_name="FamilyName",
        PatientID="INSTRUMENT_SURNAME^FIRST_NAME",
        StudyID="Study1",
        AccessionNumber="Accession1",
        StudyInstanceUID="StudyInstanceUID1",
    )

    sessions = ImagingSession.from_paths(
        f"{raw_data_dir}/**/*.ptd",
        datatypes=[SyngoMi_Vr20b_ListMode, SyngoMi_Vr20b_CountRate],
        session_field=[IDSpec("StudyInstanceUID")],
        scan_field=[IDSpec("SeriesNumber")],
        resource_field=[IDSpec("ImageType[2:]")],
    )

    assert len(sessions) == 1

    sessions[0].assign(
        project_field="StudyID",
        subject_field="PatientID",
        session_field="AccessionNumber",
    )
    assert sessions[0].subject_id == "INSTRUMENT_SURNAME_FIRST_NAME"


def test_assign_unresolvable_field_uses_placeholder_instead_of_raising(
    imaging_session: ImagingSession,
) -> None:
    """A project/subject/session field that can't be resolved from the session's
    metadata should produce a placeholder ID (and flag the session via
    'invalid_ids'), rather than raising and losing the session entirely"""
    imaging_session.assign(
        project_field="ThisFieldDoesNotExistInTheMetadata",
        subject_field="PatientID",
        session_field="AccessionNumber",
    )
    assert imaging_session.project_id.startswith(
        "INVALID_NOTFOUND_THISFIELDDOESNOTEXISTINTHEMETADATA_"
    )
    assert imaging_session.invalid_ids
    # the other, resolvable fields are unaffected
    assert not imaging_session.subject_id.startswith("INVALID_NOTFOUND_")


# ---------------------------------------------------------------------------
# ImagingSession.deidentify tests
# ---------------------------------------------------------------------------

DEIDENTIFY_REID_MDATA = {"PatientName": "John Doe", "DOB": "19800101"}


def _make_deid_fileset(seed: int, expected_reid: dict) -> File:
    """Return a File instance with contains_phi=True and an injected deidentify().

    Setting contains_phi=True routes it through the deidentify branch in
    session.deidentify().  The injected method is called as an unbound function
    (instance attribute), so it receives no implicit ``self``.
    """
    f = File.sample(seed=seed)
    f.contains_phi = True

    def _deidentify(spec: ty.Any = None, out_dir: ty.Optional[Path] = None) -> tuple:
        dest = Path(out_dir)
        dest.mkdir(parents=True, exist_ok=True)
        return f.copy(dest), dict(expected_reid)

    f.deidentify = _deidentify
    return f


def test_deidentify_empty_session(tmp_path: Path) -> None:
    session = ImagingSession(
        uid="12345", project_id="PROJ", subject_id="SUBJ", session_id="SESS", scans=[]
    )
    deid_session, reid_mdata = session.deidentify(tmp_path / "dest")
    assert deid_session.project_id == "PROJ"
    assert deid_session.scans == {}
    assert reid_mdata == {}


def test_deidentify_no_phi_copies_files(tmp_path: Path) -> None:
    """Resources without contains_phi are copied as-is; no reid metadata collected."""
    f = File.sample(seed=1)  # no contains_phi attr → getattr returns False → copy path
    session = ImagingSession(
        uid="12345",
        project_id="PROJ",
        subject_id="SUBJ",
        session_id="SESS",
        scans=[ImagingScan(id="1", type="test-scan", resources={"FILE": f})],
    )
    deid_session, reid_mdata = session.deidentify(tmp_path / "dest")
    assert "1" in deid_session.scans
    assert reid_mdata == {}
    for scan in deid_session.scans.values():
        for resource in scan.resources.values():
            for fspath in resource.fileset.fspaths:
                assert fspath.exists()


def test_deidentify_collects_reid_metadata(tmp_path: Path) -> None:
    """deidentify() returns reid metadata from resources that implement deidentify."""
    f = _make_deid_fileset(seed=1, expected_reid=DEIDENTIFY_REID_MDATA)
    session = ImagingSession(
        uid="12345",
        project_id="PROJ",
        subject_id="SUBJ",
        session_id="SESS",
        scans=[ImagingScan(id="1", type="test-scan", resources={"FILE": f})],
    )
    deid_session, reid_mdata = session.deidentify(tmp_path / "dest", specs={File: {}})
    assert reid_mdata == DEIDENTIFY_REID_MDATA
    assert "1" in deid_session.scans


def test_deidentify_missing_spec_raises(tmp_path: Path) -> None:
    """Empty project_spec with require_matching_spec=True raises KeyError."""
    f = _make_deid_fileset(seed=1, expected_reid=DEIDENTIFY_REID_MDATA)
    session = ImagingSession(
        uid="12345",
        project_id="PROJ",
        subject_id="SUBJ",
        session_id="SESS",
        scans=[ImagingScan(id="1", type="test-scan", resources={"FILE": f})],
    )
    with pytest.raises(KeyError):
        session.deidentify(tmp_path / "dest", specs={}, require_matching_spec=True)


def test_deidentify_missing_spec_warns(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Empty project_spec with require_matching_spec=False logs a warning and proceeds."""
    f = _make_deid_fileset(seed=1, expected_reid=DEIDENTIFY_REID_MDATA)
    session = ImagingSession(
        uid="12345",
        project_id="PROJ",
        subject_id="SUBJ",
        session_id="SESS",
        scans=[ImagingScan(id="1", type="test-scan", resources={"FILE": f})],
    )
    with caplog.at_level(logging.WARNING, logger="xnat-ingest"):
        deid_session, reid_mdata = session.deidentify(
            tmp_path / "dest", specs={}, require_matching_spec=False
        )
    assert "No deidentification specification" in caplog.text
    assert "1" in deid_session.scans
    assert reid_mdata == DEIDENTIFY_REID_MDATA


def test_deidentify_merges_reid_metadata_across_resources(tmp_path: Path) -> None:
    """Reid metadata from multiple resources is collated into a single dict."""
    f1 = _make_deid_fileset(seed=1, expected_reid={"PatientName": "Alice"})
    f2 = _make_deid_fileset(seed=2, expected_reid={"DOB": "19901201"})
    session = ImagingSession(
        uid="12345",
        project_id="PROJ",
        subject_id="SUBJ",
        session_id="SESS",
        scans=[
            ImagingScan(id="1", type="scan-a", resources={"FILE": f1}),
            ImagingScan(id="2", type="scan-b", resources={"FILE": f2}),
        ],
    )
    _, reid_mdata = session.deidentify(tmp_path / "dest", specs={File: {}})
    assert reid_mdata == {"PatientName": "Alice", "DOB": "19901201"}
