import functools
import logging
import typing as ty
from pathlib import Path

import pytest
import yaml
from fileformats.core import from_mime
from fileformats.generic import File, SetOf
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
from xnat_ingest.model.session import (
    ImagingScan,
    ImagingSession,
    _metadata_diff,
    _type_name_resource_label,
)
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


def test_from_paths_injects_datatype_metadata_field(tmp_path: Path) -> None:
    """The resolved fileset type name is exposed as the '__datatype__' metadata
    field and is usable from an IDSpec (including a format string)."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("x")

    sessions = ImagingSession.from_paths(
        src,
        datatypes=[File],
        session_field=[IDSpec("sess")],
        scan_field=[IDSpec("sess")],
        resource_field=[IDSpec("res_{__datatype__}")],
        path_metadata_regex=[PathMetadataRegex(r".*/(?P<sess>[^/]+)\.txt", File)],
    )

    scan = next(iter(sessions[0].scans.values()))
    assert list(scan.resources) == ["res_File"]
    resource = next(iter(scan.resources.values()))
    assert resource.fileset.metadata["__datatype__"] == "File"


def test_from_paths_resource_label_defaults_to_mime_like_type_name(
    tmp_path: Path,
) -> None:
    """With no resource_field, each resource is labelled with the mime-like
    rendering of its fileset's type name ('-' kept, '.'/'+' collapsed to '_')."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("x")

    sessions = ImagingSession.from_paths(
        src,
        datatypes=[File],
        session_field=[IDSpec("__datatype__")],
        scan_field=[IDSpec("__datatype__")],
        # resource_field omitted -> mime-like(type_name)
    )

    assert list(next(iter(sessions[0].scans.values())).resources) == ["file"]


@pytest.mark.parametrize(
    "type_name, expected",
    [
        ("VectraExport", "vectra-export"),
        ("Sqlite3Db", "sqlite3-db"),
        ("SyngoMi_Vr20b_ListMode", "syngo-mi_vr20b_list-mode"),
        ("Png___SetOf", "png_set-of"),
    ],
)
def test_type_name_resource_label(type_name: str, expected: str) -> None:
    assert _type_name_resource_label(type_name) == expected


def _tree(root: Path) -> Path:
    """A VectraExport-shaped tree for the recursive-directory tests:
    ``analysis/`` holds a .json (the wanted nested dir), ``lesion/`` holds a .csv
    (an ignored sibling dir), plus a loose file at the top."""
    (root / "analysis").mkdir(parents=True)
    (root / "analysis" / "result.json").write_text("{}")
    (root / "analysis" / "notes.txt").write_text("inside the wanted dir")
    (root / "lesion").mkdir()
    (root / "lesion" / "data.csv").write_text("a,b\n1,2\n")
    (root / "plain").mkdir()
    (root / "plain" / "deep").mkdir()
    (root / "plain" / "deep" / "result.json").write_text("{}")
    (root / "scan.txt").write_text("a loose file")
    return root


def test_recursive_collect_prunes_on_match(tmp_path: Path) -> None:
    from fileformats.application import Json
    from fileformats.generic import DirectoryOf
    from fileformats.text import Csv

    from xnat_ingest.model.session import _recursive_collect

    root = _tree(tmp_path / "export")
    got = set(_recursive_collect(root, [DirectoryOf[Json]], [DirectoryOf[Csv]]))

    assert got == {
        root / "analysis",  # wanted dir: yielded whole, not descended
        root / "plain" / "deep",  # found by descending the unmatched 'plain'
        root / "scan.txt",  # loose file
    }
    # 'lesion/' matched an ignore_datatype -> skipped, its .csv never surfaces
    # 'analysis/' not descended -> its own files never surface


def test_from_paths_recursive_pulls_nested_directory_datatype(tmp_path: Path) -> None:
    from fileformats.application import Json
    from fileformats.generic import DirectoryOf
    from fileformats.text import Csv, Plain

    root = _tree(tmp_path / "export")
    sessions = ImagingSession.from_paths(
        root,
        datatypes=[DirectoryOf[Json]],
        session_field=[IDSpec("__datatype__")],
        scan_field=[IDSpec("name")],
        resource_field=[IDSpec("name")],
        recursive=True,
        path_metadata_regex=[
            PathMetadataRegex(r".*/(?P<name>[^/]+)$", DirectoryOf[Json])
        ],
        ignore_datatypes=[DirectoryOf[Csv], Plain],
    )

    assert len(sessions) == 1
    scans = sessions[0].scans
    # both DirectoryOf[Json] dirs (analysis/ and plain/deep/) pulled, nothing else
    assert set(scans) == {"analysis", "deep"}


def test_from_paths_recursive_ignore_path_wildcard_pulls_only_wanted_dirs(
    tmp_path: Path,
) -> None:
    """The wanted directory formats are nested inside larger 'clutter' directories
    whose internal structure we don't want to track. ``ignore_paths=['.*']`` mops
    up every non-matching path so only the requested nested dirs come through - no
    ``ignore_datatypes`` enumeration needed."""
    from fileformats.application import Json
    from fileformats.generic import DirectoryOf

    root = tmp_path / "export"
    # WholeBodyCapture-shaped clutter dir with the wanted analysis dir buried in it
    capture = root / "WBcapture"
    (capture / "analysis").mkdir(parents=True)
    (capture / "analysis" / "result.json").write_text("{}")
    (capture / "raw1.bin").write_bytes(b"junk")
    (capture / "thumbs").mkdir()
    (capture / "thumbs" / "t1.jpg").write_bytes(b"junk")
    # LesionAnalysis-shaped clutter dir with the wanted dexi dir buried in it
    lesion = root / "lesionAnalysis"
    (lesion / "dexi").mkdir(parents=True)
    (lesion / "dexi" / "meta.json").write_text("{}")
    (lesion / "report.csv").write_text("a,b\n1,2\n")
    (root / "loose_at_root.txt").write_text("junk")

    sessions = ImagingSession.from_paths(
        root,
        datatypes=[DirectoryOf[Json]],
        session_field=[IDSpec("__datatype__")],
        scan_field=[IDSpec("name")],
        resource_field=[IDSpec("name")],
        recursive=True,
        ignore_paths=[".*"],
        path_metadata_regex=[
            PathMetadataRegex(r".*/(?P<name>[^/]+)$", DirectoryOf[Json])
        ],
    )

    assert len(sessions) == 1
    assert set(sessions[0].scans) == {"analysis", "dexi"}


def test_from_paths_recursive_raises_on_unlisted_type(tmp_path: Path) -> None:
    from fileformats.application import Json
    from fileformats.core.exceptions import FormatRecognitionError
    from fileformats.generic import DirectoryOf
    from fileformats.text import Csv

    root = _tree(tmp_path / "export")
    (root / "mystery.unknownext").write_text("not a recognised type")

    with pytest.raises(FormatRecognitionError, match="mystery"):
        ImagingSession.from_paths(
            root,
            datatypes=[DirectoryOf[Json]],
            session_field=[IDSpec("__datatype__")],
            scan_field=[IDSpec("__datatype__")],
            recursive=True,
            ignore_datatypes=[DirectoryOf[Csv]],  # doesn't cover the loose files
        )


def test_from_paths_recursive_rejects_bare_generic_directory(tmp_path: Path) -> None:
    from fileformats.generic import Directory

    root = _tree(tmp_path / "export")
    with pytest.raises(ValueError, match="generic/directory"):
        ImagingSession.from_paths(
            root,
            datatypes=[Directory],
            session_field=[IDSpec("__datatype__")],
            scan_field=[IDSpec("__datatype__")],
            recursive=True,
        )


def _canfield_shaped_tree(root: Path) -> Path:
    """A stripped-down stand-in for a Canfield Vectra export: a session directory
    holding several 'capture' directories, each of which buries a nested analysis
    directory (two different formats) among loose files, plus sibling clutter
    directories and loose files at every level. Generic ``DirectoryOf`` types
    stand in for the real vendor formats:

    - ``DirectoryOf[Json]`` == the photogrammetry lesion-analysis dir (has a .json)
    - ``DirectoryOf[Csv]``  == the dexi-analysis dir (has a .csv), 2 instances
    """
    session = root / "SESSION-abc"

    # photogrammetry capture: loads of loose files + the wanted analysis dir + a
    # non-wanted sibling 'calib' directory
    photo = session / "20260805133937"
    photo.mkdir(parents=True)
    for name in ("a1A.CR2", "a1B.CR2", "f1A.CR2", "capture.cptr", "addtexture-log.txt"):
        (photo / name).write_bytes(b"raw")
    (photo / "analysis").mkdir()
    (photo / "analysis" / "lesion_data.json").write_text("{}")
    (photo / "analysis" / "exitstatus.txt").write_text("ok")  # not descended -> unseen
    (photo / "calib").mkdir()
    (photo / "calib" / "a1A.sfcm").write_bytes(b"cal")  # unrecognised, in a clutter dir

    # two dexi captures, each burying a DexiData dir among loose files
    for stamp in ("20260805135303357", "20260805135325459"):
        dexi_capture = session / stamp
        dexi_capture.mkdir()
        (dexi_capture / "captureinfo_scope").write_bytes(b"info")
        (dexi_capture / "XP.png").write_bytes(b"png")
        (dexi_capture / "DexiData_2.1").mkdir()
        (dexi_capture / "DexiData_2.1" / "result.csv").write_text("a,b\n1,2\n")
        (dexi_capture / "DexiData_2.1" / "heatmap.jpg").write_bytes(b"jpg")  # unseen

    # loose files hanging directly off the session dir, and a sibling of it
    (session / "lesion.t2k").write_bytes(b"t2k")
    (session / "DermX Report.pdf").write_bytes(b"pdf")
    (root / "testExternalID.db").write_bytes(b"SQLite format 3\x00")
    return root


def test_from_paths_recursive_extracts_nested_dirs_from_canfield_shaped_tree(
    tmp_path: Path,
) -> None:
    """End-to-end on the Canfield-shaped tree: two distinct nested directory
    formats are pulled from wherever they are buried, one of them appearing more
    than once, while every loose file and every non-matching directory (the
    capture dirs, ``calib/``, the session dir, the sibling ``.db``) is left alone
    via ``ignore_paths=['.*']`` - no ``ignore_datatypes`` enumeration."""
    from fileformats.application import Json
    from fileformats.generic import DirectoryOf
    from fileformats.text import Csv

    root = _canfield_shaped_tree(tmp_path / "raw")

    sessions = ImagingSession.from_paths(
        root,
        datatypes=[DirectoryOf[Json], DirectoryOf[Csv]],
        session_field=[IDSpec("session")],
        scan_field=[IDSpec("capture")],
        resource_field=[IDSpec("name")],
        recursive=True,
        ignore_paths=[".*"],
        path_metadata_regex=[
            PathMetadataRegex(
                r".*/(?P<session>SESSION-[^/]+)/(?P<capture>[^/]+)/(?P<name>[^/]+)$",
                DirectoryOf,
            )
        ],
    )

    assert len(sessions) == 1
    scans = sessions[0].scans
    assert set(scans) == {"20260805133937", "20260805135303357", "20260805135325459"}
    assert list(scans["20260805133937"].resources) == ["analysis"]
    # '.' in 'DexiData_2.1' is escaped to '_' like any other ID/label
    assert list(scans["20260805135303357"].resources) == ["DexiData_2_1"]
    assert list(scans["20260805135325459"].resources) == ["DexiData_2_1"]
    resources = [r for s in scans.values() for r in s.resources.values()]
    assert {type(r.fileset).__name__ for r in resources} == {
        DirectoryOf[Json].__name__,
        DirectoryOf[Csv].__name__,
    }


def test_from_paths_scan_id_defaults_to_resource_label(tmp_path: Path) -> None:
    """With no scan_field, each resource sits in a scan of the same name."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("x")

    sessions = ImagingSession.from_paths(
        src,
        datatypes=[File],
        session_field=[IDSpec("__datatype__")],
        # scan_field / resource_field omitted
    )

    scan = next(iter(sessions[0].scans.values()))
    assert scan.id == "file"
    assert list(scan.resources) == ["file"]


def test_from_paths_datatype_scoped_scan_spec_falls_through(tmp_path: Path) -> None:
    """A datatype-scoped --scan spec that doesn't apply to a fileset's type (the
    DICOM 'SeriesNumber' default vs a plain File) names the scan after the resource
    rather than raising."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("x")

    sessions = ImagingSession.from_paths(
        src,
        datatypes=[File],
        session_field=[IDSpec("__datatype__")],
        scan_field=[IDSpec("SeriesNumber", "medimage/dicom-collection")],
    )

    assert next(iter(sessions[0].scans.values())).id == "file"


def test_from_paths_session_spec_not_matching_type_raises_clearly(
    tmp_path: Path,
) -> None:
    """The session UID has no auto-fallback: a session spec whose datatype doesn't
    apply to a fileset (the DICOM-scoped default vs a plain File) raises an
    actionable error rather than a bare 'resource label' TypeError."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("x")

    with pytest.raises(TypeError, match="apply to a File"):
        ImagingSession.from_paths(
            src,
            datatypes=[File],
            session_field=[IDSpec("StudyInstanceUID", "medimage/dicom-collection")],
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
        on_clash="overwrite",
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
        on_clash="avoid",
    )
    assert "to avoid clash with existing resources" in caplog.text
    assert sorted(session.scans[CLASH_SCAN_ID].resources) == [
        CLASH_RESOURCE_NAME,
        CLASH_RESOURCE_NAME + "__2",
    ]


def _clash_session() -> ImagingSession:
    return ImagingSession(
        uid="12345",
        project_id="PROJECTID",
        subject_id="SUBJECTID",
        session_id="SESSIONID",
        scans=[
            ImagingScan(
                id=CLASH_SCAN_ID,
                type=CLASH_SCAN_TYPE,
                resources={CLASH_RESOURCE_NAME: File.sample(seed=1)},
            )
        ],
    )


def test_add_resource_clash_hint_in_error() -> None:
    with pytest.raises(KeyError, match="auto-derived from its fileset type"):
        _clash_session().add_resource(
            scan_id=CLASH_SCAN_ID,
            scan_type=CLASH_SCAN_TYPE,
            resource_name=CLASH_RESOURCE_NAME,
            fileset=File.sample(seed=2),
            on_clash="error",
            clash_hint=(
                "the --scan and --resource ID(s) for this resource were "
                "auto-derived from its fileset type; pass explicit --scan / "
                "--resource specifier(s) to control grouping"
            ),
        )


def test_add_resource_clash_hint_in_avoid_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logging.getLogger("xnat-ingest").setLevel(logging.WARNING)
    _clash_session().add_resource(
        scan_id=CLASH_SCAN_ID,
        scan_type=CLASH_SCAN_TYPE,
        resource_name=CLASH_RESOURCE_NAME,
        fileset=File.sample(seed=2),
        on_clash="avoid",
        clash_hint="pass explicit --scan / --resource specifier(s) to control grouping",
    )
    assert "pass explicit --scan / --resource specifier(s)" in caplog.text


def test_clash_merge(caplog: pytest.LogCaptureFixture) -> None:

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
        on_clash="merge",
    )
    assert "Merging resource" in caplog.text
    merged = session.scans[CLASH_SCAN_ID].resources[CLASH_RESOURCE_NAME]
    assert isinstance(merged.fileset, SetOf)
    assert merged.fileset.content_types == (File,)
    assert set(merged.fileset.fspaths) == set(file1.fspaths) | set(file2.fspaths)


def test_clash_merge_saves_combined_resource(tmp_path: Path) -> None:
    """A merged resource combines the files of both filesets into a single
    ``SetOf[...]``, collates the members' metadata (scalar where they agree,
    aligned list where they differ), and round-trips through ``save()``/``load()``."""

    src_dir = tmp_path / "src"
    src_dir.mkdir()
    view1 = src_dir / "lesion-dermoscopy.dat"
    view2 = src_dir / "lesion-clinical.dat"
    view1.write_text("first view")
    view2.write_text("second view")
    fileset1 = File(view1)
    fileset2 = File(view2)
    # 'LesionID' agrees across the two views, 'View' differs
    fileset1.metadata.update({"LesionID": "L1", "View": "dermoscopy"})
    fileset2.metadata.update({"LesionID": "L1", "View": "clinical"})

    session = ImagingSession(
        uid="12345",
        project_id="PROJECTID",
        subject_id="SUBJECTID",
        session_id="SESSIONID",
        scans=[
            ImagingScan(
                id=CLASH_SCAN_ID,
                type=CLASH_SCAN_TYPE,
                resources={CLASH_RESOURCE_NAME: fileset1},
            )
        ],
    )

    session.add_resource(
        scan_id=CLASH_SCAN_ID,
        scan_type=CLASH_SCAN_TYPE,
        resource_name=CLASH_RESOURCE_NAME,
        fileset=fileset2,
        on_clash="merge",
    )

    merged = session.scans[CLASH_SCAN_ID].resources[CLASH_RESOURCE_NAME]
    assert isinstance(merged.fileset, SetOf)
    assert merged.fileset.content_types == (File,)
    assert sorted(p.name for p in merged.fileset.fspaths) == [
        "lesion-clinical.dat",
        "lesion-dermoscopy.dat",
    ]
    # agreed field stays scalar, differing field is collated into an aligned list
    assert merged.metadata["LesionID"] == "L1"
    assert merged.metadata["View"] == ["dermoscopy", "clinical"]

    saved, _ = session.save(tmp_path / "staged")
    session_dir = (tmp_path / "staged").joinpath(*session.staging_relpath)
    reloaded = ImagingSession.load(session_dir)

    reloaded_resource = reloaded.scans[CLASH_SCAN_ID].resources[CLASH_RESOURCE_NAME]
    assert sorted(p.name for p in reloaded_resource.fileset.fspaths) == [
        "lesion-clinical.dat",
        "lesion-dermoscopy.dat",
    ]
    assert reloaded_resource.metadata["LesionID"] == "L1"
    assert reloaded_resource.metadata["View"] == ["dermoscopy", "clinical"]
    assert (
        reloaded_resource.checksums
        == saved.scans[CLASH_SCAN_ID].resources[CLASH_RESOURCE_NAME].checksums
    )


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


def test_session_save_filters_scan_and_session_resources(
    tmp_path: Path, imaging_session: ImagingSession
) -> None:
    imaging_session.add_session_resource("report", File.sample(seed=42))

    dicom_saved, _ = imaging_session.save(tmp_path / "dicom", include=[DicomSeries])
    report_saved, _ = imaging_session.save(tmp_path / "report", include=[File])

    assert dicom_saved.scans
    assert dicom_saved.session_resources == {}
    assert all(
        isinstance(resource.fileset, DicomSeries) for resource in dicom_saved.resources
    )
    assert report_saved.scans == {}
    assert set(report_saved.session_resources) == {"report"}
    assert isinstance(report_saved.session_resources["report"].fileset, File)


def test_session_save_include_requires_a_matching_resource(
    tmp_path: Path, imaging_session: ImagingSession
) -> None:
    with pytest.raises(ValueError, match="No resources .* match"):
        imaging_session.save(tmp_path, include=[SyngoMi_Vr20b_ListMode])

    assert not (tmp_path / imaging_session.name).exists()


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
        "INVALID_MISSING_THISFIELDDOESNOTEXISTINTHEMETADATA_"
    )
    assert imaging_session.invalid_ids
    # the other, resolvable fields are unaffected
    assert not imaging_session.subject_id.startswith("INVALID_MISSING_")


# ---------------------------------------------------------------------------
# _metadata_diff tests
# ---------------------------------------------------------------------------


def test_metadata_diff_identical_returns_empty() -> None:
    orig = {"PatientName": "John Doe", "DOB": "19800101"}
    assert _metadata_diff(orig, dict(orig)) == {}


def test_metadata_diff_missing_key_included() -> None:
    """A key present in `orig` but absent from `new` is reported (KeyError branch)."""
    orig = {"PatientName": "John Doe", "DOB": "19800101"}
    new = {"DOB": "19800101"}
    assert _metadata_diff(orig, new) == {"PatientName": "John Doe"}


def test_metadata_diff_changed_value_included() -> None:
    """A key present in both but with a different value is reported (elif branch)."""
    orig = {"PatientName": "John Doe", "DOB": "19800101"}
    new = {"PatientName": "Anonymous", "DOB": "19800101"}
    assert _metadata_diff(orig, new) == {"PatientName": "John Doe"}


def test_metadata_diff_unchanged_value_excluded() -> None:
    orig = {"PatientName": "John Doe", "DOB": "19800101"}
    new = {"PatientName": "John Doe", "DOB": "19790101"}
    assert _metadata_diff(orig, new) == {"DOB": "19800101"}


def test_metadata_diff_nested_mapping_recurses() -> None:
    """Nested mappings are diffed recursively rather than compared wholesale."""
    orig = {"PatientInfo": {"Name": "John Doe", "Sex": "M"}}
    new = {"PatientInfo": {"Name": "Anonymous", "Sex": "M"}}
    assert _metadata_diff(orig, new) == {"PatientInfo": {"Name": "John Doe"}}


def test_metadata_diff_nested_mapping_unchanged_excluded() -> None:
    """A nested mapping with no internal differences is omitted entirely."""
    orig = {"PatientInfo": {"Name": "John Doe"}}
    new = {"PatientInfo": {"Name": "John Doe"}}
    assert _metadata_diff(orig, new) == {}


def test_metadata_diff_mapping_replaced_by_non_mapping() -> None:
    """When `orig`'s value is a mapping but `new`'s isn't, fall back to a plain
    equality comparison rather than recursing."""
    orig = {"PatientInfo": {"Name": "John Doe"}}
    new = {"PatientInfo": "stripped"}
    assert _metadata_diff(orig, new) == {"PatientInfo": {"Name": "John Doe"}}


# ---------------------------------------------------------------------------
# ImagingSession.deidentify tests
# ---------------------------------------------------------------------------

DEIDENTIFY_REID_MDATA = {"PatientName": "John Doe", "DOB": "19800101"}


def _deidentify_test_impl(
    fileset: File,
    out_dir: Path,
    spec: ty.Any = None,
    **kwargs: ty.Any,
) -> File:
    dest = Path(out_dir)
    dest.mkdir(parents=True, exist_ok=True)
    deidentified = fileset.copy(dest)
    # session.deidentify() now reconstructs reid metadata itself by diffing
    # `metadata` before/after calling deidentify(), so the stand-in "stripped"
    # fileset needs to actually report different metadata to the original. A fresh
    # copy of a generic ``File`` has no metadata reader, so its metadata is already
    # empty - nothing to strip here.
    return deidentified


def _make_deid_fileset(seed: int, expected_reid: dict) -> File:
    """Return a File instance with contains_phi=True and an injected deidentify().

    Setting contains_phi=True routes it through the deidentify branch in
    session.deidentify(). expected_reid is written to the fileset's metadata overlay
    so that session.deidentify()'s before/after diff reconstructs it. The injected
    method is a functools.partial binding a module-level function (not a closure),
    just for consistency/reuse across the fixtures in this module.
    """
    f = File.sample(seed=seed)
    f.contains_phi = True
    f.metadata.update(expected_reid)
    f.deidentify = functools.partial(_deidentify_test_impl, f)
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


def test_deidentify_passes_max_workers_to_resource(tmp_path: Path) -> None:
    """max_workers passed to session.deidentify() should reach each resource's
    own FileSet.deidentify() call unchanged.
    """
    received_max_workers: list = []

    def _capturing_deidentify_impl(
        fileset: File,
        out_dir: Path,
        spec: ty.Any = None,
        **kwargs: ty.Any,
    ) -> File:
        received_max_workers.append(kwargs.get("max_workers"))
        return _deidentify_test_impl(fileset, out_dir, spec=spec, **kwargs)

    f = _make_deid_fileset(seed=1, expected_reid=DEIDENTIFY_REID_MDATA)
    f.deidentify = functools.partial(_capturing_deidentify_impl, f)
    session = ImagingSession(
        uid="12345",
        project_id="PROJ",
        subject_id="SUBJ",
        session_id="SESS",
        scans=[ImagingScan(id="1", type="test-scan", resources={"FILE": f})],
    )
    session.deidentify(tmp_path / "dest", specs={File: {}}, max_workers=3)
    assert received_max_workers == [3]


def test_deidentify_carries_session_level_resources(tmp_path: Path) -> None:
    """A resource attached to the SESSION must survive de-identification.

    deidentify() builds its output from new_empty(), which copies the ids and
    nothing else, and then walks self.scans. Session-level resources were in
    neither, so they were silently dropped: not de-identified, not copied, and
    nothing reported.

    It is worse than a plain loss. The per-session completeness gate in
    deidentify_api counts data files on both sides, so a session carrying one
    comes out short, is reported incomplete, and correctly refuses to unlink its
    input -- for ever, because the next run drops it again.
    """
    from xnat_ingest.model.scan import ImagingScan
    from xnat_ingest.model.session import ImagingSession

    session = ImagingSession(
        uid="PROJ.SUBJ.SESS",
        project_id="PROJ",
        subject_id="SUBJ",
        session_id="SESS",
        scans=[ImagingScan(id="1", type="T", resources={"RES": File.sample(seed=1)})],
    )
    session.add_session_resource("report", File.sample(seed=42))
    assert "report" in session.session_resources

    deid, _ = session.deidentify(tmp_path / "out", require_matching_spec=False)

    assert "report" in deid.session_resources, (
        "the session-level resource was dropped by deidentify(), so it never "
        "reaches XNAT and the completeness gate refuses the unlink for ever"
    )
    assert set(deid.scans) == set(session.scans), "scans must be unaffected"
