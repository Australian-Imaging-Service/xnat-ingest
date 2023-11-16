import os
import shutil
from pathlib import Path
from arcana.core.cli.dataset import (
    define as dataset_define,
    add_source as dataset_add_source,
)
import xnat4tests
from arcana.core.cli.store import add as store_add
from xnat_ingest.cli.upload import upload
from xnat_ingest.utils import show_cli_trace
from fileformats.medimage import DicomSeries
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


PATTERN = "{PatientName.given_name}_{PatientName.family_name}_{SeriesDate}.*"


def test_upload(
    xnat_project,
    xnat_config,
    xnat_server,
    cli_runner,
    run_prefix,
    tmp_path: Path,
    tmp_gen_dir: Path,
):
    # Get test image data

    dicoms_dir = tmp_path / "dicoms"
    dicoms_dir.mkdir(exist_ok=True)

    associated_files_dir = tmp_path / "non-dicoms"
    associated_files_dir.mkdir(exist_ok=True)

    staging_dir = tmp_path / "staging"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir()

    log_file = tmp_path / "logging.log"
    if log_file.exists():
        os.unlink(log_file)

    # Delete any existing sessions from previous test runs
    session_ids = []
    with xnat4tests.connect() as xnat_login:
        for i, c in enumerate("abc"):
            first_name = f"First{c.upper()}"
            last_name = f"Last{c.upper()}"
            PatientName = f"{first_name}^{last_name}"
            PatientID = f"subject{i}"
            AccessionNumber = f"98765432{i}"
            session_ids.append(AccessionNumber)

            StudyInstanceUID = (
                f"1.3.12.2.1107.5.1.4.10016.3000002308242209356530000001{i}"
            )

            series = DicomSeries(
                get_pet_image(
                    tmp_gen_dir / f"pet{i}",
                    PatientName=PatientName,
                    StudyInstanceUID=StudyInstanceUID,
                    PatientID=PatientID,
                    AccessionNumber=AccessionNumber,
                    StudyID=xnat_project,
                ).iterdir()
            )
            for dcm in series.contents:
                os.link(dcm, dicoms_dir / f"pet{i}-{dcm.fspath.name}")
            series = DicomSeries(
                get_ac_image(
                    tmp_gen_dir / f"ac{i}",
                    PatientName=PatientName,
                    StudyInstanceUID=StudyInstanceUID,
                    PatientID=PatientID,
                    AccessionNumber=AccessionNumber,
                    StudyID=xnat_project,
                ).iterdir()
            )
            for dcm in series.contents:
                os.link(dcm, dicoms_dir / f"ac{i}-{dcm.fspath.name}")
            series = DicomSeries(
                get_topogram_image(
                    tmp_gen_dir / f"topogram{i}",
                    PatientName=PatientName,
                    StudyInstanceUID=StudyInstanceUID,
                    PatientID=PatientID,
                    AccessionNumber=AccessionNumber,
                    StudyID=xnat_project,
                ).iterdir()
            )
            for dcm in series.contents:
                os.link(dcm, dicoms_dir / f"topogram{i}-{dcm.fspath.name}")
            series = DicomSeries(
                get_statistics_image(
                    tmp_gen_dir / f"statistics{i}",
                    PatientName=PatientName,
                    StudyInstanceUID=StudyInstanceUID,
                    PatientID=PatientID,
                    AccessionNumber=AccessionNumber,
                    StudyID=xnat_project,
                ).iterdir()
            )
            for dcm in series.contents:
                os.link(dcm, dicoms_dir / f"statistics{i}-{dcm.fspath.name}")
            nd_fspaths = get_raw_data_files(
                tmp_gen_dir / f"non-dicom{i}",
                first_name=first_name,
                last_name=last_name,
                date_time=f"2023.08.25.15.50.5{i}",
            )
            for nd_fspath in nd_fspaths:
                os.link(
                    dcm,
                    associated_files_dir / f"{nd_fspath.stem}-{i}{nd_fspath.suffix}",
                )

    # Create data store
    result = cli_runner(
        store_add,
        [
            "testxnat",
            "xnat:Xnat",
            "--server",
            xnat_server,
            "--user",
            xnat_config.xnat_user,
            "--password",
            xnat_config.xnat_password,
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)

    dataset_locator = f"testxnat//{xnat_project}"

    # Create dataset definition
    result = cli_runner(dataset_define, [dataset_locator])
    assert result.exit_code == 0, show_cli_trace(result)

    for col_name, col_type, col_pattern in [
        ("pet", "medimage/dicom-series", "PET SWB 8MIN"),
        ("topogram", "medimage/dicom-series", "Topogram.*"),
        ("atten_corr", "medimage/dicom-series", "AC CT.*"),
        (
            "listmode",
            "medimage/vnd.siemens.biograph128-vision.vr20b.pet-list-mode",
            ".*(PET_LISTMODE).*",
        ),
        (
            "sinogram",
            "medimage/vnd.siemens.biograph128-vision.vr20b.pet-sinogram",
            ".*(PET_EM_SINO).*",
        ),
        (
            "countrate",
            "medimage/vnd.siemens.biograph128-vision.vr20b.pet-count-rate",
            ".*(PET_COUNTRATE).*",
        ),
    ]:
        # Add dataset columns
        result = cli_runner(
            dataset_add_source,
            [
                dataset_locator,
                col_name,
                col_type,
                "--path",
                col_pattern,
            ],
        )
        assert result.exit_code == 0, show_cli_trace(result)

    result = cli_runner(
        upload,
        [
            str(dicoms_dir),
            str(staging_dir),
            "--assoc-files-glob",
            str(associated_files_dir)
            + "/{PatientName.given_name}_{PatientName.family_name}*.ptd",
            "--log-file",
            str(log_file),
            "--raise-errors",
            "--include-dicoms",
            "--delete",
        ],
        env={
            "XNAT_INGEST_HOST": xnat_server,
            "XNAT_INGEST_USER": "admin",
            "XNAT_INGEST_PASS": "admin",
        },
    )

    assert result.exit_code == 0, show_cli_trace(result)

    with xnat4tests.connect() as xnat_login:
        xproject = xnat_login.projects[xnat_project]
        for session_id in session_ids:
            xsession = xproject.experiments[session_id]
            scan_ids = sorted(xsession.scans)

            assert scan_ids == [
                "1",
                "2",
                "4",
                "listmode",
                "sinogram",
                "countrate",
            ]
