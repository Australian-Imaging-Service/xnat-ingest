from arcana.cli.dataset import (
    define as dataset_define,
    add_source as dataset_add_source,
)
from arcana.cli.store import add as store_add
from xnat_ingest.cli.upload import upload
from xnat_ingest.utils import show_cli_trace
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (
    get_image as get_pet_image,
)

# from medimages4tests.dummy.dicom.ct.wholebody.siemens.biograph_vision.vr20b import (
#     get_image as get_ct_image,
# )
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
    xnat_project, xnat_config, xnat_server, cli_runner, export_dir, tmp_path
):
    # Get test image data

    dicoms_dir = tmp_path / "dicoms"
    dicoms_dir.mkdir()

    non_dicoms_dir = tmp_path / "non-dicoms"
    non_dicoms_dir.mkdir()

    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()

    log_file = tmp_path / "logging.log"

    for i, c in enumerate("abc"):
        first_name = f"First{c.upper()}"
        last_name = f"Last{c.upper()}"
        PatientName = f"{first_name}^{last_name}"
        StudyInstanceUID = f"1.3.12.2.1107.5.1.4.10016.3000002308242209356530000001{i}"

        get_pet_image(
            dicoms_dir, PatientName=PatientName, StudyInstanceUID=StudyInstanceUID
        )
        get_ac_image(
            dicoms_dir, PatientName=PatientName, StudyInstanceUID=StudyInstanceUID
        )
        get_topogram_image(
            dicoms_dir, PatientName=PatientName, StudyInstanceUID=StudyInstanceUID
        )
        get_statistics_image(
            dicoms_dir, PatientName=PatientName, StudyInstanceUID=StudyInstanceUID
        )

        get_raw_data_files(
            non_dicoms_dir,
            first_name=first_name,
            last_name=last_name,
            date_time=f"2023.08.25.15.50.5{i}",
        )

    SESSION_ID = "987654321"
    # Delete any existing sessions from previous test runs
    try:
        xsession = xnat_project.experiments[SESSION_ID]
    except KeyError:
        pass
    else:
        xsession.delete()

    # Create data store
    result = cli_runner(
        store_add,
        [
            "testxnat",
            "xnat:Xnat",
            "--server",
            xnat_server,
            "--user",
            xnat_config.user,
            "--password",
            xnat_config.password,
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)

    dataset_locator = f"testxnat//{xnat_project.id}"

    # Create dataset definition
    result = cli_runner(dataset_define, [dataset_locator])
    assert result.exit_code == 0, show_cli_trace(result)

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
            "medimage/vnd.siemens.biograph-vision-vr20b.pet-countrate",
            ".*PET_COUNTRATE.*",
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
            "--non-dicoms-pattern",
            str(non_dicoms_dir)
            + "/{PatientName.given_name}_{PatientName.family_name}*.ptd"
            "--log-file",
            str(log_file),
        ],
        env={
            "XNAT_INGEST_HOST": xnat_server,
            "XNAT_INGEST_USER": "admin",
            "XNAT_INGEST_PASS": "admin",
        },
    )

    assert result.exit_code == 0, show_cli_trace(result)

    xsession = xnat_project.experiments[SESSION_ID]
    scan_ids = sorted(xsession.scans)

    assert scan_ids == [
        "1",
        "2",
        "4",
        "countrate",
        "sinogram",
        "listmode",
    ]

    assert list(export_dir.iterdir()) == ["upload-logs"]
