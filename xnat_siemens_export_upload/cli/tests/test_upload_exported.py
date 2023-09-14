import shutil
import xnat4tests
from medimages4tests.dummy.dicom.pet.tbp.siemens.quadra.s7vb10b import (
    get_image,
    get_raw_data_files,
)
from xnat_siemens_export_upload.cli.upload_exported import upload_exported
from xnat_siemens_export_upload.utils import show_cli_trace


def test_upload_exported(xnat_server, cli_runner, work_dir, run_prefix):
    # Get test image data
    dicom_dir = get_image()
    export_dir = work_dir / "export-dir"
    export_dir.mkdir()
    session_dir = export_dir / "test-session"
    shutil.copytree(dicom_dir, session_dir)
    get_raw_data_files(session_dir)

    # Create project on test XNAT
    project_id = "PROJECT_ID"
    session_id = "987654321"
    with xnat4tests.connect() as xnat_login:
        try:
            xnat_login.projects[project_id]
        except KeyError:
            xnat_login.put(f"/data/archive/projects/{project_id}")

    with xnat4tests.connect() as xnat_login:
        xproject = xnat_login.projects[project_id]
        try:
            xsession = xproject.experiments[session_id]
        except KeyError:
            pass
        else:
            xsession.delete()

    result = cli_runner(
        upload_exported,
        [
            str(export_dir),
        ],
        env={
            "XNAT_HOST": xnat_server,
            "XNAT_USER": "admin",
            "XNAT_PASS": "admin",
        }
    )

    assert result.exit_code == 0, show_cli_trace(result)

    with xnat4tests.connect() as xnat_login:
        xproject = xnat_login.projects[project_id]
        xsession = xproject.experiments[session_id]
        scan_ids = sorted(xsession.scans)

    assert scan_ids == [
        "1",
        "2",
        "3",
        "4",
        "5",
        "502",
        "6",
        "calibration",
        "calibration2",
        "countrate",
        "ct_spl",
        "em_sino",
        "listmode",
        "replay_param",
    ]

    assert not session_dir.exists()
