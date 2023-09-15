from xnat_exported_scans.cli.upload import upload
from xnat_exported_scans.utils import show_cli_trace


def test_upload(xnat_project, xnat_server, cli_runner, export_dir):
    # Get test image data

    SESSION_ID = "987654321"
    # Delete any existing sessions from previous test runs
    try:
        xsession = xnat_project.experiments[SESSION_ID]
    except KeyError:
        pass
    else:
        xsession.delete()

    result = cli_runner(
        upload,
        [
            str(export_dir),
            "--non-dicom",
            r".*PET_?(\w+).*PET_?(\w+).*\.IMA"
        ],
        env={
            "XNAT_EXPORTED_SCANS_HOST": xnat_server,
            "XNAT_EXPORTED_SCANS_USER": "admin",
            "XNAT_EXPORTED_SCANS_PASS": "admin",
        }
    )

    assert result.exit_code == 0, show_cli_trace(result)

    xsession = xnat_project.experiments[SESSION_ID]
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

    assert list(export_dir.iterdir()) == ["upload-logs"]
