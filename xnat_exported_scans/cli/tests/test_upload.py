from xnat_siemens_export_upload.cli.upload import upload
from xnat_siemens_export_upload.utils import show_cli_trace


def test_upload(xnat_project, xnat_server, cli_runner, run_prefix, export_dir):
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
        ],
        env={
            "XNAT_HOST": xnat_server,
            "XNAT_USER": "admin",
            "XNAT_PASS": "admin",
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
