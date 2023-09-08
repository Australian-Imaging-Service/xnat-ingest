import xnat4tests
from xnat_siemens_export_upload.cli.upload_exported import upload_exported
from xnat_siemens_export_upload.utils import show_cli_trace


def test_remove_shadow_scans(xnat_server, cli_runner, work_dir, run_prefix):

    output_dir = work_dir / "output"
    project_id = f"{run_prefix}uploadexported"
    with xnat4tests.connect() as xnat_login:
        xnat_login.put(f"/data/archive/projects/{project_id}")
    with xnat4tests.connect() as xnat_login:
        xproject = xnat_login.projects[project_id]

        xsession = xsubject.experiments["session"]
