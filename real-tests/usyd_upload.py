import os

from click.testing import CliRunner

from accessionflow.cli import upload_cli
from accessionflow.utils import show_cli_trace

runner = CliRunner()

result = runner.invoke(
    upload_cli,
    [],
    env={
        "ACFLOW_STAGED": "s3://ais-s3-tbp-s3bucket-1afz0bzdw5jd6/STAGING/STAGED",  # os.environ["ACFLOW_STAGED"],
        "ACFLOW_HOST": "https://xnat.sydney.edu.au",
        "ACFLOW_USER": os.environ["ACFLOW_USER"],
        "ACFLOW_PASS": os.environ["ACFLOW_PASS"],
        "ACFLOW_ALWAYSINCLUDE": "medimage/dicom-series",
        "ACFLOW_STORE_CREDENTIALS": os.environ["ACFLOW_STORE_CREDENTIALS"],
        "ACFLOW_LOGGERS": os.environ["ACFLOW_LOGGERS"],
        "ACFLOW_DELETE": "0",
        "ACFLOW_TEMPDIR": os.environ.get("ACFLOW_TEMPDIR", "/tmp"),
        "ACFLOW_REQUIRE_MANIFEST": "1",
        "ACFLOW_CLEANUP_OLDER_THAN": "0",
    },
    catch_exceptions=False,
)

assert result.exit_code == 0, show_cli_trace(result)
