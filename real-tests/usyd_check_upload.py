import os

from click.testing import CliRunner

from accessionflow.stores.xnat.cli import check_store_cmd
from accessionflow.utils import show_cli_trace

runner = CliRunner()

result = runner.invoke(
    check_store_cmd,
    [
        "s3://ais-s3-tbp-s3bucket-1afz0bzdw5jd6/NEW-STAGING",
        "https://xnat.sydney.edu.au",
        "--disable-progress",
    ],
    env={
        "ACFLOW_ADDITIONAL_LOGGERS": "",
        "ACFLOW_ALWAYSINCLUDE": "medimage/dicom-series",
        "ACFLOW_USE_CURL_JSESSION": "0",
        "ACFLOW_VERIFY_SSL": "0",
        "ACFLOW_LOGGERS": os.environ["ACFLOW_LOGGERS"],
        "ACFLOW_USER": os.environ["ACFLOW_USER"],
        "ACFLOW_PASS": os.environ["ACFLOW_PASS"],
        "ACFLOW_STORE_CREDENTIALS": os.environ["ACFLOW_STORE_CREDENTIALS"],
    },
    catch_exceptions=False,
)

assert result.exit_code == 0, show_cli_trace(result)
