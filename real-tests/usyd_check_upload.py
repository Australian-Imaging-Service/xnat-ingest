import os

from click.testing import CliRunner

from xnat_ingest.cli import check_upload
from xnat_ingest.utils import show_cli_trace

runner = CliRunner()

result = runner.invoke(
    check_upload,
    [
        "s3://ais-s3-tbp-s3bucket-1afz0bzdw5jd6/NEW-STAGING",
        "https://xnat.sydney.edu.au",
        "--disable-progress",
    ],
    env={
        "XINGEST_ADDITIONAL_LOGGERS": "",
        "XINGEST_ALWAYSINCLUDE": "medimage/dicom-series",
        "XINGEST_USE_CURL_JSESSION": "0",
        "XINGEST_VERIFY_SSL": "0",
        "XINGEST_LOGGERS": os.environ["XINGEST_LOGGERS"],
        "XINGEST_USER": os.environ["XINGEST_USER"],
        "XINGEST_PASS": os.environ["XINGEST_PASS"],
        "XINGEST_STORE_CREDENTIALS": os.environ["XINGEST_STORE_CREDENTIALS"],
    },
    catch_exceptions=False,
)

assert result.exit_code == 0, show_cli_trace(result)
