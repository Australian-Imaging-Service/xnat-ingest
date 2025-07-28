import os
from xnat_ingest.cli import upload
from xnat_ingest.utils import show_cli_trace
from click.testing import CliRunner

PATTERN = "{PatientName.family_name}_{PatientName.given_name}_{SeriesDate}.*"

runner = CliRunner()

result = runner.invoke(
    upload,
    [
        "/Users/tclo7153/Data/TBP/NEXT_STAGED",
        "http://localhost:8080",
        "admin",
        "--password",
        "admin",
        "--always-include",
        "medimage/dicom-series",
        "--raise-errors",
        "--dont-require-manifest",
    ],
    env={
        "XINGEST_WAIT_PERIOD": "0",
        "XINGEST_VERIFY_SSL": "0",
        "XINGEST_LOGGERS": "stream,debug,stdout;file,debug,/Users/tclo7153/Data/TBP/logs/upload.log",
        "XINGEST_USE_CURL_JSESSION": "0",
        "XINGEST_CLEANUP_OLDER_THAN": "-1",
        "XINGEST_STORE_CREDENTIALS": os.environ["XINGEST_STORE_CREDENTIALS"],
        "XINGEST_ALWAYSINCLUDE": "medimage/dicom-series",
        "XINGEST_REQUIRE_MANIFEST": "1",
        "XINGEST_LOOP": "-1",
        "XINGEST_TEMPDIR": "/Users/tclo7153/Data/TBP/tmp/",
    },
    catch_exceptions=False,
)

assert result.exit_code == 0, show_cli_trace(result)
