from click.testing import CliRunner

from accessionflow.stores.xnat.cli import store_cmd
from accessionflow.utils import show_cli_trace

PATTERN = "{PatientName.family_name}_{PatientName.given_name}_{SeriesDate}.*"

runner = CliRunner()

result = runner.invoke(
    store_cmd,
    [
        "/Users/tclo7153/Data/TBP/TEST-STAGING/",
        "http://localhost:8080",
        "--user",
        "admin",
        "--password",
        "admin",
        "--always-include",
        "medimage/dicom-series",
        "--raise-errors",
        "--dont-require-manifest",
    ],
    env={
        "ACFLOW_WAIT_PERIOD": "0",
        "ACFLOW_VERIFY_SSL": "0",
        "ACFLOW_LOGGERS": "stream,debug,stdout;file,debug,/Users/tclo7153/Data/TBP/logs/upload.log",
        "ACFLOW_USE_CURL_JSESSION": "0",
        "ACFLOW_CLEANUP_OLDER_THAN": "-1",
        "ACFLOW_ALWAYSINCLUDE": "medimage/dicom-series",
        "ACFLOW_REQUIRE_MANIFEST": "1",
        "ACFLOW_LOOP": "-1",
        "ACFLOW_TEMPDIR": "/Users/tclo7153/Data/TBP/tmp/",
    },
    catch_exceptions=False,
)

assert result.exit_code == 0, show_cli_trace(result)
