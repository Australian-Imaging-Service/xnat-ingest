import os

from click.testing import CliRunner

from xnat_ingest.cli import upload
from xnat_ingest.utils import show_cli_trace

runner = CliRunner()

result = runner.invoke(
    upload,
    [],
    env={
        "XINGEST_STAGED": "s3://ais-s3-tbp-s3bucket-1afz0bzdw5jd6/STAGING/STAGED",  # os.environ["XINGEST_STAGED"],
        "XINGEST_HOST": "https://xnat.sydney.edu.au",
        "XINGEST_USER": os.environ["XINGEST_USER"],
        "XINGEST_PASS": os.environ["XINGEST_PASS"],
        "XINGEST_ALWAYSINCLUDE": "medimage/dicom-series",
        "XINGEST_STORE_CREDENTIALS": os.environ["XINGEST_STORE_CREDENTIALS"],
        "XINGEST_LOGFILE": os.environ["XINGEST_LOGFILE"],
        "XINGEST_DELETE": "0",
        "XINGEST_TEMPDIR": os.environ.get("XINGEST_TEMPDIR", "/tmp"),
        "XINGEST_REQUIRE_MANIFEST": "1",
        "XINGEST_CLEANUP_OLDER_THAN": "0",
    },
    catch_exceptions=False,
)

assert result.exit_code == 0, show_cli_trace(result)
