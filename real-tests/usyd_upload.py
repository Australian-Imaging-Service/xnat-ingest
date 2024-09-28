from click.testing import CliRunner
from xnat_ingest.cli import upload
from xnat_ingest.utils import show_cli_trace

runner = CliRunner()

result = runner.invoke(
    upload,
    [],
    env={
        "XINGEST_STAGED": "<s3-bucket>",
        "XINGEST_HOST": "https://xnat.sydney.edu.au",
        "XINGEST_USER": "<role-account-user>",
        "XINGEST_PASS": "<role-account-pass>",
        "XINGEST_ALWAYSINCLUDE": "medimage/dicom-series",
        "XINGEST_STORE_CREDENTIALS": "<s3-bucket-access-key>,<s3-bucket-access-secret>",
        "XINGEST_LOGFILE": "<somewhere-sensible>,INFO",
        "XINGEST_DELETE": "0",
        "XINGEST_TEMPDIR": "<somewhere-else-sensible>",
        "XINGEST_REQUIRE_MANIFEST": "1",
        "XINGEST_CLEANUP_OLDER_THAN": "30",
    },
    catch_exceptions=False,
)

assert result.exit_code == 0, show_cli_trace(result)
