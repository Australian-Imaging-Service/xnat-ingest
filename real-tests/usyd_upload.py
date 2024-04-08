from click.testing import CliRunner
from xnat_ingest.cli import upload
from xnat_ingest.utils import show_cli_trace

runner = CliRunner()

result = runner.invoke(
    upload,
    [],
    env={
        "XNAT_INGEST_UPLOAD_STAGED": "<s3-bucket>",
        "XNAT_INGEST_UPLOAD_HOST": "https://xnat.sydney.edu.au",
        "XNAT_INGEST_UPLOAD_USER": "<role-account-user>",
        "XNAT_INGEST_UPLOAD_PASS": "<role-account-pass>",
        # "XNAT_INGEST_UPLOAD_ALWAYSINCLUDE": "medimage/dicom-series",
        # "XNAT_INGEST_UPLOAD_STORE_CREDENTIALS": "<s3-bucket-access-key>,<s3-bucket-access-secret>",
        # "XNAT_INGEST_UPLOAD_LOGFILE": "<somewhere-sensible>,INFO",
        # "XNAT_INGEST_UPLOAD_DELETE": "0",
        # "XNAT_INGEST_UPLOAD_TEMPDIR": "<somewhere-else-sensible>",
        # "XNAT_INGEST_UPLOAD_REQUIRE_MANIFEST": "1",
        # "XNAT_INGEST_UPLOAD_CLEANUP_OLDER_THAN": "30",
    },
    catch_exceptions=False,
)

assert result.exit_code == 0, show_cli_trace(result)
