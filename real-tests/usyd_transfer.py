import os
from click.testing import CliRunner
from xnat_ingest.cli import transfer
from xnat_ingest.utils import show_cli_trace

runner = CliRunner()

result = runner.invoke(
    transfer,
    [],
    env={
        "XNAT_INGEST_STAGE_DIR": os.getcwd(),
        "XNAT_INGEST_TRANSFER_REMOTE_STORE": "<s3-bucket>",
        "XNAT_INGEST_TRANSFER_STORE_CREDENTIALS": "<s3-bucket-access-key>,<s3-bucket-access-secret>",
        "XNAT_INGEST_TRANSFER_LOGFILE": "<somewhere-else-sensible>,INFO",
        "XNAT_INGEST_TRANSFER_DELETE": "0",
        "XNAT_INGEST_TRANSFER_XNAT_LOGIN": "https://xnat.sydney.edu.au,<role-account-user>,<role-account-pass>",
    },
    catch_exceptions=False,
)

assert result.exit_code == 0, show_cli_trace(result)
