from click.testing import CliRunner
from xnat_ingest.cli import transfer
from xnat_ingest.utils import show_cli_trace

runner = CliRunner()


result = runner.invoke(
    transfer,
    [],
    env={
        "XINGEST_DIR": "/Users/tclose/Data/testing/staging-test/",
        "XNAT_INGEST_TRANSFER_LOGFILE": "/Users/tclose/Desktop/test-log.log,INFO",
        "XNAT_INGEST_TRANSFER_DELETE": "0",
    },
    catch_exceptions=False,
)

assert result.exit_code == 0, show_cli_trace(result)
