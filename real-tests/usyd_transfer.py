from accessionflow.utils import show_cli_trace
from click.testing import CliRunner

from accessionflow.cli import transfer

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
