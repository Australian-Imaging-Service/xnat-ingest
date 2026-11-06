from accessionflow.cli.sort import sort
from accessionflow.utils import show_cli_trace
from click.testing import CliRunner

runner = CliRunner()
result = runner.invoke(
    sort,
    [
        "/Users/tclose/.medimages4tests/cache/dicom/mri/t1w/siemens/skyra/syngo_d13c",
        "/Users/tclose/test-stage",
        "--project-id",
        "TESTSTAGE",
        "--log-level",
        "INFO",
    ],
    catch_exceptions=False,
)

assert result.exit_code == 0, show_cli_trace(result)

print(result.stdout)
