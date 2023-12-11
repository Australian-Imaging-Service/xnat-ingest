from click.testing import CliRunner
from xnat_ingest.cli.stage import stage
from xnat_ingest.utils import show_cli_trace

runner = CliRunner()
result = runner.invoke(
    stage,
    [
        "/Users/tclose/.medimages4tests/cache/dicom/mri/t1w/siemens/skyra/syngo_d13c",
        "/Users/tclose/test-stage",
        "--project-id",
        "TESTSTAGE",
        "--log-level",
        "INFO"
    ],
    catch_exceptions=False
)

assert result.exit_code == 0, show_cli_trace(result)

print(result.stdout)
