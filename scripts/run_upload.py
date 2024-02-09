from xnat_ingest.cli import upload
from xnat_ingest.utils import show_cli_trace
from click.testing import CliRunner

PATTERN = "{PatientName.given_name}_{PatientName.family_name}_{SeriesDate}.*"

runner = CliRunner()

result = runner.invoke(
    upload,
    [
        "s3://ais-s3-tbp-s3bucket-1afz0bzdw5jd6/STAGE-TEST",
    ],
    env={
        "XNAT_INGEST_HOST": "https://xnat.sydney.edu.au",

    },
    catch_exceptions=False,
)

assert result.exit_code == 0, show_cli_trace(result)
