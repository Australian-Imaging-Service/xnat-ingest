import os
from xnat_ingest.cli import upload
from xnat_ingest.utils import show_cli_trace
from click.testing import CliRunner

PATTERN = "{PatientName.given_name}_{PatientName.family_name}_{SeriesDate}.*"

runner = CliRunner()

result = runner.invoke(
    upload,
    [
        "s3://ais-s3-tbp-s3bucket-1afz0bzdw5jd6/venture-stage",
        "https://xnat.sydney.edu.au",
        "8c6ddc67-fd72-4ef3-858d-94dfd3a1d904",
        "--always-include",
        "all",
    ],
    env=os.environ,
    catch_exceptions=False,
)

assert result.exit_code == 0, show_cli_trace(result)
