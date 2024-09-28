from click.testing import CliRunner
from xnat_ingest.cli import stage
from xnat_ingest.utils import show_cli_trace

runner = CliRunner()


result = runner.invoke(
    stage,
    [],
    env={
        "XINGEST_DICOMS_PATH": "/vol/vmhost/kubernetes/<path>/<to>/<dicom>/<store>/**/*.IMA",
        "XINGEST_DIR": "/vol/vmhost/usyd-data-export/STAGING",
        "XINGEST_PROJECT": "ProtocolName",
        "XINGEST_SUBJECT": "PatientID",
        "XINGEST_VISIT": "AccessionNumber",
        "XINGEST_ASSOCIATED": '"/vol/vmhost/usyd-data-export/RAW-DATA-EXPORT/{PatientName.family_name}_{PatientName.given_name}/.ptd","./[^\\.]+.[^\\.]+.[^\\.]+.(?P\\d+).[A-Z]+_(?P[^\\.]+)."',
        "XINGEST_DELETE": "0",
        "XINGEST_LOGFILE": "<somewhere-sensible>,INFO",
        "XINGEST_DEIDENTIFY": "1",
    },
    catch_exceptions=False,
)


assert result.exit_code == 0, show_cli_trace(result)
