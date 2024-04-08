from click.testing import CliRunner
from xnat_ingest.cli import stage
from xnat_ingest.utils import show_cli_trace

runner = CliRunner()


result = runner.invoke(
    stage,
    [],
    env={
        "XNAT_INGEST_STAGE_DICOMS_PATH": "/vol/vmhost/kubernetes/<path>/<to>/<dicom>/<store>/**/*.IMA",
        "XNAT_INGEST_STAGE_DIR": "/vol/vmhost/usyd-data-export/STAGING",
        "XNAT_INGEST_STAGE_PROJECT": "ProtocolName",
        "XNAT_INGEST_STAGE_SUBJECT": "PatientID",
        "XNAT_INGEST_STAGE_VISIT": "AccessionNumber",
        "XNAT_INGEST_STAGE_ASSOCIATED": '"/vol/vmhost/usyd-data-export/RAW-DATA-EXPORT/{PatientName.family_name}_{PatientName.given_name}/.ptd","./[^\\.]+.[^\\.]+.[^\\.]+.(?P\\d+).[A-Z]+_(?P[^\\.]+)."',
        "XNAT_INGEST_STAGE_DELETE": "0",
        "XNAT_INGEST_STAGE_LOGFILE": "<somewhere-sensible>,INFO",
        "XNAT_INGEST_STAGE_DEIDENTIFY": "1",
    },
    catch_exceptions=False,
)


assert result.exit_code == 0, show_cli_trace(result)
