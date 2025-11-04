import logging

from click.testing import CliRunner

from xnat_ingest.cli import stage
from xnat_ingest.utils import show_cli_trace

runner = CliRunner()

WORK_DIR = "/Users/tclo7153/Data/TBP"

logging.basicConfig(level=logging.DEBUG)

result = runner.invoke(
    stage,
    [
        f"{WORK_DIR}/RAW-DATA-EXPORT/VENTURE_P001_Galligas_Post-RT",
        f"{WORK_DIR}/TEST-STAGED",
    ],  # XINGEST_DIR
    env={
        "XINGEST_ADDITIONAL_LOGGERS": "fileformats.",
        "XINGEST_AWS_BUCKET": "s3://ais-s3-tbp-s3bucket-1afz0bzdw5jd6/STAGING",
        "XINGEST_COPY_MODE": "hardlink_or_copy",
        "XINGEST_DATATYPES": (
            "medimage/dicom-series;"
            "medimage/vnd.siemens.syngo-mi.vr20b.count-rate;"
            "medimage/vnd.siemens.syngo-mi.vr20b.list-mode;"
            "medimage/vnd.siemens.syngo-mi.vr20b.normalisation;"
            "medimage/vnd.siemens.syngo-mi.vr20b.sinogram;"
            "medimage/vnd.siemens.syngo-mi.vr20b.dynamic-sinogram-series;"
            "medimage/vnd.siemens.syngo-mi.vr20b.parameterisation;"
            "medimage/vnd.siemens.syngo-mi.vr20b.ct-spl;"
            "medimage/vnd.siemens.syngo-mi.vr20b.physio"
        ),
        "XINGEST_DEIDENTIFIED_DIR_NAME": "DEIDENTIFIED",
        "XINGEST_DEIDENTIFY": "0",
        "XINGEST_DELETE": "0",
        "XINGEST_INVALID_DIR_NAME": "INVALID",
        "XINGEST_LOGGERS": f"stream,info,stdout;file,debug,{WORK_DIR}/xnat-ingest-test-stage.log",
        "XINGEST_PRE_STAGE_DIR_NAME": "PRE-STAGE",
        "XINGEST_PROJECT": "PatientComments",
        "XINGEST_RESOURCE": "ImageType[2:],medimage/vnd.siemens.syngo-mi.vr20b.raw-data",
        "XINGEST_SCAN_DESC": "SeriesDescription",
        "XINGEST_SCAN_ID": "SeriesNumber",
        "XINGEST_SPACES_TO_UNDERSCORES": "0",
        "XINGEST_STAGED_DIR_NAME": "STAGED",
        "XINGEST_SUBJECT": "PatientID",
        "XINGEST_VISIT": "AccessionNumber",
        "XINGEST_WORK_DIR": "/Users/tclo7153/Data/TBP/XNAT-INGEST-WORK",
        # "XINGEST_DICOMS_PATH": "/vol/vmhost/kubernetes/<path>/<to>/<dicom>/<store>/**/*.IMA",
        # "XINGEST_DIR": "/vol/vmhost/usyd-data-export/STAGING",
        # "XINGEST_PROJECT": "ProtocolName",
        # "XINGEST_SUBJECT": "PatientID",
        # "XINGEST_VISIT": "AccessionNumber",
        # "XINGEST_ASSOCIATED": "/vol/vmhost/usyd-data-export/RAW-DATA-EXPORT/{PatientName.family_name}_{PatientName.given_name}/.ptd","./[^\\.]+.[^\\.]+.[^\\.]+.(?P\\d+).[A-Z]+_(?P[^\\.]+).",
        # "XINGEST_DELETE": "0",
        # "XINGEST_LOGFILE": "<somewhere-sensible>,INFO",
        # "XINGEST_DEIDENTIFY": "1",
    },
    catch_exceptions=False,
)


if result.exit_code != 0:
    show_cli_trace(result)
