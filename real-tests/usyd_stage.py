import logging

from click.testing import CliRunner

from accessionflow.cli import sort
from accessionflow.utils import show_cli_trace

runner = CliRunner()

WORK_DIR = "/Users/tclo7153/Data/TBP"

logging.basicConfig(level=logging.DEBUG)

result = runner.invoke(
    sort,
    [
        f"{WORK_DIR}/RAW-DATA-EXPORT/VENTURE_P001_Galligas_Post-RT",
        f"{WORK_DIR}/TEST-STAGED",
    ],  # ACFLOW_DIR
    env={
        "ACFLOW_ADDITIONAL_LOGGERS": "fileformats.",
        "ACFLOW_AWS_BUCKET": "s3://ais-s3-tbp-s3bucket-1afz0bzdw5jd6/STAGING",
        "ACFLOW_COPY_MODE": "hardlink_or_copy",
        "ACFLOW_DATATYPES": (
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
        "ACFLOW_DEIDENTIFIED_DIR_NAME": "DEIDENTIFIED",
        "ACFLOW_DEIDENTIFY": "0",
        "ACFLOW_DELETE": "0",
        "ACFLOW_INVALID_DIR_NAME": "INVALID",
        "ACFLOW_LOGGERS": f"stream,info,stdout;file,debug,{WORK_DIR}/accessionflow-test-stage.log",
        "ACFLOW_PRE_STAGE_DIR_NAME": "PRE-STAGE",
        "ACFLOW_PROJECT": "PatientComments",
        "ACFLOW_RESOURCE": "ImageType[2:],medimage/vnd.siemens.syngo-mi.vr20b.raw-data",
        "ACFLOW_SCAN_DESC": "SeriesDescription",
        "ACFLOW_SCAN_ID": "SeriesNumber",
        "ACFLOW_SPACES_TO_UNDERSCORES": "0",
        "ACFLOW_STAGED_DIR_NAME": "STAGED",
        "ACFLOW_SUBJECT": "PatientID",
        "ACFLOW_VISIT": "AccessionNumber",
        "ACFLOW_WORK_DIR": "/Users/tclo7153/Data/TBP/XNAT-INGEST-WORK",
        # "ACFLOW_DICOMS_PATH": "/vol/vmhost/kubernetes/<path>/<to>/<dicom>/<store>/**/*.IMA",
        # "ACFLOW_DIR": "/vol/vmhost/usyd-data-export/STAGING",
        # "ACFLOW_PROJECT": "ProtocolName",
        # "ACFLOW_SUBJECT": "PatientID",
        # "ACFLOW_VISIT": "AccessionNumber",
        # "ACFLOW_ASSOCIATED": "/vol/vmhost/usyd-data-export/RAW-DATA-EXPORT/{PatientName.family_name}_{PatientName.given_name}/.ptd","./[^\\.]+.[^\\.]+.[^\\.]+.(?P\\d+).[A-Z]+_(?P[^\\.]+).",
        # "ACFLOW_DELETE": "0",
        # "ACFLOW_LOGFILE": "<somewhere-sensible>,INFO",
        # "ACFLOW_DEIDENTIFY": "1",
    },
    catch_exceptions=False,
)


if result.exit_code != 0:
    show_cli_trace(result)
