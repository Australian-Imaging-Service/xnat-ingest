from click.testing import CliRunner

from xnat_ingest.cli import stage
from xnat_ingest.utils import show_cli_trace

runner = CliRunner()


result = runner.invoke(
    stage,
    [
        "/Users/tclo7153/Data/TBP/venture-raw-data",
        "/Users/tclo7153/Data/TBP/TEST-STAGED",
    ],  # XINGEST_DIR
    env={
        "XINGEST_ADDITIONAL_LOGGERS": "fileformats.",
        "XINGEST_AWS_BUCKET": "s3://ais-s3-tbp-s3bucket-1afz0bzdw5jd6/STAGING",
        "XINGEST_COPY_MODE": "hardlink_or_copy",
        "XINGEST_DATATYPES": (
            "medimage/dicom-series;"
            "medimage/vnd.siemens.syngo-mi.count-rate.vr20b;"
            "medimage/vnd.siemens.syngo-mi.list-mode.vr20b;"
            "medimage/vnd.siemens.syngo-mi.normalisation.vr20b;"
            "medimage/vnd.siemens.syngo-mi.sinogram.vr20b;"
            "medimage/vnd.siemens.syngo-mi.dynamic-sinogram-series.vr20b;"
            "medimage/vnd.siemens.syngo-mi.parameterisation.vr20b;"
            "medimage/vnd.siemens.syngo-mi.ct-spl.vr20b;"
            "medimage/vnd.siemens.syngo-mi.physio.vr20b"
        ),
        "XINGEST_DEIDENTIFIED_DIR_NAME": "DEIDENTIFIED",
        "XINGEST_DEIDENTIFY": "0",
        "XINGEST_DELETE": "0",
        "XINGEST_INVALID_DIR_NAME": "INVALID",
        "XINGEST_LOGGERS": "stream,info,stdout;file,debug,/tmp/LOGS/xnat-ingest-stage.log",
        "XINGEST_PRE_STAGE_DIR_NAME": "PRE-STAGE",
        "XINGEST_PROJECT": "PatientComments",
        "XINGEST_RESOURCE": "ImageType[2:],medimage/vnd.siemens.syngo-mi.raw-data.vr20b",
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
