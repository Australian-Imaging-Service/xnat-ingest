import os
import shutil
import time
import typing as ty
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import click
import pytest
import xnat4tests  # type: ignore[import-untyped]
from fileformats.medimage import DicomSeries
from frametree.core.cli import add_source as dataset_add_source
from frametree.core.cli import define as dataset_define  # type: ignore[import-untyped]
from frametree.core.cli.store import add as store_add  # type: ignore[import-untyped]
from medimages4tests.dummy.dicom.ct.ac.siemens.biograph_vision.vr20b import (
    get_image as get_ac_image,  # type: ignore[import-untyped]
)
from medimages4tests.dummy.dicom.pet.statistics.siemens.biograph_vision.vr20b import (
    get_image as get_statistics_image,  # type: ignore[import-untyped]
)
from medimages4tests.dummy.dicom.pet.topogram.siemens.biograph_vision.vr20b import (
    get_image as get_topogram_image,  # type: ignore[import-untyped]
)
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (
    get_image as get_pet_image,  # type: ignore[import-untyped]
)

from conftest import TEST_S3, get_raw_data_files
from xnat_ingest.cli import check_upload, stage, upload
from xnat_ingest.cli.stage import INVALID_NAME_DEFAULT, STAGED_NAME_DEFAULT
from xnat_ingest.utils import (
    FieldSpec,
    MimeType,  # type: ignore[import-untyped]
    XnatLogin,
    show_cli_trace,
    upload_file_to_s3,
)

PATTERN = "{PatientName.family_name}_{PatientName.given_name}_{SeriesDate}.*"


@click.command(
    help="""Stages images found in the input directories into separate directories for each
imaging acquisition session

FILES_PATH is either the path to a directory containing the files to upload, or
a glob pattern that selects the paths directly

OUTPUT_DIR is the directory that the files for each session are collated to before they
are uploaded to XNAT
""",
)
@click.argument("out_file", type=click.Path(exists=False, path_type=Path))
@click.option(
    "--datatype",
    type=MimeType.cli_type,
    metavar="<mime-type>",
    multiple=True,
    default=None,
    envvar="XINGEST_DATATYPES",
    help=(
        'The MIME-type(s) (or "MIME-like" see FileFormats docs) of potential datatype(s) '
        "of the primary files to to upload, defaults to 'medimage/dicom-series'. "
        "Any formats implemented in the FileFormats Python package "
        "(https://github.com/ArcanaFramework/fileformats) that implement the 'read_metadata' "
        '"extra" are supported, see FF docs on how to add support for new formats.'
    ),
)
@click.option(
    "--xnat-login",
    nargs=3,
    type=XnatLogin.cli_type,
    default=None,
    metavar="<host> <user> <password>",
    help="The XNAT server to upload to plus the user and password to use",
    envvar="XINGEST_XNAT_LOGIN",
)
def test_cli_types(out_file: Path, datatype: ty.List[MimeType]):
    fileformats = [m.datatype for m in datatype]
    with open(out_file, "w") as f:
        f.write("\n".join(f"{f.__module__}.{f.__name__}" for f in fileformats))


def test_mime_type_cli_envvar(tmp_path: Path, cli_runner):

    @click.command()
    @click.argument("out_file", type=click.Path(exists=False, path_type=Path))
    @click.option(
        "--datatype",
        type=MimeType.cli_type,
        metavar="<mime-type>",
        multiple=True,
        default=None,
        envvar="XINGEST_DATATYPES",
        help=(
            'The MIME-type(s) (or "MIME-like" see FileFormats docs) of potential datatype(s) '
            "of the primary files to to upload, defaults to 'medimage/dicom-series'. "
            "Any formats implemented in the FileFormats Python package "
            "(https://github.com/ArcanaFramework/fileformats) that implement the 'read_metadata' "
            '"extra" are supported, see FF docs on how to add support for new formats.'
        ),
    )
    def test_cli_types(out_file: Path, datatype: ty.List[MimeType]):
        fileformats = [m.datatype for m in datatype]
        with open(out_file, "w") as f:
            f.write("\n".join(f"{f.__module__}.{f.__name__}" for f in fileformats))

    out_file = tmp_path / "out.txt"

    # Patch the environment to set the XINGEST_DATATYPES variable using unittest.mock

    with patch.dict(
        os.environ,
        {
            "XINGEST_DATATYPES": (
                "medimage/dicom-series;" "medimage/vnd.siemens.syngo-mi.vr20b.list-mode"
            )
        },
    ):
        result = cli_runner(test_cli_types, [str(out_file)])

    assert result.exit_code == 0, show_cli_trace(result)

    assert out_file.read_text().split("\n") == [
        "fileformats.medimage.dicom.DicomSeries",
        "fileformats.vendor.siemens.medimage.syngo_mi.SyngoMi_Vr20b_ListMode",
    ]


def test_xnat_login_cli_envvar(tmp_path: Path, cli_runner):

    @click.command()
    @click.argument("out_file", type=click.Path(exists=False, path_type=Path))
    @click.option(
        "--xnat-login",
        nargs=3,
        type=XnatLogin.cli_type,
        default=None,
        metavar="<host> <user> <password>",
        help="The XNAT server to upload to plus the user and password to use",
        envvar="XINGEST_XNAT_LOGIN",
    )
    def test_cli_types(out_file: Path, xnat_login: XnatLogin):
        with open(out_file, "w") as f:
            f.write(xnat_login.host + "\n")
            f.write(xnat_login.user + "\n")
            f.write(xnat_login.password)

    out_file = tmp_path / "out.txt"

    # Patch the environment to set the XINGEST_DATATYPES variable using unittest.mock

    with patch.dict(
        os.environ,
        {
            "XINGEST_XNAT_LOGIN": (
                "https://xnat.example.com:8888,a_user,a_passwordwithspecialchars*#%,;##@"
            )
        },
    ):
        result = cli_runner(test_cli_types, [str(out_file)])

    assert result.exit_code == 0, show_cli_trace(result)

    assert out_file.read_text().split("\n") == [
        "https://xnat.example.com:8888",
        "a_user",
        "a_passwordwithspecialchars*#%,;##@",
    ]


def test_field_spec_cli_envvar(tmp_path: Path, cli_runner):

    @click.command()
    @click.argument("out_file", type=click.Path(exists=False, path_type=Path))
    @click.option(
        "--field",
        type=FieldSpec.cli_type,
        nargs=2,
        multiple=True,
        default=[["ImageType[2:]", "generic/file-set"]],
        metavar="<field> <datatype>",
        envvar="XINGEST_FIELD",
        help=(
            "The keywords of the metadata field to extract the XNAT imaging resource ID from "
            "for different datatypes (use `generic/file-set` as a catch-all if required)."
        ),
    )
    def test_cli_types(out_file: Path, field: ty.List[FieldSpec]):
        with open(out_file, "w") as f:
            for field_spec in field:
                f.write(f"{field_spec.field},{field_spec.datatype.mime_like}\n")

    out_file = tmp_path / "out.txt"

    # Patch the environment to set the XINGEST_DATATYPES variable using unittest.mock

    for val, expected in [
        ["ImageType[2:]", ["ImageType[2:],core/file-set"]],
        [
            "ImageType[-1],medimage/vnd.siemens.syngo-mi.vr20b.large-raw-data",
            ["ImageType[-1],medimage/vnd.siemens.syngo-mi.vr20b.large-raw-data"],
        ],
        [
            "SeriesNumber,medimage/dicom-series;UID,medimage/vnd.siemens.syngo-mi.vr20b.large-raw-data",
            [
                "SeriesNumber,medimage/dicom-series",
                "UID,medimage/vnd.siemens.syngo-mi.vr20b.large-raw-data",
            ],
        ],
    ]:
        with patch.dict(os.environ, {"XINGEST_FIELD": val}):
            result = cli_runner(test_cli_types, [str(out_file)])

        assert result.exit_code == 0, show_cli_trace(result)

        assert out_file.read_text().split("\n")[:-1] == expected


@pytest.mark.parametrize(
    "upload_source",
    [
        "local-dir",
        pytest.param(
            "s3-bucket",
            marks=pytest.mark.skipif(TEST_S3 is None, reason="S3 not configured"),
        ),
    ],
)
def test_stage_and_upload(
    xnat_project,
    xnat_config,
    xnat_server,
    cli_runner,
    tmp_path: Path,
    tmp_gen_dir: Path,
    upload_source: str,
):
    # Get test image data

    dicoms_dir = tmp_path / "dicoms"
    dicoms_dir.mkdir(exist_ok=True)

    associated_files_dir = tmp_path / "non-dicoms"
    associated_files_dir.mkdir(exist_ok=True)

    staging_dir = tmp_path / "staging"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir()

    stage_log_file = tmp_path / "stage-logs.log"
    if stage_log_file.exists():
        os.unlink(stage_log_file)

    upload_log_file = tmp_path / "upload-logs.log"
    if upload_log_file.exists():
        os.unlink(upload_log_file)

    check_upload_log_file = tmp_path / "check-upload-logs.log"
    if check_upload_log_file.exists():
        os.unlink(check_upload_log_file)

    # Delete any existing sessions from previous test runs
    session_ids = []
    with xnat4tests.connect() as xnat_login:
        for i, c in enumerate("abc"):
            first_name = f"First{c.upper()}"
            last_name = f"Last{c.upper()}"
            PatientID = f"subject{i}"
            AccessionNumber = f"98765432{i}"
            session_ids.append(f"{PatientID}_{AccessionNumber}")

            StudyInstanceUID = (
                f"1.3.12.2.1107.5.1.4.10016.3000002308242209356530000001{i}"
            )

            series = DicomSeries(
                get_pet_image(
                    tmp_gen_dir / f"pet{i}",
                    first_name=first_name,
                    last_name=last_name,
                    StudyInstanceUID=StudyInstanceUID,
                    PatientID=PatientID,
                    AccessionNumber=AccessionNumber,
                    StudyID=xnat_project,
                ).iterdir()
            )
            for dcm in series.contents:
                os.link(dcm, dicoms_dir / f"pet{i}-{dcm.fspath.name}")
            series = DicomSeries(
                get_ac_image(
                    tmp_gen_dir / f"ac{i}",
                    first_name=first_name,
                    last_name=last_name,
                    StudyInstanceUID=StudyInstanceUID,
                    PatientID=PatientID,
                    AccessionNumber=AccessionNumber,
                    StudyID=xnat_project,
                ).iterdir()
            )
            for dcm in series.contents:
                os.link(dcm, dicoms_dir / f"ac{i}-{dcm.fspath.name}")
            series = DicomSeries(
                get_topogram_image(
                    tmp_gen_dir / f"topogram{i}",
                    first_name=first_name,
                    last_name=last_name,
                    StudyInstanceUID=StudyInstanceUID,
                    PatientID=PatientID,
                    AccessionNumber=AccessionNumber,
                    StudyID=xnat_project,
                ).iterdir()
            )
            for dcm in series.contents:
                os.link(dcm, dicoms_dir / f"topogram{i}-{dcm.fspath.name}")
            series = DicomSeries(
                get_statistics_image(
                    tmp_gen_dir / f"statistics{i}",
                    first_name=first_name,
                    last_name=last_name,
                    StudyInstanceUID=StudyInstanceUID,
                    PatientID=PatientID,
                    AccessionNumber=AccessionNumber,
                    StudyID=xnat_project,
                ).iterdir()
            )
            for dcm in series.contents:
                os.link(dcm, dicoms_dir / f"statistics{i}-{dcm.fspath.name}")
            assoc_fspaths = get_raw_data_files(
                tmp_gen_dir / f"non-dicom{i}",
                first_name=first_name,
                last_name=last_name,
                date_time=datetime(2023, 8, 25, 15, 50, 5, i),
            )
            for assoc_fspath in assoc_fspaths:
                os.link(
                    assoc_fspath,
                    associated_files_dir
                    / f"{assoc_fspath.stem}-{i}{assoc_fspath.suffix}",
                )

    # Create data store
    result = cli_runner(
        store_add,
        [
            "xnat",
            "testxnat",
            "--server",
            xnat_server,
            "--user",
            xnat_config.xnat_user,
            "--password",
            xnat_config.xnat_password,
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)

    dataset_address = f"testxnat//{xnat_project}"

    # Create dataset definition
    result = cli_runner(dataset_define, [dataset_address])
    assert result.exit_code == 0, show_cli_trace(result)

    for col_name, col_type, col_pattern in [
        ("pet", "medimage/dicom-series", "PET SWB 8MIN"),
        ("topogram", "medimage/dicom-series", "Topogram.*"),
        ("atten_corr", "medimage/dicom-series", "AC CT.*"),
        (
            "listmode",
            "medimage/vnd.siemens.syngo-mi.vr20b.list-mode",
            ".*/LISTMODE",
        ),
        # (
        #     "sinogram",
        #     "medimage/vnd.siemens.syngo-mi.vr20b.sinogram",
        #     ".*/EM_SINO",
        # ),
        (
            "countrate",
            "medimage/vnd.siemens.syngo-mi.vr20b.count-rate",
            ".*/COUNTRATE",
        ),
    ]:
        # Add dataset columns
        result = cli_runner(
            dataset_add_source,
            [
                dataset_address,
                col_name,
                col_type,
                "--path",
                col_pattern,
            ],
        )
        assert result.exit_code == 0, show_cli_trace(result)

    result = cli_runner(
        stage,
        [
            str(dicoms_dir),
            str(staging_dir),
            "--resource-field",
            "ImageType[-1]",
            "medimage/vnd.siemens.syngo-mi.vr20b.raw-data",
            "--associated-files",
            "medimage/vnd.siemens.syngo-mi.vr20b.count-rate|medimage/vnd.siemens.syngo-mi.vr20b.list-mode",
            str(associated_files_dir)
            + "/{PatientName.family_name}_{PatientName.given_name}*.ptd",
            r".*/[^\.]+.[^\.]+.[^\.]+.(?P<id>\d+)\.[A-Z]+_(?P<resource>[^\.]+).*",
            "--additional-logger",
            "xnat",
            "--additional-logger",
            "fileformats",
            "--raise-errors",
            "--delete",
            "--xnat-login",
            "http://localhost:8080",
            "admin",
            "admin",
        ],
        env={
            "XINGEST_LOGGERS": f"file,debug,{stage_log_file};stream,info,stdout",
            "XINGEST_DEIDENTIFY": "0",
        },
    )

    assert result.exit_code == 0, show_cli_trace(result)
    logs = stage_log_file.read_text()
    assert "Staging completed successfully" in logs, show_cli_trace(result)
    assert " - fileformats - " in logs, show_cli_trace(result)
    stdout_logs = result.stdout
    assert "Staging completed successfully" in stdout_logs, show_cli_trace(result)

    if upload_source == "s3-bucket":
        # Upload staged data to S3 bucket

        test_s3 = TEST_S3[5:] if TEST_S3.startswith("s3://") else TEST_S3
        if "/" in test_s3:
            s3_bucket, s3_prefix = test_s3.split("/", maxsplit=1)
            s3_prefix += "/"
        else:
            s3_bucket = test_s3
            s3_prefix = ""

        s3_prefix += f"xnat-ingest-tests/{xnat_project}"
        stage_dir = staging_dir / STAGED_NAME_DEFAULT
        for fspath in stage_dir.glob("**/*"):
            if fspath.is_file():
                upload_file_to_s3(
                    fspath,
                    s3_bucket,
                    f"{s3_prefix.rstrip('/')}/{fspath.relative_to(stage_dir)}",
                )

        staging_source = f"s3://{s3_bucket}/{s3_prefix}"

    else:
        staging_source = str(staging_dir / STAGED_NAME_DEFAULT)

    result = cli_runner(
        upload,
        [
            staging_source,
            "--additional-logger",
            "xnat",
            "--always-include",
            "medimage/dicom-series",
            "--raise-errors",
            "--method",
            "tgz_file",
            "medimage/dicom-series",
            "--method",
            "tar_file",
            "medimage/vnd.siemens.syngo-mi.vr20b.raw-data",
            "--use-curl-jsession",
            "--wait-period",
            "0",
            "--num-files-per-batch",
            "107",
        ],
        env={
            "XINGEST_HOST": xnat_server,
            "XINGEST_USER": "admin",
            "XINGEST_PASS": "admin",
            "XINGEST_LOGGERS": f"file,debug,{upload_log_file};stream,info,stdout",
        },
    )

    assert result.exit_code == 0, show_cli_trace(result)
    file_logs = upload_log_file.read_text()
    assert "Upload completed successfully" in file_logs, show_cli_trace(result)
    assert " - xnat - " in file_logs, show_cli_trace(result)
    stdout_logs = result.stdout
    assert "Upload completed successfully" in stdout_logs, show_cli_trace(result)
    assert "as all the resources already exist on XNAT" not in stdout_logs

    # Run upload a second time, and check that already uploaded sessions are skipped
    result = cli_runner(
        upload,
        [
            str(staging_dir / STAGED_NAME_DEFAULT),
            "--additional-logger",
            "xnat",
            "--always-include",
            "medimage/dicom-series",
            "--raise-errors",
            "--method",
            "tgz_file",
            "medimage/dicom-series",
            "--method",
            "tar_file",
            "medimage/vnd.siemens.syngo-mi.vr20b.raw-data",
            "--use-curl-jsession",
            "--wait-period",
            "0",
            "--num-files-per-batch",
            "107",
        ],
        env={
            "XINGEST_HOST": xnat_server,
            "XINGEST_USER": "admin",
            "XINGEST_PASS": "admin",
            "XINGEST_LOGGERS": f"file,debug,{upload_log_file};stream,info,stdout",
        },
    )

    assert result.exit_code == 0, show_cli_trace(result)
    assert (
        "as all the resources already exist on XNAT" in result.stdout
    ), show_cli_trace(result)

    with xnat4tests.connect() as xnat_login:
        xproject = xnat_login.projects[xnat_project]
        for session_id in session_ids:
            xsession = xproject.experiments[session_id]
            scan_ids = sorted(s.id for s in xsession.scans)

            assert scan_ids == [
                "1",
                "2",
                "4",
                "6",
                "602",
                # "603",
            ]

    # Run upload a second time, and check that already uploaded sessions are skipped
    result = cli_runner(
        check_upload,
        [
            str(staging_dir / STAGED_NAME_DEFAULT),
            "--always-include",
            "medimage/dicom-series",
            "--raise-errors",
            "--use-curl-jsession",
        ],
        env={
            "XINGEST_HOST": xnat_server,
            "XINGEST_USER": "admin",
            "XINGEST_PASS": "admin",
            "XINGEST_LOGGERS": f"file,debug,{check_upload_log_file};stream,info,stdout",
        },
    )

    assert result.exit_code == 0, show_cli_trace(result)
    file_logs = check_upload_log_file.read_text()
    assert (
        "No issues found with the upload, staged files can be removed" in file_logs
    ), show_cli_trace(result)


def test_stage_wait_period(
    cli_runner,
    tmp_path: Path,
    capsys,
):
    # Get test image data

    staging_dir = tmp_path / "staging"
    dicoms_path = tmp_path / "dicoms"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir()

    staged_dir = staging_dir / STAGED_NAME_DEFAULT

    stage_log_file = tmp_path / "stage-logs.log"
    if stage_log_file.exists():
        os.unlink(stage_log_file)

    # Generate a test DICOM image
    get_pet_image(dicoms_path)

    result = cli_runner(
        stage,
        [
            str(dicoms_path),
            str(staging_dir),
            "--raise-errors",
            "--delete",
            "--wait-period",
            "10",
        ],
        env={
            "XINGEST_DEIDENTIFY": "0",
            "XINGEST_LOGGERS": f"file,debug,{stage_log_file};stream,info,stdout",
        },
    )

    assert result.exit_code == 0, show_cli_trace(result)
    logs = stage_log_file.read_text()
    assert " as it was last modified " in logs, show_cli_trace(result)
    assert not list(staged_dir.iterdir())

    time.sleep(10)

    result = cli_runner(
        stage,
        [
            str(dicoms_path),
            str(staging_dir),
            "--raise-errors",
            "--delete",
            "--wait-period",
            "10",
        ],
        env={
            "XINGEST_DEIDENTIFY": "0",
            "XINGEST_LOGGERS": f"file,debug,{stage_log_file};stream,info,stdout",
        },
    )

    assert result.exit_code == 0, show_cli_trace(result)
    logs = stage_log_file.read_text()
    assert "Successfully staged " in logs, show_cli_trace(result)
    assert list(staged_dir.iterdir())


def test_stage_invalid_ids(
    cli_runner,
    tmp_path: Path,
    capsys,
):
    # Get test image data

    staging_dir = tmp_path / "staging"
    dicoms_path = tmp_path / "dicoms"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir()

    staged_dir = staging_dir / STAGED_NAME_DEFAULT
    invalid_dir = staging_dir / INVALID_NAME_DEFAULT

    stage_log_file = tmp_path / "stage-logs.log"
    if stage_log_file.exists():
        os.unlink(stage_log_file)

    # Generate a test DICOM image without a patient ID
    get_pet_image(dicoms_path, PatientID="")

    result = cli_runner(
        stage,
        [
            str(dicoms_path),
            str(staging_dir),
            "--subject-field",
            "PatientID",
            "generic/file-set",
            "--raise-errors",
        ],
        env={
            "XINGEST_DEIDENTIFY": "0",
            "XINGEST_LOGGERS": f"file,debug,{stage_log_file};stream,info,stdout",
        },
    )

    assert result.exit_code == 0, show_cli_trace(result)
    logs = stage_log_file.read_text()
    assert "-INVALID_MISSING_PATIENTID_" in logs, show_cli_trace(result)
    assert not list(staged_dir.iterdir())
    assert len(list(invalid_dir.iterdir())) == 1


def test_check_upload_error(
    cli_runner,
    tmp_path: Path,
    capsys,
):
    # Get test image data

    input_dir = tmp_path / "inputs"
    staging_dir = tmp_path / "staging"
    staged_dir = staging_dir / STAGED_NAME_DEFAULT
    check_upload_log_file = tmp_path / "stage-logs.log"

    result = cli_runner(
        stage,
        [
            str(dicoms_path),
            str(staging_dir),
            "--raise-errors",
            "--delete",
            "--wait-period",
            "10",
        ],
        env={
            "XINGEST_DEIDENTIFY": "0",
            "XINGEST_LOGGERS": "stream,info,stdout",
        },
    )

    assert result.exit_code == 0, show_cli_trace(result)

    time.sleep(10)

    result = cli_runner(
        stage,
        [
            str(dicoms_path),
            str(staging_dir),
            "--raise-errors",
            "--delete",
            "--wait-period",
            "10",
        ],
        env={
            "XINGEST_DEIDENTIFY": "0",
            "XINGEST_LOGGERS": f"file,debug,{stage_log_file};stream,info,stdout",
        },
    )

    assert result.exit_code == 0, show_cli_trace(result)
    logs = stage_log_file.read_text()
    assert "Successfully staged " in logs, show_cli_trace(result)
    assert list(staged_dir.iterdir())
