import os
import shutil
import time
from datetime import datetime
import typing as ty
from pathlib import Path
from frametree.core.cli import (  # type: ignore[import-untyped]
    define as dataset_define,
    add_source as dataset_add_source,
)
import click
from xnat_ingest.utils import MimeType, XnatLogin  # type: ignore[import-untyped]
import xnat4tests  # type: ignore[import-untyped]
from frametree.core.cli.store import add as store_add  # type: ignore[import-untyped]
from xnat_ingest.cli import stage, upload
from xnat_ingest.cli.stage import STAGED_NAME_DEFAULT
from xnat_ingest.utils import show_cli_trace
from fileformats.medimage import DicomSeries
from medimages4tests.dummy.dicom.pet.wholebody.siemens.biograph_vision.vr20b import (  # type: ignore[import-untyped]
    get_image as get_pet_image,
)
from medimages4tests.dummy.dicom.ct.ac.siemens.biograph_vision.vr20b import (  # type: ignore[import-untyped]
    get_image as get_ac_image,
)
from medimages4tests.dummy.dicom.pet.topogram.siemens.biograph_vision.vr20b import (  # type: ignore[import-untyped]
    get_image as get_topogram_image,
)
from medimages4tests.dummy.dicom.pet.statistics.siemens.biograph_vision.vr20b import (  # type: ignore[import-untyped]
    get_image as get_statistics_image,
)
from conftest import get_raw_data_files
from unittest.mock import patch


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
                "medimage/dicom-series;" "medimage/vnd.siemens.syngo-mi.list-mode.vr20b"
            )
        },
    ):
        result = cli_runner(test_cli_types, [str(out_file)])

    assert result.exit_code == 0, show_cli_trace(result)

    assert out_file.read_text().split("\n") == [
        "fileformats.medimage.dicom.DicomSeries",
        "fileformats.vendor.siemens.medimage.syngo_mi.SyngoMi_ListMode_Vr20b",
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


def test_stage_and_upload(
    xnat_project,
    xnat_config,
    xnat_server,
    cli_runner,
    run_prefix,
    tmp_path: Path,
    tmp_gen_dir: Path,
    capsys,
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

    dataset_locator = f"testxnat//{xnat_project}"

    # Create dataset definition
    result = cli_runner(dataset_define, [dataset_locator])
    assert result.exit_code == 0, show_cli_trace(result)

    for col_name, col_type, col_pattern in [
        ("pet", "medimage/dicom-series", "PET SWB 8MIN"),
        ("topogram", "medimage/dicom-series", "Topogram.*"),
        ("atten_corr", "medimage/dicom-series", "AC CT.*"),
        (
            "listmode",
            "medimage/vnd.siemens.syngo-mi.list-mode.vr20b",
            ".*/LISTMODE",
        ),
        # (
        #     "sinogram",
        #     "medimage/vnd.siemens.syngo-mi.sinogram.vr20b",
        #     ".*/EM_SINO",
        # ),
        (
            "countrate",
            "medimage/vnd.siemens.syngo-mi.count-rate.vr20b",
            ".*/COUNTRATE",
        ),
    ]:
        # Add dataset columns
        result = cli_runner(
            dataset_add_source,
            [
                dataset_locator,
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
            "--associated-files",
            "medimage/vnd.siemens.syngo-mi.count-rate.vr20b,medimage/vnd.siemens.syngo-mi.list-mode.vr20b",
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
            "medimage/vnd.siemens.syngo-mi.raw-data.vr20b",
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
