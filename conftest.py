import os
import logging
from pathlib import Path
import shutil
from logging.handlers import SMTPHandler
import pytest
from click.testing import CliRunner
import xnat4tests
from medimages4tests.dummy.dicom.pet.tbp.siemens.quadra.s7vb10b import (
    get_image,
    get_raw_data_files,
)
from xnat_ingest.utils import logger

# Set DEBUG logging for unittests

sch = logging.StreamHandler()
sch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sch.setFormatter(formatter)
logger.addHandler(sch)

PROJECT_ID = "PROJECT_ID"

# For debugging in IDE's don't catch raised exceptions and let the IDE
# break at it
if os.getenv("_PYTEST_RAISE", "0") != "0":

    @pytest.hookimpl(tryfirst=True)
    def pytest_exception_interact(call):
        raise call.excinfo.value

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(excinfo):
        raise excinfo.value

    CATCH_CLI_EXCEPTIONS = False
else:
    CATCH_CLI_EXCEPTIONS = True


@pytest.fixture
def catch_cli_exceptions():
    return CATCH_CLI_EXCEPTIONS


@pytest.fixture(scope="session")
def xnat_repository():
    xnat4tests.start_xnat()


@pytest.fixture(scope="session")
def xnat_archive_dir(xnat_repository):
    return xnat4tests.Config().xnat_root_dir / "archive"


@pytest.fixture(scope="session")
def xnat_login(xnat_repository):
    # Ensure that project ID is present in test XNAT before we connect, as new projects
    # often don't show up until you log-off/log-in again
    with xnat4tests.connect() as xlogin:
        try:
            xlogin.projects[PROJECT_ID]
        except KeyError:
            xlogin.put(f"/data/archive/projects/{PROJECT_ID}")
    return xnat4tests.connect()


@pytest.fixture(scope="session")
def xnat_server(xnat_repository):
    return xnat4tests.Config().xnat_uri


@pytest.fixture
def cli_runner(catch_cli_exceptions):
    def invoke(*args, catch_exceptions=catch_cli_exceptions, **kwargs):
        runner = CliRunner()
        result = runner.invoke(*args, catch_exceptions=catch_exceptions, **kwargs)
        return result

    return invoke


@pytest.fixture
def export_dir(tmp_path: Path) -> Path:
    dicom_dir = get_image()
    export_dir = tmp_path / "export-dir"
    export_dir.mkdir()
    session_dir = export_dir / "test-session"
    shutil.copytree(dicom_dir, session_dir)
    get_raw_data_files(session_dir)
    return export_dir


@pytest.fixture
def xnat_project(xnat_login, scope="session"):
    return xnat_login.projects[PROJECT_ID]


# Create a custom handler that captures email messages for testing
class TestSMTPHandler(SMTPHandler):
    def __init__(
        self, mailhost, fromaddr, toaddrs, subject, credentials=None, secure=None
    ):
        super().__init__(mailhost, fromaddr, toaddrs, subject, credentials, secure)
        self.emails = []  # A list to store captured email messages

    def emit(self, record):
        # Capture the email message and append it to the list
        msg = self.format(record)
        self.emails.append(msg)
