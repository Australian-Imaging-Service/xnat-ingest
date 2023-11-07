import os
from pathlib import Path
import logging
from logging.handlers import SMTPHandler
import pytest
from click.testing import CliRunner
import xnat4tests
from datetime import datetime
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
def run_prefix():
    "A datetime string used to avoid stale data left over from previous tests"
    return datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")


@pytest.fixture(scope="session")
def xnat_repository():
    xnat4tests.start_xnat()


@pytest.fixture(scope="session")
def xnat_archive_dir(xnat_repository):
    return xnat4tests.Config().xnat_root_dir / "archive"


@pytest.fixture(scope="session")
def tmp_gen_dir():
    tmp_gen_dir = Path("~").expanduser() / ".xnat-ingest-work2"
    tmp_gen_dir.mkdir(exist_ok=True)
    return tmp_gen_dir


@pytest.fixture(scope="session")
def xnat_login(xnat_repository):
    return xnat4tests.connect()


@pytest.fixture(scope="session")
def xnat_project(xnat_login, run_prefix):
    project_id = f"INGESTUPLOAD"  # {run_prefix}"
    with xnat4tests.connect() as xnat_login:
        xnat_login.put(f"/data/archive/projects/{project_id}")
    return project_id


@pytest.fixture(scope="session")
def xnat_server(xnat_config):
    return xnat_config.xnat_uri


@pytest.fixture(scope="session")
def xnat_config(xnat_repository):
    return xnat4tests.Config()


@pytest.fixture
def cli_runner(catch_cli_exceptions):
    def invoke(*args, catch_exceptions=catch_cli_exceptions, **kwargs):
        runner = CliRunner()
        result = runner.invoke(*args, catch_exceptions=catch_exceptions, **kwargs)
        return result

    return invoke


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
