import os
from pathlib import Path
import logging
import typing as ty
import tempfile

# from logging.handlers import SMTPHandler
import pytest
import click.testing
from click.testing import CliRunner
import xnat4tests  # type: ignore[import-untyped]
from datetime import datetime
from xnat_ingest.utils import logger
from medimages4tests.dummy.raw.pet.siemens.biograph_vision.vr20b.pet_listmode import (
    get_data as get_listmode_data,
)
from medimages4tests.dummy.raw.pet.siemens.biograph_vision.vr20b.pet_countrate import (
    get_data as get_countrate_data,
)

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
    def pytest_exception_interact(call: pytest.CallInfo[ty.Any]) -> None:
        if call.excinfo is not None:
            raise call.excinfo.value

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(excinfo: pytest.ExceptionInfo[BaseException]) -> None:
        raise excinfo.value

    CATCH_CLI_EXCEPTIONS = False
else:
    CATCH_CLI_EXCEPTIONS = True


@pytest.fixture
def catch_cli_exceptions() -> bool:
    return CATCH_CLI_EXCEPTIONS


@pytest.fixture(scope="session")
def run_prefix() -> str:
    "A datetime string used to avoid stale data left over from previous tests"
    return datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")


@pytest.fixture(scope="session")
def xnat_repository() -> None:
    xnat4tests.start_xnat()


@pytest.fixture(scope="session")
def xnat_archive_dir(xnat_repository: None) -> Path:
    return xnat4tests.Config().xnat_root_dir / "archive"  # type: ignore[no-any-return]


@pytest.fixture(scope="session")
def tmp_gen_dir() -> Path:
    # tmp_gen_dir = Path("~").expanduser() / ".xnat-ingest-work3"
    # tmp_gen_dir.mkdir(exist_ok=True)
    # return tmp_gen_dir
    return Path(tempfile.mkdtemp())


@pytest.fixture(scope="session")
def xnat_login(xnat_repository: str) -> ty.Any:
    return xnat4tests.connect()


@pytest.fixture(scope="session")
def xnat_project(xnat_login: ty.Any, run_prefix: str) -> ty.Any:
    project_id = f"INGESTUPLOAD{run_prefix}"
    with xnat4tests.connect() as xnat_login:
        xnat_login.put(f"/data/archive/projects/{project_id}")
    return project_id


@pytest.fixture(scope="session")
def xnat_server(xnat_config: xnat4tests.Config) -> str:
    return xnat_config.xnat_uri  # type: ignore[no-any-return]


@pytest.fixture(scope="session")
def xnat_config(xnat_repository: str) -> xnat4tests.Config:
    return xnat4tests.Config()


@pytest.fixture
def cli_runner(catch_cli_exceptions: bool) -> ty.Callable[..., ty.Any]:
    def invoke(
        *args: ty.Any, catch_exceptions: bool = catch_cli_exceptions, **kwargs: ty.Any
    ) -> click.testing.Result:
        runner = CliRunner()
        result = runner.invoke(*args, catch_exceptions=catch_exceptions, **kwargs)  # type: ignore[misc]
        return result

    return invoke


# # Create a custom handler that captures email messages for testing
# class TestSMTPHandler(SMTPHandler):
#     def __init__(
#         self, mailhost, fromaddr, toaddrs, subject, credentials=None, secure=None
#     ):
#         super().__init__(mailhost, fromaddr, toaddrs, subject, credentials, secure)
#         self.emails = []  # A list to store captured email messages

#     def emit(self, record):
#         # Capture the email message and append it to the list
#         msg = self.format(record)
#         self.emails.append(msg)


def get_raw_data_files(
    out_dir: ty.Optional[Path] = None, **kwargs: ty.Any
) -> ty.List[Path]:
    if out_dir is None:
        out_dir = Path(tempfile.mkdtemp())
    return get_listmode_data(out_dir, skip_unknown=True, **kwargs) + get_countrate_data(  # type: ignore[no-any-return]
        out_dir, skip_unknown=True, **kwargs
    )
