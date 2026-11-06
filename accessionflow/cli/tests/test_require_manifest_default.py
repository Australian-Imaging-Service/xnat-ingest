"""The manifest check is on unless the caller turns it off.

`upload` and `attach` declared --require-manifest/--dont-require-manifest with
default None, and the loaders read None as "not required". So a resource without
its manifest was uploaded as it was, although store() and attach() both default
to True.
"""

import typing as ty
from unittest.mock import MagicMock, patch

import pytest

from accessionflow.cli import attach_cmd
from accessionflow.stores.xnat.cli import store_cmd
from conftest import show_cli_trace

CLEAN_ENV = {
    "ACFLOW_REQUIRE_MANIFEST": None,
    "ACFLOW_STORE_CREDENTIALS": None,
    "ACFLOW_HOST": None,
    "ACFLOW_USER": None,
    "ACFLOW_PASS": None,
    "ACFLOW_LOOP": None,
}


@pytest.mark.parametrize("cmd", [store_cmd, attach_cmd])
def test_require_manifest_option_defaults_to_true(cmd: ty.Any) -> None:
    (option,) = [p for p in cmd.params if p.name == "require_manifest"]
    assert option.default is True


@pytest.mark.parametrize(
    "flags, env_value, expected",
    [
        ([], None, True),
        (["--require-manifest"], None, True),
        (["--dont-require-manifest"], None, False),
        ([], "false", False),
        ([], "true", True),
        (["--require-manifest"], "false", True),
    ],
)
def test_upload_passes_require_manifest(
    cli_runner: ty.Any, flags: list[str], env_value: ty.Optional[str], expected: bool
) -> None:
    with (
        patch("accessionflow.stores.xnat.cli.store_cli.Xnat") as mock_xnat_cls,
        patch("accessionflow.stores.xnat.cli.store_cli.store") as mock_upload,
    ):
        mock_xnat_cls.return_value = MagicMock()
        mock_upload.return_value = []
        result = cli_runner(
            store_cmd,
            ["/staged", "https://xnat.example.org", "--user", "u", "--password", "p"]
            + flags,
            env={**CLEAN_ENV, "ACFLOW_REQUIRE_MANIFEST": env_value},
        )
    assert result.exit_code == 0, show_cli_trace(result)
    assert mock_upload.call_args.kwargs["require_manifest"] is expected


@pytest.mark.parametrize(
    "flags,env_value,expected",
    [
        ([], None, False),
        (["--no-uid-check"], None, True),
        ([], "1", True),
    ],
)
def test_upload_passes_no_uid_check(
    cli_runner: ty.Any, flags: list[str], env_value: ty.Optional[str], expected: bool
) -> None:
    with (
        patch("accessionflow.stores.xnat.cli.store_cli.Xnat") as mock_xnat_cls,
        patch("accessionflow.stores.xnat.cli.store_cli.store") as mock_upload,
    ):
        mock_xnat_cls.return_value = MagicMock()
        mock_upload.return_value = []
        result = cli_runner(
            store_cmd,
            ["/staged", "https://xnat.example.org", "--user", "u", "--password", "p"]
            + flags,
            env={**CLEAN_ENV, "ACFLOW_NO_UID_CHECK": env_value},
        )
    assert result.exit_code == 0, show_cli_trace(result)
    assert mock_upload.call_args.kwargs["skip_uid_check"] is expected
