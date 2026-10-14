"""The manifest check is on unless the caller turns it off.

`upload` and `associate` declared --require-manifest/--dont-require-manifest with
default None, and the loaders read None as "not required". So a resource without
its manifest was uploaded as it was, although upload() and associate() both default
to True.
"""

import typing as ty
from unittest.mock import MagicMock, patch

import pytest

from conftest import show_cli_trace
from xnat_ingest.cli import associate_cmd, upload_cmd

CLEAN_ENV = {
    "XINGEST_REQUIRE_MANIFEST": None,
    "XINGEST_STORE_CREDENTIALS": None,
    "XINGEST_HOST": None,
    "XINGEST_USER": None,
    "XINGEST_PASS": None,
    "XINGEST_LOOP": None,
}


@pytest.mark.parametrize("cmd", [upload_cmd, associate_cmd])
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
        patch("xnat_ingest.cli.upload_cli.Xnat") as mock_xnat_cls,
        patch("xnat_ingest.cli.upload_cli.upload") as mock_upload,
    ):
        mock_xnat_cls.return_value = MagicMock()
        mock_upload.return_value = []
        result = cli_runner(
            upload_cmd,
            ["/staged", "https://xnat.example.org", "--user", "u", "--password", "p"]
            + flags,
            env={**CLEAN_ENV, "XINGEST_REQUIRE_MANIFEST": env_value},
        )
    assert result.exit_code == 0, show_cli_trace(result)
    assert mock_upload.call_args.kwargs["require_manifest"] is expected
