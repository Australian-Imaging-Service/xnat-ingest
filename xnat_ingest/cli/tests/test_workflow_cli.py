import typing as ty
from pathlib import Path
from unittest.mock import patch

import click
import yaml

from conftest import show_cli_trace
from xnat_ingest.cli.workflow_cli import check_cmd, deploy_cmd


def _write_spec(tmp_path: Path, content: dict) -> Path:
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(content))
    return path


def test_workflow_check_valid_spec_reports_stage_order(
    cli_runner: ty.Any, tmp_path: Path
) -> None:
    spec_path = _write_spec(
        tmp_path,
        {
            "name": "acemid",
            "params": {
                "xnat_password": {"description": "XNAT password", "secret": True}
            },
            "stages": [
                {
                    "name": "grp",
                    "command": "group",
                    "args": {"input_paths": [str(tmp_path)]},
                },
                {
                    "name": "asn",
                    "command": "assign",
                    "input": "grp",
                    "args": {
                        "project": "StudyComments",
                        "subject": "PatientID",
                        "session": "AccessionNumber",
                    },
                },
                {
                    "name": "up",
                    "command": "upload",
                    "input": "asn",
                    "args": {
                        "server": "https://xnat.example.org",
                        "user": "u",
                        "password": "${xnat_password}",
                    },
                },
            ],
        },
    )
    result = cli_runner(check_cmd, [str(spec_path), "-p", "xnat_password=hunter2"])
    assert result.exit_code == 0, show_cli_trace(result)
    assert "OK: 'acemid'" in result.output
    assert "xnat_password (required) [secret] - XNAT password" in result.output
    assert "grp (group)" in result.output
    assert "asn (assign)  <- grp" in result.output
    assert "up (upload)  <- asn" in result.output


def test_workflow_check_invalid_spec_reports_error(
    cli_runner: ty.Any, tmp_path: Path
) -> None:
    spec_path = _write_spec(
        tmp_path,
        {"stages": [{"name": "a", "command": "bogus-command"}]},
    )
    result = cli_runner(check_cmd, [str(spec_path)], catch_exceptions=True)
    assert result.exit_code != 0
    assert "INVALID" in result.output
    assert "bogus-command" in result.output


def test_workflow_check_disabled_stage_marked(
    cli_runner: ty.Any, tmp_path: Path
) -> None:
    spec_path = _write_spec(
        tmp_path,
        {
            "stages": [
                {
                    "name": "d",
                    "command": "deidentify",
                    "args": {
                        "input_dir": str(tmp_path),
                        "spec_dir": str(tmp_path),
                        "reid_dir": str(tmp_path),
                    },
                    "enabled": False,
                }
            ]
        },
    )
    result = cli_runner(check_cmd, [str(spec_path)])
    assert result.exit_code == 0, show_cli_trace(result)
    assert "[disabled]" in result.output


def test_workflow_check_missing_required_param_reports_error(
    cli_runner: ty.Any, tmp_path: Path
) -> None:
    spec_path = _write_spec(
        tmp_path,
        {
            "params": {
                "xnat_password": {"description": "XNAT password", "secret": True}
            },
            "stages": [
                {
                    "name": "up",
                    "command": "upload",
                    "args": {
                        "input_dir": str(tmp_path),
                        "server": "https://xnat.example.org",
                        "password": "${xnat_password}",
                    },
                }
            ],
        },
    )
    result = cli_runner(check_cmd, [str(spec_path)], catch_exceptions=True)
    assert result.exit_code != 0
    assert "INVALID" in result.output
    assert "xnat_password" in result.output


def test_workflow_check_param_option_malformed_raises(
    cli_runner: ty.Any, tmp_path: Path
) -> None:
    spec_path = _write_spec(
        tmp_path,
        {"stages": [{"name": "a", "command": "assign", "args": {"input_dir": "/x"}}]},
    )
    result = cli_runner(
        check_cmd, [str(spec_path), "-p", "not-a-key-value-pair"], catch_exceptions=True
    )
    assert result.exit_code != 0


def test_workflow_deploy_cli_forwards_options_to_api(
    cli_runner: ty.Any, tmp_path: Path
) -> None:
    with patch("xnat_ingest.cli.workflow_cli.deploy_workflow", return_value=[]) as m:
        result = cli_runner(
            deploy_cmd,
            [
                str(tmp_path),
                "--prefect-api-url",
                "https://prefect.example.org/api",
                "--prefect-api-key",
                "secret",
                "--work-pool",
                "my-pool",
                "-p",
                "input_dir=/data/in",
            ],
        )
    assert result.exit_code == 0, show_cli_trace(result)
    m.assert_called_once()
    _, kwargs = m.call_args
    assert kwargs["prefect_api_url"] == "https://prefect.example.org/api"
    assert kwargs["prefect_api_key"] == "secret"
    assert kwargs["work_pool"] == "my-pool"
    assert kwargs["param_overrides"] == {"input_dir": "/data/in"}


def test_workflow_deploy_cli_reports_errors_and_exits_nonzero(
    cli_runner: ty.Any, tmp_path: Path
) -> None:
    with patch(
        "xnat_ingest.cli.workflow_cli.deploy_workflow",
        return_value=["some/spec.yaml: boom"],
    ):
        result = cli_runner(deploy_cmd, [str(tmp_path)], catch_exceptions=True)
    assert result.exit_code != 0
    assert "boom" in result.output


def test_workflow_group_registered_on_root_cli() -> None:
    from xnat_ingest.cli import cli

    assert "workflow" in cli.commands
    assert isinstance(cli.commands["workflow"], click.Group)
    assert {"check", "run", "serve", "deploy"} <= set(cli.commands["workflow"].commands)
