import typing as ty
from pathlib import Path

import click
import yaml

from conftest import show_cli_trace
from xnat_ingest.cli.workflow_cli import check_cmd


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
            "xnat": {
                "server": "https://xnat.example.org",
                "user": "u",
                "password": "p",
            },
            "stages": [
                {
                    "name": "grp",
                    "command": "group",
                    "args": {"input_paths": [str(tmp_path)]},
                },
                {"name": "asn", "command": "assign", "input": "grp"},
                {"name": "up", "command": "upload", "input": "asn"},
            ],
        },
    )
    result = cli_runner(check_cmd, [str(spec_path)])
    assert result.exit_code == 0, show_cli_trace(result)
    assert "OK: 'acemid'" in result.output
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
                    "args": {"input_dir": str(tmp_path), "spec_dir": str(tmp_path)},
                    "enabled": False,
                }
            ]
        },
    )
    result = cli_runner(check_cmd, [str(spec_path)])
    assert result.exit_code == 0, show_cli_trace(result)
    assert "[disabled]" in result.output


def test_workflow_group_registered_on_root_cli() -> None:
    from xnat_ingest.cli import cli

    assert "workflow" in cli.commands
    assert isinstance(cli.commands["workflow"], click.Group)
    assert {"check", "run", "serve"} <= set(cli.commands["workflow"].commands)
