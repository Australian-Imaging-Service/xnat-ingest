import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from accessionflow.cli import deidentify_cmd

CLEAN_ENV = {key: None for key in os.environ if key.startswith("ACFLOW_")}


@pytest.mark.parametrize("envvar", ["ACFLOW_RECIPE_DIR", "ACFLOW_SPEC_DIR"])
def test_recipe_dir_from_env(
    envvar: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The recipe directory can be given by its env var, or by the env var's name
    from before the directory was renamed from 'spec' to 'recipe'"""
    import accessionflow.cli.deidentify_cli as deidentify_cli

    deidentify = MagicMock(return_value=[])
    monkeypatch.setattr(deidentify_cli, "deidentify", deidentify)
    for name in ("in", "recipes"):
        (tmp_path / name).mkdir()
    result = CliRunner().invoke(
        deidentify_cmd,
        [str(tmp_path / "in"), str(tmp_path / "out")],
        env={**CLEAN_ENV, envvar: str(tmp_path / "recipes")},
    )
    assert result.exit_code == 0, result.output
    assert deidentify.call_args.kwargs["recipe_dir"] == tmp_path / "recipes"
