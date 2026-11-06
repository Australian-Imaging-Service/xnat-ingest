"""The stages whose subcommands are provided by store plugins"""

import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from accessionflow.cli import cli, label_group, make_label_command
from accessionflow.model.store_config import (
    LabelLevel,
    Store,
    get_store,
    store_plugins,
)

CLEAN_ENV = {key: None for key in os.environ if key.startswith("ACFLOW_")}


def test_xnat_plugin_is_found() -> None:
    assert "xnat" in store_plugins()
    xnat = get_store("xnat")
    assert [level.name for level in xnat.labels] == ["project", "subject", "session"]
    assert xnat.acquisition is not None and xnat.acquisition.name == "scan"


def test_get_store_names_the_installed_plugins() -> None:
    with pytest.raises(ValueError, match="installed: .*xnat"):
        get_store("not-a-store")


def test_store_needs_three_label_levels() -> None:
    with pytest.raises(ValueError, match="exactly 3"):
        Store(name="flat", title="Flat", labels=(LabelLevel("dataset"),))


def test_store_rejects_repeated_label_names() -> None:
    with pytest.raises(ValueError, match="repeated"):
        Store(
            name="dup",
            title="Dup",
            labels=(LabelLevel("a"), LabelLevel("b"), LabelLevel("c")),
            acquisition=LabelLevel("a"),
        )


def test_stages_list_the_plugin_subcommands() -> None:
    runner = CliRunner()
    for stage in ("label", "upload", "check-upload"):
        result = runner.invoke(cli, [stage, "--help"], env=CLEAN_ENV)
        assert result.exit_code == 0, result.output
        assert "xnat" in result.output


def test_missing_store_plugin_is_explained() -> None:
    result = CliRunner().invoke(cli, ["upload", "nope", "x"], env=CLEAN_ENV)
    assert result.exit_code == 2
    assert "no store plugin named 'nope' is installed" in result.output
    assert "accessionflow.stores.nope" in result.output
    assert "xnat" in result.output  # the stores that are available


def test_label_xnat_maps_scan_to_acquisition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import accessionflow.cli.label_cli as label_cli

    label = MagicMock(return_value=[])
    monkeypatch.setattr(label_cli, "label", label)
    (tmp_path / "in").mkdir()
    result = CliRunner().invoke(
        cli,
        [
            "label",
            "xnat",
            str(tmp_path / "in"),
            str(tmp_path / "out"),
            "--subject",
            "{PatientID}_{StudyDate:%Y%m%d}",
            "--scan",
            "ProtocolName",
            "--constant-project-id",
            "PROJ",
        ],
        env=CLEAN_ENV,
    )
    assert result.exit_code == 0, result.output
    kwargs = label.call_args.kwargs
    assert kwargs["project_field"] == "StudyComments"
    assert kwargs["subject_field"] == "{PatientID}_{StudyDate:%Y%m%d}"
    assert kwargs["session_field"] == "AccessionNumber"
    assert kwargs["acquisition_field"] == "ProtocolName"
    assert kwargs["project_id"] == "PROJ"
    assert kwargs["single_resource"] is False  # XNAT scans hold several resources


def test_label_single_resource_follows_store_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import accessionflow.cli.label_cli as label_cli

    label = MagicMock(return_value=[])
    monkeypatch.setattr(label_cli, "label", label)
    store = Store(
        name="singleres",
        title="Single",
        labels=(LabelLevel("a", "A"), LabelLevel("b", "B"), LabelLevel("c", "C")),
        multiple_resources=False,
    )
    make_label_command(store)
    try:
        (tmp_path / "in").mkdir()
        result = CliRunner().invoke(
            cli,
            ["label", "singleres", str(tmp_path / "in"), str(tmp_path / "out")],
            env=CLEAN_ENV,
        )
        assert result.exit_code == 0, result.output
        assert label.call_args.kwargs["single_resource"] is True
    finally:
        del label_group.commands["singleres"]


def test_label_xnat_options_keep_their_env_vars(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import accessionflow.cli.label_cli as label_cli

    label = MagicMock(return_value=[])
    monkeypatch.setattr(label_cli, "label", label)
    (tmp_path / "in").mkdir()
    env = dict(
        CLEAN_ENV,
        ACFLOW_PROJECT="StudyID",
        ACFLOW_SESSION="StudyInstanceUID",
        ACFLOW_SCAN="SeriesNumber",
        ACFLOW_CONSTANT_PROJECT_ID="PROJ",
    )
    result = CliRunner().invoke(
        cli, ["label", "xnat", str(tmp_path / "in"), str(tmp_path / "out")], env=env
    )
    assert result.exit_code == 0, result.output
    kwargs = label.call_args.kwargs
    assert kwargs["project_field"] == "StudyID"
    assert kwargs["session_field"] == "StudyInstanceUID"
    assert kwargs["acquisition_field"] == "SeriesNumber"
    assert kwargs["project_id"] == "PROJ"


def test_label_command_follows_store_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A store with no acquisition label, and a level without a default"""
    import accessionflow.cli.label_cli as label_cli

    label = MagicMock(return_value=[])
    monkeypatch.setattr(label_cli, "label", label)
    store = Store(
        name="testrepo",
        title="Test",
        labels=(
            LabelLevel("study", default="StudyID"),
            LabelLevel("participant"),
            LabelLevel("visit", default="AccessionNumber"),
        ),
        envvar_prefix="TESTREPO_",
    )
    command = make_label_command(store)
    try:
        assert label_group.commands["testrepo"] is command
        options = {opt for param in command.params for opt in param.opts}
        assert {"--study", "--participant", "--visit", "--constant-study-id"} <= (
            options
        )
        assert "--scan" not in options
        (tmp_path / "in").mkdir()
        args = ["label", "testrepo", str(tmp_path / "in"), str(tmp_path / "out")]
        result = CliRunner().invoke(cli, args, env=CLEAN_ENV)
        assert result.exit_code == 2  # --participant has no default
        assert "--participant" in result.output
        result = CliRunner().invoke(
            cli, args, env=dict(CLEAN_ENV, TESTREPO_PARTICIPANT="PatientID")
        )
        assert result.exit_code == 0, result.output
        kwargs = label.call_args.kwargs
        assert (
            kwargs["project_field"],
            kwargs["subject_field"],
            kwargs["session_field"],
            kwargs["acquisition_field"],
        ) == ("StudyID", "PatientID", "AccessionNumber", None)
        assert kwargs["single_resource"] is False
    finally:
        del label_group.commands["testrepo"]
