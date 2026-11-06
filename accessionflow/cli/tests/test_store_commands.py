"""The stages whose subcommands are provided by store plugins"""

import os
from pathlib import Path
from unittest.mock import MagicMock

import click
import pytest
from click.testing import CliRunner

from accessionflow.cli import cli, label_group, make_label_command
from accessionflow.model.store import (
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


def test_store_needs_a_label_level() -> None:
    with pytest.raises(ValueError, match="at least one"):
        Store(name="flat", title="Flat", labels=())
    # Any number of levels, or levels named when labelling, are fine
    Store(name="flat", title="Flat", labels=(LabelLevel("dataset"),))
    Store(name="free", title="Free", labels=None)


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
    for stage in ("label", "store", "check-store"):
        result = runner.invoke(cli, [stage, "--help"], env=CLEAN_ENV)
        assert result.exit_code == 0, result.output
        assert "xnat" in result.output


def test_missing_store_plugin_is_explained() -> None:
    result = CliRunner().invoke(cli, ["store", "nope", "x"], env=CLEAN_ENV)
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
    assert kwargs["label_fields"] == {
        "project": "StudyComments",
        "subject": "{PatientID}_{StudyDate:%Y%m%d}",
        "session": "AccessionNumber",
    }
    assert list(kwargs["label_fields"]) == ["project", "subject", "session"]
    assert kwargs["acquisition_field"] == "ProtocolName"
    assert kwargs["constant_labels"] == {"project": "PROJ"}
    assert kwargs["store_type"] == "xnat"
    assert kwargs["single_resource"] is False  # XNAT scans hold several resources


def test_label_single_resource_follows_store_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import accessionflow.cli.label_cli as label_cli

    label = MagicMock(return_value=[])
    monkeypatch.setattr(label_cli, "label", label)
    data_store = Store(
        name="singleres",
        title="Single",
        labels=(LabelLevel("a", "A"), LabelLevel("b", "B"), LabelLevel("c", "C")),
        multiple_resources=False,
    )
    make_label_command(data_store)
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
    assert kwargs["label_fields"]["project"] == "StudyID"
    assert kwargs["label_fields"]["session"] == "StudyInstanceUID"
    assert kwargs["acquisition_field"] == "SeriesNumber"
    assert kwargs["constant_labels"] == {"project": "PROJ"}


def test_label_command_follows_store_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A store with no acquisition label, and a level without a default"""
    import accessionflow.cli.label_cli as label_cli

    label = MagicMock(return_value=[])
    monkeypatch.setattr(label_cli, "label", label)
    data_store = Store(
        name="testrepo",
        title="Test",
        labels=(
            LabelLevel("study", default="StudyID"),
            LabelLevel("participant"),
            LabelLevel("visit", default="AccessionNumber"),
        ),
        envvar_prefix="TESTREPO_",
    )
    command = make_label_command(data_store)
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
        assert kwargs["label_fields"] == {
            "study": "StudyID",
            "participant": "PatientID",
            "visit": "AccessionNumber",
        }
        assert kwargs["acquisition_field"] is None
        assert kwargs["store_type"] == "testrepo"
        assert kwargs["single_resource"] is False
    finally:
        del label_group.commands["testrepo"]


@pytest.mark.parametrize(
    "old,new", [("upload", "store"), ("check-upload", "check-store")]
)
def test_former_stage_names_still_work(old: str, new: str) -> None:
    """The stages' names before they were renamed run the same subcommands, warn
    that they are deprecated, and are hidden from the help"""
    runner = CliRunner()
    help_result = runner.invoke(cli, ["--help"], env=CLEAN_ENV)
    assert new in help_result.output
    assert f"  {old} " not in help_result.output
    result = runner.invoke(cli, [old, "xnat", "--help"], env=CLEAN_ENV)
    assert result.exit_code == 0, result.output
    assert f"'accessionflow {old}' is deprecated" in result.output
    assert f"'accessionflow {new}' instead" in result.output
    alias, target = cli.commands[old], cli.commands[new]
    assert isinstance(alias, click.Group) and isinstance(target, click.Group)
    ctx = click.Context(cli)
    assert alias.get_command(ctx, "xnat") is target.get_command(ctx, "xnat")


def test_former_stage_name_warns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import accessionflow.stores.xnat.cli.store_cli as store_cli

    monkeypatch.setattr(store_cli, "Xnat", MagicMock())
    monkeypatch.setattr(store_cli, "store", MagicMock(return_value=[]))
    (tmp_path / "staged").mkdir()
    result = CliRunner().invoke(
        cli,
        [
            "upload",
            "xnat",
            str(tmp_path / "staged"),
            "https://xnat.test",
            "--user",
            "u",
            "--password",
            "p",
        ],
        env=CLEAN_ENV,
    )
    assert result.exit_code == 0, result.output
    assert "'accessionflow upload' is deprecated" in result.output
    assert "'accessionflow store'" in result.output
    store_cli.store.assert_called_once()  # type: ignore[attr-defined]


def test_former_fields_name_still_works(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import accessionflow.cli.select_fields_cli as select_fields_cli

    select_fields = MagicMock(return_value=[])
    monkeypatch.setattr(select_fields_cli, "select_fields", select_fields)
    (tmp_path / "in").mkdir()
    runner = CliRunner()
    help_result = runner.invoke(cli, ["--help"], env=CLEAN_ENV)
    assert "select-fields" in help_result.output
    assert "  fields " not in help_result.output
    result = runner.invoke(
        cli,
        ["fields", str(tmp_path / "in"), str(tmp_path / "out"), "--extract", "a", "b"],
        env=CLEAN_ENV,
    )
    assert result.exit_code == 0, result.output
    assert "'accessionflow fields' is deprecated" in result.output
    assert "'accessionflow select-fields' instead" in result.output
    select_fields.assert_called_once()
