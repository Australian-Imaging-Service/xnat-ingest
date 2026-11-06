"""CLI loops keep a start-to-start interval despite work time or clock changes."""

import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner


class StopLoop(Exception):
    pass


# Commands provided by the XNAT plugin, and the module that the API they call is
# looked up in
XNAT_COMMANDS = {
    "label": "accessionflow.cli.label_cli",
    "store": "accessionflow.stores.xnat.cli.store_cli",
}

CASES = [
    ("label", "label", []),
    ("attach", "attach", ["generic/file", "*", ".*"]),
    ("deidentify", "deidentify", ["{spec}"]),
    (
        "collate",
        "collate_orthanc",
        ["http://orthanc.test", "{in}", "{out}", "user", "password"],
    ),
    (
        "store",
        "store",
        ["{in}", "https://xnat.test", "--user", "user", "--password", "password"],
    ),
]


@pytest.mark.parametrize("module_name,api_name,extra", CASES)
@pytest.mark.parametrize(
    "elapsed,loop,expected_sleep", [(2, 10, 8), (12, 10, 0), (2, -1, None)]
)
def test_loop_interval(
    module_name, api_name, extra, elapsed, loop, expected_sleep, tmp_path, monkeypatch
):
    module = importlib.import_module("accessionflow.cli." + module_name + "_cli")
    if module_name in XNAT_COMMANDS:
        # The command is provided by the XNAT plugin, the API it calls is either the
        # plugin's or the core one
        command = getattr(
            importlib.import_module(
                "accessionflow.stores.xnat.cli." + module_name + "_cli"
            ),
            module_name + "_cmd",
        )
        module = importlib.import_module(XNAT_COMMANDS[module_name])
    else:
        command = getattr(module, api_name + "_cmd")
    paths = {name: str(tmp_path / name) for name in ["in", "out", "spec"]}
    for name in ["in", "spec"]:
        (tmp_path / name).mkdir()
    args = [arg.format(**paths) for arg in extra]
    if api_name not in ("collate_orthanc", "store"):
        args = [paths["in"], paths["out"]] + args
    args += ["--loop", str(loop)]
    now = [100.0]
    sleeps = []

    def work(**kwargs):
        now[0] += elapsed
        return []

    def sleep(seconds):
        sleeps.append(seconds)
        raise StopLoop

    monkeypatch.setattr(module, api_name, MagicMock(side_effect=work))
    monkeypatch.setattr(
        module, "time", SimpleNamespace(monotonic=lambda: now[0], sleep=sleep)
    )
    # A backwards wall-clock adjustment must not affect the elapsed duration.
    wall = MagicMock()
    wall.now.side_effect = [
        datetime.datetime(2026, 1, 1, 0, 10),
        datetime.datetime(2026, 1, 1),
    ]
    monkeypatch.setattr(
        module, "datetime", SimpleNamespace(datetime=wall), raising=False
    )
    log = MagicMock()
    monkeypatch.setattr(module, "logger", log)
    if api_name == "store":
        monkeypatch.setattr(module, "Xnat", MagicMock())
    # Keep a developer's ACFLOW_* environment out of this CLI invocation.
    import os

    env = {key: None for key in os.environ if key.startswith("ACFLOW_")}
    result = CliRunner().invoke(command, args, env=env)
    assert getattr(module, api_name).call_count == 1, result.output
    if expected_sleep is None:
        assert result.exit_code == 0, result.exception
        assert sleeps == []
    else:
        assert isinstance(result.exception, StopLoop), (result.output, result.exception)
        assert sleeps == [expected_sleep]
        timing = log.info.call_args.args
        assert timing[1:3] == (elapsed, expected_sleep)
