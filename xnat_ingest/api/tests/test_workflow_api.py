"""Tests workflow_api.py's check/run/serve/deploy against a minimal fake `prefect`
(+ `prefect.settings`) module - no real Prefect install needed - so these stay
fast and dependency-free."""

import sys
import types
import typing as ty
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from xnat_ingest.api import workflow_api
from xnat_ingest.workflow.errors import WorkflowSpecError
from xnat_ingest.workflow.stages import STAGES


class _FakeTask:
    def __init__(self, fn: ty.Callable) -> None:
        self.fn = fn

    def __call__(self, *args: ty.Any, **kwargs: ty.Any) -> ty.Any:
        return self.fn(*args, **kwargs)


class _FakeFlow:
    def __init__(self, fn: ty.Callable, name: str) -> None:
        self.fn = fn
        self.name = name
        self.deployed: ty.Optional[dict] = None

    def __call__(self, *args: ty.Any, **kwargs: ty.Any) -> ty.Any:
        return self.fn(*args, **kwargs)

    def deploy(
        self,
        name: ty.Optional[str] = None,
        work_pool_name: ty.Optional[str] = None,
        cron: ty.Optional[str] = None,
    ) -> None:
        self.deployed = {"name": name, "work_pool_name": work_pool_name, "cron": cron}


class _SettingsContext:
    def __init__(self, updates: dict, log: list) -> None:
        self.updates = updates
        self.log = log

    def __enter__(self) -> "_SettingsContext":
        self.log.append(dict(self.updates))
        return self

    def __exit__(self, *exc: ty.Any) -> bool:
        return False


@pytest.fixture
def fake_prefect(monkeypatch: pytest.MonkeyPatch) -> ty.Any:
    created_flows: list = []
    module = types.ModuleType("prefect")

    def task(*_a: ty.Any, **_kw: ty.Any) -> ty.Callable:
        def decorator(fn: ty.Callable) -> _FakeTask:
            return _FakeTask(fn)

        return decorator

    def flow(*_a: ty.Any, **kw: ty.Any) -> ty.Callable:
        def decorator(fn: ty.Callable) -> _FakeFlow:
            f = _FakeFlow(fn, name=kw.get("name", fn.__name__))
            created_flows.append(f)
            return f

        return decorator

    module.task = task  # type: ignore[attr-defined]
    module.flow = flow  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "prefect", module)

    settings_module = types.ModuleType("prefect.settings")
    settings_calls: list = []
    settings_module.PREFECT_API_URL = "PREFECT_API_URL"  # type: ignore[attr-defined]
    settings_module.PREFECT_API_KEY = "PREFECT_API_KEY"  # type: ignore[attr-defined]
    settings_module.temporary_settings = lambda updates: _SettingsContext(  # type: ignore[attr-defined]
        updates, settings_calls
    )
    monkeypatch.setitem(sys.modules, "prefect.settings", settings_module)

    return types.SimpleNamespace(
        module=module, created_flows=created_flows, settings_calls=settings_calls
    )


def _write_spec(path: Path, content: dict) -> Path:
    path.write_text(yaml.safe_dump(content))
    return path


def _group_only_spec(name: str) -> dict:
    return {
        "name": name,
        "stages": [
            {"name": "grp", "command": "group", "args": {"input_paths": ["/data/in"]}}
        ],
    }


def test_check_returns_loaded_spec(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path / "spec.yaml", _group_only_spec("wf1"))
    spec = workflow_api.check(spec_path)
    assert spec.name == "wf1"


def test_check_raises_workflow_spec_error(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path / "spec.yaml", {"stages": []})
    with pytest.raises(WorkflowSpecError):
        workflow_api.check(spec_path)


def test_run_executes_stages(tmp_path: Path, fake_prefect: ty.Any) -> None:
    spec_path = _write_spec(tmp_path / "spec.yaml", _group_only_spec("wf1"))
    calls = []

    def fake_group(input_paths, output_dir, **kw):  # type: ignore[no-untyped-def]
        calls.append(input_paths)
        return []

    with patch.object(STAGES["group"], "api_fn", fake_group):
        errors = workflow_api.run(spec_path, work_dir=tmp_path / "work")

    assert errors == {}
    assert calls == [["/data/in"]]


def test_deploy_registers_each_spec_against_work_pool(
    tmp_path: Path, fake_prefect: ty.Any
) -> None:
    _write_spec(tmp_path / "a.yaml", _group_only_spec("wf-a"))
    _write_spec(tmp_path / "b.yaml", _group_only_spec("wf-b"))

    errors = workflow_api.deploy(
        tmp_path, work_pool="my-pool", work_dir=tmp_path / "work"
    )

    assert errors == []
    deployed_names = {f.name: f.deployed for f in fake_prefect.created_flows}
    assert deployed_names["wf-a"] == {
        "name": "wf-a",
        "work_pool_name": "my-pool",
        "cron": None,
    }
    assert deployed_names["wf-b"] == {
        "name": "wf-b",
        "work_pool_name": "my-pool",
        "cron": None,
    }


def test_deploy_passes_schedule_as_cron(tmp_path: Path, fake_prefect: ty.Any) -> None:
    spec = _group_only_spec("wf-scheduled")
    spec["schedule"] = "0 2 * * *"
    _write_spec(tmp_path / "a.yaml", spec)

    workflow_api.deploy(tmp_path, work_pool="my-pool")

    (flow,) = fake_prefect.created_flows
    assert flow.deployed["cron"] == "0 2 * * *"


def test_deploy_applies_param_overrides_uniformly(
    tmp_path: Path, fake_prefect: ty.Any
) -> None:
    for name in ("a", "b"):
        _write_spec(
            tmp_path / f"{name}.yaml",
            {
                "name": f"wf-{name}",
                "params": {"input_dir": {}},
                "stages": [
                    {
                        "name": "grp",
                        "command": "group",
                        "args": {"input_paths": ["${input_dir}"]},
                    }
                ],
            },
        )

    errors = workflow_api.deploy(
        tmp_path, work_pool="pool", param_overrides={"input_dir": "/shared/data"}
    )
    assert errors == []
    assert len(fake_prefect.created_flows) == 2


def test_deploy_no_matching_files_raises(tmp_path: Path, fake_prefect: ty.Any) -> None:
    with pytest.raises(ValueError, match="No spec files"):
        workflow_api.deploy(tmp_path, work_pool="pool")


def test_deploy_collects_errors_by_default(
    tmp_path: Path, fake_prefect: ty.Any
) -> None:
    _write_spec(tmp_path / "good.yaml", _group_only_spec("wf-good"))
    _write_spec(tmp_path / "bad.yaml", {"stages": []})  # invalid: no stages

    errors = workflow_api.deploy(tmp_path, work_pool="pool")

    assert len(errors) == 1
    assert "bad.yaml" in errors[0]
    # the good spec still deployed despite the bad one failing
    assert any(f.name == "wf-good" for f in fake_prefect.created_flows)


def test_deploy_raise_errors_stops_immediately(
    tmp_path: Path, fake_prefect: ty.Any
) -> None:
    _write_spec(tmp_path / "bad.yaml", {"stages": []})

    with pytest.raises(WorkflowSpecError):
        workflow_api.deploy(tmp_path, work_pool="pool", raise_errors=True)


def test_deploy_with_explicit_server_uses_temporary_settings(
    tmp_path: Path, fake_prefect: ty.Any
) -> None:
    _write_spec(tmp_path / "a.yaml", _group_only_spec("wf-a"))

    workflow_api.deploy(
        tmp_path,
        prefect_api_url="https://prefect.example.org/api",
        prefect_api_key="secret-key",
        work_pool="pool",
    )

    assert len(fake_prefect.settings_calls) == 1
    updates = fake_prefect.settings_calls[0]
    assert updates["PREFECT_API_URL"] == "https://prefect.example.org/api"
    assert updates["PREFECT_API_KEY"] == "secret-key"


def test_deploy_without_server_details_skips_settings_context(
    tmp_path: Path, fake_prefect: ty.Any
) -> None:
    _write_spec(tmp_path / "a.yaml", _group_only_spec("wf-a"))

    workflow_api.deploy(tmp_path, work_pool="pool")

    assert fake_prefect.settings_calls == []
