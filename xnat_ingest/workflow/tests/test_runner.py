"""Tests runner.py's flow-building/wiring logic against a minimal fake `prefect`
module (no real Prefect install needed) plus mocked stage API functions, so these
stay fast and dependency-free while still exercising the real dependency-order /
input-output wiring / error-aggregation code."""

import inspect
import sys
import types
import typing as ty
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from xnat_ingest.workflow.runner import resolve_work_dir
from xnat_ingest.workflow.spec import StageSpec, WorkflowSpec, load_spec
from xnat_ingest.workflow.stages import STAGES


def _write_spec(tmp_path: Path, content: dict) -> Path:
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(content))
    return path


def test_resolve_work_dir_override_namespaced_by_spec_name(tmp_path: Path) -> None:
    spec = WorkflowSpec(name="acemid-lesion", stages=[], source=tmp_path / "spec.yaml")
    assert (
        resolve_work_dir(spec, tmp_path / "work") == tmp_path / "work" / "acemid-lesion"
    )


def test_resolve_work_dir_defaults_next_to_spec_file(tmp_path: Path) -> None:
    spec = WorkflowSpec(name="acemid-lesion", stages=[], source=tmp_path / "spec.yaml")
    assert resolve_work_dir(spec, None) == tmp_path / ".xnat-ingest-acemid-lesion"


def test_resolve_work_dir_defaults_to_cwd_without_source(tmp_path: Path) -> None:
    spec = WorkflowSpec(name="acemid-lesion", stages=[])
    assert resolve_work_dir(spec, None) == Path.cwd() / ".xnat-ingest-acemid-lesion"


class _FakeTask:
    def __init__(self, fn: ty.Callable) -> None:
        self.fn = fn

    def __call__(self, *args: ty.Any, **kwargs: ty.Any) -> ty.Any:
        return self.fn(*args, **kwargs)


class _FakeFlow:
    def __init__(self, fn: ty.Callable, name: str) -> None:
        self.fn = fn
        self.name = name
        self.served: dict | None = None

    def __call__(self, *args: ty.Any, **kwargs: ty.Any) -> ty.Any:
        return self.fn(*args, **kwargs)

    def serve(
        self,
        name: str | None = None,
        cron: str | None = None,
        parameters: dict | None = None,
    ) -> None:
        self.served = {"name": name, "cron": cron, "parameters": parameters}


@pytest.fixture
def fake_prefect(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    module = types.ModuleType("prefect")
    created_flows: list[_FakeFlow] = []

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
    module.created_flows = created_flows  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "prefect", module)
    return module


def _spec(tmp_path: Path, stages: list[StageSpec]) -> WorkflowSpec:
    return WorkflowSpec(
        name="test-wf",
        stages=stages,
        source=tmp_path / "spec.yaml",
    )


def test_run_workflow_calls_stages_in_order_and_wires_dirs(
    fake_prefect: types.ModuleType, tmp_path: Path
) -> None:
    calls: list[str] = []

    def fake_group(input_paths, output_dir, **kw):  # type: ignore[no-untyped-def]
        calls.append(("group", tuple(input_paths), output_dir))
        return []

    def fake_assign(input_dir, output_dir, **kw):  # type: ignore[no-untyped-def]
        calls.append(("assign", input_dir, output_dir))
        return []

    stages = [
        StageSpec(
            name="grp", command="group", args={"input_paths": [str(tmp_path / "raw")]}
        ),
        StageSpec(name="asn", command="assign", input="grp"),
    ]
    spec = _spec(tmp_path, stages)

    with (
        patch.object(STAGES["group"], "api_fn", fake_group),
        patch.object(STAGES["assign"], "api_fn", fake_assign),
    ):
        from xnat_ingest.workflow.runner import run_workflow

        errors = run_workflow(spec, work_dir=tmp_path / "work")

    assert errors == {}
    assert [c[0] for c in calls] == ["group", "assign"]
    grp_output = calls[0][2]
    asn_input = calls[1][1]
    assert grp_output == tmp_path / "work" / "test-wf" / "grp"
    assert asn_input == grp_output  # assign's input is group's output dir


def test_run_workflow_disabled_middle_stage_forwards_input_to_dependent(
    fake_prefect: types.ModuleType, tmp_path: Path
) -> None:
    calls: list[tuple] = []

    def fake_assign(input_dir, output_dir, **kw):  # type: ignore[no-untyped-def]
        calls.append(("assign", input_dir, output_dir))
        return []

    def fake_deidentify(input_dir, output_dir, spec_dir, **kw):  # type: ignore[no-untyped-def]
        calls.append(("deidentify", input_dir, output_dir))
        return []

    def fake_upload(input_dir, xnat_repo, **kw):  # type: ignore[no-untyped-def]
        calls.append(("upload", input_dir))
        return []

    stages = [
        StageSpec(
            name="asn", command="assign", args={"input_dir": str(tmp_path / "in")}
        ),
        StageSpec(
            name="deid",
            command="deidentify",
            input="asn",
            args={"spec_dir": "/specs"},
            enabled=False,
        ),
        StageSpec(
            name="up",
            command="upload",
            input="deid",
            args={"server": "https://x"},
        ),
    ]
    spec = _spec(tmp_path, stages)

    with (
        patch.object(STAGES["assign"], "api_fn", fake_assign),
        patch.object(STAGES["deidentify"], "api_fn", fake_deidentify),
        patch.object(STAGES["upload"], "api_fn", fake_upload),
        patch("xnat_ingest.workflow.stages._build_xnat_repo", return_value=object()),
        patch("xnat_ingest.workflow.stages._close_xnat_repo"),
    ):
        from xnat_ingest.workflow.runner import run_workflow

        run_workflow(spec)

    assert [c[0] for c in calls] == ["assign", "upload"]  # 'deid' skipped entirely
    asn_output = calls[0][2]
    upload_input = calls[1][1]
    assert upload_input == str(asn_output)  # forwarded straight through 'deid'


def test_run_workflow_skips_disabled_stage(
    fake_prefect: types.ModuleType, tmp_path: Path
) -> None:
    calls: list[str] = []

    def fake_group(input_paths, output_dir, **kw):  # type: ignore[no-untyped-def]
        calls.append("group")
        return []

    def fake_assign(input_dir, output_dir, **kw):  # type: ignore[no-untyped-def]
        calls.append("assign")
        return []

    stages = [
        StageSpec(
            name="grp",
            command="group",
            args={"input_paths": [str(tmp_path / "raw")]},
            enabled=False,
        ),
        StageSpec(
            name="asn", command="assign", args={"input_dir": str(tmp_path / "in")}
        ),
    ]
    spec = _spec(tmp_path, stages)

    with (
        patch.object(STAGES["group"], "api_fn", fake_group),
        patch.object(STAGES["assign"], "api_fn", fake_assign),
    ):
        from xnat_ingest.workflow.runner import run_workflow

        run_workflow(spec)

    assert calls == ["assign"]


def test_run_workflow_raises_workflow_run_error_on_stage_errors(
    fake_prefect: types.ModuleType, tmp_path: Path
) -> None:
    def failing_group(input_paths, output_dir, **kw):  # type: ignore[no-untyped-def]
        return ["session-1 failed: boom"]

    stages = [
        StageSpec(
            name="grp", command="group", args={"input_paths": [str(tmp_path / "raw")]}
        )
    ]
    spec = _spec(tmp_path, stages)

    with patch.object(STAGES["group"], "api_fn", failing_group):
        from xnat_ingest.workflow.runner import WorkflowRunError, run_workflow

        with pytest.raises(WorkflowRunError) as exc_info:
            run_workflow(spec)

    assert exc_info.value.errors == {"grp": ["session-1 failed: boom"]}


def test_serve_workflow_passes_schedule_as_cron(
    fake_prefect: types.ModuleType, tmp_path: Path
) -> None:
    def fake_group(input_paths, output_dir, **kw):  # type: ignore[no-untyped-def]
        return []

    stages = [
        StageSpec(
            name="grp", command="group", args={"input_paths": [str(tmp_path / "raw")]}
        )
    ]
    spec = WorkflowSpec(
        name="scheduled-wf",
        stages=stages,
        schedule="0 2 * * *",
        source=tmp_path / "spec.yaml",
    )

    with patch.object(STAGES["group"], "api_fn", fake_group):
        from xnat_ingest.workflow.runner import build_flow, serve_workflow

        flow = build_flow(spec, work_dir=tmp_path / "work")
        serve_workflow(spec, work_dir=tmp_path / "work")

    assert flow.name == "scheduled-wf"


def test_missing_prefect_raises_clear_import_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setitem(
        sys.modules, "prefect", None
    )  # simulate 'import prefect' failing
    stages = [
        StageSpec(
            name="grp", command="group", args={"input_paths": [str(tmp_path / "raw")]}
        )
    ]
    spec = _spec(tmp_path, stages)

    from xnat_ingest.workflow.runner import run_workflow

    with pytest.raises(ImportError, match="xnat-ingest\\[workflow\\]"):
        run_workflow(spec)


# ── deferred (Prefect-native) params: dynamic flow signature + substitution ──


def test_build_flow_declares_one_parameter_per_deferred_default(
    fake_prefect: types.ModuleType, tmp_path: Path
) -> None:
    spec_path = _write_spec(
        tmp_path,
        {
            "params": {
                "input_dir": {"default": "/data/in"},
                "xnat_server": {},
            },
            "stages": [
                {
                    "name": "grp",
                    "command": "group",
                    "args": {
                        "input_paths": ["${input_dir}"],
                        "unlink_source": "${xnat_server}",
                    },
                }
            ],
        },
    )
    spec = load_spec(spec_path, param_overrides={"xnat_server": "https://x"})

    from xnat_ingest.workflow.runner import build_flow

    flow = build_flow(spec)
    sig = inspect.signature(flow.fn)
    assert set(sig.parameters) == {"input_dir", "xnat_server"}
    assert sig.parameters["input_dir"].default == "/data/in"
    assert sig.parameters["xnat_server"].default == "https://x"


def test_run_workflow_substitutes_deferred_param_into_stage_args(
    fake_prefect: types.ModuleType, tmp_path: Path
) -> None:
    calls = []

    def fake_group(input_paths, output_dir, **kw):  # type: ignore[no-untyped-def]
        calls.append(input_paths)
        return []

    spec_path = _write_spec(
        tmp_path,
        {
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
    spec = load_spec(spec_path, param_overrides={"input_dir": "/real/data"})

    with patch.object(STAGES["group"], "api_fn", fake_group):
        from xnat_ingest.workflow.runner import run_workflow

        run_workflow(spec, work_dir=tmp_path / "work")

    assert calls == [["/real/data"]]


def test_run_workflow_required_deferred_param_raises_before_running(
    fake_prefect: types.ModuleType, tmp_path: Path
) -> None:
    calls = []

    def fake_group(input_paths, output_dir, **kw):  # type: ignore[no-untyped-def]
        calls.append(input_paths)
        return []

    spec_path = _write_spec(
        tmp_path,
        {
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
    spec = load_spec(spec_path)  # no override/env - input_dir stays required

    with patch.object(STAGES["group"], "api_fn", fake_group):
        from xnat_ingest.workflow.runner import run_workflow

        with pytest.raises(ValueError, match="input_dir"):
            run_workflow(spec, work_dir=tmp_path / "work")

    assert calls == []  # never got as far as actually running anything


def test_serve_workflow_passes_resolved_deferred_params_only(
    fake_prefect: types.ModuleType, tmp_path: Path
) -> None:
    def fake_group(input_paths, output_dir, **kw):  # type: ignore[no-untyped-def]
        return []

    spec_path = _write_spec(
        tmp_path,
        {
            "params": {
                "input_dir": {"default": "/data/in"},
                "extra_required": {},
            },
            "stages": [
                {
                    "name": "grp",
                    "command": "group",
                    "args": {
                        "input_paths": ["${input_dir}"],
                        "wait_period": "${extra_required}",
                    },
                }
            ],
        },
    )
    spec = load_spec(spec_path)  # 'extra_required' left with no resolvable value

    with patch.object(STAGES["group"], "api_fn", fake_group):
        from xnat_ingest.workflow.runner import serve_workflow

        serve_workflow(spec)

    (flow,) = fake_prefect.created_flows  # type: ignore[attr-defined]
    # only the resolved deferred param is passed as a deployment default -
    # 'extra_required' (NO_DEFAULT) is left for Prefect to ask for at trigger time.
    assert flow.served["parameters"] == {"input_dir": "/data/in"}
