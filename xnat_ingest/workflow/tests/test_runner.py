"""Tests runner.py's flow-building/wiring logic against a minimal fake `prefect`
module (no real Prefect install needed) plus mocked stage API functions, so these
stay fast and dependency-free while still exercising the real dependency-order /
input-output wiring / error-aggregation code."""

import sys
import types
import typing as ty
from pathlib import Path
from unittest.mock import patch

import pytest

from xnat_ingest.workflow.spec import StageSpec, WorkflowSpec
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
        self.served: dict | None = None

    def __call__(self, *args: ty.Any, **kwargs: ty.Any) -> ty.Any:
        return self.fn(*args, **kwargs)

    def serve(self, name: str | None = None, cron: str | None = None) -> None:
        self.served = {"name": name, "cron": cron}


@pytest.fixture
def fake_prefect(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    module = types.ModuleType("prefect")

    def task(*_a: ty.Any, **_kw: ty.Any) -> ty.Callable:
        def decorator(fn: ty.Callable) -> _FakeTask:
            return _FakeTask(fn)

        return decorator

    def flow(*_a: ty.Any, **kw: ty.Any) -> ty.Callable:
        def decorator(fn: ty.Callable) -> _FakeFlow:
            return _FakeFlow(fn, name=kw.get("name", fn.__name__))

        return decorator

    module.task = task  # type: ignore[attr-defined]
    module.flow = flow  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "prefect", module)
    return module


def _spec(tmp_path: Path, stages: list[StageSpec], xnat: ty.Any = None) -> WorkflowSpec:
    return WorkflowSpec(
        name="test-wf",
        stages=stages,
        work_dir=tmp_path / "work",
        source=tmp_path / "spec.yaml",
        xnat=xnat,
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

        errors = run_workflow(spec)

    assert errors == {}
    assert [c[0] for c in calls] == ["group", "assign"]
    grp_output = calls[0][2]
    asn_input = calls[1][1]
    assert grp_output == tmp_path / "work" / "grp"
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

    from xnat_ingest.workflow.spec import XnatConnectionSpec

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
        StageSpec(name="up", command="upload", input="deid"),
    ]
    spec = _spec(tmp_path, stages, xnat=XnatConnectionSpec(server="https://x"))

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
        work_dir=tmp_path / "work",
        schedule="0 2 * * *",
        source=tmp_path / "spec.yaml",
    )

    with patch.object(STAGES["group"], "api_fn", fake_group):
        from xnat_ingest.workflow.runner import build_flow, serve_workflow

        flow = build_flow(spec)
        serve_workflow(spec)

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
