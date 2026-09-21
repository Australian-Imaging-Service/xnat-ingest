"""Integration checks against a *real* Prefect install (skipped where the optional
``xnat-ingest[workflow]`` extra isn't present). The rest of the runner tests use a
fake ``prefect`` module - these lock in the two things that fake can't vouch for:
that Prefect's own parameter-schema introspection honours the dynamically-built
``__signature__``, and that ``serve``/``deploy`` accept ``parameters=``.
"""

import inspect
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

prefect = pytest.importorskip("prefect")

from xnat_ingest.workflow.runner import build_flow, run_workflow  # noqa: E402
from xnat_ingest.workflow.spec import load_spec  # noqa: E402
from xnat_ingest.workflow.stages import STAGES  # noqa: E402


def _spec(tmp_path: Path, content: dict) -> Path:
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(content))
    return path


def _deferred_params_spec(tmp_path: Path) -> Path:
    return _spec(
        tmp_path,
        {
            "name": "prefect-probe",
            "params": {
                "input_dir": {"default": "/default/data"},
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


def test_real_prefect_flow_exposes_deferred_params_in_its_schema(
    tmp_path: Path,
) -> None:
    spec = load_spec(
        _deferred_params_spec(tmp_path),
        param_overrides={"xnat_server": "srv-value"},
    )
    flow = build_flow(spec, work_dir=tmp_path / "work")

    schema = flow.parameters  # Prefect's own ParameterSchema, built via inspect
    props = schema.properties
    assert set(props) == {"input_dir", "xnat_server"}
    assert props["input_dir"]["default"] == "/default/data"
    assert props["xnat_server"]["default"] == "srv-value"
    assert schema.required == []  # both resolved -> neither required


def test_real_prefect_flow_marks_unresolved_deferred_param_required(
    tmp_path: Path,
) -> None:
    spec = load_spec(_deferred_params_spec(tmp_path))  # xnat_server left unresolved
    flow = build_flow(spec, work_dir=tmp_path / "work")
    assert flow.parameters.required == ["xnat_server"]


def test_real_prefect_serve_and_deploy_accept_parameters_kwarg() -> None:
    assert "parameters" in inspect.signature(prefect.Flow.serve).parameters
    assert "parameters" in inspect.signature(prefect.Flow.deploy).parameters


def test_real_prefect_run_required_deferred_param_raises_before_server_starts(
    tmp_path: Path,
) -> None:
    spec = load_spec(_deferred_params_spec(tmp_path))  # xnat_server unresolved

    def fake_group(*a, **kw):  # type: ignore[no-untyped-def]
        raise AssertionError("stage should never run")

    with patch.object(STAGES["group"], "api_fn", fake_group):
        with pytest.raises(ValueError, match="xnat_server"):
            run_workflow(spec, work_dir=tmp_path / "work")
