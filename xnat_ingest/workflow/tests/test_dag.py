import pytest

from xnat_ingest.workflow.dag import resolve_order, validate_stage_inputs
from xnat_ingest.workflow.errors import WorkflowSpecError
from xnat_ingest.workflow.spec import StageSpec


def _stage(name: str, command: str = "assign", **kwargs) -> StageSpec:  # type: ignore[no-untyped-def]
    return StageSpec(name=name, command=command, **kwargs)


def test_resolve_order_linear_chain() -> None:
    a = _stage("a", command="group", args={"input_paths": ["/x"]})
    b = _stage("b", input="a")
    c = _stage("c", input="b")
    ordered = resolve_order([c, b, a])  # deliberately out of order
    assert [s.name for s in ordered] == ["a", "b", "c"]


def test_resolve_order_independent_branches_both_included() -> None:
    a = _stage("a", command="group", args={"input_paths": ["/x"]})
    b = _stage("b", command="group", args={"input_paths": ["/y"]})
    c = _stage("c", input="a", after=["b"])
    ordered = resolve_order([c, a, b])
    names = [s.name for s in ordered]
    assert names.index("a") < names.index("c")
    assert names.index("b") < names.index("c")


def test_resolve_order_after_only_no_input() -> None:
    a = _stage("a", command="group", args={"input_paths": ["/x"]})
    b = _stage("b", command="group", args={"input_paths": ["/y"]}, after=["a"])
    ordered = resolve_order([b, a])
    assert [s.name for s in ordered] == ["a", "b"]


def test_resolve_order_cycle_raises() -> None:
    a = _stage("a", input="b")
    b = _stage("b", input="a")
    with pytest.raises(WorkflowSpecError, match="dependency cycle"):
        resolve_order([a, b])


def test_validate_stage_inputs_ok_with_input_ref() -> None:
    validate_stage_inputs([_stage("a", input="upstream")])


def test_validate_stage_inputs_ok_with_explicit_arg() -> None:
    validate_stage_inputs([_stage("a", command="assign", args={"input_dir": "/x"})])


def test_validate_stage_inputs_group_needs_input_paths() -> None:
    validate_stage_inputs([_stage("a", command="group", args={"input_paths": ["/x"]})])


def test_validate_stage_inputs_missing_raises() -> None:
    with pytest.raises(WorkflowSpecError, match="input_dir"):
        validate_stage_inputs([_stage("a", command="assign", args={})])


def test_validate_stage_inputs_group_missing_raises() -> None:
    with pytest.raises(WorkflowSpecError, match="input_paths"):
        validate_stage_inputs([_stage("a", command="group", args={})])
