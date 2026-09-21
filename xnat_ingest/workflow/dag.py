"""Dependency resolution and structural validation for a WorkflowSpec's stages."""

from __future__ import annotations

import typing as ty

from .errors import WorkflowSpecError

if ty.TYPE_CHECKING:
    from .spec import StageSpec


def dependencies(stage: "StageSpec") -> set[str]:
    """The names of the stages a stage depends on: its 'input:' (a data dependency)
    plus any pure-ordering 'after:' entries."""
    deps = set(stage.after)
    if stage.input is not None:
        deps.add(stage.input)
    return deps


def resolve_order(stages: ty.List["StageSpec"]) -> ty.List["StageSpec"]:
    """Topologically sort stages by their 'input'/'after' dependencies (Kahn's
    algorithm), so each stage is ordered after everything it depends on. Raises
    WorkflowSpecError if the dependency graph has a cycle. Assumes every reference
    already names a real stage (validated separately at spec-load time)."""
    by_name = {s.name: s for s in stages}
    remaining = {s.name: dependencies(s) for s in stages}
    ordered: ty.List["StageSpec"] = []
    while remaining:
        ready = sorted(name for name, deps in remaining.items() if not deps)
        if not ready:
            cycle = ", ".join(sorted(remaining))
            raise WorkflowSpecError(
                f"$.stages: dependency cycle among stage(s): {cycle}"
            )
        for name in ready:
            ordered.append(by_name[name])
            del remaining[name]
        for deps in remaining.values():
            deps.difference_update(ready)
    return ordered


def validate_stage_inputs(stages: ty.List["StageSpec"]) -> None:
    """Every stage must either chain from another stage via 'input:', or supply its
    command's input argument (e.g. 'input_paths' for group, 'input_dir' for the
    rest) directly under 'args:'."""
    from .stages import STAGES

    for stage in stages:
        reg = STAGES[stage.command]
        if stage.input is None and reg.input_arg not in stage.args:
            raise WorkflowSpecError(
                f"stages ('{stage.name}'), command '{stage.command}': needs either "
                f"'input: <stage-name>' or an explicit '{reg.input_arg}' under 'args:'"
            )
