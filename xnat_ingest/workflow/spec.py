"""Loading and validating ``xnat-ingest workflow`` YAML specs.

Deliberately NOT a general orchestration language: no expressions, no shell steps,
no templating beyond ``${ENV_VAR}`` interpolation for secrets. A spec is a flat list
of xnat-ingest stages (one of ``group``/``assign``/``deidentify``/``associate``/
``upload``) chained by ``input:``/``after:`` references; everything else under a
stage's ``args:`` is exactly the keyword arguments of the matching
``xnat_ingest.api.*`` function, expressed as YAML instead of CLI tokens or
``;``-packed env vars - see ``workflow.coerce`` for how each YAML shape maps onto
the underlying (mostly ``attrs``) argument types.

``load_spec()`` fully validates a spec - including resolving 'extends', dependency
cycles, unknown stage references, and (by dry-running each stage's kwarg builder)
unknown/malformed ``args:`` - without touching Prefect or the filesystem beyond
reading the YAML file(s) themselves. That's what backs ``xnat-ingest workflow
check``.
"""

from __future__ import annotations

import inspect
import os
import re
import typing as ty
from pathlib import Path

import attrs
import yaml

from . import dag
from .errors import WorkflowSpecError
from .stages import STAGE_NAMES, STAGES, StageContext

__all__ = [
    "WorkflowSpecError",
    "XnatConnectionSpec",
    "StageSpec",
    "WorkflowSpec",
    "load_spec",
]

_ENV_VAR_RE = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)\}")

# Dummy paths used only to dry-run a stage's kwarg builder at validation time - never
# touched on disk, since build_kwargs functions are pure (no I/O of their own).
_DUMMY_INPUT = Path("__xnat_ingest_check__/input")
_DUMMY_OUTPUT = Path("__xnat_ingest_check__/output")


def _interpolate_env(value: ty.Any, path: str) -> ty.Any:
    if isinstance(value, str):

        def _sub(match: "re.Match[str]") -> str:
            name = match.group("name")
            try:
                return os.environ[name]
            except KeyError:
                raise WorkflowSpecError(
                    f"{path}: references undefined environment variable "
                    f"'${{{name}}}'"
                ) from None

        return _ENV_VAR_RE.sub(_sub, value)
    if isinstance(value, list):
        return [_interpolate_env(v, f"{path}[{i}]") for i, v in enumerate(value)]
    if isinstance(value, dict):
        return {k: _interpolate_env(v, f"{path}.{k}") for k, v in value.items()}
    return value


def _deep_merge(base: dict, override: dict) -> dict:
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_raw(path: Path, _seen: ty.Tuple[Path, ...] = ()) -> dict:
    path = path.resolve()
    if path in _seen:
        chain = " -> ".join(str(p) for p in (*_seen, path))
        raise WorkflowSpecError(f"circular 'extends' chain: {chain}")
    if not path.is_file():
        raise WorkflowSpecError(f"spec file not found: {path}")
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise WorkflowSpecError(
            f"{path}: the top level of a workflow spec must be a mapping"
        )
    extends = raw.pop("extends", None)
    if extends is None:
        return raw
    parents = [extends] if isinstance(extends, str) else extends
    merged: dict = {}
    for parent in parents:
        parent_path = (path.parent / parent).resolve()
        merged = _deep_merge(merged, _load_raw(parent_path, _seen + (path,)))
    return _deep_merge(merged, raw)


@attrs.define
class XnatConnectionSpec:
    server: str
    user: ty.Optional[str] = None
    password: ty.Optional[str] = None
    verify_ssl: bool = True


@attrs.define
class StageSpec:
    name: str
    command: str
    input: ty.Optional[str] = None
    after: ty.List[str] = attrs.field(factory=list)
    args: ty.Dict[str, ty.Any] = attrs.field(factory=dict)
    enabled: bool = True
    retries: int = 0
    retry_delay_seconds: float = 10.0


@attrs.define
class WorkflowSpec:
    name: str
    stages: ty.List[StageSpec]
    work_dir: ty.Optional[Path] = None
    xnat: ty.Optional[XnatConnectionSpec] = None
    schedule: ty.Optional[str] = None
    source: ty.Optional[Path] = None


def _require_mapping(value: ty.Any, path: str) -> dict:
    if not isinstance(value, dict):
        raise WorkflowSpecError(
            f"{path}: expected a mapping, got {type(value).__name__}"
        )
    return value


def _stage_spec(raw: ty.Any, index: int) -> StageSpec:
    path = f"stages[{index}]"
    raw = dict(_require_mapping(raw, path))
    try:
        name = raw.pop("name")
    except KeyError:
        raise WorkflowSpecError(f"{path}: missing required 'name'") from None
    try:
        command = raw.pop("command")
    except KeyError:
        raise WorkflowSpecError(
            f"{path} ('{name}'): missing required 'command'"
        ) from None
    if command not in STAGE_NAMES:
        raise WorkflowSpecError(
            f"{path} ('{name}'): unknown command '{command}', expected one of "
            f"{sorted(STAGE_NAMES)}"
        )
    input_ = raw.pop("input", None)
    after = raw.pop("after", [])
    if isinstance(after, str):
        after = [after]
    enabled = raw.pop("enabled", True)
    retries = raw.pop("retries", 0)
    retry_delay_seconds = raw.pop("retry_delay_seconds", 10.0)
    args = _require_mapping(raw.pop("args", {}) or {}, f"{path} ('{name}').args")
    if raw:
        raise WorkflowSpecError(f"{path} ('{name}'): unknown field(s) {sorted(raw)}")
    return StageSpec(
        name=name,
        command=command,
        input=input_,
        after=list(after),
        args=dict(args),
        enabled=bool(enabled),
        retries=int(retries),
        retry_delay_seconds=float(retry_delay_seconds),
    )


def _validate_stage_args(stage: StageSpec) -> None:
    """Dry-run the stage's kwarg builder against placeholder paths (no filesystem
    or network I/O) to catch bad composite args (an unrecognised mime-type, an
    invalid --on-resource-clash policy, ...) and unknown 'args:' keys at load time
    rather than only surfacing them when the workflow actually runs."""
    reg = STAGES[stage.command]
    dummy_ctx = StageContext(
        input_path=_DUMMY_INPUT, output_path=_DUMMY_OUTPUT, xnat=None
    )
    try:
        kwargs = reg.build_kwargs(dict(stage.args), dummy_ctx)
    except Exception as e:
        raise WorkflowSpecError(f"stages ('{stage.name}').args: {e}") from e
    sig_params = set(inspect.signature(reg.api_fn).parameters)
    unknown = set(kwargs) - sig_params
    if unknown:
        raise WorkflowSpecError(
            f"stages ('{stage.name}').args: unknown argument(s) for command "
            f"'{stage.command}': {sorted(unknown)}"
        )


def load_spec(path: ty.Union[str, Path]) -> WorkflowSpec:
    """Load and fully validate a workflow YAML spec: resolves any 'extends' chain,
    '${ENV_VAR}' interpolation, stage dependency cycles/unknown references, and
    (via a dry run of each stage's kwarg builder) unknown/malformed 'args:'.
    Raises WorkflowSpecError on anything malformed. Never imports Prefect."""
    path = Path(path)
    raw = _load_raw(path)
    raw = _interpolate_env(raw, "$")

    name = raw.pop("name", path.stem)
    work_dir = raw.pop("work_dir", None)
    schedule = raw.pop("schedule", None)

    xnat_raw = raw.pop("xnat", None)
    xnat = None
    if xnat_raw is not None:
        xnat_raw = _require_mapping(xnat_raw, "$.xnat")
        try:
            xnat = XnatConnectionSpec(**xnat_raw)
        except TypeError as e:
            raise WorkflowSpecError(f"$.xnat: {e}") from None

    stages_raw = raw.pop("stages", None)
    if not stages_raw:
        raise WorkflowSpecError("$: spec must have at least one entry under 'stages'")
    if not isinstance(stages_raw, list):
        raise WorkflowSpecError("$.stages: expected a list")

    if raw:
        raise WorkflowSpecError(f"$: unknown top-level field(s) {sorted(raw)}")

    stages = [_stage_spec(s, i) for i, s in enumerate(stages_raw)]

    names = [s.name for s in stages]
    dupes = {n for n in names if names.count(n) > 1}
    if dupes:
        raise WorkflowSpecError(f"$.stages: duplicate stage name(s): {sorted(dupes)}")

    for stage in stages:
        refs = [(stage.input, "input")] + [(a, "after") for a in stage.after]
        for ref, field in refs:
            if ref is not None and ref not in names:
                raise WorkflowSpecError(
                    f"stages ('{stage.name}').{field}: refers to unknown stage "
                    f"'{ref}'"
                )

    dag.validate_stage_inputs(stages)
    dag.resolve_order(stages)  # raises on a dependency cycle

    for stage in stages:
        _validate_stage_args(stage)
        if STAGES[stage.command].needs_xnat and xnat is None:
            raise WorkflowSpecError(
                f"stages ('{stage.name}'), command '{stage.command}': needs a "
                "top-level 'xnat:' block (server/user/password) in the workflow spec"
            )

    return WorkflowSpec(
        name=name,
        stages=stages,
        work_dir=Path(work_dir) if work_dir else None,
        xnat=xnat,
        schedule=schedule,
        source=path,
    )
