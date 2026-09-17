"""Loading and validating ``xnat-ingest workflow`` YAML specs.

Deliberately NOT a general orchestration language: no expressions, no shell steps,
no templating beyond ``${NAME}`` placeholders resolved against declared ``params:``,
``--param``/``-p`` CLI overrides, and the environment - see :func:`_resolve_param`
for the exact precedence. A spec is a flat list of xnat-ingest stages (one of
``group``/``assign``/``deidentify``/``associate``/``upload``) chained by
``input:``/``after:`` references; everything else under a stage's ``args:`` is
exactly the keyword arguments of the matching ``xnat_ingest.api.*`` function,
expressed as YAML instead of CLI tokens or ``;``-packed env vars - see
``workflow.coerce`` for how each YAML shape maps onto the underlying (mostly
``attrs``) argument types.

There is deliberately no spec-level concept of "the" backend to upload to (no
top-level ``xnat:`` block auto-wired into every stage that needs one): credentials
are plain ``args:`` on the ``upload`` stage(s) that need them, like any other
argument, resolved through the same ``params:`` mechanism as everything else. That
keeps the schema backend-agnostic - a future non-XNAT upload stage just declares
its own ``args:`` shape, with no spec format change needed.

A ``${NAME}`` reference used as the *whole value* of a plain (non-coerced -
``Stage.coerced_args``) ``args:`` entry, for a **non-secret** declared param, is
left unresolved here and instead becomes a real Prefect flow parameter (see
``workflow.runner``) - editable/re-triggerable via Prefect's own UI/API/orchestration
DB. Everything else (a ``secret: true`` param, a placeholder inside a coerced/
structured arg, one embedded in a larger string, or an undeclared name) is resolved
eagerly, here, and baked into the spec - most importantly, a secret is never passed
through Prefect's parameter store. See :func:`_resolve_stage_args`.

``load_spec()`` fully validates a spec - including resolving 'extends', param/env
placeholders, stage dependency cycles, unknown stage references, and (by
dry-running each stage's kwarg builder) unknown/malformed ``args:`` - without
touching Prefect or the filesystem beyond reading the YAML file(s) themselves.
That's what backs ``xnat-ingest workflow check``.
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
    "ParamSpec",
    "StageSpec",
    "WorkflowSpec",
    "load_spec",
    "NO_DEFAULT",
    "substitute_deferred_params",
]

_PLACEHOLDER_RE = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)\}")
# A string that is *only* one placeholder, with nothing else around it - only this
# shape can be deferred to a Prefect parameter (see module docstring); a value that
# merely contains a placeholder alongside other text is always resolved eagerly.
_WHOLE_PLACEHOLDER_RE = re.compile(r"^\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)\}$")

# Sentinel distinguishing "no default given" (-> the param is required) from a real
# falsy default such as `None`/`""`.
NO_DEFAULT: ty.Any = object()

# Dummy paths used only to dry-run a stage's kwarg builder at validation time - never
# touched on disk, since build_kwargs functions are pure (no I/O of their own).
_DUMMY_INPUT = Path("__xnat_ingest_check__/input")
_DUMMY_OUTPUT = Path("__xnat_ingest_check__/output")


@attrs.define
class ParamSpec:
    """A workflow parameter declared under the top-level ``params:`` block - purely
    documentation plus an optional default; it doesn't change how ``${NAME}`` is
    written anywhere else in the spec, just what a reference to ``NAME`` resolves to
    and how clear the error is when it can't."""

    name: str
    description: ty.Optional[str] = None
    default: ty.Any = NO_DEFAULT
    secret: bool = False

    @property
    def required(self) -> bool:
        return self.default is NO_DEFAULT


def _parse_params(raw: ty.Any) -> ty.Dict[str, ParamSpec]:
    if not raw:
        return {}
    raw = _require_mapping(raw, "$.params")
    params = {}
    for name, entry in raw.items():
        entry = _require_mapping(entry or {}, f"$.params.{name}")
        params[name] = ParamSpec(
            name=name,
            description=entry.get("description"),
            default=entry.get("default", NO_DEFAULT),
            secret=bool(entry.get("secret", False)),
        )
    return params


def _resolve_param(
    name: str,
    params: ty.Dict[str, ParamSpec],
    overrides: ty.Dict[str, str],
    path: str,
) -> str:
    """Resolve one ``${NAME}`` reference: an explicit ``--param``/``-p`` override
    wins outright, then a same-named environment variable (so a plain env var - a
    k8s Secret mounted via ``env:``/``envFrom:`` - Just Works with no CLI flag
    needed), then a declared param's ``default:``, else a clear error naming what's
    missing. A name with no ``params:`` entry at all still resolves via
    ``--param``/the environment - declaring it just adds a default and a better
    error message."""
    if name in overrides:
        return overrides[name]
    if name in os.environ:
        return os.environ[name]
    spec = params.get(name)
    if spec is not None and not spec.required:
        return spec.default
    if spec is not None:
        hint = f" ({spec.description})" if spec.description else ""
        raise WorkflowSpecError(
            f"{path}: required parameter '{name}' was not supplied{hint} - pass "
            f"--param {name}=<value>, set the {name} environment variable, or add "
            f"a 'default:' to its 'params: {name}:' entry"
        )
    raise WorkflowSpecError(
        f"{path}: references undefined parameter/environment variable "
        f"'${{{name}}}' (declare it under 'params:' for a default or a clearer "
        "error)"
    )


def _resolve_placeholders(
    value: ty.Any,
    path: str,
    params: ty.Dict[str, ParamSpec],
    overrides: ty.Dict[str, str],
) -> ty.Any:
    if isinstance(value, str):

        def _sub(match: "re.Match[str]") -> str:
            return _resolve_param(match.group("name"), params, overrides, path)

        return _PLACEHOLDER_RE.sub(_sub, value)
    if isinstance(value, list):
        return [
            _resolve_placeholders(v, f"{path}[{i}]", params, overrides)
            for i, v in enumerate(value)
        ]
    if isinstance(value, dict):
        return {
            k: _resolve_placeholders(v, f"{path}.{k}", params, overrides)
            for k, v in value.items()
        }
    return value


def substitute_deferred_params(
    value: ty.Any, resolved: ty.Mapping[str, ty.Any]
) -> ty.Any:
    """Replace a whole-value ``${NAME}`` placeholder (recursively) with
    ``resolved[NAME]`` wherever ``NAME`` is a key of ``resolved``, leaving anything
    else untouched. Used by ``workflow.runner`` to substitute a deferred (Prefect
    flow parameter) placeholder with its real value at actual run time - the
    counterpart to :func:`_resolve_stage_args` deferring it here in the first
    place."""
    if isinstance(value, str):
        m = _WHOLE_PLACEHOLDER_RE.match(value)
        if m and m.group("name") in resolved:
            return resolved[m.group("name")]
        return value
    if isinstance(value, list):
        return [substitute_deferred_params(v, resolved) for v in value]
    if isinstance(value, dict):
        return {k: substitute_deferred_params(v, resolved) for k, v in value.items()}
    return value


def _resolve_stage_args(
    args: ty.Dict[str, ty.Any],
    coerced_keys: ty.FrozenSet[str],
    params: ty.Dict[str, ParamSpec],
    overrides: ty.Dict[str, str],
    path: str,
) -> ty.Tuple[ty.Dict[str, ty.Any], ty.Set[str]]:
    """Resolve one stage's ``args:``, leaving a whole-value ``${NAME}`` placeholder
    unresolved (deferred to a Prefect flow parameter - see module docstring) when
    ``NAME`` names a non-secret declared param and the placeholder sits directly
    under a *plain* (non-``coerced_keys``) argument - at any nesting depth, so e.g.
    ``input_paths: ["${input_dir}"]`` still defers ``input_dir``. Returns the
    resolved ``args`` dict plus the set of deferred param names found."""
    deferred: ty.Set[str] = set()

    def _resolve(value: ty.Any, coerced: bool, subpath: str) -> ty.Any:
        if isinstance(value, str):
            match = _WHOLE_PLACEHOLDER_RE.match(value)
            if match and not coerced:
                name = match.group("name")
                pspec = params.get(name)
                if pspec is not None and not pspec.secret:
                    deferred.add(name)
                    return value
            return _resolve_placeholders(value, subpath, params, overrides)
        if isinstance(value, list):
            return [
                _resolve(v, coerced, f"{subpath}[{i}]") for i, v in enumerate(value)
            ]
        if isinstance(value, dict):
            return {k: _resolve(v, coerced, f"{subpath}.{k}") for k, v in value.items()}
        return value

    resolved = {
        key: _resolve(value, key in coerced_keys, f"{path}.{key}")
        for key, value in args.items()
    }
    return resolved, deferred


def _resolve_deferred_default(
    name: str, params: ty.Dict[str, ParamSpec], overrides: ty.Dict[str, str]
) -> ty.Any:
    """The value a deferred param's Prefect flow parameter should default to: the
    same ``--param`` > environment > declared-``default:`` precedence as an eager
    ``${NAME}``, except a required param with none of those available returns
    ``NO_DEFAULT`` instead of raising - Prefect enforces "required" itself when the
    flow is actually triggered, which for ``serve``/``deploy`` may be well after
    load time (``run``, which executes immediately, still requires a value - see
    ``workflow.runner.run_workflow``)."""
    if name in overrides:
        return overrides[name]
    if name in os.environ:
        return os.environ[name]
    pspec = params[name]
    return pspec.default if not pspec.required else NO_DEFAULT


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
    schedule: ty.Optional[str] = None
    source: ty.Optional[Path] = None
    params: ty.Dict[str, ParamSpec] = attrs.field(factory=dict)
    # Non-secret params deferred to a real Prefect flow parameter (see module
    # docstring), mapped to their resolved default - or NO_DEFAULT if none of
    # --param/the environment/a declared default supplied one (a required Prefect
    # parameter, left for 'serve'/'deploy' to ask for at trigger time).
    deferred_defaults: ty.Dict[str, ty.Any] = attrs.field(factory=dict)
    # Deliberately no 'work_dir' here: it's per-host scratch space, not part of
    # what the pipeline does, so it's a --work-dir CLI/API argument (see
    # workflow.runner.resolve_work_dir) rather than something declared in the
    # spec's own YAML/params.


def _require_mapping(value: ty.Any, path: str) -> dict:
    if not isinstance(value, dict):
        raise WorkflowSpecError(
            f"{path}: expected a mapping, got {type(value).__name__}"
        )
    return value


def _stage_spec(
    raw: ty.Any,
    index: int,
    params: ty.Dict[str, ParamSpec],
    overrides: ty.Dict[str, str],
) -> ty.Tuple[StageSpec, ty.Set[str]]:
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
    resolved_args, deferred = _resolve_stage_args(
        dict(args),
        STAGES[command].coerced_args,
        params,
        overrides,
        f"{path} ('{name}').args",
    )
    stage = StageSpec(
        name=name,
        command=command,
        input=input_,
        after=list(after),
        args=resolved_args,
        enabled=bool(enabled),
        retries=int(retries),
        retry_delay_seconds=float(retry_delay_seconds),
    )
    return stage, deferred


def _strip_reserved(kwargs: dict) -> dict:
    """Drop the internal, leading-underscore keys a stage's build_kwargs() may
    return (e.g. 'upload's '_xnat_connection') before checking the rest against
    its API function's real parameters - these are consumed by run_stage(), not
    passed to the API function itself."""
    return {k: v for k, v in kwargs.items() if not k.startswith("_")}


def _validate_stage_args(stage: StageSpec, deferred_names: ty.Set[str]) -> None:
    """Dry-run the stage's kwarg builder against placeholder paths (no filesystem
    or network I/O) to catch bad composite args (an unrecognised mime-type, an
    invalid --on-resource-clash policy, a missing upload 'server', ...) and unknown
    'args:' keys at load time rather than only surfacing them when the workflow
    actually runs. A deferred (Prefect-parameterised) placeholder is stood in for
    with a dummy value - its real value isn't known until the flow actually runs,
    but that shouldn't stop everything else about the stage from being checked now.
    """
    reg = STAGES[stage.command]
    dummy_ctx = StageContext(input_path=_DUMMY_INPUT, output_path=_DUMMY_OUTPUT)
    dry_run_args = substitute_deferred_params(
        stage.args, {name: f"__deferred_param_{name}__" for name in deferred_names}
    )
    try:
        kwargs = reg.build_kwargs(dict(dry_run_args), dummy_ctx)
    except Exception as e:
        raise WorkflowSpecError(f"stages ('{stage.name}').args: {e}") from e
    if reg.needs_xnat and "_xnat_connection" not in kwargs:
        raise WorkflowSpecError(
            f"stages ('{stage.name}'), command '{stage.command}': needs 'server' "
            "(and usually 'user'/'password') under its own 'args:'"
        )
    checked = _strip_reserved(kwargs)
    sig = inspect.signature(reg.api_fn)
    unknown = set(checked) - set(sig.parameters)
    if unknown:
        raise WorkflowSpecError(
            f"stages ('{stage.name}').args: unknown argument(s) for command "
            f"'{stage.command}': {sorted(unknown)}"
        )
    try:
        # injected_args (e.g. upload's 'xnat_repo') are supplied by run_stage()
        # itself, not build_kwargs() - stand a dummy in for them so a required
        # argument the workflow machinery provides isn't flagged as missing.
        sig.bind(**checked, **dict.fromkeys(reg.injected_args))
    except TypeError as e:
        raise WorkflowSpecError(
            f"stages ('{stage.name}').args: {e} for command '{stage.command}'"
        ) from e


def load_spec(
    path: ty.Union[str, Path],
    param_overrides: ty.Optional[ty.Dict[str, str]] = None,
) -> WorkflowSpec:
    """Load and fully validate a workflow YAML spec: resolves any 'extends' chain,
    'params:'/'--param'/environment placeholders, stage dependency cycles/unknown
    references, and (via a dry run of each stage's kwarg builder) unknown/malformed
    'args:'. Raises WorkflowSpecError on anything malformed. Never imports Prefect.

    Parameters
    ----------
    param_overrides
        Values for ``${NAME}`` placeholders supplied via ``--param``/``-p`` on the
        CLI, taking priority over any same-named environment variable or declared
        default - see :func:`_resolve_param`.
    """
    path = Path(path)
    raw = _load_raw(path)

    params = _parse_params(raw.pop("params", None))
    overrides = dict(param_overrides or {})

    # name/schedule are plain top-level scalars, always resolved eagerly - a
    # workflow's own name or cron schedule isn't a per-run Prefect parameter.
    name = _resolve_placeholders(
        raw.pop("name", path.stem), "$.name", params, overrides
    )
    schedule = raw.pop("schedule", None)
    if schedule is not None:
        schedule = _resolve_placeholders(schedule, "$.schedule", params, overrides)

    stages_raw = raw.pop("stages", None)
    if not stages_raw:
        raise WorkflowSpecError("$: spec must have at least one entry under 'stages'")
    if not isinstance(stages_raw, list):
        raise WorkflowSpecError("$.stages: expected a list")

    if raw:
        raise WorkflowSpecError(f"$: unknown top-level field(s) {sorted(raw)}")

    stages = []
    deferred_names: ty.Set[str] = set()
    for i, s in enumerate(stages_raw):
        stage, stage_deferred = _stage_spec(s, i, params, overrides)
        stages.append(stage)
        deferred_names.update(stage_deferred)

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
        _validate_stage_args(stage, deferred_names)

    deferred_defaults = {
        n: _resolve_deferred_default(n, params, overrides) for n in deferred_names
    }

    return WorkflowSpec(
        name=name,
        stages=stages,
        schedule=schedule,
        source=path,
        params=params,
        deferred_defaults=deferred_defaults,
    )
