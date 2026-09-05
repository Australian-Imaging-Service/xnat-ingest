"""Builds and runs a Prefect flow from a WorkflowSpec.

Prefect is imported lazily, only by the functions in this module - ``workflow.spec``
(and so ``xnat-ingest workflow check``) never touches it. Stages run synchronously in
dependency order (from ``workflow.dag.resolve_order``): each is wrapped in a Prefect
``@task`` (so it gets its own retries/observability), but there is no fan-out - a
workflow spec is a DAG in name, not yet in execution.
"""

from __future__ import annotations

import typing as ty
from pathlib import Path

from ..helpers.logging import logger
from .dag import resolve_order
from .spec import StageSpec, WorkflowSpec
from .stages import STAGES, StageContext, run_stage


class WorkflowRunError(RuntimeError):
    """Raised by :func:`run_workflow` when one or more stages reported errors.
    ``errors`` maps stage name to the list of per-session error strings that
    stage's API function returned."""

    def __init__(self, errors: ty.Dict[str, ty.List[str]]):
        self.errors = errors
        summary = "; ".join(
            f"'{name}': {len(errs)} error(s)" for name, errs in errors.items()
        )
        super().__init__(f"workflow run reported errors - {summary}")


def _require_prefect() -> ty.Any:
    try:
        import prefect
    except ImportError as e:
        raise ImportError(
            "the 'workflow' command needs Prefect - install it with "
            "'pip install xnat-ingest[workflow]'"
        ) from e
    return prefect


def _default_work_dir(spec: WorkflowSpec) -> Path:
    if spec.work_dir is not None:
        return spec.work_dir
    base = spec.source.parent if spec.source is not None else Path.cwd()
    return base / f".xnat-ingest-{spec.name}"


def _stage_output_dir(stage: StageSpec, work_dir: Path) -> ty.Optional[Path]:
    if not STAGES[stage.command].takes_output_dir:
        return None
    override = stage.args.get("output_dir")
    return Path(override) if override is not None else work_dir / stage.name


def _stage_input_path(
    stage: StageSpec, outputs: ty.Dict[str, ty.Optional[Path]]
) -> ty.Optional[Path]:
    if stage.input is None:
        # This stage's own args must supply whatever input its command needs
        # (input_paths/input_dir) - validated at spec-load time.
        return None
    resolved = outputs.get(stage.input)
    if resolved is None:
        raise ValueError(
            f"stage '{stage.name}' has input: '{stage.input}', but that stage was "
            "skipped or produces no output directory to draw from"
        )
    return resolved


def build_flow(spec: WorkflowSpec) -> ty.Any:
    """Build (but don't run) the Prefect flow for a workflow spec."""
    prefect = _require_prefect()
    ordered = resolve_order(spec.stages)
    work_dir = _default_work_dir(spec)

    def _make_task(stage: StageSpec) -> ty.Any:
        @prefect.task(
            name=stage.name,
            retries=stage.retries,
            retry_delay_seconds=stage.retry_delay_seconds,
        )
        def _task(
            input_path: ty.Optional[Path],
        ) -> ty.Tuple[ty.Optional[Path], ty.List[str]]:
            output_path = _stage_output_dir(stage, work_dir)
            if output_path is not None:
                output_path.mkdir(parents=True, exist_ok=True)
            ctx = StageContext(
                input_path=input_path, output_path=output_path, xnat=spec.xnat
            )
            logger.info("Running workflow stage '%s' (%s)", stage.name, stage.command)
            errors = run_stage(stage, ctx)
            if errors:
                logger.error(
                    "Stage '%s' completed with %d error(s)", stage.name, len(errors)
                )
            else:
                logger.info("Stage '%s' completed successfully", stage.name)
            return output_path, errors

        return _task

    tasks = {stage.name: _make_task(stage) for stage in ordered}

    @prefect.flow(name=spec.name)
    def _flow() -> ty.Dict[str, ty.List[str]]:
        outputs: ty.Dict[str, ty.Optional[Path]] = {}
        all_errors: ty.Dict[str, ty.List[str]] = {}
        for stage in ordered:
            input_path = _stage_input_path(stage, outputs)
            if not stage.enabled:
                # Forward the input unchanged so a dependent stage's 'input:' still
                # resolves to something - e.g. disabling 'deidentify' between
                # 'assign' and 'upload' means 'upload' gets 'assign's output
                # directly, with no code change needed at either of its neighbours.
                logger.info(
                    "Skipping disabled stage '%s' - forwarding its input unchanged",
                    stage.name,
                )
                outputs[stage.name] = input_path
                continue
            output_path, errors = tasks[stage.name](input_path)
            outputs[stage.name] = output_path
            if errors:
                all_errors[stage.name] = errors
        return all_errors

    return _flow


def run_workflow(spec: WorkflowSpec) -> ty.Dict[str, ty.List[str]]:
    """Run a workflow spec once, synchronously. Raises WorkflowRunError if any
    stage reported errors."""
    flow = build_flow(spec)
    errors = flow()
    if errors:
        raise WorkflowRunError(errors)
    return errors


def serve_workflow(spec: WorkflowSpec) -> None:
    """Serve a workflow spec as a long-running Prefect deployment, scheduled by
    ``spec.schedule`` (a standard cron expression) if given, otherwise only
    available to trigger on demand. Blocks until interrupted."""
    flow = build_flow(spec)
    flow.serve(name=spec.name, cron=spec.schedule)
