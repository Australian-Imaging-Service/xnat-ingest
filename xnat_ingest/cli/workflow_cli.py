import sys
import typing as ty
from pathlib import Path

import click

from xnat_ingest.cli.base import cli

from ..helpers.arg_types import LoggerConfig
from ..helpers.logging import logger, set_logger_handling
from ..workflow.dag import dependencies, resolve_order
from ..workflow.errors import WorkflowSpecError
from ..workflow.spec import load_spec


@cli.group(
    name="workflow",
    help=(
        "Run a multi-stage xnat-ingest pipeline (group/assign/deidentify/upload) "
        "from a single YAML spec, via Prefect. See docs/source/how_to/workflow.rst "
        "and example-specs/ for the spec format."
    ),
)
def workflow() -> None:
    pass


@workflow.command(
    name="check",
    help=(
        "Validate a workflow YAML spec and print its resolved stage order, without "
        "running it or requiring Prefect to be installed."
    ),
)
@click.argument("spec_path", type=click.Path(exists=True, path_type=Path))
def check_cmd(spec_path: Path) -> None:
    try:
        spec = load_spec(spec_path)
    except WorkflowSpecError as e:
        click.echo(f"INVALID: {e}", err=True)
        sys.exit(1)
    click.echo(f"OK: '{spec.name}' ({len(spec.stages)} stage(s))")
    for stage in resolve_order(spec.stages):
        deps = ", ".join(sorted(dependencies(stage)))
        arrow = f"  <- {deps}" if deps else ""
        marker = "" if stage.enabled else "  [disabled]"
        click.echo(f"  - {stage.name} ({stage.command}){arrow}{marker}")


@workflow.command(name="run", help="Run a workflow spec once, synchronously.")
@click.argument("spec_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--logger",
    "loggers",
    multiple=True,
    type=LoggerConfig.cli_type,
    envvar="XINGEST_LOGGERS",
    nargs=3,
    default=(),
    metavar="<logtype> <loglevel> <location>",
    help=(
        "Setup handles to capture logs that are generated (XINGEST_LOGGERS env. var)"
    ),
)
def run_cmd(spec_path: Path, loggers: ty.List[LoggerConfig]) -> None:
    set_logger_handling(logger_configs=loggers)
    try:
        spec = load_spec(spec_path)
    except WorkflowSpecError as e:
        click.echo(f"INVALID: {e}", err=True)
        sys.exit(1)
    from ..workflow.runner import WorkflowRunError, run_workflow

    try:
        run_workflow(spec)
    except WorkflowRunError as e:
        logger.error(str(e))
        sys.exit(1)
    logger.info("Workflow '%s' completed successfully", spec.name)


@workflow.command(
    name="serve",
    help=(
        "Serve a workflow spec as a long-running Prefect deployment, scheduled by "
        "its 'schedule:' cron expression if given. Blocks until interrupted."
    ),
)
@click.argument("spec_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--logger",
    "loggers",
    multiple=True,
    type=LoggerConfig.cli_type,
    envvar="XINGEST_LOGGERS",
    nargs=3,
    default=(),
    metavar="<logtype> <loglevel> <location>",
    help=(
        "Setup handles to capture logs that are generated (XINGEST_LOGGERS env. var)"
    ),
)
def serve_cmd(spec_path: Path, loggers: ty.List[LoggerConfig]) -> None:
    set_logger_handling(logger_configs=loggers)
    try:
        spec = load_spec(spec_path)
    except WorkflowSpecError as e:
        click.echo(f"INVALID: {e}", err=True)
        sys.exit(1)
    from ..workflow.runner import serve_workflow

    serve_workflow(spec)
