import sys
import typing as ty
from pathlib import Path

import click

from xnat_ingest.cli.base import cli

from ..api.workflow_api import check as check_workflow
from ..api.workflow_api import deploy as deploy_workflow
from ..api.workflow_api import run as run_workflow
from ..api.workflow_api import serve as serve_workflow
from ..helpers.arg_types import LoggerConfig
from ..helpers.logging import logger, set_logger_handling
from ..workflow.dag import dependencies, resolve_order
from ..workflow.errors import WorkflowSpecError

_PARAM_OPTION = click.option(
    "--param",
    "-p",
    "params",
    type=str,
    multiple=True,
    metavar="<name>=<value>",
    help=(
        "Supply or override a workflow parameter declared under the spec's "
        "'params:' block, e.g. '-p xnat_password=hunter2'. Repeatable. Takes "
        "priority over a same-named environment variable (which in turn is "
        "used ahead of any declared 'default:', e.g. for a k8s Secret mounted "
        "as an env var)."
    ),
)

_WORK_DIR_OPTION = click.option(
    "--work-dir",
    type=click.Path(path_type=Path),
    default=None,
    envvar="XINGEST_WORK_DIR",
    help=(
        "Root scratch directory each stage's output is staged under, namespaced "
        "as <work-dir>/<workflow-name> so several workflows can share one root "
        "(e.g. everything deployed from one directory) without colliding. Not "
        "part of the spec's own YAML - a runtime concern, so the same spec runs "
        "on a different host with a different --work-dir unmodified. Defaults "
        "to '.xnat-ingest-<name>' next to the spec file (XINGEST_WORK_DIR env. "
        "var)"
    ),
)


def _parse_params(params: ty.Sequence[str]) -> ty.Dict[str, str]:
    overrides = {}
    for entry in params:
        if "=" not in entry:
            raise click.BadOptionUsage(
                "--param",
                f"--param must be given as <name>=<value>, got '{entry}'",
            )
        name, value = entry.split("=", 1)
        overrides[name] = value
    return overrides


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
        "Validate a workflow YAML spec and print its declared params and resolved "
        "stage order, without running it or requiring Prefect to be installed."
    ),
)
@click.argument("spec_path", type=click.Path(exists=True, path_type=Path))
@_PARAM_OPTION
def check_cmd(spec_path: Path, params: ty.Sequence[str]) -> None:
    overrides = _parse_params(params)
    try:
        spec = check_workflow(spec_path, param_overrides=overrides)
    except WorkflowSpecError as e:
        click.echo(f"INVALID: {e}", err=True)
        sys.exit(1)
    click.echo(f"OK: '{spec.name}' ({len(spec.stages)} stage(s))")
    if spec.params:
        click.echo("  params:")
        for p in spec.params.values():
            if p.secret:
                default_desc = "required" if p.required else "default: <hidden>"
            else:
                default_desc = "required" if p.required else f"default: {p.default!r}"
            secret_marker = " [secret]" if p.secret else ""
            desc = f" - {p.description}" if p.description else ""
            click.echo(f"    - {p.name} ({default_desc}){secret_marker}{desc}")
    for stage in resolve_order(spec.stages):
        deps = ", ".join(sorted(dependencies(stage)))
        arrow = f"  <- {deps}" if deps else ""
        marker = "" if stage.enabled else "  [disabled]"
        click.echo(f"  - {stage.name} ({stage.command}){arrow}{marker}")


@workflow.command(name="run", help="Run a workflow spec once, synchronously.")
@click.argument("spec_path", type=click.Path(exists=True, path_type=Path))
@_PARAM_OPTION
@_WORK_DIR_OPTION
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
def run_cmd(
    spec_path: Path,
    params: ty.Sequence[str],
    work_dir: ty.Optional[Path],
    loggers: ty.List[LoggerConfig],
) -> None:
    set_logger_handling(logger_configs=loggers)
    overrides = _parse_params(params)
    from ..workflow.runner import WorkflowRunError

    try:
        run_workflow(spec_path, param_overrides=overrides, work_dir=work_dir)
    except WorkflowSpecError as e:
        click.echo(f"INVALID: {e}", err=True)
        sys.exit(1)
    except WorkflowRunError as e:
        logger.error(str(e))
        sys.exit(1)
    logger.info("Workflow completed successfully")


@workflow.command(
    name="serve",
    help=(
        "Serve a workflow spec as a long-running local Prefect deployment, "
        "scheduled by its 'schedule:' cron expression if given. Blocks until "
        "interrupted. For deploying to a remote Prefect server instead, see "
        "'workflow deploy'."
    ),
)
@click.argument("spec_path", type=click.Path(exists=True, path_type=Path))
@_PARAM_OPTION
@_WORK_DIR_OPTION
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
def serve_cmd(
    spec_path: Path,
    params: ty.Sequence[str],
    work_dir: ty.Optional[Path],
    loggers: ty.List[LoggerConfig],
) -> None:
    set_logger_handling(logger_configs=loggers)
    overrides = _parse_params(params)
    try:
        serve_workflow(spec_path, param_overrides=overrides, work_dir=work_dir)
    except WorkflowSpecError as e:
        click.echo(f"INVALID: {e}", err=True)
        sys.exit(1)


@workflow.command(
    name="deploy",
    help=(
        "Deploy every workflow spec in a directory as a Prefect deployment "
        "(e.g. 'xnat-ingest workflow deploy example-specs/'). Registers each "
        "with the target Prefect server/work pool for scheduled or on-demand "
        "runs - a Prefect worker must be running against that work pool to "
        "actually execute them; see docs/source/how_to/workflow.rst."
    ),
)
@click.argument(
    "specs_dir", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.option(
    "--prefect-api-url",
    envvar="PREFECT_API_URL",
    default=None,
    help=(
        "The Prefect server API URL to deploy to. Falls back to the ambient "
        "Prefect profile/PREFECT_API_URL env. var if not given."
    ),
)
@click.option(
    "--prefect-api-key",
    envvar="PREFECT_API_KEY",
    default=None,
    help=(
        "API key for the Prefect server (Prefect Cloud, or a self-hosted server "
        "with auth enabled). Falls back to PREFECT_API_KEY env. var if not given."
    ),
)
@click.option(
    "--work-pool",
    default="xnat-ingest",
    envvar="XINGEST_WORK_POOL",
    help=(
        "The Prefect work pool each deployment is registered against; a worker "
        "must be running against it to execute scheduled/triggered runs "
        "(XINGEST_WORK_POOL env. var)"
    ),
)
@click.option(
    "--pattern",
    default="*.yaml",
    help="Glob, relative to SPECS_DIR, selecting which files to treat as workflow specs",
)
@click.option(
    "--raise-errors/--dont-raise-errors",
    default=False,
    help=(
        "Whether to stop at the first spec that fails to deploy instead of "
        "logging it and continuing with the rest"
    ),
)
@_PARAM_OPTION
@_WORK_DIR_OPTION
def deploy_cmd(
    specs_dir: Path,
    prefect_api_url: ty.Optional[str],
    prefect_api_key: ty.Optional[str],
    work_pool: str,
    pattern: str,
    raise_errors: bool,
    params: ty.Sequence[str],
    work_dir: ty.Optional[Path],
) -> None:
    overrides = _parse_params(params)
    try:
        errors = deploy_workflow(
            specs_dir,
            prefect_api_url=prefect_api_url,
            prefect_api_key=prefect_api_key,
            work_pool=work_pool,
            param_overrides=overrides,
            work_dir=work_dir,
            pattern=pattern,
            raise_errors=raise_errors,
        )
    except ValueError as e:
        click.echo(str(e), err=True)
        sys.exit(1)
    if errors:
        for e in errors:
            click.echo(e, err=True)
        sys.exit(1)
