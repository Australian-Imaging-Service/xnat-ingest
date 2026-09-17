"""API for loading, running and deploying ``xnat-ingest workflow`` YAML specs -
the plain-Python layer ``xnat_ingest.cli.workflow_cli`` is a thin wrapper around,
same as every other ``*_api.py``/``*_cli.py`` pair in this package.

Prefect itself is only ever imported lazily, inside ``run``/``serve``/``deploy``
(via ``workflow.runner``) - ``check`` (and so ``xnat-ingest workflow check``) needs
only PyYAML, which is a core dependency.
"""

from __future__ import annotations

import contextlib
import typing as ty
from pathlib import Path

from ..helpers.logging import logger

if ty.TYPE_CHECKING:
    from ..workflow.spec import WorkflowSpec

__all__ = ["check", "run", "serve", "deploy"]

# NB: ``..workflow.spec``/``..workflow.runner`` are imported lazily inside each
# function below, not at module level - ``xnat_ingest.workflow.stages`` imports
# from this package (``..api.assign_api`` etc.), so an eager import here would be
# circular (this module <-> workflow.spec <-> workflow.stages <-> this package).


def check(
    spec_path: ty.Union[str, Path],
    param_overrides: ty.Optional[ty.Dict[str, str]] = None,
) -> "WorkflowSpec":
    """Load and fully validate a workflow spec without running it. See
    ``workflow.spec.load_spec`` for exactly what's checked."""
    from ..workflow.spec import load_spec

    return load_spec(spec_path, param_overrides=param_overrides)


def run(
    spec_path: ty.Union[str, Path],
    param_overrides: ty.Optional[ty.Dict[str, str]] = None,
    work_dir: ty.Optional[Path] = None,
) -> ty.Dict[str, ty.List[str]]:
    """Run a workflow spec once, synchronously. Raises
    ``workflow.runner.WorkflowRunError`` if any stage reported errors."""
    from ..workflow.runner import run_workflow
    from ..workflow.spec import load_spec

    spec = load_spec(spec_path, param_overrides=param_overrides)
    return run_workflow(spec, work_dir=work_dir)


def serve(
    spec_path: ty.Union[str, Path],
    param_overrides: ty.Optional[ty.Dict[str, str]] = None,
    work_dir: ty.Optional[Path] = None,
) -> None:
    """Serve a workflow spec as a long-running local Prefect deployment (blocks
    until interrupted) - see ``workflow.runner.serve_workflow``."""
    from ..workflow.runner import serve_workflow
    from ..workflow.spec import load_spec

    spec = load_spec(spec_path, param_overrides=param_overrides)
    serve_workflow(spec, work_dir=work_dir)


@contextlib.contextmanager
def _prefect_server_context(
    prefect_api_url: ty.Optional[str],
    prefect_api_key: ty.Optional[str],
) -> ty.Iterator[None]:
    """Point Prefect at an explicit server for the duration of the ``with`` block,
    without needing PREFECT_API_URL/PREFECT_API_KEY already set in the
    environment. Falls back to the ambient Prefect profile/environment if
    neither is given."""
    if not prefect_api_url and not prefect_api_key:
        yield
        return
    try:
        from prefect.settings import (
            PREFECT_API_KEY,
            PREFECT_API_URL,
            temporary_settings,
        )
    except ImportError as e:
        raise ImportError(
            "the 'workflow deploy' command needs Prefect - install it with "
            "'pip install xnat-ingest[workflow]'"
        ) from e
    updates: ty.Dict[ty.Any, ty.Any] = {}
    if prefect_api_url:
        updates[PREFECT_API_URL] = prefect_api_url
    if prefect_api_key:
        updates[PREFECT_API_KEY] = prefect_api_key
    with temporary_settings(updates=updates):
        yield


def deploy(
    specs_dir: ty.Union[str, Path],
    prefect_api_url: ty.Optional[str] = None,
    prefect_api_key: ty.Optional[str] = None,
    work_pool: str = "xnat-ingest",
    param_overrides: ty.Optional[ty.Dict[str, str]] = None,
    work_dir: ty.Optional[Path] = None,
    pattern: str = "*.yaml",
    raise_errors: bool = False,
) -> ty.List[str]:
    """Deploy every workflow spec matching ``pattern`` under ``specs_dir`` as a
    Prefect deployment - e.g. everything in ``example-specs/``.

    Registers each spec's flow with the target Prefect server against
    ``work_pool``, scheduled by that spec's own ``schedule:`` if it has one. A
    Prefect worker (``prefect worker start --pool <work_pool>``) must be running
    against that work pool for a deployment to actually execute anything - this
    only registers it with the server; it doesn't start one.

    ``param_overrides`` is resolved once per spec at deploy time (same as
    ``run``/``serve``) and baked into that deployment's flow closure - it is
    never passed through Prefect's own parameter/orchestration layer, so a
    secret such as an XNAT password never ends up stored in, or visible via, the
    Prefect API/UI. Rotating a value means redeploying with a new override.

    Applies the same ``param_overrides``/``work_dir``/``work_pool`` to every spec
    found - a batch that needs genuinely different values per spec (e.g.
    different sites' credentials) should be deployed with separate ``deploy``
    calls, one per subset.

    Parameters
    ----------
    specs_dir
        Directory containing one or more workflow YAML specs.
    prefect_api_url, prefect_api_key
        Explicit Prefect server connection details. If neither is given, falls
        back to the ambient Prefect profile/environment (``PREFECT_API_URL``/
        ``PREFECT_API_KEY``).
    work_pool
        The Prefect work pool each deployment is registered against.
    pattern
        Glob, relative to ``specs_dir``, selecting which files to treat as
        workflow specs.
    raise_errors
        If True, stop at the first spec that fails to deploy instead of logging
        it and continuing with the rest.

    Returns
    -------
    list[str]
        One error message per spec that failed to deploy; empty if every spec in
        ``specs_dir`` deployed successfully.
    """
    from ..workflow.runner import deploy_flow
    from ..workflow.spec import load_spec

    specs_dir = Path(specs_dir)
    spec_paths = sorted(specs_dir.glob(pattern))
    if not spec_paths:
        raise ValueError(
            f"No spec files matching '{pattern}' found under '{specs_dir}'"
        )

    errors: ty.List[str] = []
    with _prefect_server_context(prefect_api_url, prefect_api_key):
        for spec_path in spec_paths:
            try:
                spec = load_spec(spec_path, param_overrides=param_overrides)
                deploy_flow(spec, work_pool=work_pool, work_dir=work_dir)
                logger.info(
                    "Deployed workflow '%s' from '%s' to work pool '%s'",
                    spec.name,
                    spec_path,
                    work_pool,
                )
            except Exception as e:
                if raise_errors:
                    raise
                logger.error("Failed to deploy '%s': %s", spec_path, e)
                errors.append(f"{spec_path}: {e}")
    return errors
