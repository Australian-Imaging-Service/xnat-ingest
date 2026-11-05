"""Registry mapping a workflow stage's ``command`` to the ``xnat_ingest.api``
function it calls and the ``args:`` -> kwargs adaptation for that function.

Each ``_build_*`` function is deliberately explicit (mirroring the corresponding
``*_cli.py`` command body) rather than table-driven, since every stage has its own
small quirks: ``group`` folds ``collate_resources``/``convert`` lists into the
``collation_map``/``conversion_map`` dicts its API wants, ``assign`` renames
``project``/``subject``/``session``/``scan`` to the API's ``*_field`` parameters,
and ``upload`` needs a live ``Xnat`` connection built (and closed) around the call
rather than passed in as a plain argument.
"""

from __future__ import annotations

import tempfile
import typing as ty
from pathlib import Path

import attrs

from ..api.assign_api import assign
from ..api.associate_api import associate
from ..api.deidentify_api import deidentify
from ..api.group_api import group
from ..api.upload_api import upload
from . import coerce

if ty.TYPE_CHECKING:
    from frametree.xnat import Xnat

    from .spec import StageSpec, XnatConnectionSpec


@attrs.define
class StageContext:
    """What a stage's kwarg-builder is given beyond its own ``args:`` dict: its
    resolved input path (``None`` if the stage's own ``args`` must supply one, e.g.
    the first stage of a pipeline), resolved output directory (``None`` for a
    terminal stage such as ``upload``), and the workflow's shared ``xnat:`` block."""

    input_path: ty.Optional[Path]
    output_path: ty.Optional[Path]
    xnat: "ty.Optional[XnatConnectionSpec]"


def _build_group(args: dict, ctx: StageContext) -> dict:
    args = dict(args)
    input_paths = args.pop(
        "input_paths", [str(ctx.input_path)] if ctx.input_path is not None else []
    )
    kwargs: dict[str, ty.Any] = {
        "input_paths": [str(p) for p in input_paths],
        "output_dir": ctx.output_path,
    }
    if "datatypes" in args:
        kwargs["datatypes"] = coerce.datatypes(args.pop("datatypes"))
    if "ignore_datatypes" in args:
        kwargs["ignore_datatypes"] = coerce.datatypes(args.pop("ignore_datatypes"))
    if "session" in args:
        kwargs["session"] = coerce.id_specs(args.pop("session"))
    if "scan" in args:
        kwargs["scan"] = coerce.id_specs(args.pop("scan"))
    if "resource" in args:
        kwargs["resource"] = coerce.id_specs(args.pop("resource"))
    if "path_metadata_regex" in args:
        kwargs["path_metadata_regex"] = coerce.path_metadata_regexes(
            args.pop("path_metadata_regex")
        )
    if "on_resource_clash" in args:
        kwargs["on_resource_clash"] = coerce.on_resource_clash(
            args.pop("on_resource_clash")
        )
    if "metadata_tables" in args:
        kwargs["metadata_tables"] = coerce.metadata_tables(args.pop("metadata_tables"))
    if "collate_resources" in args:
        kwargs["collation_map"] = coerce.collation_map(args.pop("collate_resources"))
    if "convert" in args:
        kwargs["conversion_map"] = coerce.conversion_map(args.pop("convert"))
    if "copy_mode" in args:
        kwargs["copy_mode"] = coerce.copy_mode(args.pop("copy_mode"))
    # Remaining plain scalars pass straight through unchanged: recursive,
    # wait_period, allow_unrecognised, exclude_paths, unlink_source, raise_errors.
    kwargs.update(args)
    return kwargs


def _build_assign(args: dict, ctx: StageContext) -> dict:
    args = dict(args)
    kwargs: dict[str, ty.Any] = {
        "input_dir": ctx.input_path,
        "output_dir": ctx.output_path,
    }
    for yaml_key, api_key in (
        ("project", "project_field"),
        ("subject", "subject_field"),
        ("session", "session_field"),
        ("scan", "scan_field"),
    ):
        if yaml_key in args:
            kwargs[api_key] = args.pop(yaml_key)
    if "constant_project_id" in args:
        kwargs["project_id"] = args.pop("constant_project_id")
    if "include" in args:
        kwargs["include"] = coerce.datatypes(args.pop("include"))
    if "copy_mode" in args:
        kwargs["copy_mode"] = coerce.copy_mode(args.pop("copy_mode"))
    kwargs.update(args)  # unlink_source, raise_errors, ...
    return kwargs


def _build_deidentify(args: dict, ctx: StageContext) -> dict:
    args = dict(args)
    kwargs: dict[str, ty.Any] = {
        "input_dir": ctx.input_path,
        "output_dir": ctx.output_path,
    }
    if "spec_dir" in args:
        kwargs["spec_dir"] = Path(args.pop("spec_dir"))
    if "reid_dir" in args:
        reid_dir = args.pop("reid_dir")
        kwargs["reid_dir"] = Path(reid_dir) if reid_dir else None
    if "reid_encrypt_key" in args:
        key = args.pop("reid_encrypt_key")
        kwargs["reid_encrypt_key"] = key.encode() if key else None
    if "copy_mode" in args:
        kwargs["copy_mode"] = coerce.copy_mode(args.pop("copy_mode"))
    # on_resource_clash stays a bare policy string here - deidentify() doesn't
    # support datatype-scoped ClashSpec entries.
    kwargs.update(args)
    return kwargs


def _build_associate(args: dict, ctx: StageContext) -> dict:
    args = dict(args)
    kwargs: dict[str, ty.Any] = {
        "input_dir": ctx.input_path,
        "output_dir": ctx.output_path,
    }
    if "datatype" in args:
        kwargs["datatype"] = coerce.datatype(args.pop("datatype"))
    if "copy_mode" in args:
        kwargs["copy_mode"] = coerce.copy_mode(args.pop("copy_mode"))
    kwargs.update(args)  # glob, identity_pattern, spaces_to_underscores, ...
    return kwargs


def _build_upload(args: dict, ctx: StageContext) -> dict:
    args = dict(args)
    kwargs: dict[str, ty.Any] = {"input_dir": str(ctx.input_path)}
    if "always_include" in args:
        kwargs["always_include"] = list(args.pop("always_include"))
    if "methods" in args:
        kwargs["methods"] = coerce.upload_methods(args.pop("methods"))
    if "store_credentials" in args:
        kwargs["store_credentials"] = coerce.store_credentials(
            args.pop("store_credentials")
        )
    kwargs.update(args)  # wait_period, check_checksums, dry_run, ...
    return kwargs  # 'xnat_repo' is injected by run_stage(), not built here


@attrs.define
class Stage:
    command: str
    api_fn: ty.Callable[..., list[str]]
    build_kwargs: ty.Callable[[dict, StageContext], dict]
    # The args: key a stage needs (directly, or via 'input:') to know what to
    # process - used to validate every stage has an input at spec-load time.
    input_arg: str = "input_dir"
    takes_output_dir: bool = True
    needs_xnat: bool = False


STAGES: dict[str, Stage] = {
    "group": Stage(
        command="group",
        api_fn=group,
        build_kwargs=_build_group,
        input_arg="input_paths",
    ),
    "assign": Stage(command="assign", api_fn=assign, build_kwargs=_build_assign),
    "deidentify": Stage(
        command="deidentify", api_fn=deidentify, build_kwargs=_build_deidentify
    ),
    "associate": Stage(
        command="associate", api_fn=associate, build_kwargs=_build_associate
    ),
    "upload": Stage(
        command="upload",
        api_fn=upload,
        build_kwargs=_build_upload,
        takes_output_dir=False,
        needs_xnat=True,
    ),
}

STAGE_NAMES = frozenset(STAGES)


def _build_xnat_repo(xnat: "XnatConnectionSpec") -> "Xnat":
    from frametree.xnat import Xnat

    repo = Xnat(
        server=xnat.server,
        user=xnat.user,
        password=xnat.password,
        cache_dir=Path(tempfile.mkdtemp()),
        verify_ssl=xnat.verify_ssl,
    )
    repo.connection.__enter__()
    return repo


def _close_xnat_repo(repo: "Xnat") -> None:
    try:
        repo.connection.__exit__(None, None, None)
    except Exception:  # noqa: BLE001 - best-effort cleanup of a possibly dead session
        pass


def run_stage(stage_spec: "StageSpec", ctx: StageContext) -> list[str]:
    """Build the kwargs for one stage from its spec + resolved context and call its
    API function, returning whatever list of per-session error strings that
    function returns. For 'upload', opens an Xnat connection around the call and
    closes it afterwards."""
    stage = STAGES[stage_spec.command]
    kwargs = stage.build_kwargs(stage_spec.args, ctx)
    if stage.needs_xnat:
        if ctx.xnat is None:
            raise ValueError(
                f"stage '{stage_spec.name}' ({stage_spec.command}) needs a "
                "top-level 'xnat:' block (server/user/password) in the workflow spec"
            )
        repo = _build_xnat_repo(ctx.xnat)
        try:
            kwargs["xnat_repo"] = repo
            return stage.api_fn(**kwargs)
        finally:
            _close_xnat_repo(repo)
    return stage.api_fn(**kwargs)
