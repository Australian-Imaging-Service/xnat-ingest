import time
import typing as ty
from pathlib import Path

import click
from fileformats.core import FileSet

from accessionflow.api.label_api import label
from accessionflow.helpers.arg_types import CopyModeParamType, LoggerConfig, MimeType
from accessionflow.helpers.logging import logger, set_logger_handling
from accessionflow.model.store import Store, check_level_names

from .base import data_store_group

label_group = data_store_group(
    name="label",
    help="""Assigns the IDs a data store needs, extracted from session metadata, to
sessions that have already been collated into acquisitions/resources""",
)

LABEL_HELP = """Labels sessions for {title} data stores with their {levels} labels,
extracted from session metadata, after they have been collated into
{acquisitions}/resources

INPUT_DIR is the path to the directory containing the collated-but-not-yet-labelled
sessions (the output of the 'collate' command)

OUTPUT_DIR is the directory that the labelled sessions will be written to, each in a
directory named '{dirname}'

If a label can't be resolved from a session's metadata, it is given a unique
'INVALID_MISSING_<FIELD>_<random>' placeholder instead of being dropped, and the whole
session is saved under an '__invalid__' subdirectory of OUTPUT_DIR instead of
alongside normally-labelled ones, so it can be found and manually
reviewed/reprocessed rather than silently lost.
"""


def make_label_command(data_store: Store) -> click.Command:
    """Make the 'label <store>' subcommand from the store's configuration
    and register it with the 'label' group.

    If the store has fixed label levels, the command has an option for each of them,
    e.g. '--project', '--subject' and '--session' for XNAT. Otherwise, the levels are
    named, in order from the top, with a repeatable '--level NAME FIELD' option. The
    command also has an option for the store's name for an acquisition, e.g. '--scan'.
    Each option is settable by an environment variable named after it, e.g.
    'ACFLOW_PROJECT'.

    Parameters
    ----------
    data_store : Store
        the configuration of the store

    Returns
    -------
    click.Command
        the 'label <store>' subcommand
    """
    prefix = data_store.envvar_prefix
    fixed_levels = data_store.labels
    acquisition = data_store.acquisition

    def label_cmd(
        input_dir: Path,
        output_dir: Path,
        include_types: tuple[MimeType, ...],
        unlink_source: str | None,
        loggers: ty.List[LoggerConfig],
        additional_loggers: ty.List[str],
        raise_errors: bool,
        loop: int,
        copy_mode: FileSet.CopyMode,
        acquisition_field: str | None = None,
        constant_top_label: str | None = None,
        levels: ty.Sequence[tuple[str, str]] = (),
        constant_label_pairs: ty.Sequence[tuple[str, str]] = (),
        **fixed_level_fields: str,
    ) -> None:
        if raise_errors and loop >= 0:
            raise ValueError(
                "Cannot use --raise-errors and --loop together as the loop will "
                "continue to run even if an error occurs"
            )

        set_logger_handling(
            logger_configs=loggers,
            additional_loggers=additional_loggers,
        )

        label_fields: dict[str, str]
        constant_labels: dict[str, str]
        if fixed_levels is not None:
            label_fields = {
                level.name: fixed_level_fields[f"label_{i}"]
                for i, level in enumerate(fixed_levels)
            }
            constant_labels = (
                {fixed_levels[0].name: constant_top_label}
                if constant_top_label is not None
                else {}
            )
        else:
            names = [name for name, _ in levels]
            check_level_names(
                data_store.name, names, acquisition.name if acquisition else None
            )
            label_fields = dict(levels)
            constant_labels = dict(constant_label_pairs)
            if unknown := set(constant_labels) - set(label_fields):
                raise click.UsageError(
                    f"--constant-label names {sorted(unknown)} that aren't levels "
                    f"given by --level ({names})"
                )

        # Run the label process in a loop if loop is set to a positive value,
        # otherwise just run it once
        while True:
            start_time = time.monotonic()
            label(
                input_dir=input_dir,
                output_dir=output_dir,
                label_fields=label_fields,
                acquisition_field=acquisition_field,
                constant_labels=constant_labels,
                store_type=data_store.name,
                unlink_source=unlink_source,
                raise_errors=raise_errors,
                copy_mode=copy_mode,
                include=[include_type.datatype for include_type in include_types],
                single_resource=not data_store.multiple_resources,
            )
            if loop < 0:
                break
            end_time = time.monotonic()
            elapsed_seconds = end_time - start_time
            sleep_time = max(loop - elapsed_seconds, 0)
            logger.info(
                "Label took %s seconds, waiting another %s seconds before running "
                "again (loop every %s seconds)",
                elapsed_seconds,
                sleep_time,
                loop,
            )
            time.sleep(sleep_time)

    label_options: list[click.Parameter] = []
    if fixed_levels is not None:
        for i, level in enumerate(fixed_levels):
            envvar = level.envvar(prefix)
            # An explicit default of None would count as a value for 'required' in
            # click >= 8.3, so a level without a default is given none at all
            default: dict[str, ty.Any] = (
                {} if level.default is None else {"default": level.default}
            )
            label_options.append(
                click.Option(
                    [level.option_name, f"label_{i}"],
                    type=str,
                    required=level.default is None,
                    envvar=envvar,
                    **default,
                    help=(
                        f"The keyword of the metadata field to extract the "
                        f"{level.description or level.name} from, or a Python format "
                        "string over several fields, e.g. "
                        "'{PatientID}_{StudyDate:%Y%m%d}', to compose one "
                        f"({envvar} env. var)"
                    ),
                )
            )
    else:
        envvar = f"{prefix}LEVELS"
        label_options.append(
            click.Option(
                ["--level", "levels"],
                type=(str, str),
                multiple=True,
                required=True,
                envvar=envvar,
                metavar="<name> <field>",
                help=(
                    "A level of the folder hierarchy the sessions are labelled with, "
                    "and the keyword of the metadata field to extract its label from "
                    "(or a Python format string over several fields, e.g. "
                    "'{PatientID}_{StudyDate:%Y%m%d}'). Repeat for each level, in "
                    "order from the top, e.g. '--level site SiteID --level subject "
                    f"PatientID' ({envvar} env. var, as space-separated pairs)"
                ),
            )
        )
    if acquisition is not None:
        envvar = acquisition.envvar(prefix)
        label_options.append(
            click.Option(
                [acquisition.option_name, "acquisition_field"],
                type=str,
                default=acquisition.default,
                envvar=envvar,
                help=(
                    "The keyword of the metadata field to extract the "
                    f"{acquisition.description or 'description'} of each "
                    f"{acquisition.name} from. {acquisition.name.capitalize()}s for "
                    "which the field can't be resolved are left without one "
                    f"({envvar} env. var)"
                ),
            )
        )
    if fixed_levels is not None:
        top = fixed_levels[0]
        constant_envvar = f"{prefix}CONSTANT_{top.name.upper()}_ID"
        label_options.append(
            click.Option(
                [f"--constant-{top.name}-id", "constant_top_label"],
                type=str,
                default=None,
                envvar=constant_envvar,
                help=(
                    f"Fix the {top.description or top.name} as a constant for all "
                    f"data matched by this command ({constant_envvar} env. var)"
                ),
            )
        )
    else:
        constant_envvar = f"{prefix}CONSTANT_LABELS"
        label_options.append(
            click.Option(
                ["--constant-label", "constant_label_pairs"],
                type=(str, str),
                multiple=True,
                envvar=constant_envvar,
                metavar="<name> <value>",
                help=(
                    "Fix the label of a level given by --level as a constant for all "
                    "data matched by this command, e.g. '--constant-label site "
                    f"SYDNEY'. May be repeated ({constant_envvar} env. var)"
                ),
            )
        )

    if fixed_levels is not None:
        level_names = [level.name for level in fixed_levels]
        levels_desc = "/".join(level_names)
        dirname = ".".join(f"<{n}>" for n in level_names)
    else:
        levels_desc = "user-defined (--level)"
        dirname = "<label1>.<label2>..."
    command = _common_label_options(
        prefix,
        click.command(
            name=data_store.name,
            help=LABEL_HELP.format(
                title=data_store.title,
                levels=levels_desc,
                acquisitions=f"{acquisition.name}s" if acquisition else "acquisitions",
                dirname=dirname,
            ),
        ),
    )(label_cmd)
    # After the arguments, before the options that all stores share
    command.params[2:2] = label_options
    label_group.add_command(command)
    return command


def _common_label_options(
    prefix: str, command: ty.Callable[[ty.Callable[..., None]], click.Command]
) -> ty.Callable[[ty.Callable[..., None]], click.Command]:
    """The arguments and options of 'label' that don't depend on the store"""
    decorators = [
        click.argument(
            "input_dir",
            type=click.Path(exists=True, path_type=Path),
            envvar=f"{prefix}INPUT_DIR",
        ),
        click.argument(
            "output_dir", type=click.Path(path_type=Path), envvar=f"{prefix}OUTPUT_DIR"
        ),
        click.option(
            "--include",
            "include_types",
            type=MimeType.cli_type,
            multiple=True,
            default=(),
            envvar=f"{prefix}INCLUDE",
            metavar="<mime-type>",
            help=(
                "Only label resources matching this fileformats MIME-like datatype. "
                f"May be repeated to include multiple datatypes ({prefix}INCLUDE "
                "env. var)"
            ),
        ),
        click.option(
            "--loop",
            type=int,
            default=-1,
            envvar=f"{prefix}LOOP",
            help=(
                f"Run the label process continuously every LOOP seconds ({prefix}LOOP "
                "env. var). "
            ),
        ),
        click.option(
            "--copy-mode",
            type=CopyModeParamType(),
            default=FileSet.CopyMode.hardlink_or_copy,
            envvar=f"{prefix}COPY_MODE",
            help=f"The method to use for copying files ({prefix}COPY_MODE env. var)",
        ),
        click.option(
            "--unlink-source",
            type=click.Choice(["all", "keep-metadata"]),
            default=None,
            envvar=f"{prefix}UNLINK_SOURCE",
            help=(
                "Whether to unlink the collated session directories after they have "
                "been labelled. 'all' removes the whole collated session directory; "
                "'keep-metadata' removes the resource data but leaves the "
                "session/acquisition-level metadata behind, so a lightweight skeleton "
                "of the session survives (e.g. for 'attach' to use later). If not "
                "set, the collated directories are left in place "
                f"({prefix}UNLINK_SOURCE env. var)"
            ),
        ),
        click.option(
            "--logger",
            "loggers",
            multiple=True,
            type=LoggerConfig.cli_type,
            envvar=f"{prefix}LOGGERS",
            nargs=3,
            default=(),
            metavar="<logtype> <loglevel> <location>",
            help=(
                "Setup handles to capture logs that are generated "
                f"({prefix}LOGGERS env. var)"
            ),
        ),
        click.option(
            "--additional-logger",
            "additional_loggers",
            type=str,
            multiple=True,
            default=(),
            envvar=f"{prefix}ADDITIONAL_LOGGERS",
            help=(
                "The loggers to use for logging. By default just the 'accessionflow' "
                "logger is used. But additional loggers can be included (e.g. 'xnat') "
                f"can be specified here ({prefix}ADDITIONAL_LOGGERS env. var)"
            ),
        ),
        click.option(
            "--raise-errors/--dont-raise-errors",
            default=False,
            type=bool,
            help=(
                "Whether to raise errors instead of logging them (typically for "
                "debugging)"
            ),
        ),
    ]

    def decorate(func: ty.Callable[..., None]) -> click.Command:
        for decorator in reversed(decorators):
            func = decorator(func)
        return command(func)

    return decorate
