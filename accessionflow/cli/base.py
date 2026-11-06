import click

from accessionflow.helpers import __version__
from accessionflow.model.store_config import load_store_plugins, store_plugins


@click.group(
    help="Collates, labels, deidentifies and uploads data exported from instruments "
    "to data stores"
)
@click.version_option(version=__version__)
def cli() -> None:
    pass


class DataStoreGroup(click.Group):
    """A stage with one subcommand per store, e.g. 'label xnat'.

    The subcommands are registered by the store plugins, which are loaded the
    first time the group is asked for its subcommands.
    """

    def list_commands(self, ctx: click.Context) -> list[str]:
        load_store_plugins()
        return super().list_commands(ctx)

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        load_store_plugins()
        return super().get_command(ctx, cmd_name)

    def resolve_command(
        self, ctx: click.Context, args: list[str]
    ) -> tuple[str | None, click.Command | None, list[str]]:
        if args and not args[0].startswith("-") and not ctx.resilient_parsing:
            if self.get_command(ctx, args[0]) is None:
                if args[0] in store_plugins():
                    hint = (
                        f"the '{args[0]}' store plugin does not provide a "
                        f"'{self.name}' command"
                    )
                else:
                    hint = (
                        f"no store plugin named '{args[0]}' is installed. "
                        "Store plugins are subpackages of "
                        f"'accessionflow.stores', e.g. "
                        f"'accessionflow.stores.{args[0]}'"
                    )
                available = ", ".join(self.list_commands(ctx)) or "none"
                ctx.fail(
                    f"Cannot run '{self.name} {args[0]}': {hint} (stores "
                    f"available for '{self.name}': {available})"
                )
        return super().resolve_command(ctx, args)


def data_store_group(name: str, help: str) -> DataStoreGroup:
    """Add a stage to the CLI whose subcommands are provided by store plugins"""
    group = DataStoreGroup(
        name=name,
        help=f"{help}\n\nRun 'accessionflow {name} <data-store> --help' for the "
        "options for each data store.",
    )
    cli.add_command(group)
    return group


class DeprecatedAlias(DataStoreGroup):
    """A hidden former name of a stage, e.g. 'upload' for 'store', which runs the
    stage's subcommands after warning that the name is deprecated"""

    def __init__(self, name: str, target: DataStoreGroup) -> None:
        super().__init__(
            name=name,
            hidden=True,
            help=f"Deprecated name of '{target.name}'",
        )
        self.target = target

    def list_commands(self, ctx: click.Context) -> list[str]:
        return self.target.list_commands(ctx)

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        return self.target.get_command(ctx, cmd_name)

    def invoke(self, ctx: click.Context) -> object:
        click.echo(
            f"Warning: 'accessionflow {self.name}' is deprecated and will be removed "
            f"in a future release, use 'accessionflow {self.target.name}' instead",
            err=True,
        )
        return super().invoke(ctx)


def deprecated_alias(name: str, target: DataStoreGroup) -> DeprecatedAlias:
    """Add a hidden former name of a stage to the CLI"""
    alias = DeprecatedAlias(name, target)
    cli.add_command(alias)
    return alias


def deprecated_command_alias(name: str, target: click.Command) -> click.Command:
    """Add a hidden former name of a command to the CLI, which runs the command after
    warning that the name is deprecated"""

    def callback(**kwargs: object) -> object:
        click.echo(
            f"Warning: 'accessionflow {name}' is deprecated and will be removed in a "
            f"future release, use 'accessionflow {target.name}' instead",
            err=True,
        )
        assert target.callback is not None
        return target.callback(**kwargs)

    alias = click.Command(
        name=name,
        params=target.params,
        callback=callback,
        help=f"Deprecated name of '{target.name}'",
        hidden=True,
    )
    cli.add_command(alias)
    return alias
