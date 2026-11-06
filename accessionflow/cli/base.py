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


class StoreGroup(click.Group):
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


def store_group(name: str, help: str) -> StoreGroup:
    """Add a stage to the CLI whose subcommands are provided by store plugins"""
    group = StoreGroup(
        name=name,
        help=f"{help}\n\nRun 'accessionflow {name} <store> --help' for the "
        "options for each store.",
    )
    cli.add_command(group)
    return group
