import click

from accessionflow.helpers import __version__
from accessionflow.model.repository import load_repository_plugins, repository_plugins


@click.group(
    help="Collates, labels, deidentifies and uploads data exported from instruments "
    "to repositories"
)
@click.version_option(version=__version__)
def cli() -> None:
    pass


class RepositoryGroup(click.Group):
    """A stage with one subcommand per repository, e.g. 'label xnat'.

    The subcommands are registered by the repository plugins, which are loaded the
    first time the group is asked for its subcommands.
    """

    def list_commands(self, ctx: click.Context) -> list[str]:
        load_repository_plugins()
        return super().list_commands(ctx)

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        load_repository_plugins()
        return super().get_command(ctx, cmd_name)

    def resolve_command(
        self, ctx: click.Context, args: list[str]
    ) -> tuple[str | None, click.Command | None, list[str]]:
        if args and not args[0].startswith("-") and not ctx.resilient_parsing:
            if self.get_command(ctx, args[0]) is None:
                if args[0] in repository_plugins():
                    hint = (
                        f"the '{args[0]}' repository plugin does not provide a "
                        f"'{self.name}' command"
                    )
                else:
                    hint = (
                        f"no repository plugin named '{args[0]}' is installed. "
                        "Repository plugins are subpackages of "
                        f"'accessionflow.repositories', e.g. "
                        f"'accessionflow.repositories.{args[0]}'"
                    )
                available = ", ".join(self.list_commands(ctx)) or "none"
                ctx.fail(
                    f"Cannot run '{self.name} {args[0]}': {hint} (repositories "
                    f"available for '{self.name}': {available})"
                )
        return super().resolve_command(ctx, args)


def repository_group(name: str, help: str) -> RepositoryGroup:
    """Add a stage to the CLI whose subcommands are provided by repository plugins"""
    group = RepositoryGroup(
        name=name,
        help=f"{help}\n\nRun 'accessionflow {name} <repository> --help' for the "
        "options for each repository.",
    )
    cli.add_command(group)
    return group
