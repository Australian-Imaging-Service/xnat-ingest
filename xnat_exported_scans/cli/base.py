import click
from .. import __version__


@click.group(help="Checks and uploads scans exported from scanner consoles to XNAT")
@click.version_option(version=__version__)
def cli():
    pass
