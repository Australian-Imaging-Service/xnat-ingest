import click
import click_log
from xnat_siemens_export_upload import __version__
from xnat_siemens_export_upload.utils import logger

click_log.basic_config(logger)


@click.group(help="Checks to run against an XNAT instance to ensure its integrity")
@click.version_option(version=__version__)
@click_log.simple_verbosity_option(logger)
def cli():
    pass
