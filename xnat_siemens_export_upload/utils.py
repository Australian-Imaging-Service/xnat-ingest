from logging import getLogger
import traceback

logger = getLogger("xnat-siemens-export-upload")


def log(msg):
    logger.info(msg)


def log_error(level, msg):
    logger.error(msg)


def show_cli_trace(result):
    """Show the exception traceback from CLIRunner results"""
    return "".join(traceback.format_exception(*result.exc_info))
