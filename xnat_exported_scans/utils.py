import logging
import traceback

logger = logging.getLogger("xnat-upload-exported-scans")
logger.setLevel(logging.INFO)


def show_cli_trace(result):
    """Show the exception traceback from CLIRunner results"""
    return "".join(traceback.format_exception(*result.exc_info))
