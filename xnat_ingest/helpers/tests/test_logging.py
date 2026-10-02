import io
import logging
import sys
import typing as ty

import pytest

from xnat_ingest.helpers.arg_types import LoggerConfig
from xnat_ingest.helpers.logging import _HANDLER_FLAG, logger, set_logger_handling


@pytest.fixture
def restore_logger() -> ty.Iterator[None]:
    handlers = list(logger.handlers)
    level = logger.level
    yield
    for handler in list(logger.handlers):
        if handler not in handlers:
            logger.removeHandler(handler)
    logger.setLevel(level)


def _flagged_handlers() -> list[logging.Handler]:
    return [h for h in logger.handlers if getattr(h, _HANDLER_FLAG, False)]


def test_stream_handler_follows_current_stdout(
    restore_logger: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Logging after the stdout the handler was set up with has been replaced and
    closed (as click's CliRunner does) writes to the current stdout instead of
    raising a "Logging error" for the closed stream"""
    original = io.StringIO()
    monkeypatch.setattr(sys, "stdout", original)
    set_logger_handling([LoggerConfig("stream", "info", "stdout")], clean_format=True)
    logger.info("first")
    original.close()

    replacement = io.StringIO()
    monkeypatch.setattr(sys, "stdout", replacement)
    monkeypatch.setattr(logging, "raiseExceptions", True)
    errors: list[logging.LogRecord] = []
    monkeypatch.setattr(
        logging.Handler, "handleError", lambda self, record: errors.append(record)
    )
    logger.info("second")

    assert not errors
    assert replacement.getvalue() == "second\n"


def test_repeated_calls_replace_handlers(restore_logger: None) -> None:
    """Calling set_logger_handling again (e.g. for each CLI command invoked in the
    same process) replaces the handlers it previously added rather than adding more,
    and leaves handlers added by others alone"""
    other_handler = logging.NullHandler()
    logger.addHandler(other_handler)

    config = [LoggerConfig("stream", "info", "stdout")]
    set_logger_handling(config)
    set_logger_handling(config)

    assert len(_flagged_handlers()) == 1
    assert other_handler in logger.handlers
