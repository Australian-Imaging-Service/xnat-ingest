"""Helper functions and classes for logging"""

import asyncio
import logging
import sys
import typing as ty
from pathlib import Path

import discord

from .arg_types import LoggerConfig

logger = logging.getLogger("xnat-ingest")


def set_logger_handling(
    logger_configs: ty.Sequence[LoggerConfig],
    additional_loggers: ty.Sequence[str] = (),
    clean_format: bool = False,
) -> None:
    """Set up logging for the application"""

    if not logger_configs:
        logger_configs = [LoggerConfig("stream", "info", "stdout")]

    loggers = [logger]
    for log in additional_loggers:
        loggers.append(logging.getLogger(log))

    min_log_level = min(ll.loglevel_int for ll in logger_configs)

    for logr in loggers:
        logr.setLevel(min_log_level)

    # Configure the file logger
    for config in logger_configs:
        log_handle: logging.Handler
        if config.type == "file":
            Path(config.location).parent.mkdir(parents=True, exist_ok=True)
            log_handle = logging.FileHandler(config.location)
        elif config.type == "stream":
            stream = sys.stderr if config.location == "stderr" else sys.stdout
            log_handle = logging.StreamHandler(stream)
        elif config.type == "discord":
            log_handle = DiscordHandler(config.location)
        else:
            raise ValueError(f"Unknown logger type: {config.type}")
        log_handle.setLevel(config.loglevel_int)
        if clean_format:
            log_handle.setFormatter(logging.Formatter("%(message)s"))
        else:
            log_handle.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )
        for logr in loggers:
            logr.addHandler(log_handle)


class DiscordHandler(logging.Handler):
    """A logging handler that sends log messages to a Discord webhook"""

    def __init__(self, webhook_url: str):
        super().__init__()
        self.webhook_url = webhook_url
        self.client = discord.Webhook.from_url(webhook_url)

    def emit(self, record: logging.LogRecord) -> None:
        asyncio.run(self.client.send(record.msg))
