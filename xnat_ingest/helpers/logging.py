"""Helper functions and classes for logging"""

import asyncio
import json
import logging
import os
import sys
import typing as ty
from pathlib import Path

import discord

from .arg_types import LoggerConfig

logger = logging.getLogger("xnat-ingest")


class JsonFormatter(logging.Formatter):
    """One JSON object per line — designed for log-aggregation tools
    (Loki / Vector / Fluent Bit) to parse without grok/regex.

    Activated by setting AIS_LOG_FORMAT=json in the environment. Falls back
    to the default human-readable format otherwise. No CLI changes; no
    behavioural changes anywhere else in the codebase."""

    def format(self, record: logging.LogRecord) -> str:
        payload: ty.Dict[str, ty.Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        # Anything attached via logger.info("...", extra={...}) passes through.
        reserved = set(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
            "message",
            "asctime",
        }
        for key, value in record.__dict__.items():
            if key in reserved or key.startswith("_"):
                continue
            try:
                json.dumps(value)
                payload[key] = value
            except (TypeError, ValueError):
                payload[key] = repr(value)
        return json.dumps(payload, default=str)


def _select_formatter(clean_format: bool) -> logging.Formatter:
    """Pick the formatter based on AIS_LOG_FORMAT (env-driven; default unchanged)."""
    if os.environ.get("AIS_LOG_FORMAT", "").lower() == "json":
        return JsonFormatter()
    if clean_format:
        return logging.Formatter("%(message)s")
    return logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")


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
        log_handle.setFormatter(_select_formatter(clean_format))
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
