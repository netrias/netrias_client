"""Centralize logger creation.

'why': provide a unified logging approach configured once via settings
"""
from __future__ import annotations

import logging


_LOGGER_NAME = "netrias_client"
_logger: logging.Logger | None = None


def get_logger() -> logging.Logger:
    """Return the module logger, creating it if necessary."""

    global _logger
    if _logger is not None:
        return _logger
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s netrias_client: %(message)s",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.propagate = False
    _logger = logger
    return logger


def set_log_level(level: str) -> None:
    """Apply the provided log level to the module logger."""

    logger = get_logger()
    levels = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
    }
    logger.setLevel(levels.get(level.upper(), logging.INFO))
