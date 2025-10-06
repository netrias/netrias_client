"""Logger helpers for the Netrias client."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Final

from ._models import LogLevel


_FORMAT: Final[str] = "%(asctime)s %(levelname)s netrias_client: %(message)s"


def configure_logger(
    name: str,
    level: LogLevel,
    log_directory: Path | None,
) -> logging.Logger:
    """Configure and return a logger dedicated to a Netrias client instance."""

    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(fmt=_FORMAT)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_directory is not None:
        log_directory.mkdir(parents=True, exist_ok=True)
        file_path = log_directory / f"{name.replace('.', '_')}.log"
        file_handler = logging.FileHandler(file_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    mapping = {
        LogLevel.CRITICAL: logging.CRITICAL,
        LogLevel.ERROR: logging.ERROR,
        LogLevel.WARNING: logging.WARNING,
        LogLevel.INFO: logging.INFO,
        LogLevel.DEBUG: logging.DEBUG,
    }
    logger.setLevel(mapping[level])
    return logger
