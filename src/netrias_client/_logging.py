"""Logger helpers for the Netrias client."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Final

from ._models import LogLevel


_FORMAT: Final[str] = "%(asctime)s %(levelname)s netrias_client: %(message)s"
LOGGER_NAMESPACE: Final[str] = "netrias_client"
"""Parent logger namespace for external configuration.

Users can configure logging externally via::

    logging.getLogger("netrias_client").setLevel(logging.WARNING)
"""


def configure_logger(
    name: str,
    level: LogLevel,
    log_directory: Path | None,
) -> logging.Logger:
    """Configure and return a logger dedicated to a Netrias client instance.

    If the parent logger ('netrias_client') has handlers configured externally,
    propagation is enabled to use those handlers. Otherwise, a default stream
    handler is added to this logger.
    """

    logger = logging.getLogger(name)
    _close_and_clear_handlers(logger)

    formatter = logging.Formatter(fmt=_FORMAT)
    parent = logging.getLogger(LOGGER_NAMESPACE)

    # Enable propagation to respect external logger configuration
    if parent.handlers:
        # Parent has handlers; propagate to them
        logger.propagate = True
    else:
        # No external handlers; add our own stream handler
        logger.propagate = False
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    # File handler is instance-specific, always add if requested
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


def _close_and_clear_handlers(logger: logging.Logger) -> None:
    """Close all handlers before removing to avoid resource leaks."""

    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
