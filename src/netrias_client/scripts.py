"""Coordinate project command entry points.

'why': centralize developer workflows for `uv run` execution
"""
from __future__ import annotations

import logging
import subprocess
from collections.abc import Sequence
from typing import Final

_LOGGER = logging.getLogger("netrias_client.scripts")
_COMMANDS: Final[tuple[tuple[str, ...], ...]] = (
    ("pytest",),
    ("ruff", "check", "."),
    ("basedpyright", "check", "."),
)


def check() -> None:
    """Run the combined test and lint pipeline invoked by `uv run check`.

    'why': provide a single entry point that exits early on the first failure
    """

    _ensure_logging()
    for command in _COMMANDS:
        exit_code = _run_command(command)
        if exit_code != 0:
            raise SystemExit(exit_code)

def live_check() -> None:
    _run_command("python -m netrias_client.live_test.test")



def _ensure_logging() -> None:
    """Provision a minimalist logging configuration for script execution."""

    if not _LOGGER.handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")


def _run_command(command: Sequence[str]) -> int:
    """Run `command` and return its exit status without raising on failure."""

    _LOGGER.info("→ %s", " ".join(command))
    completed = subprocess.run(command, check=False)
    return completed.returncode

