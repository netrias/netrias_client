"""Shared values for live API smoke tests."""

from __future__ import annotations

from pathlib import Path
from typing import Final

LIVE_TEST_DIR: Final[Path] = Path(__file__).resolve().parent
ENV_PATH: Final[Path] = LIVE_TEST_DIR / ".env"
DATA_DIR: Final[Path] = LIVE_TEST_DIR / "data"
CSV_PATH: Final[Path] = DATA_DIR / "primary_diagnosis_1.csv"

MODEL_KEY: Final[str] = "gc"
DISCOVERY_EXTERNAL_VERSION_NUMBER: Final[str] = "11.0.4"
EXTERNAL_VERSION_NUMBER: Final[str] = "11.0.4"
CDE_KEY: Final[str] = "sex"
