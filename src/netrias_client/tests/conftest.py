"""Provide shared pytest fixtures.

'why': centralize configuration resets and fixture paths across scenarios
"""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from netrias_client import configure


@pytest.fixture(autouse=True)
def reset_configuration(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Clear global client settings before and after each test.

    'why': ensure scenarios remain isolated regardless of configure usage
    """

    monkeypatch.setattr("netrias_client._config._settings", None, raising=False)
    yield
    monkeypatch.setattr("netrias_client._config._settings", None, raising=False)


@pytest.fixture
def configured_client() -> Iterator[None]:
    """Configure the client with deterministic test credentials.

    'why': provide a ready-to-use setup for harmonization scenarios
    """

    configure(api_key="test-api-key", api_url="https://api.netrias.test", timeout=5, log_level="INFO")
    yield


@pytest.fixture
def sample_csv_path() -> Path:
    """Return the canonical CSV fixture path.

    'why': reuse a stable CSV input across validation and harmonization tests
    """

    return Path(__file__).parent / "fixtures" / "sample.csv"


@pytest.fixture
def sample_manifest_path() -> Path:
    """Return the canonical manifest JSON path.

    'why': keep manifest fixtures consistent across tests
    """

    return Path(__file__).parent / "fixtures" / "sample_manifest.json"


@pytest.fixture
def output_directory(tmp_path: Path) -> Path:
    """Provide an empty directory for harmonization outputs.

    'why': avoid polluting the repository root during tests
    """

    dest = tmp_path / "outputs"
    dest.mkdir(parents=True, exist_ok=True)
    return dest
