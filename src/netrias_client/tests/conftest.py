"""Provide shared pytest fixtures.

'why': centralize configuration resets and fixture paths across scenarios
"""
from __future__ import annotations

from pathlib import Path

from typing import cast

import pytest

from netrias_client import NetriasClient


@pytest.fixture
def configured_client() -> NetriasClient:
    """Return a client configured with deterministic credentials.

    'why': provide a ready-to-use setup for discovery and harmonization scenarios
    """

    client = NetriasClient()
    client.configure(
        api_key="test-api-key",
        timeout=5,
        log_level="INFO",
        discovery_use_gateway_bypass=False,
    )
    return client


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


@pytest.fixture
def sample_manifest_mapping(sample_manifest_path: Path) -> dict[str, object]:
    """Return the manifest payload as a Python mapping."""

    import json

    return cast(dict[str, object], json.loads(sample_manifest_path.read_text(encoding="utf-8")))
