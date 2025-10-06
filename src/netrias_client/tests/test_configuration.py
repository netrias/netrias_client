"""Validate configuration guardrails.

'why': ensure the client enforces explicit setup and rejects invalid inputs
"""
from __future__ import annotations

from pathlib import Path

import pytest

from netrias_client import NetriasClient
from netrias_client._models import LogLevel
from netrias_client._errors import ClientConfigurationError


def test_harmonize_requires_configuration(
    sample_csv_path: Path,
    sample_manifest_path: Path,
    output_directory: Path,
) -> None:
    """Ensure harmonize fails fast when configure has not been called."""

    client = NetriasClient()

    with pytest.raises(ClientConfigurationError) as exc:
        _ = client.harmonize(
            source_path=sample_csv_path,
            manifest=sample_manifest_path,
            output_path=output_directory,
        )

    assert "call configure" in str(exc.value)


def test_configure_rejects_blank_api_key() -> None:
    """Blank API key is rejected with a descriptive message."""

    client = NetriasClient()
    with pytest.raises(ClientConfigurationError) as exc:
        client.configure(api_key="")

    assert "api_key must be a non-empty string" in str(exc.value)


def test_configure_rejects_custom_api_url() -> None:
    """api_url overrides are unsupported and raise a TypeError."""

    client = NetriasClient()
    with pytest.raises(TypeError):
        getattr(client, "configure")(api_key="token", **{"api_url": "https://example.invalid"})


def test_configure_rejects_unsupported_log_level() -> None:
    """Unsupported log levels are rejected immediately."""

    client = NetriasClient()
    with pytest.raises(ClientConfigurationError) as exc:
        client.configure(api_key="token", log_level="VERBOSE")

    assert "unsupported log_level" in str(exc.value)


def test_configure_rejects_non_positive_timeout() -> None:
    """Non-positive timeouts trigger configuration errors."""

    client = NetriasClient()
    with pytest.raises(ClientConfigurationError) as exc:
        client.configure(api_key="token", timeout=0.0)

    assert "timeout must be positive" in str(exc.value)


def test_configure_rejects_invalid_confidence_threshold() -> None:
    """Confidence threshold must fall within the inclusive [0.0, 1.0] range."""

    client = NetriasClient()
    with pytest.raises(ClientConfigurationError) as exc:
        client.configure(api_key="token", confidence_threshold=1.5)

    assert "confidence_threshold" in str(exc.value)


def test_configure_accepts_log_level_enum() -> None:
    """Log level accepts enum values and persists them on settings."""

    client = NetriasClient()
    client.configure(api_key="token", log_level=LogLevel.DEBUG)
    assert client.settings.log_level is LogLevel.DEBUG


def test_configure_creates_log_directory(tmp_path: Path) -> None:
    """Providing a log directory ensures it is created and stored on settings."""

    client = NetriasClient()
    target = tmp_path / "logs"

    client.configure(api_key="token", log_directory=target)

    assert target.exists()
    assert client.settings.log_directory == target
