"""Validate configuration guardrails.

'why': ensure the client enforces explicit setup and rejects invalid inputs
"""
from __future__ import annotations

from pathlib import Path

import pytest

from netrias_client import configure, harmonize
from netrias_client._errors import ClientConfigurationError


def test_harmonize_requires_configuration(sample_csv_path: Path, sample_manifest_path: Path, output_directory: Path) -> None:
    """Ensure harmonize fails fast when configure has not been called.

    'why': protect against accidental usage without credentials
    """

    # Given harmonize is invoked without prior configure call
    # When harmonize executes
    with pytest.raises(ClientConfigurationError) as exc:
        _ = harmonize(source_path=sample_csv_path, manifest_path=sample_manifest_path, output_path=output_directory)

    # Then the error instructs the caller to perform configuration first
    assert "call configure" in str(exc.value)


def test_configure_rejects_blank_api_key() -> None:
    """Blank API key is rejected with a descriptive message."""

    with pytest.raises(ClientConfigurationError) as exc:
        configure(api_key="", api_url="https://api.netrias.test")

    assert "api_key must be a non-empty string" in str(exc.value)


def test_configure_rejects_blank_api_url() -> None:
    """Blank API URL is rejected with a descriptive message."""

    with pytest.raises(ClientConfigurationError) as exc:
        configure(api_key="token", api_url="")

    assert "api_url must be a non-empty string" in str(exc.value)


def test_configure_rejects_unsupported_log_level() -> None:
    """Unsupported log levels are rejected immediately."""

    with pytest.raises(ClientConfigurationError) as exc:
        configure(api_key="token", api_url="https://api.netrias.test", log_level="VERBOSE")

    assert "unsupported log_level" in str(exc.value)


def test_configure_rejects_non_positive_timeout() -> None:
    """Non-positive timeouts trigger configuration errors."""

    with pytest.raises(ClientConfigurationError) as exc:
        configure(api_key="token", api_url="https://api.netrias.test", timeout=0.0)

    assert "timeout must be positive" in str(exc.value)


def test_configure_rejects_invalid_confidence_threshold() -> None:
    """Confidence threshold must fall within the inclusive [0.0, 1.0] range."""

    with pytest.raises(ClientConfigurationError) as exc:
        configure(
            api_key="token",
            api_url="https://api.netrias.test",
            confidence_threshold=1.5,
        )

    assert "confidence_threshold" in str(exc.value)
