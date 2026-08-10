"""Validate configuration guardrails.

'why': ensure the client enforces explicit setup and rejects invalid inputs
"""
from __future__ import annotations

from pathlib import Path

import pytest

from netrias_client import Environment, NetriasClient
from netrias_client._config import (
    DATA_MODEL_STORE_BASE_URL,
    DISCOVERY_BASE_URL,
    HARMONIZATION_BASE_URL,
    _ENVIRONMENT_URLS,
    build_settings,
)
from netrias_client._errors import ClientConfigurationError


def test_init_rejects_blank_api_key() -> None:
    """Blank API key is rejected with a descriptive message."""

    with pytest.raises(ClientConfigurationError) as exc:
        _ = NetriasClient(api_key="")

    assert "api_key must be a non-empty string" in str(exc.value)


def test_configure_accepts_url_overrides() -> None:
    """URL overrides for testing/staging are supported."""

    # Given a client with default URLs
    client = NetriasClient(api_key="token")

    # When configuring with custom URLs
    client.configure(
        discovery_url="https://staging.example.com/discovery",
        harmonization_url="https://staging.example.com/harmonize",
        data_model_store_url="https://staging.example.com/dms",
    )

    # Then the URLs are updated
    assert client.settings.discovery_url == "https://staging.example.com/discovery"
    assert client.settings.harmonization_url == "https://staging.example.com/harmonize"
    assert client.settings.data_model_store_endpoints is not None
    assert client.settings.data_model_store_endpoints.base_url == "https://staging.example.com/dms"


def test_configure_rejects_unsupported_log_level() -> None:
    """Unsupported log levels are rejected immediately."""

    client = NetriasClient(api_key="token")
    with pytest.raises(ClientConfigurationError) as exc:
        client.configure(log_level="VERBOSE")

    assert "unsupported log_level" in str(exc.value)


def test_configure_rejects_non_positive_timeout() -> None:
    """Non-positive timeouts trigger configuration errors."""

    client = NetriasClient(api_key="token")
    with pytest.raises(ClientConfigurationError) as exc:
        client.configure(timeout=0.0)

    assert "timeout must be positive" in str(exc.value)


def test_configure_rejects_invalid_log_level_string() -> None:
    """Invalid log level strings are rejected with a descriptive message."""

    client = NetriasClient(api_key="token")
    with pytest.raises(ClientConfigurationError) as exc:
        client.configure(log_level="TRACE")

    assert "unsupported log_level" in str(exc.value)


def test_configure_accepts_log_level_string() -> None:
    """Log level accepts string values and persists them on settings."""

    client = NetriasClient(api_key="token")
    client.configure(log_level="DEBUG")
    assert client.settings.log_level.value == "DEBUG"


def test_configure_creates_log_directory(tmp_path: Path) -> None:
    """Providing a log directory ensures it is created and stored on settings."""

    client = NetriasClient(api_key="token")
    target = tmp_path / "logs"

    client.configure(log_directory=target)

    assert target.exists()
    assert client.settings.log_directory == target


def test_configure_preserves_api_key() -> None:
    """Calling configure() preserves the api_key from initialization."""

    client = NetriasClient(api_key="original-key")
    client.configure(timeout=100.0)

    assert client.settings.api_key == "original-key"
    assert client.settings.timeout == 100.0


def test_configure_preserves_unspecified_settings() -> None:
    """Calling configure() with partial parameters preserves other settings.

    'why': users expect incremental configuration, not full replacement
    """

    # Given a client with custom timeout and log_directory
    client = NetriasClient(api_key="token")
    client.configure(timeout=100.0, discovery_use_gateway_bypass=False)

    # When configuring only log_level
    client.configure(log_level="DEBUG")

    # Then timeout and discovery_use_gateway_bypass are preserved
    assert client.settings.timeout == 100.0
    assert client.settings.discovery_use_gateway_bypass is False
    assert client.settings.log_level.value == "DEBUG"


# ---------------------------------------------------------------------------
# TS-7: Environment URL resolution
# ---------------------------------------------------------------------------


def test_environment_prod_resolves_urls() -> None:
    """Environment.PROD selects prod URL defaults.

    Given: No individual URL overrides provided
    When: build_settings() is called with environment=PROD
    Then: URLs match the prod environment registry
    """
    # Given / When
    settings = build_settings(api_key="key", environment=Environment.PROD)

    # Then
    prod = _ENVIRONMENT_URLS[Environment.PROD]
    assert settings.harmonization_url == prod["harmonization"]
    assert settings.discovery_url == prod["discovery"]
    assert settings.data_model_store_endpoints is not None
    assert settings.data_model_store_endpoints.base_url == prod["data_model_store"]


def test_environment_staging_resolves_urls() -> None:
    """Environment.STAGING selects staging URL defaults."""
    settings = build_settings(api_key="key", environment=Environment.STAGING)

    staging = _ENVIRONMENT_URLS[Environment.STAGING]
    assert settings.harmonization_url == staging["harmonization"]
    assert settings.discovery_url == staging["discovery"]


def test_environment_url_overridden_by_explicit_param() -> None:
    """Individual URL params take precedence over environment defaults."""
    custom = "https://custom.example.com"
    settings = build_settings(
        api_key="key",
        environment=Environment.STAGING,
        harmonization_url=custom,
    )

    assert settings.harmonization_url == custom
    # Other URLs still come from staging
    staging = _ENVIRONMENT_URLS[Environment.STAGING]
    assert settings.discovery_url == staging["discovery"]


def test_client_init_with_environment() -> None:
    """NetriasClient accepts environment param and resolves URLs accordingly."""
    client = NetriasClient(api_key="key", environment=Environment.PROD)

    prod = _ENVIRONMENT_URLS[Environment.PROD]
    assert client.settings.harmonization_url == prod["harmonization"]


# ---------------------------------------------------------------------------
# TS-8: Backward compatibility — no environment param
# ---------------------------------------------------------------------------


def test_no_environment_preserves_defaults() -> None:
    """No environment parameter preserves current default URLs (backward compatible).

    Given: No environment parameter passed
    When: build_settings() is called
    Then: URLs match the legacy module-level constants (no behavior change)
    """
    settings = build_settings(api_key="key")

    assert settings.discovery_url == DISCOVERY_BASE_URL
    assert settings.harmonization_url == HARMONIZATION_BASE_URL
    assert settings.data_model_store_endpoints is not None
    assert settings.data_model_store_endpoints.base_url == DATA_MODEL_STORE_BASE_URL
