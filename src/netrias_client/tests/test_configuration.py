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
        discovery_url="https://staging.example.com/api/v2",
        harmonization_url="https://staging.example.com/api/v3",
        data_model_store_url="https://staging.example.com/dms",
    )

    # Then the URLs are updated
    assert client.settings.discovery_url == "https://staging.example.com/api/v2"
    assert client.settings.harmonization_url == "https://staging.example.com/api/v3"
    assert client.settings.data_model_store_endpoints is not None
    assert client.settings.data_model_store_endpoints.base_url == "https://staging.example.com/dms"


def test_configure_preserves_existing_positional_url_parameters() -> None:
    """Adding base_url does not change the meaning of existing positional calls."""

    # Given: the positional service values supported before base_url was added
    client = NetriasClient(api_key="token")
    discovery_url = "https://discovery.example/api/v2"
    harmonization_url = "https://harmonization.example/api/v3"
    data_model_store_url = "https://models.example/api"

    # When: an existing caller uses those positional parameters
    client.configure(
        None,
        None,
        None,
        None,
        None,
        discovery_url,
        harmonization_url,
        data_model_store_url,
    )

    # Then: every value keeps its original meaning
    assert client.settings.discovery_url == discovery_url
    assert client.settings.harmonization_url == harmonization_url
    assert client.settings.data_model_store_endpoints is not None
    assert client.settings.data_model_store_endpoints.base_url == data_model_store_url


@pytest.mark.parametrize(
    ("base_url", "expected_root"),
    [
        ("https://data-chord.example", "https://data-chord.example/api/v1"),
        ("https://data-chord.example/", "https://data-chord.example/api/v1"),
    ],
)
def test_configure_base_url_derives_versioned_service_roots(
    base_url: str,
    expected_root: str,
) -> None:
    """A DataChord deployment root owns both versioned programmatic APIs."""

    # Given: a client that still uses its initial service endpoints
    client = NetriasClient(api_key="token")

    # When: the caller selects one DataChord deployment
    client.configure(base_url=base_url)

    # Then: both service roots use the same versioned DataChord API
    assert client.settings.discovery_url == expected_root
    assert client.settings.harmonization_url == expected_root


def test_configure_service_overrides_win_over_same_call_base_url() -> None:
    """One service can use another versioned API root when needed."""

    # Given: a client and two explicit service roots
    client = NetriasClient(api_key="token")
    discovery_override = "https://discovery.example/api/v2"
    harmonization_override = "https://harmonization.example/api/v3"

    # When: the caller supplies a deployment root and both service overrides
    client.configure(
        base_url="https://data-chord.example",
        discovery_url=discovery_override,
        harmonization_url=harmonization_override,
    )

    # Then: each explicit service root wins
    assert client.settings.discovery_url == discovery_override
    assert client.settings.harmonization_url == harmonization_override


def test_configure_later_base_url_replaces_earlier_service_overrides() -> None:
    """Selecting a deployment replaces both previously selected services."""

    # Given: a client with two earlier service overrides
    client = NetriasClient(api_key="token")
    client.configure(
        discovery_url="https://old-discovery.example/api/v2",
        harmonization_url="https://old-harmonization.example/api/v2",
    )

    # When: the caller selects one DataChord deployment
    client.configure(base_url="https://new-data-chord.example")

    # Then: both service roots move to that deployment
    expected_root = "https://new-data-chord.example/api/v1"
    assert client.settings.discovery_url == expected_root
    assert client.settings.harmonization_url == expected_root


def test_configure_later_service_override_changes_only_that_service() -> None:
    """An independent service override preserves the other derived root."""

    # Given: both services use one DataChord deployment
    client = NetriasClient(api_key="token")
    client.configure(base_url="https://data-chord.example")

    # When: the caller moves only harmonization
    client.configure(harmonization_url="https://harmonization.example/api/v2")

    # Then: discovery stays on DataChord and harmonization uses the override
    assert client.settings.discovery_url == "https://data-chord.example/api/v1"
    assert client.settings.harmonization_url == "https://harmonization.example/api/v2"


def test_configure_unrelated_update_preserves_derived_service_roots() -> None:
    """Runtime tuning does not change the selected deployment."""

    # Given: both services use one DataChord deployment
    client = NetriasClient(api_key="token")
    client.configure(base_url="https://data-chord.example")

    # When: the caller changes only the timeout
    client.configure(timeout=30)

    # Then: both service roots remain unchanged
    assert client.settings.discovery_url == "https://data-chord.example/api/v1"
    assert client.settings.harmonization_url == "https://data-chord.example/api/v1"


@pytest.mark.parametrize(
    "base_url",
    [
        "",
        "data-chord.example",
        "https://",
        "https://data-chord.example/api",
        "https://data-chord.example?target=staging",
        "https://data-chord.example#staging",
        "https://user@data-chord.example",
        "https://[bad",
        "https://exa\nmple.com",
        "https://exa\rmple.com",
    ],
)
def test_configure_rejects_invalid_data_chord_base_url(base_url: str) -> None:
    """A base URL identifies a deployment, not an API path or credential."""

    # Given: a client and a value that is not a DataChord deployment root
    client = NetriasClient(api_key="token")

    # When: the caller tries to use that value as the base URL
    with pytest.raises(ClientConfigurationError, match="base_url"):
        client.configure(base_url=base_url)

    # Then: the existing service roots remain unchanged
    assert client.settings.discovery_url == DISCOVERY_BASE_URL
    assert client.settings.harmonization_url == HARMONIZATION_BASE_URL


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
