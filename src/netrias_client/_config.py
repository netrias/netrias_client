"""Manage runtime client configuration.

'why': centralize settings creation and validation for NetriasClient
"""
from __future__ import annotations

from enum import Enum
from pathlib import Path

from ._errors import ClientConfigurationError
from ._models import DataModelStoreEndpoints, LogLevel, Settings


class Environment(str, Enum):
    """Target deployment environment for URL resolution."""

    PROD = "prod"
    STAGING = "staging"


_ENVIRONMENT_URLS: dict[Environment, dict[str, str]] = {
    Environment.PROD: {
        "discovery": "https://6lvkljeyod.execute-api.us-east-2.amazonaws.com/prod",
        "harmonization": "https://93y57g8ouk.execute-api.us-east-2.amazonaws.com/prod",
        "data_model_store": "https://85fnwlcuc2.execute-api.us-east-2.amazonaws.com/default",
    },
    Environment.STAGING: {
        "discovery": "https://s7a8ekw0yd.execute-api.us-east-2.amazonaws.com",
        "harmonization": "https://p9r0fv2o5g.execute-api.us-east-2.amazonaws.com/staging",
        "data_model_store": "https://85fnwlcuc2.execute-api.us-east-2.amazonaws.com/default",
    },
}

# Legacy constants preserved for backward compatibility
DISCOVERY_BASE_URL = "https://api.netriasbdf.cloud"
HARMONIZATION_BASE_URL = "https://93y57g8ouk.execute-api.us-east-2.amazonaws.com/prod"
DATA_MODEL_STORE_BASE_URL = "https://85fnwlcuc2.execute-api.us-east-2.amazonaws.com/default"
# TODO: remove once API Gateway latency constraints are resolved.
BYPASS_FUNCTION = "cde-recommend-prod"
BYPASS_ALIAS: str | None = None
BYPASS_REGION = "us-east-2"
# Async Step Functions API Gateway endpoint
ASYNC_API_URL = "https://6ueocdz4mc.execute-api.us-east-2.amazonaws.com/staging"
ASYNC_POLL_INTERVAL_SECONDS = 3.0


def build_settings(
    api_key: str,
    timeout: float | None = None,
    log_level: str | None = None,
    discovery_use_gateway_bypass: bool | None = None,
    discovery_use_async_api: bool | None = None,
    log_directory: Path | str | None = None,
    discovery_url: str | None = None,
    harmonization_url: str | None = None,
    data_model_store_url: str | None = None,
    environment: Environment | None = None,
) -> Settings:
    """Return a validated Settings snapshot for the provided configuration.

    'why': environment param selects a URL set; individual URL params still override
    """

    key = (api_key or "").strip()
    if not key:
        raise ClientConfigurationError("api_key must be a non-empty string; call configure(api_key=...) before use")

    level = _normalized_level(log_level)
    timeout_value = _validated_timeout(timeout)
    bypass_enabled = _normalized_bool(discovery_use_gateway_bypass, default=False)
    async_api_enabled = _normalized_bool(discovery_use_async_api, default=False)
    directory = _validated_log_directory(log_directory)

    env_urls = _ENVIRONMENT_URLS.get(environment) if environment else None
    resolved_discovery_url = discovery_url or (env_urls or {}).get("discovery", DISCOVERY_BASE_URL)
    resolved_harmonization_url = harmonization_url or (env_urls or {}).get("harmonization", HARMONIZATION_BASE_URL)
    resolved_dms_url = data_model_store_url or (env_urls or {}).get("data_model_store", DATA_MODEL_STORE_BASE_URL)

    data_model_store_endpoints = DataModelStoreEndpoints(
        base_url=resolved_dms_url,
    )

    return Settings(
        api_key=key,
        discovery_url=resolved_discovery_url,
        harmonization_url=resolved_harmonization_url,
        timeout=timeout_value,
        log_level=level,
        discovery_use_gateway_bypass=bypass_enabled,
        log_directory=directory,
        data_model_store_endpoints=data_model_store_endpoints,
        discovery_use_async_api=async_api_enabled,
    )


def _normalized_level(level: str | None) -> LogLevel:
    if level is None:
        return LogLevel.INFO
    upper = level.upper()
    try:
        return LogLevel[upper]
    except KeyError as exc:
        raise ClientConfigurationError(f"unsupported log_level: {level}") from exc


def _validated_timeout(timeout: float | None) -> float:
    if timeout is None:
        return 1200.0  # default to 20 minutes
    if timeout <= 0:
        raise ClientConfigurationError("timeout must be positive when provided")
    return float(timeout)


def validated_confidence_threshold(value: float | None, default: float = 0.8) -> float:
    """Validate and return a confidence threshold value.

    'why': discovery methods need per-call threshold validation
    """

    if value is None:
        return default
    if not (0.0 <= value <= 1.0):
        raise ClientConfigurationError("confidence_threshold must be between 0.0 and 1.0")
    return float(value)


def _normalized_bool(value: bool | None, default: bool = False) -> bool:
    if value is None:
        return default
    return bool(value)


def _validated_log_directory(value: Path | str | None) -> Path | None:
    if value is None:
        return None
    directory = Path(value)
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ClientConfigurationError(f"unable to create log directory {directory}: {exc}") from exc
    return directory
