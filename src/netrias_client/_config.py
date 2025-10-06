"""Manage runtime client configuration.

'why': centralize settings creation and validation for NetriasClient
"""
from __future__ import annotations

from pathlib import Path

from ._errors import ClientConfigurationError
from ._models import LogLevel, Settings


DISCOVERY_BASE_URL = "https://api.netriasbdf.cloud"
HARMONIZATION_BASE_URL = "https://tbdxz7nffi.execute-api.us-east-2.amazonaws.com"
# TODO: remove once API Gateway latency constraints are resolved.
BYPASS_FUNCTION = "cde-recommendation"
BYPASS_ALIAS = "prod"
BYPASS_REGION = "us-east-2"


def build_settings(
    api_key: str,
    timeout: float | None = None,
    log_level: LogLevel | str | None = None,
    confidence_threshold: float | None = None,
    discovery_use_gateway_bypass: bool | None = None,
    log_directory: Path | str | None = None,
) -> Settings:
    """Return a validated Settings snapshot for the provided configuration."""

    key = (api_key or "").strip()
    if not key:
        raise ClientConfigurationError("api_key must be a non-empty string; call configure(api_key=...) before use")

    level = _normalized_level(log_level)
    timeout_value = _validated_timeout(timeout)
    threshold = _validated_confidence_threshold(confidence_threshold)
    bypass_enabled = _normalized_bool(discovery_use_gateway_bypass, default=True)
    directory = _validated_log_directory(log_directory)

    return Settings(
        api_key=key,
        discovery_url=DISCOVERY_BASE_URL,
        harmonization_url=HARMONIZATION_BASE_URL,
        timeout=timeout_value,
        log_level=level,
        confidence_threshold=threshold,
        discovery_use_gateway_bypass=bypass_enabled,
        log_directory=directory,
    )


def _normalized_level(level: LogLevel | str | None) -> LogLevel:
    if level is None:
        return LogLevel.INFO
    if isinstance(level, LogLevel):
        return level
    upper = level.upper()
    try:
        return LogLevel[upper]
    except KeyError as exc:
        raise ClientConfigurationError(f"unsupported log_level: {level}") from exc


def _validated_timeout(timeout: float | None) -> float:
    if timeout is None:
        return 21600.0  # default to 6 hours to accommodate long-running jobs
    if timeout <= 0:
        raise ClientConfigurationError("timeout must be positive when provided")
    return float(timeout)


def _validated_confidence_threshold(value: float | None) -> float:
    if value is None:
        return 0.8
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
