"""Manage runtime client configuration.

'why': centralize settings mutation and validation; require explicit configuration
"""
from __future__ import annotations

import threading
from dataclasses import replace

from ._errors import ClientConfigurationError
from ._logging import set_log_level
from ._models import Settings


_lock = threading.Lock()
_settings: Settings | None = None


def _normalized_level(level: str | None) -> str:
    if not level:
        return "INFO"
    upper = level.upper()
    if upper not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
        raise ClientConfigurationError(f"unsupported log_level: {level}")
    return upper


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


def _normalized_bool(value: bool | None) -> bool:
    if value is None:
        return False
    return bool(value)


def _normalized_identifier(value: str | None, *, default: str) -> str:
    candidate = (value or "").strip()
    return candidate if candidate else default


def _normalized_profile(value: str | None) -> str | None:
    if value is None:
        return None
    trimmed = value.strip()
    return trimmed or None


def configure(
    *,
    api_key: str,
    api_url: str,
    timeout: float | None = None,
    log_level: str | None = None,
    confidence_threshold: float | None = None,
    discovery_use_gateway_bypass: bool | None = None,
    discovery_bypass_function: str | None = None,
    discovery_bypass_alias: str | None = None,
    discovery_bypass_region: str | None = None,
    discovery_bypass_profile: str | None = None,
) -> None:
    """Configure the client for subsequent calls.

    'why': normalize client setup and logging without exposing transport internals
    """

    key = (api_key or "").strip()
    url = (api_url or "").strip()
    if not key:
        raise ClientConfigurationError("api_key must be a non-empty string; call configure(api_key=..., api_url=...) before use")
    if not url:
        raise ClientConfigurationError("api_url must be a non-empty string; call configure(api_key=..., api_url=...) before use")

    level = _normalized_level(log_level)
    to = _validated_timeout(timeout)
    threshold = _validated_confidence_threshold(confidence_threshold)
    bypass_enabled = _normalized_bool(discovery_use_gateway_bypass)
    bypass_function = _normalized_identifier(
        discovery_bypass_function,
        default="cde-recommendation",
    )
    bypass_alias = _normalized_identifier(discovery_bypass_alias, default="prod")
    bypass_region = _normalized_identifier(discovery_bypass_region, default="us-east-2")
    bypass_profile = _normalized_profile(discovery_bypass_profile)

    with _lock:
        global _settings
        _settings = Settings(
            api_key=key,
            api_url=url,
            timeout=to,
            log_level=level,
            confidence_threshold=threshold,
            discovery_use_gateway_bypass=bypass_enabled,
            discovery_bypass_function=bypass_function,
            discovery_bypass_alias=bypass_alias,
            discovery_bypass_region=bypass_region,
            discovery_bypass_profile=bypass_profile,
        )
        set_log_level(level)


def get_settings() -> Settings:
    """Return an immutable snapshot of current settings.

    Raises ClientConfigurationError if not configured.
    """

    if _settings is None:
        raise ClientConfigurationError("client not configured; call configure(api_key=..., api_url=...) before use")
    # return a shallow copy to discourage mutation
    return replace(_settings)
