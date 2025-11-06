"""Coordinate stateful access to discovery and harmonization APIs.

'why': provide a single, inspectable entry point that captures configuration once
and exposes typed discovery and harmonization helpers (sync/async) for consumers
"""
from __future__ import annotations

import logging
import threading
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

from importlib.metadata import PackageNotFoundError, version as package_version

from ._core import harmonize as _harmonize
from ._core import harmonize_async as _harmonize_async
from ._discovery import (
    discover_cde_mapping as _discover_cde_mapping,
    discover_mapping as _discover_mapping,
    discover_mapping_async as _discover_mapping_async,
    discover_mapping_from_csv_async as _discover_mapping_from_csv_async,
)
from ._config import build_settings
from ._errors import ClientConfigurationError
from ._logging import configure_logger
from ._models import HarmonizationResult, LogLevel, Settings


ManifestPayload = dict[str, dict[str, dict[str, object]]]


class NetriasClient:
    """Expose discovery and harmonization workflows behind instance state.

    A `NetriasClient` manages configuration snapshots (API key, URLs, thresholds,
    bypass preferences) and threads them through every outbound call. Consumers
    typically instantiate a client, call :meth:`configure`, and then interact via
    the discovery/harmonization methods below.
    """

    def __init__(self) -> None:
        """Initialise an empty client awaiting configuration."""

        self._lock: threading.Lock = threading.Lock()
        self._settings: Settings | None = None
        self._logger_name: str = f"netrias_client.{uuid4().hex}"
        self._logger: logging.Logger | None = None

    def configure(
        self,
        api_key: str,
        timeout: float | None = None,
        log_level: LogLevel | str | None = None,
        confidence_threshold: float | None = None,
        discovery_use_gateway_bypass: bool | None = None,
        log_directory: Path | str | None = None,
    ) -> None:
        """Validate inputs and persist a new immutable settings snapshot.

        Parameters
        ----------
        api_key:
            Netrias API bearer token used for authentication.
        timeout:
            Overall request timeout in seconds (defaults to six hours).
        log_level:
            Desired logging verbosity as a :class:`~netrias_client._models.LogLevel`
            (string aliases are also accepted for convenience).
        confidence_threshold:
            Minimum confidence score required for discovery recommendations.
        discovery_use_gateway_bypass:
            When ``True`` (default) calls the temporary Lambda bypass instead of
            API Gateway.
        log_directory:
            Optional directory where this client's log files should be written.
            When omitted, logging remains stream-only.

        Calling this method multiple times replaces the active snapshot and
        reconfigures the package logger.
        """

        settings = build_settings(
            api_key=api_key,
            timeout=timeout,
            log_level=log_level,
            confidence_threshold=confidence_threshold,
            discovery_use_gateway_bypass=discovery_use_gateway_bypass,
            log_directory=log_directory,
        )
        logger = configure_logger(
            self._logger_name,
            settings.log_level,
            settings.log_directory,
        )
        with self._lock:
            self._settings = settings
            self._logger = logger
        _emit_configuration_summary(settings=settings, logger=logger)

    @property
    def settings(self) -> Settings:
        """Return a defensive copy of the current settings.

        'why': aid observability without exposing internal state for mutation
        """

        return self._snapshot_settings()

    def discover_mapping(
        self,
        target_schema: str,
        column_samples: Mapping[str, Sequence[object]],
    ) -> ManifestPayload:
        """Perform synchronous mapping discovery for the provided schema."""

        settings = self._snapshot_settings()

        return _discover_mapping(
            settings=settings,
            target_schema=target_schema,
            column_samples=column_samples,
            logger=self._require_logger(),
        )

    async def discover_mapping_async(
        self,
        target_schema: str,
        column_samples: Mapping[str, Sequence[object]],
    ) -> ManifestPayload:
        """Async variant of :meth:`discover_mapping` with identical semantics."""

        settings = self._snapshot_settings()

        return await _discover_mapping_async(
            settings=settings,
            target_schema=target_schema,
            column_samples=column_samples,
            logger=self._require_logger(),
        )

    def discover_mapping_from_csv(
        self,
        source_csv: Path,
        target_schema: str,
        sample_limit: int = 25,
    ) -> ManifestPayload:
        """Derive column samples from a CSV file then perform mapping discovery."""

        settings = self._snapshot_settings()

        return _discover_cde_mapping(
            settings=settings,
            source_csv=source_csv,
            target_schema=target_schema,
            sample_limit=sample_limit,
            logger=self._require_logger(),
        )

    def discover_cde_mapping(
        self,
        source_csv: Path,
        target_schema: str,
        sample_limit: int = 25,
    ) -> ManifestPayload:
        """Compatibility alias for :meth:`discover_mapping_from_csv`."""

        return self.discover_mapping_from_csv(
            source_csv=source_csv,
            target_schema=target_schema,
            sample_limit=sample_limit,
        )

    async def discover_mapping_from_csv_async(
        self,
        source_csv: Path,
        target_schema: str,
        sample_limit: int = 25,
    ) -> ManifestPayload:
        """Async variant of :meth:`discover_mapping_from_csv`."""

        settings = self._snapshot_settings()

        return await _discover_mapping_from_csv_async(
            settings=settings,
            source_csv=source_csv,
            target_schema=target_schema,
            sample_limit=sample_limit,
            logger=self._require_logger(),
        )

    async def discover_cde_mapping_async(
        self,
        source_csv: Path,
        target_schema: str,
        sample_limit: int = 25,
    ) -> ManifestPayload:
        """Async compatibility alias for :meth:`discover_mapping_from_csv_async`."""

        return await self.discover_mapping_from_csv_async(
            source_csv=source_csv,
            target_schema=target_schema,
            sample_limit=sample_limit,
        )

    def harmonize(
        self,
        source_path: Path,
        manifest: Path | Mapping[str, object],
        output_path: Path | None = None,
        manifest_output_path: Path | None = None,
    ) -> HarmonizationResult:
        """Execute the harmonization workflow synchronously and block.

        The method accepts either a manifest mapping or a JSON file path and
        writes the harmonized CSV to the resolved output location (which may be
        auto-versioned). A :class:`HarmonizationResult` is always returned even on
        failure, allowing callers to inspect status and description.
        """

        settings = self._snapshot_settings()

        return _harmonize(
            settings=settings,
            source_path=source_path,
            manifest=manifest,
            output_path=output_path,
            manifest_output_path=manifest_output_path,
            logger=self._require_logger(),
        )

    async def harmonize_async(
        self,
        source_path: Path,
        manifest: Path | Mapping[str, object],
        output_path: Path | None = None,
        manifest_output_path: Path | None = None,
    ) -> HarmonizationResult:
        """Async counterpart to :meth:`harmonize` with identical semantics."""

        settings = self._snapshot_settings()

        return await _harmonize_async(
            settings=settings,
            source_path=source_path,
            manifest=manifest,
            output_path=output_path,
            manifest_output_path=manifest_output_path,
            logger=self._require_logger(),
        )

    def _snapshot_settings(self) -> Settings:
        """Return a copy of the current settings or raise if not configured."""

        with self._lock:
            if self._settings is None:
                raise ClientConfigurationError(
                    "client not configured; call configure(api_key=...) before use"
                )
            return replace(self._settings)

    def _require_logger(self) -> logging.Logger:
        if self._logger is None:
            raise ClientConfigurationError(
                "client not configured; call configure(api_key=...) before use"
            )
        return self._logger


def _emit_configuration_summary(*, settings: Settings, logger: logging.Logger) -> None:
    """Log a sanitized summary of the active client configuration."""

    summary = {
        "package_version": _resolve_package_version(),
        "discovery_url": settings.discovery_url,
        "harmonization_url": settings.harmonization_url,
        "timeout": settings.timeout,
        "log_level": settings.log_level.value,
        "confidence_threshold": settings.confidence_threshold,
        "discovery_use_gateway_bypass": settings.discovery_use_gateway_bypass,
        "log_directory": str(settings.log_directory) if settings.log_directory else None,
    }
    formatted = ", ".join(f"{key}={value}" for key, value in summary.items() if value is not None)
    logger.info("client configured: %s", formatted)


def _resolve_package_version() -> str:
    """Return the installed package version or a fallback identifier."""

    try:
        return package_version("netrias_client")
    except PackageNotFoundError:
        return "0.0.0-dev"
