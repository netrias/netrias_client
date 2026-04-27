"""Coordinate stateful access to discovery and harmonization APIs.

'why': provide a single, inspectable entry point that captures configuration once
and exposes typed discovery and harmonization helpers (sync/async) for consumers
"""
from __future__ import annotations

import logging
import threading
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

from ._async_utils import run_sync
from ._config import Environment, build_settings
from ._core import harmonize_async as _harmonize_async
from ._data_model_store import (
    get_pv_set_async as _get_pv_set_async,
    list_cdes_async as _list_cdes_async,
    list_data_models_async as _list_data_models_async,
)
from ._discovery import discover_mapping_from_tabular_async as _discover_mapping_from_tabular_async
from ._logging import LOGGER_NAMESPACE, configure_logger
from ._models import (
    CDE,
    ColumnKeyedManifestPayload,
    DataModel,
    HarmonizationResult,
    OperationContext,
    Settings,
)


class NetriasClient:
    """Expose discovery and harmonization workflows behind instance state.

    A `NetriasClient` manages configuration snapshots (API key, URLs, thresholds,
    bypass preferences) and threads them through every outbound call. Consumers
    instantiate a client with an API key and optionally call :meth:`configure`
    to adjust non-default settings.
    """

    def __init__(self, api_key: str, environment: Environment | None = None) -> None:
        """Initialize the client with an API key and default settings."""

        self._lock: threading.Lock = threading.Lock()
        self._logger_name: str = f"{LOGGER_NAMESPACE}.instance.{uuid4().hex[:8]}"
        self._environment: Environment | None = environment

        settings = build_settings(api_key=api_key, environment=environment)
        logger = configure_logger(
            self._logger_name,
            settings.log_level,
            settings.log_directory,
        )
        self._settings: Settings = settings
        self._logger: logging.Logger = logger

    def configure(
        self,
        timeout: float | None = None,
        log_level: str | None = None,
        discovery_use_gateway_bypass: bool | None = None,
        discovery_use_async_api: bool | None = None,
        log_directory: Path | str | None = None,
        discovery_url: str | None = None,
        harmonization_url: str | None = None,
        data_model_store_url: str | None = None,
    ) -> None:
        """Update settings; unspecified parameters preserve their current value."""

        current = self._settings
        current_dms_url = (
            current.data_model_store_endpoints.base_url
            if current.data_model_store_endpoints
            else None
        )
        bypass = (
            discovery_use_gateway_bypass
            if discovery_use_gateway_bypass is not None
            else current.discovery_use_gateway_bypass
        )
        settings = build_settings(
            api_key=current.api_key,
            timeout=timeout if timeout is not None else current.timeout,
            log_level=log_level if log_level is not None else current.log_level.value,
            discovery_use_gateway_bypass=bypass,
            discovery_use_async_api=(
                discovery_use_async_api if discovery_use_async_api is not None else current.discovery_use_async_api
            ),
            log_directory=log_directory if log_directory is not None else current.log_directory,
            discovery_url=discovery_url if discovery_url is not None else current.discovery_url,
            harmonization_url=harmonization_url if harmonization_url is not None else current.harmonization_url,
            data_model_store_url=data_model_store_url if data_model_store_url is not None else current_dms_url,
            environment=self._environment,
        )
        logger = configure_logger(
            self._logger_name,
            settings.log_level,
            settings.log_directory,
        )
        with self._lock:
            self._settings = settings
            self._logger = logger

    @property
    def settings(self) -> Settings:
        """Return a defensive copy of the current settings.

        'why': aid observability without exposing internal state for mutation
        """

        return self._snapshot_settings()

    async def discover_mapping_from_tabular_async(
        self,
        source_path: Path,
        target_schema: str,
        target_version: str = "latest",
        sample_limit: int = 25,
        top_k: int = 3,
        confidence_threshold: float | None = None,
    ) -> ColumnKeyedManifestPayload:
        """Derive column samples from a CSV/TSV file and return mappings by column key."""

        ctx = self._snapshot_context()
        return await _discover_mapping_from_tabular_async(
            settings=ctx.settings,
            source_path=source_path,
            target_schema=target_schema,
            target_version=target_version,
            sample_limit=sample_limit,
            logger=ctx.logger,
            top_k=top_k,
            confidence_threshold=confidence_threshold,
        )

    def discover_mapping_from_tabular(
        self,
        source_path: Path,
        target_schema: str,
        target_version: str = "latest",
        sample_limit: int = 25,
        top_k: int = 3,
        confidence_threshold: float | None = None,
    ) -> ColumnKeyedManifestPayload:
        """Sync delegate for :meth:`discover_mapping_from_tabular_async`."""

        return run_sync(
            self.discover_mapping_from_tabular_async(
                source_path=source_path,
                target_schema=target_schema,
                target_version=target_version,
                sample_limit=sample_limit,
                top_k=top_k,
                confidence_threshold=confidence_threshold,
            )
        )

    async def harmonize_async(
        self,
        source_path: Path,
        manifest: Path | Mapping[str, object],
        data_commons_key: str,
        output_path: Path | None = None,
        manifest_output_path: Path | None = None,
    ) -> HarmonizationResult:
        """Execute the harmonization workflow asynchronously."""

        ctx = self._snapshot_context()
        return await _harmonize_async(
            settings=ctx.settings,
            source_path=source_path,
            manifest=manifest,
            data_commons_key=data_commons_key,
            output_path=output_path,
            manifest_output_path=manifest_output_path,
            logger=ctx.logger,
        )

    def harmonize(
        self,
        source_path: Path,
        manifest: Path | Mapping[str, object],
        data_commons_key: str,
        output_path: Path | None = None,
        manifest_output_path: Path | None = None,
    ) -> HarmonizationResult:
        """Sync delegate for :meth:`harmonize_async`."""

        return run_sync(
            self.harmonize_async(
                source_path=source_path,
                manifest=manifest,
                data_commons_key=data_commons_key,
                output_path=output_path,
                manifest_output_path=manifest_output_path,
            )
        )

    async def list_data_models_async(
        self,
        query: str | None = None,
        include_versions: bool = False,
        include_counts: bool = False,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[DataModel, ...]:
        """Fetch data models from the Data Model Store."""

        settings = self._snapshot_settings()
        return await _list_data_models_async(
            settings=settings,
            query=query,
            include_versions=include_versions,
            include_counts=include_counts,
            limit=limit,
            offset=offset,
        )

    def list_data_models(
        self,
        query: str | None = None,
        include_versions: bool = False,
        include_counts: bool = False,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[DataModel, ...]:
        """Sync delegate for :meth:`list_data_models_async`."""

        return run_sync(
            self.list_data_models_async(
                query=query,
                include_versions=include_versions,
                include_counts=include_counts,
                limit=limit,
                offset=offset,
            )
        )

    async def list_cdes_async(
        self,
        model_key: str,
        version: str,
        include_description: bool = False,
        query: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[CDE, ...]:
        """Fetch CDEs for a data model version from the Data Model Store."""

        settings = self._snapshot_settings()
        return await _list_cdes_async(
            settings=settings,
            model_key=model_key,
            version=version,
            include_description=include_description,
            query=query,
            limit=limit,
            offset=offset,
        )

    def list_cdes(
        self,
        model_key: str,
        version: str,
        include_description: bool = False,
        query: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[CDE, ...]:
        """Sync delegate for :meth:`list_cdes_async`."""

        return run_sync(
            self.list_cdes_async(
                model_key=model_key,
                version=version,
                include_description=include_description,
                query=query,
                limit=limit,
                offset=offset,
            )
        )

    async def get_pv_set_async(
        self,
        model_key: str,
        version: str,
        cde_key: str,
        include_inactive: bool = False,
    ) -> frozenset[str]:
        """Return permissible values for a CDE as a frozenset for O(1) membership."""

        settings = self._snapshot_settings()
        return await _get_pv_set_async(
            settings=settings,
            model_key=model_key,
            version=version,
            cde_key=cde_key,
            include_inactive=include_inactive,
        )

    def get_pv_set(
        self,
        model_key: str,
        version: str,
        cde_key: str,
        include_inactive: bool = False,
    ) -> frozenset[str]:
        """Sync delegate for :meth:`get_pv_set_async`."""

        return run_sync(
            self.get_pv_set_async(
                model_key=model_key,
                version=version,
                cde_key=cde_key,
                include_inactive=include_inactive,
            )
        )

    def _snapshot_settings(self) -> Settings:
        with self._lock:
            return replace(self._settings)

    def _snapshot_context(self) -> OperationContext:
        """Return an atomic snapshot of settings and logger.

        'why': ensure settings and logger are consistent for thread-safe operations
        """

        with self._lock:
            return OperationContext(
                settings=replace(self._settings),
                logger=self._logger,
            )
