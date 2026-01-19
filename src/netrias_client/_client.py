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

from ._core import harmonize as _harmonize
from ._core import harmonize_async as _harmonize_async
from ._data_model_store import (
    get_pv_set as _get_pv_set,
    get_pv_set_async as _get_pv_set_async,
    list_cdes as _list_cdes,
    list_cdes_async as _list_cdes_async,
    list_data_models as _list_data_models,
    list_data_models_async as _list_data_models_async,
    list_pvs as _list_pvs,
    list_pvs_async as _list_pvs_async,
)
from ._discovery import (
    discover_cde_mapping as _discover_cde_mapping,
    discover_mapping_from_csv_async as _discover_mapping_from_csv_async,
)
from ._config import build_settings
from ._logging import configure_logger, LOGGER_NAMESPACE
from ._models import CDE, DataModel, HarmonizationResult, ManifestPayload, OperationContext, PermissibleValue, Settings


class NetriasClient:
    """Expose discovery and harmonization workflows behind instance state.

    A `NetriasClient` manages configuration snapshots (API key, URLs, thresholds,
    bypass preferences) and threads them through every outbound call. Consumers
    instantiate a client with an API key and optionally call :meth:`configure`
    to adjust non-default settings.
    """

    def __init__(self, api_key: str) -> None:
        """Initialize the client with an API key and default settings.

        Parameters
        ----------
        api_key:
            Netrias API bearer token used for authentication.
        """

        self._lock: threading.Lock = threading.Lock()
        self._logger_name: str = f"{LOGGER_NAMESPACE}.instance.{uuid4().hex[:8]}"

        settings = build_settings(api_key=api_key)
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
        """Update settings with new values.

        Parameters
        ----------
        timeout:
            Overall request timeout in seconds (default: 20 minutes).
        log_level:
            Logging verbosity: ``"CRITICAL"``, ``"ERROR"``, ``"WARNING"``,
            ``"INFO"`` (default), or ``"DEBUG"``.
        discovery_use_gateway_bypass:
            When ``True`` (default) calls the temporary Lambda bypass instead of
            API Gateway.
        discovery_use_async_api:
            When ``True`` uses the async API Gateway + Step Functions pattern for
            CDE discovery. Takes precedence over gateway bypass. Default: ``False``.
        log_directory:
            Optional directory where this client's log files should be written.
            When omitted, logging remains stream-only.
        discovery_url:
            Override the discovery API base URL (for testing/staging).
        harmonization_url:
            Override the harmonization API base URL (for testing/staging).
        data_model_store_url:
            Override the Data Model Store API base URL (for testing/staging).

        Calling this method replaces the active settings snapshot and
        reconfigures the package logger. Unspecified parameters preserve
        their current values rather than resetting to defaults.
        """

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

    def discover_mapping_from_csv(
        self,
        source_csv: Path,
        target_schema: str,
        target_version: str = "latest",
        sample_limit: int = 25,
        top_k: int = 3,
        confidence_threshold: float | None = None,
    ) -> ManifestPayload:
        """Derive column samples from a CSV file then perform mapping discovery.

        Parameters
        ----------
        source_csv:
            Path to the CSV file containing source data.
        target_schema:
            Target schema key (e.g., 'ccdi', 'sage_rnaseq_template').
        target_version:
            Schema version (default: 'latest').
        sample_limit:
            Maximum number of rows to sample from the CSV.
        top_k:
            Number of top recommendations per column (default: 3).
        confidence_threshold:
            Minimum confidence score (0–1) for keeping recommendations.
            Default: 0.8. Lower values capture more tentative matches.
        """
        ctx = self._snapshot_context()

        return _discover_cde_mapping(
            settings=ctx.settings,
            source_csv=source_csv,
            target_schema=target_schema,
            target_version=target_version,
            sample_limit=sample_limit,
            logger=ctx.logger,
            top_k=top_k,
            confidence_threshold=confidence_threshold,
        )

    async def discover_mapping_from_csv_async(
        self,
        source_csv: Path,
        target_schema: str,
        target_version: str = "latest",
        sample_limit: int = 25,
        top_k: int = 3,
        confidence_threshold: float | None = None,
    ) -> ManifestPayload:
        """Async variant of :meth:`discover_mapping_from_csv`.

        Parameters
        ----------
        confidence_threshold:
            Minimum confidence score (0–1) for keeping recommendations.
            Default: 0.8. Lower values capture more tentative matches.
        """
        ctx = self._snapshot_context()

        return await _discover_mapping_from_csv_async(
            settings=ctx.settings,
            source_csv=source_csv,
            target_schema=target_schema,
            target_version=target_version,
            sample_limit=sample_limit,
            logger=ctx.logger,
            top_k=top_k,
            confidence_threshold=confidence_threshold,
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

        ctx = self._snapshot_context()

        return _harmonize(
            settings=ctx.settings,
            source_path=source_path,
            manifest=manifest,
            output_path=output_path,
            manifest_output_path=manifest_output_path,
            logger=ctx.logger,
        )

    async def harmonize_async(
        self,
        source_path: Path,
        manifest: Path | Mapping[str, object],
        output_path: Path | None = None,
        manifest_output_path: Path | None = None,
    ) -> HarmonizationResult:
        """Async counterpart to :meth:`harmonize` with identical semantics."""

        ctx = self._snapshot_context()

        return await _harmonize_async(
            settings=ctx.settings,
            source_path=source_path,
            manifest=manifest,
            output_path=output_path,
            manifest_output_path=manifest_output_path,
            logger=ctx.logger,
        )

    # ---- Data Model Store methods ----

    def list_data_models(
        self,
        query: str | None = None,
        include_versions: bool = False,
        include_counts: bool = False,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[DataModel, ...]:
        """Fetch data models from the Data Model Store.

        Parameters
        ----------
        query:
            Substring search on model key or name.
        include_versions:
            Include version metadata per model.
        include_counts:
            Include CDE/PV counts per version.
        limit:
            Maximum number of results to return.
        offset:
            Number of results to skip.
        """

        settings = self._snapshot_settings()

        return _list_data_models(
            settings=settings,
            query=query,
            include_versions=include_versions,
            include_counts=include_counts,
            limit=limit,
            offset=offset,
        )

    async def list_data_models_async(
        self,
        query: str | None = None,
        include_versions: bool = False,
        include_counts: bool = False,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[DataModel, ...]:
        """Async variant of :meth:`list_data_models`."""

        settings = self._snapshot_settings()

        return await _list_data_models_async(
            settings=settings,
            query=query,
            include_versions=include_versions,
            include_counts=include_counts,
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
        """Fetch CDEs for a data model version from the Data Model Store.

        Parameters
        ----------
        model_key:
            Data model key (e.g., 'ccdi').
        version:
            Version label (e.g., 'v1').
        include_description:
            Include CDE descriptions.
        query:
            Substring search on cde_key.
        limit:
            Maximum number of results to return.
        offset:
            Number of results to skip.
        """

        settings = self._snapshot_settings()

        return _list_cdes(
            settings=settings,
            model_key=model_key,
            version=version,
            include_description=include_description,
            query=query,
            limit=limit,
            offset=offset,
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
        """Async variant of :meth:`list_cdes`."""

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

    def list_pvs(
        self,
        model_key: str,
        version: str,
        cde_key: str,
        include_inactive: bool = False,
        query: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[PermissibleValue, ...]:
        """Fetch permissible values for a CDE from the Data Model Store.

        Parameters
        ----------
        model_key:
            Data model key (e.g., 'ccdi').
        version:
            Version label (e.g., 'v1').
        cde_key:
            CDE key (e.g., 'sex_at_birth').
        include_inactive:
            Include inactive permissible values.
        query:
            Substring search on PV value.
        limit:
            Maximum number of results to return.
        offset:
            Number of results to skip.
        """

        settings = self._snapshot_settings()

        return _list_pvs(
            settings=settings,
            model_key=model_key,
            version=version,
            cde_key=cde_key,
            include_inactive=include_inactive,
            query=query,
            limit=limit,
            offset=offset,
        )

    async def list_pvs_async(
        self,
        model_key: str,
        version: str,
        cde_key: str,
        include_inactive: bool = False,
        query: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[PermissibleValue, ...]:
        """Async variant of :meth:`list_pvs`."""

        settings = self._snapshot_settings()

        return await _list_pvs_async(
            settings=settings,
            model_key=model_key,
            version=version,
            cde_key=cde_key,
            include_inactive=include_inactive,
            query=query,
            limit=limit,
            offset=offset,
        )

    def get_pv_set(
        self,
        model_key: str,
        version: str,
        cde_key: str,
        include_inactive: bool = False,
    ) -> frozenset[str]:
        """Return all permissible values for a CDE as a set for O(1) membership testing.

        'why': validation use case requires efficient lookup; auto-paginates all results

        Parameters
        ----------
        model_key:
            Data model key (e.g., 'ccdi').
        version:
            Version label (e.g., 'v1').
        cde_key:
            CDE key (e.g., 'sex_at_birth').
        include_inactive:
            Include inactive permissible values.
        """

        settings = self._snapshot_settings()

        return _get_pv_set(
            settings=settings,
            model_key=model_key,
            version=version,
            cde_key=cde_key,
            include_inactive=include_inactive,
        )

    async def get_pv_set_async(
        self,
        model_key: str,
        version: str,
        cde_key: str,
        include_inactive: bool = False,
    ) -> frozenset[str]:
        """Async variant of :meth:`get_pv_set`."""

        settings = self._snapshot_settings()

        return await _get_pv_set_async(
            settings=settings,
            model_key=model_key,
            version=version,
            cde_key=cde_key,
            include_inactive=include_inactive,
        )

    def validate_value(
        self,
        value: str,
        model_key: str,
        version: str,
        cde_key: str,
    ) -> bool:
        """Check if a value is in the permissible values for a CDE.

        'why': convenience wrapper for the common validation use case

        NOTE: This method makes a network call to fetch all PVs on each invocation.
        For validating multiple values against the same CDE, call :meth:`get_pv_set`
        once and reuse the returned frozenset for better performance.

        Parameters
        ----------
        value:
            The value to validate.
        model_key:
            Data model key (e.g., 'ccdi').
        version:
            Version label (e.g., 'v1').
        cde_key:
            CDE key (e.g., 'sex_at_birth').
        """

        pv_set = self.get_pv_set(model_key, version, cde_key)
        return value in pv_set

    async def validate_value_async(
        self,
        value: str,
        model_key: str,
        version: str,
        cde_key: str,
    ) -> bool:
        """Async variant of :meth:`validate_value`.

        NOTE: This method makes a network call to fetch all PVs on each invocation.
        For validating multiple values against the same CDE, call :meth:`get_pv_set_async`
        once and reuse the returned frozenset for better performance.
        """

        pv_set = await self.get_pv_set_async(model_key, version, cde_key)
        return value in pv_set

    def _snapshot_settings(self) -> Settings:
        """Return a copy of the current settings."""

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
