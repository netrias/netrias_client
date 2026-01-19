"""Query data models, CDEs, and permissible values from the Data Model Store.

'why': provide typed access to reference data for validation use cases
"""
from __future__ import annotations

import json
import logging
from collections.abc import Mapping

import httpx

from ._async_utils import run_sync
from ._errors import DataModelStoreError, NetriasAPIUnavailable
from ._http import fetch_cdes, fetch_data_models, fetch_pvs
from ._logging import LOGGER_NAMESPACE
from ._models import CDE, DataModel, DataModelStoreEndpoints, PermissibleValue, Settings

_logger = logging.getLogger(LOGGER_NAMESPACE)

MAX_PAGINATION_PAGES = 100
PV_PAGE_SIZE = 1000


def _require_endpoints(settings: Settings) -> DataModelStoreEndpoints:
    """Return endpoints or raise if not configured."""
    endpoints = settings.data_model_store_endpoints
    if endpoints is None:
        raise DataModelStoreError("data model store endpoints not configured")
    return endpoints


async def list_data_models_async(
    settings: Settings,
    query: str | None = None,
    include_versions: bool = False,
    include_counts: bool = False,
    limit: int | None = None,
    offset: int = 0,
) -> tuple[DataModel, ...]:
    """Fetch data models from the Data Model Store.

    'why': expose available data commons for schema selection
    """

    endpoints = _require_endpoints(settings)

    try:
        response = await fetch_data_models(
            base_url=endpoints.base_url,
            api_key=settings.api_key,
            timeout=settings.timeout,
            query=query,
            include_versions=include_versions,
            include_counts=include_counts,
            limit=limit,
            offset=offset,
        )
    except httpx.TimeoutException as exc:
        raise NetriasAPIUnavailable("data model store request timed out") from exc
    except httpx.HTTPError as exc:
        raise NetriasAPIUnavailable(f"data model store request failed: {exc}") from exc

    body = _interpret_response(response)
    return _parse_data_models(body)


async def list_cdes_async(
    settings: Settings,
    model_key: str,
    version: str,
    include_description: bool = False,
    query: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> tuple[CDE, ...]:
    """Fetch CDEs for a data model version from the Data Model Store.

    'why': expose available fields for a schema version
    """

    endpoints = _require_endpoints(settings)

    try:
        response = await fetch_cdes(
            base_url=endpoints.base_url,
            api_key=settings.api_key,
            timeout=settings.timeout,
            model_key=model_key,
            version=version,
            include_description=include_description,
            query=query,
            limit=limit,
            offset=offset,
        )
    except httpx.TimeoutException as exc:
        raise NetriasAPIUnavailable("data model store request timed out") from exc
    except httpx.HTTPError as exc:
        raise NetriasAPIUnavailable(f"data model store request failed: {exc}") from exc

    body = _interpret_response(response)
    return _parse_cdes(body)


async def list_pvs_async(
    settings: Settings,
    model_key: str,
    version: str,
    cde_key: str,
    include_inactive: bool = False,
    query: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> tuple[PermissibleValue, ...]:
    """Fetch permissible values for a CDE from the Data Model Store.

    'why': expose allowed values for validation
    """

    endpoints = _require_endpoints(settings)

    try:
        response = await fetch_pvs(
            base_url=endpoints.base_url,
            api_key=settings.api_key,
            timeout=settings.timeout,
            model_key=model_key,
            version=version,
            cde_key=cde_key,
            include_inactive=include_inactive,
            query=query,
            limit=limit,
            offset=offset,
        )
    except httpx.TimeoutException as exc:
        raise NetriasAPIUnavailable("data model store request timed out") from exc
    except httpx.HTTPError as exc:
        raise NetriasAPIUnavailable(f"data model store request failed: {exc}") from exc

    body = _interpret_response(response)
    return _parse_pvs(body)


async def get_pv_set_async(
    settings: Settings,
    model_key: str,
    version: str,
    cde_key: str,
    include_inactive: bool = False,
) -> frozenset[str]:
    """Return all permissible values as a set for membership testing.

    'why': validation use case requires O(1) lookup; pagination is hidden
    """

    all_values: list[str] = []
    offset = 0
    page_count = 0

    while page_count < MAX_PAGINATION_PAGES:
        pvs = await list_pvs_async(
            settings=settings,
            model_key=model_key,
            version=version,
            cde_key=cde_key,
            include_inactive=include_inactive,
            limit=PV_PAGE_SIZE,
            offset=offset,
        )
        all_values.extend(pv.value for pv in pvs)

        if len(pvs) < PV_PAGE_SIZE:
            break
        offset += PV_PAGE_SIZE
        page_count += 1

    if page_count >= MAX_PAGINATION_PAGES:
        _logger.warning(
            "get_pv_set reached pagination limit (%d pages); results may be truncated for %s/%s/%s",
            MAX_PAGINATION_PAGES,
            model_key,
            version,
            cde_key,
        )

    return frozenset(all_values)


def list_data_models(
    settings: Settings,
    query: str | None = None,
    include_versions: bool = False,
    include_counts: bool = False,
    limit: int | None = None,
    offset: int = 0,
) -> tuple[DataModel, ...]:
    """Synchronous wrapper for list_data_models_async."""

    return run_sync(
        list_data_models_async(
            settings=settings,
            query=query,
            include_versions=include_versions,
            include_counts=include_counts,
            limit=limit,
            offset=offset,
        )
    )


def list_cdes(
    settings: Settings,
    model_key: str,
    version: str,
    include_description: bool = False,
    query: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> tuple[CDE, ...]:
    """Synchronous wrapper for list_cdes_async."""

    return run_sync(
        list_cdes_async(
            settings=settings,
            model_key=model_key,
            version=version,
            include_description=include_description,
            query=query,
            limit=limit,
            offset=offset,
        )
    )


def list_pvs(
    settings: Settings,
    model_key: str,
    version: str,
    cde_key: str,
    include_inactive: bool = False,
    query: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> tuple[PermissibleValue, ...]:
    """Synchronous wrapper for list_pvs_async."""

    return run_sync(
        list_pvs_async(
            settings=settings,
            model_key=model_key,
            version=version,
            cde_key=cde_key,
            include_inactive=include_inactive,
            query=query,
            limit=limit,
            offset=offset,
        )
    )


def get_pv_set(
    settings: Settings,
    model_key: str,
    version: str,
    cde_key: str,
    include_inactive: bool = False,
) -> frozenset[str]:
    """Synchronous wrapper for get_pv_set_async."""

    return run_sync(
        get_pv_set_async(
            settings=settings,
            model_key=model_key,
            version=version,
            cde_key=cde_key,
            include_inactive=include_inactive,
        )
    )


def _interpret_response(response: httpx.Response) -> Mapping[str, object]:
    _raise_for_error_status(response)
    return _parse_json_body(response)


def _raise_for_error_status(response: httpx.Response) -> None:
    if response.status_code >= 500:
        raise NetriasAPIUnavailable(f"data model store server error: {_extract_error_message(response)}")
    if response.status_code >= 400:
        raise DataModelStoreError(f"data model store request failed: {_extract_error_message(response)}")


def _parse_json_body(response: httpx.Response) -> Mapping[str, object]:
    try:
        body = response.json()
    except (json.JSONDecodeError, ValueError) as exc:
        raise DataModelStoreError(f"invalid JSON response: {exc}") from exc

    if not isinstance(body, dict):
        raise DataModelStoreError("unexpected response format: expected object")

    return body


def _extract_error_message(response: httpx.Response) -> str:
    message = _try_extract_message_from_json(response)
    if message:
        return message
    if response.text:
        return response.text[:200]
    return f"HTTP {response.status_code}"


def _try_extract_message_from_json(response: httpx.Response) -> str | None:
    try:
        body = response.json()
        for key in ("message", "detail", "error", "description"):
            if key in body and body[key]:
                return str(body[key])
    except (json.JSONDecodeError, ValueError):
        pass
    return None


def _parse_data_models(body: Mapping[str, object]) -> tuple[DataModel, ...]:
    items = body.get("items")
    if not isinstance(items, list):
        return ()

    models: list[DataModel] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        models.append(
            DataModel(
                data_commons_id=int(item.get("data_commons_id", 0)),
                key=str(item.get("key", "")),
                name=str(item.get("name", "")),
                description=item.get("description") if item.get("description") else None,
                is_active=bool(item.get("is_active", True)),
            )
        )

    return tuple(models)


def _parse_cdes(body: Mapping[str, object]) -> tuple[CDE, ...]:
    items = body.get("items")
    if not isinstance(items, list):
        return ()

    cdes: list[CDE] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        cdes.append(
            CDE(
                cde_key=str(item.get("cde_key", "")),
                cde_id=int(item.get("cde_id", 0)),
                cde_version_id=int(item.get("cde_version_id", 0)),
                description=item.get("column_description") if item.get("column_description") else None,
            )
        )

    return tuple(cdes)


def _parse_pvs(body: Mapping[str, object]) -> tuple[PermissibleValue, ...]:
    items = body.get("items")
    if not isinstance(items, list):
        return ()

    pvs: list[PermissibleValue] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        pvs.append(
            PermissibleValue(
                pv_id=int(item.get("pv_id", 0)),
                value=str(item.get("value", "")),
                description=item.get("description") if item.get("description") else None,
                is_active=bool(item.get("is_active", True)),
            )
        )

    return tuple(pvs)
