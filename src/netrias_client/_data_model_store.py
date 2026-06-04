"""Query data models, CDEs, and permissible values from the Data Model Store.

'why': provide typed access to reference data for validation use cases
"""
from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypeGuard

import httpx

from ._errors import DataModelStoreError, NetriasAPIUnavailable
from ._http import fetch_cdes, fetch_data_models, fetch_pvs
from ._logging import LOGGER_NAMESPACE
from ._models import CDE, DataModel, DataModelStoreEndpoints, DataModelVersion, Settings

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
    return _DataModelsResponse.from_json(body).to_domain()


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
    return _CdesResponse.from_json(body).to_domain()


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

    endpoints = _require_endpoints(settings)
    all_values: list[str] = []
    offset = 0
    page_count = 0

    while page_count < MAX_PAGINATION_PAGES:
        page_values = await _fetch_pv_page_values(
            endpoints=endpoints,
            api_key=settings.api_key,
            timeout=settings.timeout,
            model_key=model_key,
            version=version,
            cde_key=cde_key,
            include_inactive=include_inactive,
            offset=offset,
        )
        all_values.extend(page_values)

        if len(page_values) < PV_PAGE_SIZE:
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


async def _fetch_pv_page_values(
    endpoints: DataModelStoreEndpoints,
    api_key: str,
    timeout: float,
    model_key: str,
    version: str,
    cde_key: str,
    include_inactive: bool,
    offset: int,
) -> list[str]:
    """Fetch a single page of PV values, returning only the 'value' string from each item."""

    response = await _request_pv_page(
        endpoints=endpoints,
        api_key=api_key,
        timeout=timeout,
        model_key=model_key,
        version=version,
        cde_key=cde_key,
        include_inactive=include_inactive,
        offset=offset,
    )
    body = _interpret_response(response)
    return list(_PvPageResponse.from_json(body).values)


async def _request_pv_page(
    endpoints: DataModelStoreEndpoints,
    api_key: str,
    timeout: float,
    model_key: str,
    version: str,
    cde_key: str,
    include_inactive: bool,
    offset: int,
) -> httpx.Response:
    try:
        return await fetch_pvs(
            base_url=endpoints.base_url,
            api_key=api_key,
            timeout=timeout,
            model_key=model_key,
            version=version,
            cde_key=cde_key,
            include_inactive=include_inactive,
            limit=PV_PAGE_SIZE,
            offset=offset,
        )
    except httpx.TimeoutException as exc:
        raise NetriasAPIUnavailable("data model store request timed out") from exc
    except httpx.HTTPError as exc:
        raise NetriasAPIUnavailable(f"data model store request failed: {exc}") from exc


def _interpret_response(response: httpx.Response) -> dict[str, object]:
    _raise_for_error_status(response)
    return _parse_json_body(response)


def _raise_for_error_status(response: httpx.Response) -> None:
    if response.status_code >= 500:
        raise NetriasAPIUnavailable(f"data model store server error: {_extract_error_message(response)}")
    if response.status_code >= 400:
        raise DataModelStoreError(f"data model store request failed: {_extract_error_message(response)}")


def _parse_json_body(response: httpx.Response) -> dict[str, object]:
    try:
        body = _decode_response_json(response)
    except (json.JSONDecodeError, ValueError) as exc:
        raise DataModelStoreError(f"invalid JSON response: {exc}") from exc

    parsed = _json_object(body)
    if parsed is None:
        raise DataModelStoreError("unexpected response format: expected object")

    return parsed


def _extract_error_message(response: httpx.Response) -> str:
    message = _try_extract_message_from_json(response)
    if message:
        return message
    if response.text:
        return response.text[:200]
    return f"HTTP {response.status_code}"


def _try_extract_message_from_json(response: httpx.Response) -> str | None:
    """Return a human-readable message from a JSON error body, if one is present.

    'why': error responses sometimes come back as null/scalars/lists instead of
    objects; the isinstance-Mapping guard keeps this helper from leaking TypeError
    so every failure path converges on the typed domain error in the caller.
    """
    body = _response_json_mapping(response)
    if body is None:
        return None
    return _ErrorResponse.from_json(body).message


def _response_json_mapping(response: httpx.Response) -> dict[str, object] | None:
    """Decode JSON body and return it only if it is a Mapping."""
    try:
        body = _decode_response_json(response)
    except (json.JSONDecodeError, ValueError):
        return None
    return _json_object(body)


def _decode_response_json(response: httpx.Response) -> object:
    return response.json()  # pyright: ignore[reportAny]


def _optional_string(raw: object) -> str | None:
    return str(raw) if raw else None


def _int_or_zero(raw: object) -> int:
    if isinstance(raw, bool):
        return int(raw)
    if isinstance(raw, int | float | str):
        return int(raw)
    return 0


def _json_object(raw: object) -> dict[str, object] | None:
    if not _is_object_dict(raw):
        return None
    parsed: dict[str, object] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            return None
        parsed[key] = value
    return parsed


def _json_array(raw: object) -> list[object] | None:
    if not _is_object_list(raw):
        return None
    parsed: list[object] = []
    for item in raw:
        parsed.append(item)
    return parsed


def _is_object_dict(raw: object) -> TypeGuard[dict[object, object]]:
    return isinstance(raw, dict)


def _is_object_list(raw: object) -> TypeGuard[list[object]]:
    return isinstance(raw, list)


@dataclass(frozen=True)
class _ErrorResponse:
    message: str | None

    @classmethod
    def from_json(cls, body: Mapping[str, object]) -> _ErrorResponse:
        for key in ("message", "detail", "error", "description"):
            value = body.get(key)
            if value:
                return cls(message=str(value))
        return cls(message=None)


@dataclass(frozen=True)
class _DataModelsResponse:
    items: tuple[_DataModelItem, ...]

    @classmethod
    def from_json(cls, body: Mapping[str, object]) -> _DataModelsResponse:
        items = _json_array(body.get("items"))
        if items is None:
            return cls(items=())
        parsed = tuple(item for raw in items if (item := _DataModelItem.from_json(raw)) is not None)
        return cls(items=parsed)

    def to_domain(self) -> tuple[DataModel, ...]:
        return tuple(item.to_domain() for item in self.items)


@dataclass(frozen=True)
class _DataModelItem:
    data_commons_id: int
    key: str
    name: str
    description: str | None
    is_active: bool
    versions: tuple[_DataModelVersionItem, ...] | None

    @classmethod
    def from_json(cls, raw: object) -> _DataModelItem | None:
        body = _json_object(raw)
        if body is None:
            return None
        return cls(
            data_commons_id=_int_or_zero(body.get("data_commons_id")),
            key=str(body.get("key", "")),
            name=str(body.get("name", "")),
            description=_optional_string(body.get("description")),
            is_active=bool(body.get("is_active", True)),
            versions=_data_model_versions_from_json(body.get("versions")),
        )

    def to_domain(self) -> DataModel:
        versions = None if self.versions is None else tuple(version.to_domain() for version in self.versions)
        return DataModel(
            data_commons_id=self.data_commons_id,
            key=self.key,
            name=self.name,
            description=self.description,
            is_active=self.is_active,
            versions=versions,
        )


@dataclass(frozen=True)
class _DataModelVersionItem:
    external_version_number: str

    @classmethod
    def from_json(cls, raw: object) -> _DataModelVersionItem | None:
        body = _json_object(raw)
        if body is None:
            return None
        external_version_number = _external_version_number_from_json(body)
        if external_version_number is None:
            return None
        return cls(external_version_number=external_version_number)

    def to_domain(self) -> DataModelVersion:
        return DataModelVersion(external_version_number=self.external_version_number)


def _data_model_versions_from_json(raw: object) -> tuple[_DataModelVersionItem, ...] | None:
    items = _json_array(raw)
    if items is None:
        return None
    versions = tuple(item for raw_item in items if (item := _DataModelVersionItem.from_json(raw_item)) is not None)
    return versions or None


def _external_version_number_from_json(body: Mapping[str, object]) -> str | None:
    for key in ("external_version_number", "version_number", "version_label"):
        if (raw := body.get(key)) is not None and str(raw):
            return str(raw)
    return None


@dataclass(frozen=True)
class _CdesResponse:
    items: tuple[_CdeItem, ...]

    @classmethod
    def from_json(cls, body: Mapping[str, object]) -> _CdesResponse:
        items = _json_array(body.get("items"))
        if items is None:
            return cls(items=())
        parsed = tuple(item for raw in items if (item := _CdeItem.from_json(raw)) is not None)
        return cls(items=parsed)

    def to_domain(self) -> tuple[CDE, ...]:
        return tuple(item.to_domain() for item in self.items)


@dataclass(frozen=True)
class _CdeItem:
    cde_key: str
    cde_id: int
    cde_version_id: int
    description: str | None

    @classmethod
    def from_json(cls, raw: object) -> _CdeItem | None:
        body = _json_object(raw)
        if body is None:
            return None
        return cls(
            cde_key=str(body.get("cde_key", "")),
            cde_id=_int_or_zero(body.get("cde_id")),
            cde_version_id=_int_or_zero(body.get("cde_version_id")),
            description=_optional_string(body.get("column_description")),
        )

    def to_domain(self) -> CDE:
        return CDE(
            cde_key=self.cde_key,
            cde_id=self.cde_id,
            cde_version_id=self.cde_version_id,
            description=self.description,
        )


@dataclass(frozen=True)
class _PvPageResponse:
    values: tuple[str, ...]

    @classmethod
    def from_json(cls, body: Mapping[str, object]) -> _PvPageResponse:
        items = _json_array(body.get("items"))
        if items is None:
            return cls(values=())

        values = [value for raw in items if (value := _pv_value_from_json(raw)) is not None]
        return cls(values=tuple(values))


def _pv_value_from_json(raw: object) -> str | None:
    item = _json_object(raw)
    if item is None:
        return None
    value = item.get("value")
    return value if isinstance(value, str) else None
