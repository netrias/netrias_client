"""Mapping discovery workflow functions.

'why': call the recommendation service and normalize responses while preserving
position-wise parity between CSV columns, request, response, and manifest
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import time
from collections.abc import Mapping
from pathlib import Path
from typing import NoReturn, cast

import httpx

from ._adapter import build_column_mapping_payload
from ._config import ASYNC_API_URL, BYPASS_ALIAS, BYPASS_FUNCTION, BYPASS_REGION, validated_confidence_threshold
from ._errors import (
    AsyncDiscoveryError,
    GatewayBypassError,
    MappingDiscoveryError,
    MappingValidationError,
    NetriasAPIUnavailable,
)
from ._gateway_bypass import invoke_cde_recommendation_alias
from ._http import request_mapping_discovery
from ._models import (
    COLUMN_NAME_KEY,
    HARMONIZATION_VALUES,
    ColumnKeyedManifestPayload,
    ColumnMappingRecord,
    ColumnSamples,
    Harmonization,
    ManifestPayload,
    MappingDiscoveryResult,
    MappingRecommendationOption,
    MappingSuggestion,
    Settings,
)
from ._sfn_discovery import discover_via_step_functions
from ._tabular import TabularDataset, read_tabular
from ._validators import validate_source_path, validate_target_schema, validate_target_version, validate_top_k


async def _discover_mapping_async(
    settings: Settings,
    target_schema: str,
    target_version: str,
    columns: list[ColumnSamples],
    logger: logging.Logger,
    top_k: int | None = None,
    confidence_threshold: float | None = None,
) -> ManifestPayload:
    """Perform mapping discovery via the recommendation endpoint."""

    schema = validate_target_schema(target_schema)
    version = validate_target_version(target_version)
    validated_top_k = validate_top_k(top_k)
    threshold = validated_confidence_threshold(confidence_threshold)
    column_count = len(columns)
    started = time.perf_counter()
    logger.info("discover mapping start: schema=%s version=%s columns=%s", schema, version, column_count)

    outbound_names = tuple(column["column_name"] for column in columns)
    try:
        result = await _discover_with_backend(
            settings, schema, version, columns, outbound_names, logger, validated_top_k
        )
    except (httpx.TimeoutException, httpx.HTTPError, GatewayBypassError, AsyncDiscoveryError) as exc:
        _handle_discovery_error(schema, started, exc, logger)

    manifest = build_column_mapping_payload(
        result,
        threshold=threshold,
        column_count=column_count,
        logger=logger,
    )
    elapsed = time.perf_counter() - started
    logger.info(
        "discover mapping complete: schema=%s version=%s columns=%s duration=%.2fs",
        schema,
        version,
        len(manifest["column_mappings"]),
        elapsed,
    )
    return manifest


async def discover_mapping_from_csv_async(
    settings: Settings,
    source_csv: Path,
    target_schema: str,
    target_version: str,
    sample_limit: int,
    logger: logging.Logger,
    top_k: int | None = None,
    confidence_threshold: float | None = None,
) -> ManifestPayload:
    """Derive positional column samples from a CSV then perform async discovery."""

    columns = _samples_from_csv(source_csv, sample_limit)
    return await _discover_mapping_async(
        settings=settings,
        target_schema=target_schema,
        target_version=target_version,
        columns=columns,
        logger=logger,
        top_k=top_k,
        confidence_threshold=confidence_threshold,
    )


async def discover_mapping_from_tabular_async(
    settings: Settings,
    source_path: Path,
    target_schema: str,
    target_version: str,
    sample_limit: int,
    logger: logging.Logger,
    top_k: int | None = None,
    confidence_threshold: float | None = None,
) -> ColumnKeyedManifestPayload:
    """Derive positional samples from a CSV/TSV file and return a column-keyed manifest."""

    dataset = read_tabular(validate_source_path(source_path))
    _require_nonempty_header(dataset.headers, source_path)
    columns = _samples_from_dataset(dataset, sample_limit)
    legacy_manifest = await _discover_mapping_async(
        settings=settings,
        target_schema=target_schema,
        target_version=target_version,
        columns=columns,
        logger=logger,
        top_k=top_k,
        confidence_threshold=confidence_threshold,
    )
    return _column_keyed_manifest(legacy_manifest, dataset)


async def _discover_with_backend(
    settings: Settings,
    schema: str,
    version: str,
    columns: list[ColumnSamples],
    outbound_names: tuple[str, ...],
    logger: logging.Logger,
    top_k: int | None = None,
) -> MappingDiscoveryResult:
    if settings.discovery_use_async_api:
        logger.debug("discover backend via Step Functions polling")
        # 'why': discover_via_step_functions uses blocking I/O; run in thread to avoid blocking event loop
        payload = await asyncio.to_thread(
            discover_via_step_functions,
            api_url=ASYNC_API_URL,
            target_schema=schema,
            target_version=version,
            columns=columns,
            timeout=settings.timeout,
            logger=logger,
            top_k=top_k or 3,
            api_key=settings.api_key,
        )
        return _result_from_payload(payload, schema, outbound_names)

    if settings.discovery_use_gateway_bypass:
        logger.debug("discover backend via bypass alias")
        # 'why': invoke_cde_recommendation_alias uses boto3 blocking I/O; run in thread
        payload = await asyncio.to_thread(
            invoke_cde_recommendation_alias,
            target_schema=schema,
            target_version=version,
            columns=columns,
            function_name=BYPASS_FUNCTION,
            alias=BYPASS_ALIAS,
            region_name=BYPASS_REGION,
            timeout_seconds=settings.timeout,
            logger=logger,
            top_k=top_k,
        )
        return _result_from_payload(payload, schema, outbound_names)

    logger.debug("discover backend via HTTP API")
    response = await request_mapping_discovery(
        base_url=settings.discovery_url,
        api_key=settings.api_key,
        timeout=settings.timeout,
        schema=schema,
        version=version,
        columns=columns,
        top_k=top_k,
    )
    return _interpret_discovery_response(response, schema, outbound_names)


def _handle_discovery_error(
    schema: str,
    started: float,
    exc: Exception,
    logger: logging.Logger,
) -> NoReturn:
    elapsed = time.perf_counter() - started
    if isinstance(exc, httpx.TimeoutException):  # pragma: no cover - exercised via integration tests
        logger.error("discover mapping timeout: schema=%s duration=%.2fs err=%s", schema, elapsed, exc)
        raise NetriasAPIUnavailable("mapping discovery timed out") from exc
    if isinstance(exc, GatewayBypassError):
        logger.error(
            "discover mapping bypass error: schema=%s duration=%.2fs err=%s",
            schema,
            elapsed,
            exc,
        )
        raise NetriasAPIUnavailable(f"gateway bypass error: {exc}") from exc
    if isinstance(exc, AsyncDiscoveryError):
        logger.error(
            "discover mapping async error: schema=%s duration=%.2fs err=%s",
            schema,
            elapsed,
            exc,
        )
        raise NetriasAPIUnavailable(f"async discovery error: {exc}") from exc

    logger.error(
        "discover mapping transport error: schema=%s duration=%.2fs err=%s",
        schema,
        elapsed,
        exc,
    )
    raise NetriasAPIUnavailable(f"mapping discovery transport error: {exc}") from exc


def _interpret_discovery_response(
    response: httpx.Response,
    requested_schema: str,
    outbound_names: tuple[str, ...],
) -> MappingDiscoveryResult:
    if response.status_code >= 500:
        message = _error_message(response)
        raise NetriasAPIUnavailable(message)
    if response.status_code >= 400:
        message = _error_message(response)
        raise MappingDiscoveryError(message)

    payload = _load_payload(response)
    return _result_from_payload(payload, requested_schema, outbound_names)


def _result_from_payload(
    payload: Mapping[str, object],
    requested_schema: str,
    outbound_names: tuple[str, ...],
) -> MappingDiscoveryResult:
    schema = _resolved_schema(payload, requested_schema)
    suggestions = _suggestions_from_payload(payload, outbound_names)
    return MappingDiscoveryResult(schema=schema, suggestions=suggestions, raw=payload)


def _error_message(response: httpx.Response) -> str:
    mapping = _mapping_or_none(_safe_json(response))
    message = _message_from_mapping(mapping)
    if message:
        return message
    return _default_error(response)


def _extract_message(payload: Mapping[str, object]) -> str | None:
    for key in ("message", "error", "detail"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _message_from_mapping(payload: Mapping[str, object] | None) -> str | None:
    if payload is None:
        return None
    direct = _extract_message(payload)
    if direct:
        return direct
    nested = _resolve_body_optional(payload)
    if nested:
        return _extract_message(nested)
    return None


def _mapping_or_none(data: object) -> Mapping[str, object] | None:
    if isinstance(data, Mapping):
        return cast(Mapping[str, object], data)
    return None


def _safe_json(response: httpx.Response) -> object:
    try:
        return cast(object, response.json())
    except json.JSONDecodeError:
        return None


def _default_error(response: httpx.Response) -> str:
    return f"mapping discovery failed (HTTP {response.status_code})"


def _resolve_body_optional(container: Mapping[str, object]) -> dict[str, object] | None:
    body = container.get("body")
    if body is None:
        return None
    parsed = _decode_body(body, strict=False)
    if isinstance(parsed, dict):
        return _coerce_mapping(cast(Mapping[object, object], parsed), strict=False)
    return None


def _expect_mapping(data: object) -> dict[str, object]:
    if isinstance(data, dict):
        mapping = _coerce_mapping(cast(Mapping[object, object], data), strict=True)
        if mapping is not None:
            return mapping
    raise MappingDiscoveryError("mapping discovery response body must be a JSON object")


def _extract_body_object(container: Mapping[str, object]) -> dict[str, object] | None:
    if "body" not in container:
        return None
    parsed = _decode_body(container["body"], strict=True)
    if isinstance(parsed, dict):
        mapping = _coerce_mapping(cast(Mapping[object, object], parsed), strict=True)
        if mapping is not None:
            return mapping
    raise MappingDiscoveryError("mapping discovery response body must be a JSON object")


def _coerce_mapping(obj: Mapping[object, object], strict: bool) -> dict[str, object] | None:
    result: dict[str, object] = {}
    for key, value in obj.items():
        if not isinstance(key, str):
            if strict:
                raise MappingDiscoveryError("mapping discovery response body must be a JSON object")
            return None
        result[key] = value
    return result


def _samples_from_csv(csv_path: Path, sample_limit: int) -> list[ColumnSamples]:
    """'why': emit one entry per CSV header position so array index == column_id downstream;
    csv.DictReader would silently merge duplicate headers, so csv.reader is used.
    Blank/whitespace-only headers get synthetic `_col_<i>` names so the backend's
    non-empty-column-name validator accepts the payload; the synthetic column will
    not match anything and lands as None in the manifest, preserving positional parity.
    An empty header row yields zero columns, which the backend would 400 on or return
    an empty results array — either way the manifest would be trivially "successful"
    for a degenerate input, so reject it here at the boundary instead."""
    dataset_path = validate_source_path(csv_path)
    if dataset_path.suffix.lower() != ".csv":
        raise MappingValidationError(
            "discover_mapping_from_csv only supports .csv inputs; "
            "use discover_mapping_from_tabular for other tabular formats"
        )
    headers, rows = _read_limited_rows(dataset_path, sample_limit)
    _require_nonempty_header(headers, dataset_path)
    column_count = len(headers)
    samples = _collect_column_samples(rows, column_count)
    return [
        ColumnSamples(column_name=_column_name_or_placeholder(headers[i], i), values=samples[i])
        for i in range(column_count)
    ]


def _samples_from_dataset(dataset: TabularDataset, sample_limit: int) -> list[ColumnSamples]:
    sample_rows = dataset.rows[:sample_limit]
    samples = _collect_column_samples(sample_rows, len(dataset.columns))
    backend_names = dataset.backend_column_names()
    return [
        ColumnSamples(column_name=backend_names[i], values=samples[i])
        for i in range(len(dataset.columns))
    ]


def _column_keyed_manifest(
    manifest: ManifestPayload,
    dataset: TabularDataset,
) -> ColumnKeyedManifestPayload:
    entries: dict[str, ColumnMappingRecord] = {}
    slots = manifest["column_mappings"]
    for column in dataset.columns:
        if column.index >= len(slots):
            continue
        entry = slots[column.index]
        if entry is None:
            continue
        entries[column.key] = {**entry, "column_name": column.header}
    return ColumnKeyedManifestPayload(column_mappings=entries)


def _require_nonempty_header(headers: list[str], dataset: Path) -> None:
    """'why': an empty header row would produce a zero-column request that the
    backend either 400s on or returns []; either way the SDK would advertise a
    trivially-successful manifest for a degenerate input. Reject at the boundary."""
    if headers:
        return
    raise MappingValidationError(
        "source CSV has no header row: expected at least one column, "
        + f"found 0 columns, source={dataset}"
    )


def _collect_column_samples(rows: list[list[str]], column_count: int) -> list[list[str]]:
    samples: list[list[str]] = [[] for _ in range(column_count)]
    for row in rows:
        padded = row + [""] * max(0, column_count - len(row))
        for i in range(column_count):
            value = padded[i].strip()
            if value:
                samples[i].append(value)
    return samples


def _column_name_or_placeholder(header: str, index: int) -> str:
    return header if header.strip() else f"_col_{index}"


def _read_limited_rows(dataset: Path, sample_limit: int) -> tuple[list[str], list[list[str]]]:
    """'why': utf-8-sig strips BOM if present; BOM in column names causes Step Functions SerializationException"""
    with dataset.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        default_row: list[str] = []
        headers = next(reader, default_row)
        rows: list[list[str]] = []
        for index, row in enumerate(reader):
            if index >= sample_limit:
                break
            rows.append(row)
    return headers, rows


def _decode_body(body: object, strict: bool) -> object:
    if not isinstance(body, str):
        return body
    try:
        return cast(object, json.loads(body))
    except json.JSONDecodeError as exc:
        if strict:
            raise MappingDiscoveryError("mapping discovery body was not valid JSON") from exc
        return None


def _load_payload(response: httpx.Response) -> dict[str, object]:
    data = _safe_json(response)
    mapping = _expect_mapping(data)
    body = _extract_body_object(mapping)
    if body is not None:
        return body
    return mapping


def _resolved_schema(payload: Mapping[str, object], requested_schema: str) -> str:
    for key in ("target_schema", "schema", "recommended_schema"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return requested_schema


def _suggestions_from_payload(
    payload: Mapping[str, object], outbound_names: tuple[str, ...]
) -> tuple[MappingSuggestion, ...]:
    """Boundary: validate response shape, length, and per-index column identity."""

    results = payload.get("results")
    if not isinstance(results, list):
        raise MappingDiscoveryError(
            "mapping discovery response missing 'results' array, source=discovery response"
        )
    return _suggestions_from_results_array(cast(list[object], results), outbound_names)


def _suggestions_from_results_array(
    results: list[object], outbound_names: tuple[str, ...]
) -> tuple[MappingSuggestion, ...]:
    """Convert array-format results to MappingSuggestion tuples, preserving position.

    'why': the consumer's column_id == array index; a silent drop, malformed entry,
    or reordered response here shifts every subsequent column's identity downstream.
    Length *and* column_name parity are cross-checked against the outbound request
    so reorder-with-equal-length cannot pass undetected.
    """

    _require_expected_length(results, len(outbound_names))
    return tuple(
        _suggestion_from_entry(entry, i, outbound_names[i]) for i, entry in enumerate(results)
    )


def _require_expected_length(results: list[object], expected_length: int) -> None:
    if len(results) == expected_length:
        return
    message = (
        f"mapping discovery response length mismatch: expected {expected_length} results, "
        + f"found {len(results)}, source=discovery response"
    )
    raise MappingDiscoveryError(message)


def _suggestion_from_entry(
    entry: object, index: int, expected_column_name: str
) -> MappingSuggestion:
    entry_map = _require_entry_mapping(entry, index)
    column_name = _require_entry_column_name(entry_map, index)
    _require_column_name_parity(column_name, expected_column_name, index)
    matches = _require_entry_matches(entry_map, index)
    options = _options_from_list(matches)
    return MappingSuggestion(source_column=column_name, options=options, raw=dict(entry_map), column_id=index)


def _require_column_name_parity(
    received: str, expected: str, index: int
) -> None:
    """'why': the SDK uses the response array index as column_id; if the backend
    reorders results (same length, different positions) the downstream manifest
    silently binds the wrong CDE to every column. Reject a mismatch at the boundary.
    """
    if received == expected:
        return
    raise MappingDiscoveryError(
        f"mapping discovery result at index {index} column_name mismatch: "
        + f"expected {expected!r}, found {received!r}, source=discovery response"
    )


def _require_entry_mapping(entry: object, index: int) -> Mapping[str, object]:
    if isinstance(entry, Mapping):
        return cast(Mapping[str, object], entry)
    raise MappingDiscoveryError(
        f"mapping discovery result at index {index} is not an object, source=discovery response"
    )


def _require_entry_column_name(entry_map: Mapping[str, object], index: int) -> str:
    column_name = entry_map.get(COLUMN_NAME_KEY)
    if isinstance(column_name, str):
        return column_name
    raise MappingDiscoveryError(
        f"mapping discovery result at index {index} missing 'column_name' string, source=discovery response"
    )


def _require_entry_matches(entry_map: Mapping[str, object], index: int) -> list[object]:
    matches = entry_map.get("matches")
    if isinstance(matches, list):
        return cast(list[object], matches)
    raise MappingDiscoveryError(
        f"mapping discovery result at index {index} missing 'matches' array, source=discovery response"
    )


def _options_from_list(options_list: list[object]) -> tuple[MappingRecommendationOption, ...]:
    options: list[MappingRecommendationOption] = []
    for item in options_list:
        if not isinstance(item, Mapping):
            continue
        mapping = cast(Mapping[str, object], item)
        target = _option_target(mapping)
        confidence = _option_confidence(mapping)
        target_cde_id = _option_target_cde_id(mapping)
        harmonization = _require_option_harmonization(mapping)
        options.append(
            MappingRecommendationOption(
                target=target,
                confidence=confidence,
                harmonization=harmonization,
                target_cde_id=target_cde_id,
                raw=mapping,
            )
        )
    return tuple(options)


def _require_option_harmonization(option: Mapping[str, object]) -> Harmonization:
    """'why': harmonization is required on every match from the Lambda; missing or
    unknown values mean the SDK is talking to an incompatible upstream — fail fast.
    """
    value = option.get("harmonization")
    if isinstance(value, str) and value in HARMONIZATION_VALUES:
        return cast(Harmonization, value)
    allowed = sorted(HARMONIZATION_VALUES)
    message = (
        f"mapping discovery match missing required 'harmonization' value "
        f"(expected one of {allowed}, found {value!r}), source=discovery response"
    )
    raise MappingDiscoveryError(message)


def _option_target(option: Mapping[str, object]) -> str | None:
    value = option.get("target")
    if isinstance(value, str):
        candidate = value.strip()
        if candidate:
            return candidate
    return None


def _option_confidence(option: Mapping[str, object]) -> float | None:
    """'why': upstream API returns the score under 'confidence'; no other key is emitted."""
    value = option.get("confidence")
    # 'why': bool is subclass of int in Python; must guard before int/float check
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _option_target_cde_id(option: Mapping[str, object]) -> int | None:
    value = option.get("target_cde_id")
    # 'why': API may return float (e.g., 900.0) or int; normalize to int
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    return None
