"""Mapping discovery workflow functions.

'why': call the recommendation service and normalize responses for callers
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import NoReturn, cast

import httpx

from ._adapter import build_column_mapping_payload
from ._async_utils import run_sync
from ._config import ASYNC_API_URL, BYPASS_ALIAS, BYPASS_FUNCTION, BYPASS_REGION, validated_confidence_threshold
from ._errors import AsyncDiscoveryError, GatewayBypassError, MappingDiscoveryError, NetriasAPIUnavailable
from ._gateway_bypass import invoke_cde_recommendation_alias
from ._http import request_mapping_discovery
from ._models import ManifestPayload, MappingDiscoveryResult, MappingRecommendationOption, MappingSuggestion, Settings
from ._sfn_discovery import discover_via_step_functions
from ._validators import validate_column_samples, validate_source_path, validate_target_schema, validate_target_version, validate_top_k


async def _discover_mapping_async(
    settings: Settings,
    target_schema: str,
    target_version: str,
    column_samples: Mapping[str, Sequence[object]],
    logger: logging.Logger,
    top_k: int | None = None,
    confidence_threshold: float | None = None,
) -> ManifestPayload:
    """Perform mapping discovery via the recommendation endpoint."""

    schema = validate_target_schema(target_schema)
    version = validate_target_version(target_version)
    validated_top_k = validate_top_k(top_k)
    threshold = validated_confidence_threshold(confidence_threshold)
    samples: dict[str, list[str]] = validate_column_samples(column_samples)
    started = time.perf_counter()
    logger.info("discover mapping start: schema=%s version=%s columns=%s", schema, version, len(samples))

    try:
        result = await _discover_with_backend(settings, schema, version, samples, logger, validated_top_k)
    except (httpx.TimeoutException, httpx.HTTPError, GatewayBypassError, AsyncDiscoveryError) as exc:
        _handle_discovery_error(schema, started, exc, logger)

    manifest = build_column_mapping_payload(
        result,
        threshold=threshold,
        logger=logger,
    )
    elapsed = time.perf_counter() - started
    logger.info(
        "discover mapping complete: schema=%s version=%s columns=%s duration=%.2fs",
        schema,
        version,
        len(manifest.get("column_mappings", {})),
        elapsed,
    )
    return manifest


def discover_mapping(
    settings: Settings,
    target_schema: str,
    target_version: str,
    column_samples: Mapping[str, Sequence[object]],
    logger: logging.Logger,
    top_k: int | None = None,
    confidence_threshold: float | None = None,
) -> ManifestPayload:
    """Sync wrapper around `_discover_mapping_async`."""

    return run_sync(
        _discover_mapping_async(
            settings=settings,
            target_schema=target_schema,
            target_version=target_version,
            column_samples=column_samples,
            logger=logger,
            top_k=top_k,
            confidence_threshold=confidence_threshold,
        )
    )


async def discover_mapping_async(
    settings: Settings,
    target_schema: str,
    target_version: str,
    column_samples: Mapping[str, Sequence[object]],
    logger: logging.Logger,
    top_k: int | None = None,
    confidence_threshold: float | None = None,
) -> ManifestPayload:
    """Async entry point mirroring `discover_mapping` semantics."""

    return await _discover_mapping_async(
        settings=settings,
        target_schema=target_schema,
        target_version=target_version,
        column_samples=column_samples,
        logger=logger,
        top_k=top_k,
        confidence_threshold=confidence_threshold,
    )


def discover_cde_mapping(
    settings: Settings,
    source_csv: Path,
    target_schema: str,
    target_version: str,
    sample_limit: int,
    logger: logging.Logger,
    top_k: int | None = None,
    confidence_threshold: float | None = None,
) -> ManifestPayload:
    """Convenience wrapper that derives column samples from a CSV file."""

    samples = _samples_from_csv(source_csv, sample_limit)
    return discover_mapping(
        settings=settings,
        target_schema=target_schema,
        target_version=target_version,
        column_samples=samples,
        logger=logger,
        top_k=top_k,
        confidence_threshold=confidence_threshold,
    )


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
    """Async variant of `discover_mapping_from_csv`."""

    samples = _samples_from_csv(source_csv, sample_limit)
    return await discover_mapping_async(
        settings=settings,
        target_schema=target_schema,
        target_version=target_version,
        column_samples=samples,
        logger=logger,
        top_k=top_k,
        confidence_threshold=confidence_threshold,
    )


async def _discover_with_backend(
    settings: Settings,
    schema: str,
    version: str,
    samples: Mapping[str, Sequence[str]],
    logger: logging.Logger,
    top_k: int | None = None,
) -> MappingDiscoveryResult:
    # Step Functions polling (highest priority when enabled)
    # 'why': discover_via_step_functions uses blocking I/O; run in thread to avoid blocking event loop
    if settings.discovery_use_async_api:
        logger.debug("discover backend via Step Functions polling")
        payload = await asyncio.to_thread(
            discover_via_step_functions,
            api_url=ASYNC_API_URL,
            target_schema=schema,
            target_version=version,
            columns=samples,
            timeout=settings.timeout,
            logger=logger,
            top_k=top_k or 3,
        )
        return _result_from_payload(payload, schema)

    # Direct Lambda invocation (gateway bypass)
    # 'why': invoke_cde_recommendation_alias uses boto3 blocking I/O; run in thread
    if settings.discovery_use_gateway_bypass:
        logger.debug("discover backend via bypass alias")
        payload = await asyncio.to_thread(
            invoke_cde_recommendation_alias,
            target_schema=schema,
            target_version=version,
            columns=samples,
            function_name=BYPASS_FUNCTION,
            alias=BYPASS_ALIAS,
            region_name=BYPASS_REGION,
            timeout_seconds=settings.timeout,
            logger=logger,
            top_k=top_k,
        )
        return _result_from_payload(payload, schema)

    # HTTP API (fallback)
    logger.debug("discover backend via HTTP API")
    response = await request_mapping_discovery(
        base_url=settings.discovery_url,
        api_key=settings.api_key,
        timeout=settings.timeout,
        schema=schema,
        version=version,
        columns=samples,
        top_k=top_k,
    )
    return _interpret_discovery_response(response, schema)


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


def _interpret_discovery_response(response: httpx.Response, requested_schema: str) -> MappingDiscoveryResult:
    if response.status_code >= 500:
        message = _error_message(response)
        raise NetriasAPIUnavailable(message)
    if response.status_code >= 400:
        message = _error_message(response)
        raise MappingDiscoveryError(message)

    payload = _load_payload(response)
    return _result_from_payload(payload, requested_schema)


def _result_from_payload(payload: Mapping[str, object], requested_schema: str) -> MappingDiscoveryResult:
    schema = _resolved_schema(payload, requested_schema)
    suggestions = _suggestions_from_payload(payload)
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


def _entries_from_value(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        return ()
    collected: list[Mapping[str, object]] = []
    items = cast(list[object], value)
    for item in items:
        if isinstance(item, Mapping):
            collected.append(cast(Mapping[str, object], item))
    return tuple(collected)


def _coerce_mapping(obj: Mapping[object, object], strict: bool) -> dict[str, object] | None:
    result: dict[str, object] = {}
    for key, value in obj.items():
        if not isinstance(key, str):
            if strict:
                raise MappingDiscoveryError("mapping discovery response body must be a JSON object")
            return None
        result[key] = value
    return result


def _samples_from_csv(csv_path: Path, sample_limit: int) -> dict[str, list[str]]:
    dataset = validate_source_path(csv_path)
    headers, rows = _read_limited_rows(dataset, sample_limit)
    samples: dict[str, list[str]] = {header: [] for header in headers}
    _fill_samples(samples, rows)
    return {key: value for key, value in samples.items() if value}


def _read_limited_rows(dataset: Path, sample_limit: int) -> tuple[list[str], list[dict[str, str | None]]]:
    headers: list[str] = []
    rows: list[dict[str, str | None]] = []
    with dataset.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = [header for header in reader.fieldnames or [] if header]
        for index, row in enumerate(reader):
            if index >= sample_limit:
                break
            rows.append(row)
    return headers, rows


def _fill_samples(samples: dict[str, list[str]], rows: list[dict[str, str | None]]) -> None:
    for row in rows:
        _append_row(samples, row)


def _append_row(samples: dict[str, list[str]], row: dict[str, str | None]) -> None:
    for header, raw_value in row.items():
        if header not in samples or raw_value is None:
            continue
        value = raw_value.strip()
        if value:
            samples[header].append(value)


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


def _suggestions_from_payload(payload: Mapping[str, object]) -> tuple[MappingSuggestion, ...]:
    # Try the new dict-based `results` format first
    results_dict = _results_dict_from_payload(payload)
    if results_dict:
        return _suggestions_from_results_dict(results_dict)

    # Fall back to the old array-based format
    raw_entries = _candidate_entries(payload)
    suggestions: list[MappingSuggestion] = []
    for entry in raw_entries:
        source = _source_column(entry)
        if not source:
            continue
        options = _options_from_entry(entry)
        suggestions.append(
            MappingSuggestion(source_column=source, options=options, raw=entry)
        )
    return tuple(suggestions)


def _results_dict_from_payload(payload: Mapping[str, object]) -> dict[str, list[object]] | None:
    """Extract the new dict-based results structure if present."""

    results = payload.get("results")
    if not isinstance(results, dict):
        return None
    return cast(dict[str, list[object]], results)


def _suggestions_from_results_dict(results: dict[str, list[object]]) -> tuple[MappingSuggestion, ...]:
    """Convert dict-keyed results to MappingSuggestion tuples."""

    suggestions: list[MappingSuggestion] = []
    for column_name, options_list in results.items():
        if not isinstance(options_list, list):
            continue
        options = _options_from_list(options_list)
        raw_entry: dict[str, object] = {"column": column_name, "options": options_list}
        suggestions.append(
            MappingSuggestion(source_column=column_name, options=options, raw=raw_entry)
        )
    return tuple(suggestions)


def _options_from_list(options_list: list[object]) -> tuple[MappingRecommendationOption, ...]:
    """Convert a list of option dicts to MappingRecommendationOption tuples."""

    options: list[MappingRecommendationOption] = []
    for item in options_list:
        if not isinstance(item, Mapping):
            continue
        mapping = cast(Mapping[str, object], item)
        target = _option_target(mapping)
        confidence = _option_confidence(mapping)
        target_cde_id = _option_target_cde_id(mapping)
        options.append(
            MappingRecommendationOption(
                target=target, confidence=confidence, target_cde_id=target_cde_id, raw=mapping
            )
        )
    return tuple(options)


def _candidate_entries(payload: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    for key in ("recommendations", "columns", "suggestions"):
        entries = _entries_from_value(payload.get(key))
        if entries:
            return entries
    return ()


def _source_column(entry: Mapping[str, object]) -> str | None:
    candidates = (
        entry.get("column"),
        entry.get("source_column"),
        entry.get("name"),
        entry.get("field"),
    )
    for candidate in candidates:
        if isinstance(candidate, str):
            name = candidate.strip()
            if name:
                return name
    return None


def _options_from_entry(entry: Mapping[str, object]) -> tuple[MappingRecommendationOption, ...]:
    raw_options = entry.get("suggestions") or entry.get("options") or entry.get("targets")
    if not isinstance(raw_options, list):
        return ()
    return _options_from_list(cast(list[object], raw_options))


def _option_target(option: Mapping[str, object]) -> str | None:
    for key in ("target", "cde", "field", "name", "qualified_name"):
        value = option.get(key)
        if isinstance(value, str):
            candidate = value.strip()
            if candidate:
                return candidate
    return None


def _option_confidence(option: Mapping[str, object]) -> float | None:
    for key in ("similarity", "confidence", "score", "probability"):
        value = option.get(key)
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
