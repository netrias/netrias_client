"""Translate discovery results into manifest-friendly mappings.

'why': bridge API recommendations to harmonization manifests while preserving
position-wise parity — array index equals CSV column_id end to end
"""
from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from ._errors import MappingValidationError
from ._models import (
    AlternativeEntry,
    ColumnMappingRecord,
    ManifestPayload,
    MappingDiscoveryResult,
    MappingRecommendationOption,
    MappingSuggestion,
)


def build_column_mapping_payload(
    result: MappingDiscoveryResult,
    threshold: float,
    column_count: int,
    logger: logging.Logger | None = None,
) -> ManifestPayload:
    """Convert discovery output into a position-indexed manifest of length column_count."""

    active_logger = logger or logging.getLogger("netrias_client")
    entries = _build_manifest_entries(result.suggestions, threshold, column_count, active_logger)
    return ManifestPayload(column_mappings=entries)


def _build_manifest_entries(
    suggestions: tuple[MappingSuggestion, ...],
    threshold: float,
    column_count: int,
    logger: logging.Logger,
) -> list[ColumnMappingRecord | None]:
    """Emit one slot per CSV column, with None where no option meets the threshold.

    'why': consumer uses array index as stable column_id; placeholders preserve that identity.
    """

    entries: list[ColumnMappingRecord | None] = [None] * column_count
    matched_names: list[str] = []
    for suggestion in suggestions:
        placement = _place_suggestion(suggestion, threshold, column_count)
        if placement is None:
            continue
        column_id, entry = placement
        entries[column_id] = entry
        matched_names.append(suggestion.source_column)
    _log_manifest_outcome(logger, matched_names)
    return entries


def _place_suggestion(
    suggestion: MappingSuggestion, threshold: float, column_count: int
) -> tuple[int, ColumnMappingRecord] | None:
    column_id = suggestion.column_id
    if column_id is None or not 0 <= column_id < column_count:
        return None
    option = _top_option(suggestion.options, threshold)
    if option is None or option.target is None:
        return None
    entry = _make_entry(suggestion, option)
    return column_id, entry


def _make_entry(
    suggestion: MappingSuggestion, option: MappingRecommendationOption
) -> ColumnMappingRecord:
    assert option.target is not None
    entry: ColumnMappingRecord = {
        "column_name": suggestion.source_column,
        "alternatives": _format_alternatives(suggestion.options),
    }
    if option.target_cde_id is not None:
        entry["cde_id"] = option.target_cde_id
    return entry


def _log_manifest_outcome(logger: logging.Logger, matched_names: list[str]) -> None:
    if matched_names:
        logger.info("adapter manifest entries: %s", matched_names)
    else:
        logger.warning("adapter manifest entries empty after filtering")


def _format_alternatives(
    options: tuple[MappingRecommendationOption, ...],
) -> list[AlternativeEntry]:
    """Sorted by confidence descending; includes all options regardless of threshold."""
    sorted_options = sorted(options, key=lambda o: o.confidence or 0.0, reverse=True)
    return [_format_alternative(opt) for opt in sorted_options if opt.target is not None]


def _format_alternative(option: MappingRecommendationOption) -> AlternativeEntry:
    assert option.target is not None
    alt: AlternativeEntry = {"target": option.target}
    if option.confidence is not None:
        alt["similarity"] = option.confidence
    if option.target_cde_id is not None:
        alt["cde_id"] = option.target_cde_id
    return alt


def _top_option(
    options: tuple[MappingRecommendationOption, ...], threshold: float
) -> MappingRecommendationOption | None:
    eligible = [opt for opt in options if _meets_threshold(opt, threshold)]
    if not eligible:
        return None
    return max(eligible, key=lambda opt: opt.confidence or float("-inf"))


def _meets_threshold(option: MappingRecommendationOption, threshold: float) -> bool:
    score = option.confidence
    if score is None:
        return False
    return score >= threshold


def load_manifest(path: Path) -> Mapping[str, object]:
    """Boundary: read and parse manifest JSON, raising typed errors on failure."""

    try:
        content = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise MappingValidationError(
            f"manifest could not be read: expected readable JSON file, found {exc}, source={path}"
        ) from exc
    try:
        parsed = cast(object, json.loads(content))
    except json.JSONDecodeError as exc:
        raise MappingValidationError(
            f"manifest was not valid JSON: {exc}, source={path}"
        ) from exc
    if not isinstance(parsed, Mapping):
        raise MappingValidationError(
            f"manifest must be a JSON object, found {type(parsed).__name__}, source={path}"
        )
    return cast(Mapping[str, object], parsed)


def normalize_manifest_mapping(
    manifest: Path | Mapping[str, object] | None,
) -> list[ColumnMappingRecord | None]:
    """Return the position-indexed column_mappings list from a manifest input."""

    if manifest is None:
        return []
    raw = load_manifest(manifest) if isinstance(manifest, Path) else manifest
    column_mappings = raw.get("column_mappings")
    if not isinstance(column_mappings, list):
        return []
    return [_coerce_entry(entry) for entry in cast(list[object], column_mappings)]


def _coerce_entry(entry: object) -> ColumnMappingRecord | None:
    if entry is None:
        return None
    if isinstance(entry, Mapping):
        # 'why': manifest JSON is an external boundary; trust structural shape here
        # and coerce to the TypedDict for downstream consumers
        return cast(ColumnMappingRecord, cast(object, dict(cast(Mapping[str, object], entry))))
    return None
