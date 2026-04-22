"""Translate discovery results into manifest-friendly mappings.

'why': bridge API recommendations to harmonization manifests while preserving
position-wise parity — array index equals CSV column_id end to end
"""
from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Final, cast

from ._errors import MappingValidationError
from ._logging import LOGGER_NAMESPACE
from ._models import (
    AlternativeEntry,
    ColumnMappingRecord,
    ManifestPayload,
    MappingDiscoveryResult,
    MappingRecommendationOption,
    MappingSuggestion,
)

MANIFEST_COLUMN_MAPPINGS_KEY: Final[str] = "column_mappings"
"""Wire-format key for the position-indexed manifest list.

'why': read by normalize_manifest_mapping, written by _http.build_harmonize_payload,
surfaced in boundary error messages — one owner so the label never drifts from the key.
"""

REQUIRED_RECORD_KEYS: tuple[str, ...] = tuple(
    key for key in ColumnMappingRecord.__annotations__ if key in ColumnMappingRecord.__required_keys__
)
"""Runtime mirror of ColumnMappingRecord's required fields.

'why': derived from the TypedDict so the shape has one owner (_models.ColumnMappingRecord)
and the validator cannot drift from the declared contract.
"""


def build_column_mapping_payload(
    result: MappingDiscoveryResult,
    threshold: float,
    column_count: int,
    logger: logging.Logger | None = None,
) -> ManifestPayload:
    """Convert discovery output into a position-indexed manifest of length column_count."""

    active_logger = logger or logging.getLogger(LOGGER_NAMESPACE)
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
    """'why': drop the whole slot if the top option lacks target_cde_id so the
    non-None entry invariant (cde_key + cde_id both populated) holds downstream.
    """
    column_id = suggestion.column_id
    if column_id is None or not 0 <= column_id < column_count:
        return None
    option = _top_option(suggestion.options, threshold)
    if option is None or option.target is None or option.target_cde_id is None:
        return None
    entry = _make_entry(suggestion, option)
    return column_id, entry


def _make_entry(
    suggestion: MappingSuggestion, option: MappingRecommendationOption
) -> ColumnMappingRecord:
    """'why': _place_suggestion guarantees both target and target_cde_id are present
    on non-None entries, so cde_key and cde_id are always emitted together.
    """
    return {
        "column_name": suggestion.source_column,
        "cde_key": cast(str, option.target),
        "cde_id": cast(int, option.target_cde_id),
        "harmonization": option.harmonization,
        "alternatives": _format_alternatives(suggestion.options),
    }


def _log_manifest_outcome(logger: logging.Logger, matched_names: list[str]) -> None:
    if matched_names:
        logger.info("adapter manifest entries: %s", matched_names)
    else:
        logger.warning("adapter manifest entries empty after filtering")


def _format_alternatives(
    options: tuple[MappingRecommendationOption, ...],
) -> list[AlternativeEntry]:
    """Sorted by confidence descending; drops options lacking a target or confidence score."""
    eligible = [opt for opt in options if opt.target is not None and opt.confidence is not None]
    sorted_options = sorted(eligible, key=lambda o: cast(float, o.confidence), reverse=True)
    return [_format_alternative(opt) for opt in sorted_options]


def _format_alternative(option: MappingRecommendationOption) -> AlternativeEntry:
    """'why': score key is 'confidence' end-to-end — same as the upstream API."""
    alt: AlternativeEntry = {
        "target": cast(str, option.target),
        "confidence": cast(float, option.confidence),
        "harmonization": option.harmonization,
    }
    if option.target_cde_id is not None:
        alt["cde_id"] = option.target_cde_id
    return alt


def _top_option(
    options: tuple[MappingRecommendationOption, ...], threshold: float
) -> MappingRecommendationOption | None:
    eligible = [opt for opt in options if _meets_threshold(opt, threshold)]
    if not eligible:
        return None
    return max(eligible, key=lambda opt: cast(float, opt.confidence))


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
    column_mappings = raw.get(MANIFEST_COLUMN_MAPPINGS_KEY)
    if not isinstance(column_mappings, list):
        return []
    return [_coerce_entry(entry, index) for index, entry in enumerate(cast(list[object], column_mappings))]


def _coerce_entry(entry: object, index: int) -> ColumnMappingRecord | None:
    """Validate one manifest slot at the external boundary.

    'why': three outcomes only — None passthrough, full Mapping to ColumnMappingRecord,
    anything else a typed boundary error. The TypedDict is authoritative, so silent
    coercion of partial/mistyped entries would hide the contract violation downstream.
    """
    if entry is None:
        return None
    if not isinstance(entry, Mapping):
        raise MappingValidationError(_wrong_type_message(entry, index))
    typed_entry = cast(Mapping[str, object], entry)
    missing = [key for key in REQUIRED_RECORD_KEYS if key not in typed_entry]
    if missing:
        raise MappingValidationError(_missing_keys_message(typed_entry, missing, index))
    return cast(ColumnMappingRecord, cast(object, dict(typed_entry)))


def _wrong_type_message(entry: object, index: int) -> str:
    return (
        f"manifest entry must be a JSON object or null, found {type(entry).__name__}, "
        + f"source={MANIFEST_COLUMN_MAPPINGS_KEY}[{index}]"
    )


def _missing_keys_message(entry: Mapping[str, object], missing: list[str], index: int) -> str:
    expected = ", ".join(REQUIRED_RECORD_KEYS)
    found = ", ".join(entry.keys())
    missing_str = ", ".join(missing)
    return (
        f"manifest entry missing required keys: expected {{{expected}}}, "
        + f"found {{{found}}}, missing={{{missing_str}}}, "
        + f"source={MANIFEST_COLUMN_MAPPINGS_KEY}[{index}]"
    )
