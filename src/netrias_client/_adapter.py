"""Translate discovery results into manifest-friendly mappings.

'why': bridge API recommendations to harmonization manifests while respecting confidence bounds
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final, cast

from ._config import get_settings
from ._logging import get_logger
from ._models import MappingDiscoveryResult, MappingRecommendationOption, MappingSuggestion


_logger = get_logger()


def build_column_mapping_payload(result: MappingDiscoveryResult) -> dict[str, dict[str, dict[str, object]]]:
    """Convert discovery output into the manifest structure expected by harmonization."""

    strongest = strongest_targets(result)
    return {"column_mappings": _column_entries(strongest)}


_COLUMN_METADATA: Final[dict[str, dict[str, object]]] = {
    # "study_name": {"route": "api:passthrough", "targetField": "study_name"},
    # "number_of_participants": {"route": "api:passthrough", "targetField": "number_of_participants"},
    # "number_of_samples": {"route": "api:passthrough", "targetField": "number_of_samples"},
    # "study_data_types": {
    #     "route": "api:passthrough",
    #     "targetField": "study_data_types",
    #     "cdeId": 12_571_096,
    #     "cde_id": 12_571_096,
    # },
    # "participant_id": {"route": "api:passthrough", "targetField": "participant_id"},
    # "sample_id": {"route": "api:passthrough", "targetField": "sample_id"},
    # "file_name": {"route": "api:passthrough", "targetField": "file_name"},
    "primary_diagnosis": {
        "route": "sagemaker:primary",
        "targetField": "primary_diagnosis",
        "cdeId": -200,
        "cde_id": -200,
    },
    "therapeutic_agents": {
        "route": "sagemaker:therapeutic_agents",
        "targetField": "therapeutic_agents",
        "cdeId": -203,
        "cde_id": -203,
    },
    "morphology": {
        "route": "sagemaker:morphology",
        "targetField": "morphology",
        "cdeId": -201,
        "cde_id": -201,
    },
    # "tissue_or_organ_of_origin": {
    #     "route": "sagemaker:tissue_origin",
    #     "targetField": "tissue_or_organ_of_origin",
    #     "cdeId": -204,
    #     "cde_id": -204,
    # },
    # "site_of_resection_or_biopsy": {
    #     "route": "sagemaker:sample_anatomic_site",
    #     "targetField": "site_of_resection_or_biopsy",
    #     "cdeId": -202,
    #     "cde_id": -202,
    # },
}


def strongest_targets(result: MappingDiscoveryResult) -> dict[str, str]:
    """Return the highest-confidence target per column, filtered by threshold."""

    settings = get_settings()
    threshold = settings.confidence_threshold

    if result.suggestions:
        selected = _from_suggestions(result.suggestions, threshold)
    else:
        selected = _from_raw_payload(result.raw, threshold)

    if selected:
        _logger.info("adapter strongest targets: %s", selected)
    else:
        _logger.warning("adapter strongest targets empty after filtering")
    return selected


def _column_entries(strongest: Mapping[str, str]) -> dict[str, dict[str, object]]:
    entries: dict[str, dict[str, object]] = {}
    missing_cde: dict[str, str] = {}
    for source, target in strongest.items():
        entry = _initial_entry(source, target)
        if _needs_cde(entry):
            missing_cde[source] = target
        entries[source] = entry

    _apply_metadata_defaults(entries)

    if missing_cde:
        _logger.info("adapter unresolved targets (no CDE id mapping): %s", missing_cde)
    return entries


def _initial_entry(source: str, target: str) -> dict[str, object]:
    metadata = _COLUMN_METADATA.get(source)
    if metadata is None:
        return {"targetField": target}
    # Preserve configured targetField when metadata defines it.
    return dict(metadata)


def _needs_cde(entry: Mapping[str, object]) -> bool:
    return "cdeId" not in entry


def _apply_metadata_defaults(entries: dict[str, dict[str, object]]) -> None:
    for source, metadata in _COLUMN_METADATA.items():
        if source not in entries:
            entries[source] = dict(metadata)


def _from_suggestions(
    suggestions: Iterable[MappingSuggestion], threshold: float
) -> dict[str, str]:
    strongest: dict[str, str] = {}
    for suggestion in suggestions:
        option = _top_option(suggestion.options, threshold)
        if option is None or option.target is None:
            continue
        strongest[suggestion.source_column] = option.target
    return strongest


def _from_raw_payload(payload: Mapping[str, object], threshold: float) -> dict[str, str]:
    strongest: dict[str, str] = {}
    for column, value in payload.items():
        options = _coerce_options(value)
        option = _top_option(options, threshold)
        if option is None or option.target is None:
            continue
        strongest[column] = option.target
    return strongest


def _coerce_options(value: object) -> tuple[MappingRecommendationOption, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(_option_iterator(cast(list[object], value)))


def _option_iterator(items: list[object]) -> Iterable[MappingRecommendationOption]:
    for item in items:
        if not isinstance(item, Mapping):
            continue
        option = _option_from_mapping(cast(Mapping[str, object], item))
        if option is not None:
            yield option


def _option_from_mapping(item: Mapping[str, object]) -> MappingRecommendationOption | None:
    target = item.get("target")
    if not isinstance(target, str):
        return None
    similarity = item.get("similarity")
    score: float | None = None
    if isinstance(similarity, (float, int)):
        score = float(similarity)
    return MappingRecommendationOption(target=target, confidence=score, raw=item)


def _top_option(
    options: Iterable[MappingRecommendationOption], threshold: float
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
