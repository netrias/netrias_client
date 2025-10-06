"""Define dataclasses and types for the client.

'why': capture configuration and results in typed, testable shapes
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass(frozen=True)
class Settings:
    """Capture runtime settings for API calls."""

    api_key: str
    api_url: str
    timeout: float
    log_level: str
    confidence_threshold: float
    discovery_use_gateway_bypass: bool
    discovery_bypass_function: str
    discovery_bypass_alias: str
    discovery_bypass_region: str
    discovery_bypass_profile: str | None


@dataclass(frozen=True)
class HarmonizationResult:
    """Communicate harmonization outcome in a consistent shape."""

    file_path: Path
    status: Literal["succeeded", "failed", "timeout"]
    description: str
    mapping_id: str | None = None


@dataclass(frozen=True)
class MappingRecommendationOption:
    """Capture a single recommended target for a source column."""

    target: str | None
    confidence: float | None
    raw: Mapping[str, object] | None = None


@dataclass(frozen=True)
class MappingSuggestion:
    """Group recommendation options for a single source column."""

    source_column: str
    options: tuple[MappingRecommendationOption, ...]
    raw: Mapping[str, object] | None = None


@dataclass(frozen=True)
class MappingDiscoveryResult:
    """Communicate column mapping recommendations for a dataset."""

    schema: str
    suggestions: tuple[MappingSuggestion, ...]
    raw: Mapping[str, object]
