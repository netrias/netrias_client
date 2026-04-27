"""Define dataclasses and types for the client.

'why': capture configuration and results in typed, testable shapes
"""
from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Final, Literal, NotRequired, TypedDict, get_args, override


COLUMN_NAME_KEY: Final[str] = "column_name"
"""Wire-format key for per-column identity across request, response, and manifest.

'why': one owner for the string literal so request/response/manifest layers cannot
drift from the TypedDict field name — boundary parsers and manifest writers both
reference this constant instead of repeating the raw literal.
"""


class ColumnSamples(TypedDict):
    """Outbound per-column request payload — one entry per tabular column position."""

    column_name: str
    values: list[str]


Harmonization = Literal["harmonizable", "no_permissible_values", "numeric"]
"""'why': mirrors the Lambda StrEnum; closed set so callers can exhaustively match.

Canonical ownership lives in the recommendation Lambda's `Harmonization` StrEnum;
this Literal is the SDK's boundary-adapted view of those values.
"""


HARMONIZATION_VALUES: Final[frozenset[str]] = frozenset(get_args(Harmonization))
"""Runtime mirror of the `Harmonization` Literal's allowed strings.

'why': boundary validators need a concrete membership set; deriving from
`get_args(Harmonization)` keeps the Literal (compile-time) and the frozenset
(runtime) from drifting — one source of truth for the allowed harmonization values.
"""


class AlternativeEntry(TypedDict):
    """A ranked candidate target for a source column, sorted by confidence descending.

    'why': the score key is 'confidence' end-to-end — same name as the upstream API,
    no translation at the SDK boundary.
    """

    target: str
    confidence: float
    harmonization: Harmonization
    cde_id: NotRequired[int]


class ColumnMappingRecord(TypedDict):
    """Manifest entry for a column matched above the confidence threshold.

    Every non-None entry carries both `cde_key` (the ontology string id) and
    `cde_id` (the numeric CDE id). The adapter drops the slot entirely if the
    top eligible option lacks a cde_id, so the invariant is real.

    Example (position 0 of a 2-column CSV with header ["dx", "site"]):
        {"column_name": "dx", "cde_key": "primary_diagnosis", "cde_id": 42,
         "harmonization": "harmonizable",
         "alternatives": [{"target": "primary_diagnosis", "confidence": 0.91,
                           "harmonization": "harmonizable", "cde_id": 42}]}
    """

    column_name: str
    cde_key: str
    cde_id: int
    harmonization: Harmonization
    alternatives: list[AlternativeEntry]


class ManifestPayload(TypedDict):
    """Backend-adapted manifest shape; list position encodes source column position."""

    column_mappings: list[ColumnMappingRecord | None]


class ColumnKeyedManifestPayload(TypedDict):
    """Manifest keyed by stable `col_0000`-style source column identity."""

    column_mappings: dict[str, ColumnMappingRecord]


class LogLevel(str, Enum):
    """Enumerate supported logging levels for the client."""

    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"


@dataclass(frozen=True)
class Settings:
    """Capture runtime settings for API calls."""

    api_key: str
    discovery_url: str
    harmonization_url: str
    timeout: float
    log_level: LogLevel
    discovery_use_gateway_bypass: bool
    log_directory: Path | None
    data_model_store_endpoints: DataModelStoreEndpoints | None = None
    discovery_use_async_api: bool = False

    @override
    def __repr__(self) -> str:
        """Mask API key to prevent accidental exposure in logs/debug output."""
        # 'why': show first 3 + last 3 only when key is long enough to avoid overlap
        masked_key = f"{self.api_key[:3]}...{self.api_key[-3:]}" if len(self.api_key) > 8 else "***"
        return (
            f"Settings(api_key={masked_key!r}, discovery_url={self.discovery_url!r}, "
            f"harmonization_url={self.harmonization_url!r}, timeout={self.timeout!r}, "
            f"log_level={self.log_level!r}, discovery_use_gateway_bypass={self.discovery_use_gateway_bypass!r}, "
            f"log_directory={self.log_directory!r}, data_model_store_endpoints={self.data_model_store_endpoints!r}, "
            f"discovery_use_async_api={self.discovery_use_async_api!r})"
        )


@dataclass(frozen=True)
class OperationContext:
    """Bundle settings and logger for atomic snapshotting.

    'why': ensure settings and logger are consistent for thread-safe operations
    """

    settings: Settings
    logger: logging.Logger


@dataclass(frozen=True)
class HarmonizationResult:
    """Communicate harmonization outcome in a consistent shape."""

    file_path: Path
    status: Literal["succeeded", "failed", "timeout"]
    description: str
    mapping_id: str | None = None
    manifest_path: Path | None = None


@dataclass(frozen=True)
class MappingRecommendationOption:
    """Capture a single recommended target for a source column."""

    target: str | None
    confidence: float | None
    harmonization: Harmonization
    target_cde_id: int | None = None
    raw: Mapping[str, object] | None = None


@dataclass(frozen=True)
class MappingSuggestion:
    """Group recommendation options for a single source column."""

    source_column: str
    options: tuple[MappingRecommendationOption, ...]
    raw: Mapping[str, object] | None = None
    column_id: int | None = None


@dataclass(frozen=True)
class MappingDiscoveryResult:
    """Communicate column mapping recommendations for a dataset."""

    schema: str
    suggestions: tuple[MappingSuggestion, ...]
    raw: Mapping[str, object]


@dataclass(frozen=True)
class DataModelStoreEndpoints:
    """Encapsulate Data Model Store endpoint URLs for swappability.

    'why': endpoints may change; grouping them enables single-point override
    """

    base_url: str


@dataclass(frozen=True)
class DataModelVersion:
    """Represent a version of a data model."""

    version_label: str


@dataclass(frozen=True)
class DataModel:
    """Represent a data commons/model from the Data Model Store."""

    data_commons_id: int
    key: str
    name: str
    description: str | None
    is_active: bool
    versions: tuple[DataModelVersion, ...] | None = None


@dataclass(frozen=True)
class CDE:
    """Represent a Common Data Element within a data model version."""

    cde_key: str
    cde_id: int
    cde_version_id: int
    description: str | None = None
