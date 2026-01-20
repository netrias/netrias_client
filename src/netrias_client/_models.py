"""Define dataclasses and types for the client.

'why': capture configuration and results in typed, testable shapes
"""
from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Literal, TypeAlias, override

ManifestPayload: TypeAlias = dict[str, dict[str, dict[str, object]]]


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


@dataclass(frozen=True)
class MappingRecommendationOption:
    """Capture a single recommended target for a source column."""

    target: str | None
    confidence: float | None
    target_cde_id: int | None = None
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


@dataclass(frozen=True)
class PermissibleValue:
    """Represent a permissible value for a CDE."""

    pv_id: int
    value: str
    description: str | None
    is_active: bool
