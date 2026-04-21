"""Expose the Netrias client facade and package metadata."""

from __future__ import annotations

from ._client import NetriasClient
from ._config import Environment
from ._logging import LOGGER_NAMESPACE
from ._errors import (
    ClientConfigurationError,
    DataModelStoreError,
    FileValidationError,
    HarmonizationJobError,
    MappingDiscoveryError,
    MappingValidationError,
    NetriasAPIUnavailable,
    NetriasClientError,
    OutputLocationError,
)
from ._models import (
    CDE,
    AlternativeEntry,
    ColumnMappingRecord,
    ColumnSamples,
    DataModel,
    DataModelVersion,
    HarmonizationResult,
    ManifestPayload,
)

__all__ = [
    # Client
    "Environment",
    "LOGGER_NAMESPACE",
    "NetriasClient",
    # Data models
    "DataModel",
    "DataModelVersion",
    "CDE",
    "HarmonizationResult",
    # Manifest wire shapes
    "AlternativeEntry",
    "ColumnMappingRecord",
    "ColumnSamples",
    "ManifestPayload",
    # Exceptions
    "NetriasClientError",
    "ClientConfigurationError",
    "DataModelStoreError",
    "FileValidationError",
    "HarmonizationJobError",
    "MappingDiscoveryError",
    "MappingValidationError",
    "NetriasAPIUnavailable",
    "OutputLocationError",
    # Metadata
    "__version__",
]

__version__ = "0.4.1"
