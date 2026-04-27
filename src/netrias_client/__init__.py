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
    ColumnKeyedManifestPayload,
    ColumnMappingRecord,
    ColumnSamples,
    DataModel,
    DataModelVersion,
    Harmonization,
    HarmonizationResult,
    ManifestPayload,
)
from ._tabular import (
    TabularColumn,
    TabularDataset,
    TabularFormat,
    column_key_for_index,
    dataset_from_rows,
    get_tabular_format,
    is_supported_tabular_content_type,
    read_tabular,
    write_tabular,
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
    "ColumnKeyedManifestPayload",
    "ColumnMappingRecord",
    "ColumnSamples",
    "Harmonization",
    "ManifestPayload",
    "TabularColumn",
    "TabularDataset",
    "TabularFormat",
    "column_key_for_index",
    "dataset_from_rows",
    "get_tabular_format",
    "is_supported_tabular_content_type",
    "read_tabular",
    "write_tabular",
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

__version__ = "0.6.0"
