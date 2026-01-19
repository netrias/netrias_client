"""Expose the Netrias client facade and package metadata."""

from __future__ import annotations

from ._client import NetriasClient
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
from ._models import CDE, DataModel, HarmonizationResult, PermissibleValue

__all__ = [
    # Client
    "LOGGER_NAMESPACE",
    "NetriasClient",
    # Data models
    "DataModel",
    "CDE",
    "PermissibleValue",
    "HarmonizationResult",
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

__version__ = "0.2.0"
