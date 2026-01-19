"""Define client-specific exceptions.

'why': keep error taxonomy explicit and lightweight
"""
from __future__ import annotations


class NetriasClientError(Exception):
    """Base class for all client-specific exceptions."""


class ClientConfigurationError(NetriasClientError):
    """Raised when configuration is incomplete or malformed."""


class FileValidationError(NetriasClientError):
    """Raised for unreadable files, unsupported extensions, or size violations."""


class MappingValidationError(NetriasClientError):
    """Raised when mapping discovery inputs fail validation."""


class OutputLocationError(NetriasClientError):
    """Raised when the output path is unwritable or collides with an existing directory."""


class NetriasAPIUnavailable(NetriasClientError):
    """Raised for timeouts or network failures."""


class MappingDiscoveryError(NetriasClientError):
    """Raised when the mapping discovery API returns an error payload."""


class DataModelStoreError(NetriasClientError):
    """Raised when the Data Model Store API returns an error."""


class HarmonizationJobError(NetriasClientError):
    """Raised when the harmonization job fails or times out."""


class AsyncDiscoveryError(NetriasClientError):
    """Raised when async Step Functions execution fails or times out."""


class GatewayBypassError(NetriasClientError):
    """Raised when the direct Lambda invocation fails."""
