"""Expose configure and harmonize API.

'why': provide a small, explicit surface for client configuration and harmonization
"""
from ._config import configure
from ._core import harmonize, harmonize_async
from ._discovery import (
    discover_mapping,
    discover_mapping_async,
    discover_cde_mapping,
    discover_mapping_from_csv_async,
)

__all__ = [
    "discover_mapping",
    "discover_mapping_async",
    "discover_cde_mapping",
    "discover_mapping_from_csv_async",
    "configure",
    "harmonize",
    "harmonize_async",
]

__version__ = "0.0.1"
