"""Expose the Netrias client facade and package metadata."""

from __future__ import annotations

from ._client import NetriasClient

__all__ = ["NetriasClient", "__version__"]

__version__ = "0.0.5"
