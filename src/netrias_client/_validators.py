"""Validate inputs for harmonization.

'why': fail fast with clear, actionable messages prior to network calls
"""
from __future__ import annotations

import os
from pathlib import Path

from ._errors import FileValidationError, MappingValidationError, OutputLocationError


# OBVIOUS HARD-CODED SIZE LIMIT: 250 MB maximum CSV size prior to upload
HARD_MAX_CSV_BYTES = 250 * 1024 * 1024


def validate_source_path(path: Path) -> Path:
    """Ensure the CSV exists, is a file, has a .csv extension, and respects size limits."""

    _require_exists(path, "source CSV not found")
    _require_is_file(path, "source path is not a file")
    _require_suffix(path, ".csv", "unsupported file extension for source CSV")
    _require_not_too_large(path)
    return path


def validate_manifest_path(path: Path) -> Path:
    """Ensure the manifest JSON exists and is a file."""

    _require_exists(path, "manifest JSON not found")
    _require_is_file(path, "manifest path is not a file")
    _require_suffix(path, ".json", "manifest must be a .json file")
    return path


def validate_output_path(path: Path | None, source_name: str, allow_versioning: bool = False) -> Path:
    """Return a valid output file path, creating parent directories when needed.

    Defaults to `<CWD>/<source_name>.harmonized.csv` when `path` is None or a directory.
    """

    candidate = _resolve_output_candidate(path, source_name)
    _ensure_parent(candidate)
    _require_parent_writable(candidate)
    if allow_versioning:
        candidate = _next_available_path(candidate)
    else:
        _require_not_exists(candidate)
    return candidate


def validate_target_schema(schema: str) -> str:
    """Ensure the target schema identifier is a non-empty string."""

    candidate = (schema or "").strip()
    if not candidate:
        raise MappingValidationError("target_schema must be a non-empty string")
    return candidate


def validate_target_version(version: str) -> str:
    """Ensure the target version identifier is a non-empty string."""

    candidate = (version or "").strip()
    if not candidate:
        raise MappingValidationError("target_version must be a non-empty string")
    return candidate


def validate_top_k(top_k: int | None) -> int | None:
    """Ensure top_k is a positive integer when provided."""

    if top_k is None:
        return None
    if top_k < 1:
        raise MappingValidationError("top_k must be a positive integer")
    return top_k


def _require_exists(path: Path, message: str) -> None:
    if not path.exists():
        raise FileValidationError(f"{message}: {path}")


def _require_is_file(path: Path, message: str) -> None:
    if not path.is_file():
        raise FileValidationError(f"{message}: {path}")


def _require_suffix(path: Path, suffix: str, message: str) -> None:
    if path.suffix.lower() != suffix:
        raise FileValidationError(f"{message}: {path.suffix}")


def _require_not_too_large(path: Path) -> None:
    try:
        size = os.path.getsize(path)
    except OSError as exc:
        raise FileValidationError(f"unable to stat source CSV: {exc}") from exc
    if size > HARD_MAX_CSV_BYTES:
        raise FileValidationError(
            f"source CSV exceeds hard-coded limit of {HARD_MAX_CSV_BYTES // (1024 * 1024)} MB (got {size} bytes)"
        )


def _resolve_output_candidate(path: Path | None, source_name: str) -> Path:
    if path is None:
        return Path.cwd() / f"{source_name}.harmonized.csv"
    if path.exists() and path.is_dir():
        return path / f"{source_name}.harmonized.csv"
    return path


def _ensure_parent(candidate: Path) -> None:
    parent = candidate.parent
    if not parent.exists():
        try:
            parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise OutputLocationError(f"unable to create output directory {parent}: {exc}") from exc


def _require_parent_writable(candidate: Path) -> None:
    parent = candidate.parent
    if parent.exists() and not os.access(parent, os.W_OK):
        raise OutputLocationError(f"output directory not writable: {parent}")


def _require_not_exists(candidate: Path) -> None:
    if candidate.exists():
        raise OutputLocationError(f"refusing to overwrite existing file: {candidate}")


def _next_available_path(candidate: Path) -> Path:
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    parent = candidate.parent
    index = 1
    while index < 1000:
        versioned = parent / f"{stem}.v{index}{suffix}"
        if not versioned.exists():
            return versioned
        index += 1
    raise OutputLocationError(
        f"unable to determine unique output path after {index - 1} attempts for {candidate}"
    )
