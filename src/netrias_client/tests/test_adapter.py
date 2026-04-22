"""Guard the manifest-normalization boundary for `normalize_manifest_mapping`.

'why': ColumnMappingRecord is the canonical wire shape — partial or
mis-typed entries must surface a typed boundary error, not silently coerce
to None or propagate downstream as a TypedDict that lies about its fields.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import pytest

from netrias_client._adapter import REQUIRED_RECORD_KEYS, normalize_manifest_mapping
from netrias_client._errors import MappingValidationError
from netrias_client._models import ColumnMappingRecord


def _complete_entry() -> ColumnMappingRecord:
    """Build a fully-formed record for positive-path tests."""

    return {
        "column_name": "dx",
        "cde_key": "primary_diagnosis",
        "cde_id": 42,
        "harmonization": "harmonizable",
        "alternatives": [
            {"target": "primary_diagnosis", "confidence": 0.91, "harmonization": "harmonizable"},
        ],
    }


def test_manifest_roundtrip_preserves_complete_entry_and_none_slots() -> None:
    """A complete entry plus a None slot round-trip in place."""

    # Given — a manifest with one full record and one None placeholder
    record = _complete_entry()
    slots: list[ColumnMappingRecord | None] = [record, None]
    manifest: dict[str, object] = {"column_mappings": slots}
    assert slots != []  # negative: not yet normalized
    assert None in slots

    # When — we normalize the manifest
    result = normalize_manifest_mapping(manifest)

    # Then — positional parity is preserved, record pass-through
    assert result == [record, None]


def test_incomplete_manifest_entry_surfaces_boundary_error() -> None:
    """A dict missing required keys raises MappingValidationError with context."""

    # Given — a manifest entry missing harmonization + alternatives
    partial: dict[str, object] = {"column_name": "x", "cde_key": "y", "cde_id": 1}
    manifest: dict[str, object] = {"column_mappings": [partial]}
    assert isinstance(partial, Mapping)
    assert "harmonization" not in partial and "alternatives" not in partial

    # When / Then — normalization raises with the missing keys and indexed source
    with pytest.raises(MappingValidationError) as exc_info:
        _ = normalize_manifest_mapping(manifest)
    message = str(exc_info.value)
    assert "harmonization" in message
    assert "alternatives" in message
    assert "column_mappings[0]" in message


def test_non_object_manifest_entry_surfaces_boundary_error() -> None:
    """A scalar/list in place of a record raises with its wrong type named."""

    # Given — a manifest with a list where a record should be, at position 1
    entries: list[object] = [None, ["not", "an", "object"]]
    manifest: dict[str, object] = {"column_mappings": entries}
    assert not isinstance(entries[1], Mapping)

    # When / Then — normalization raises naming the wrong type and slot index
    with pytest.raises(MappingValidationError) as exc_info:
        _ = normalize_manifest_mapping(manifest)
    message = str(exc_info.value)
    assert "list" in message
    assert "column_mappings[1]" in message


def test_sample_manifest_fixture_conforms_to_column_mapping_record() -> None:
    """The shipped fixture must pass boundary validation; catches fixture drift."""

    # Given — the on-disk fixture JSON
    fixture_path = Path(__file__).parent / "fixtures" / "sample_manifest.json"
    manifest = cast(Mapping[str, object], json.loads(fixture_path.read_text(encoding="utf-8")))
    assert manifest.get("column_mappings"), "fixture must be non-empty for this pin to mean anything"

    # When — we normalize it
    result = normalize_manifest_mapping(manifest)

    # Then — every slot is a fully-formed ColumnMappingRecord (no None slots in this fixture)
    assert len(result) > 0
    for entry in result:
        assert entry is not None
        for key in REQUIRED_RECORD_KEYS:
            assert key in entry, f"fixture entry missing required key {key!r}"


@pytest.mark.parametrize("omitted_key", REQUIRED_RECORD_KEYS)
def test_entry_missing_any_single_required_key_raises_boundary_error(omitted_key: str) -> None:
    """For every required key, omitting just that one must raise MappingValidationError.

    'why': exhaustive cover over the five single-omission cases stands in for
    a Hypothesis property; Hypothesis is not a project dependency.
    """

    # Given — a record missing exactly `omitted_key`
    full = _complete_entry()
    partial = {k: v for k, v in full.items() if k != omitted_key}
    manifest: dict[str, object] = {"column_mappings": [partial]}
    assert omitted_key not in partial

    # When / Then — normalization raises naming the missing key
    with pytest.raises(MappingValidationError) as exc_info:
        _ = normalize_manifest_mapping(manifest)
    assert omitted_key in str(exc_info.value)
