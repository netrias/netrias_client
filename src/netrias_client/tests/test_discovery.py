"""Test mapping discovery workflows.

'why': guarantee recommendation calls integrate cleanly and preserve positional parity
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import httpx
import pytest

from netrias_client import ColumnKeyedManifestPayload, ColumnMappingRecord, NetriasClient, column_key_for_index
from netrias_client._errors import MappingDiscoveryError, MappingValidationError, NetriasAPIUnavailable

from ._utils import install_mock_transport, json_failure, json_success, transport_error


def _array_payload(results: list[dict[str, object]]) -> dict[str, object]:
    """Build an array-format discovery response body."""

    return {
        "statusCode": 200,
        "body": json.dumps({"results": _with_backend_column_names(results)}),
    }


def _recorded_payload(recorded: dict[str, object]) -> dict[str, object]:
    results = cast(list[dict[str, object]], recorded["results"])
    return {
        "statusCode": 200,
        "body": json.dumps({**recorded, "results": _with_backend_column_names(results)}),
    }


def _mapping_payload(results: dict[str, object]) -> dict[str, object]:
    """Build the production map-format discovery response body."""

    return {
        "statusCode": 200,
        "body": json.dumps({"results": results}),
    }


def _with_backend_column_names(results: list[dict[str, object]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for index, result in enumerate(results):
        column_name = result.get("column_name")
        if not isinstance(column_name, str):
            output.append(result)
            continue
        output.append({**result, "column_name": _backend_column_name(index, column_name)})
    return output


def _backend_column_name(index: int, header: str) -> str:
    cleaned = "".join(char if char.isalnum() else "_" for char in header.strip().lower())
    collapsed = "_".join(part for part in cleaned.split("_") if part)
    return f"{column_key_for_index(index)}__{collapsed or 'blank'}"


def _column_slots(manifest: ColumnKeyedManifestPayload, column_count: int) -> list[ColumnMappingRecord | None]:
    mappings = manifest["column_mappings"]
    return [mappings.get(column_key_for_index(index)) for index in range(column_count)]


def test_discover_mapping_from_tabular_success(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Return structured recommendations when the API succeeds."""

    payload = _array_payload(
        [
            {
                "column_name": "a",
                "matches": [
                    {"target": "Sample.name", "target_cde_id": 11, "confidence": 0.92, "harmonization": "harmonizable"},
                    {"target": "Sample.display_name", "target_cde_id": 12, "confidence": 0.5, "harmonization": "harmonizable"},
                ],
            },
            {"column_name": "b", "matches": []},
            {"column_name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_tabular(
        source_path=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
    )

    column_mappings = _column_slots(manifest, 3)
    assert len(column_mappings) == 3
    first = column_mappings[0]
    assert first is not None
    assert first["column_name"] == "a"
    assert first["cde_key"] == "Sample.name"
    assert first["cde_id"] == 11
    assert "targetField" not in first

    alternatives = first["alternatives"]
    assert len(alternatives) == 2
    assert alternatives[0]["target"] == "Sample.name"
    assert alternatives[1]["target"] == "Sample.display_name"

    assert column_mappings[1] is None
    assert column_mappings[2] is None

    request = capture.requests[0]
    assert request.headers.get("x-api-key") == "test-api-key"
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    assert content.get("target_schema") == "ccdi"
    assert content.get("target_version") == "v1"


def test_discover_mapping_from_tabular_samples_data(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """tabular wrapper derives samples and forwards to discovery API."""

    payload = _array_payload(
        [
            {"column_name": "a", "matches": []},
            {"column_name": "b", "matches": []},
            {"column_name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_tabular(
        source_path=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
        sample_limit=1,
    )

    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    data = cast(dict[str, list[str]], content.get("data", {}))
    assert list(data) == ["col_0000__a", "col_0001__b", "col_0002__c"]
    assert isinstance(manifest["column_mappings"], dict)


def test_discover_mapping_from_tabular_handles_api_error(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Raise MappingDiscoveryError when the API returns a non-2xx status."""

    capture = json_failure({"message": "unsupported schema"}, status_code=400)
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(MappingDiscoveryError) as exc:
        _ = configured_client.discover_mapping_from_tabular(
            source_path=sample_csv_path,
            target_schema="bogus",
            target_version="v1",
        )

    assert "unsupported schema" in str(exc.value)


def test_discover_mapping_from_tabular_raises_on_transport_error(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Raise NetriasAPIUnavailable when transport fails."""

    capture = transport_error(httpx.ConnectError("boom"))
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(NetriasAPIUnavailable):
        _ = configured_client.discover_mapping_from_tabular(
            source_path=sample_csv_path,
            target_schema="ccdi",
            target_version="v1",
        )


@pytest.mark.asyncio
async def test_discover_mapping_from_tabular_async(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Async variant yields the same structure as the sync function."""

    payload = _array_payload(
        [
            {"column_name": "a", "matches": []},
            {"column_name": "b", "matches": []},
            {"column_name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = await configured_client.discover_mapping_from_tabular_async(
        source_path=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
    )
    assert isinstance(manifest["column_mappings"], dict)


def test_discover_mapping_from_tabular_sends_top_k_parameter(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Verify top_k parameter is included in the request payload."""

    payload = _array_payload(
        [
            {"column_name": "a", "matches": []},
            {"column_name": "b", "matches": []},
            {"column_name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    _ = configured_client.discover_mapping_from_tabular(
        source_path=sample_csv_path,
        target_schema="gc",
        target_version="v1",
        top_k=5,
    )

    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    assert content.get("top_k") == 5


@pytest.mark.asyncio
async def test_discover_mapping_from_tabular_async_includes_version(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Async tabular discovery wrapper includes target_version in request."""

    payload = _array_payload(
        [
            {"column_name": "a", "matches": []},
            {"column_name": "b", "matches": []},
            {"column_name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = await configured_client.discover_mapping_from_tabular_async(
        source_path=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
        sample_limit=1,
    )

    assert isinstance(manifest["column_mappings"], dict)
    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    assert content.get("target_version") == "v1"


def test_discover_mapping_handles_array_results_format(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Handle the canonical array-format results from the lambda."""

    payload = _array_payload(
        [
            {
                "column_name": "a",
                "matches": [
                    {"target": "age", "target_cde_id": 900, "confidence": 1.0, "harmonization": "numeric"},
                    {"target": "ageUnit", "target_cde_id": 904, "confidence": 0.1, "harmonization": "harmonizable"},
                ],
            },
            {
                "column_name": "b",
                "matches": [
                    {"target": "sex", "target_cde_id": 901, "confidence": 0.95, "harmonization": "harmonizable"},
                ],
            },
            {"column_name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_tabular(
        source_path=sample_csv_path,
        target_schema="gc",
        target_version="v1",
    )

    column_mappings = _column_slots(manifest, 3)
    first = column_mappings[0]
    assert first is not None
    assert first["column_name"] == "a"
    assert "targetField" not in first

    second = column_mappings[1]
    assert second is not None
    assert second["column_name"] == "b"
    assert "targetField" not in second

    assert column_mappings[2] is None


def test_discover_mapping_handles_production_results_mapping(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Handle the live production response keyed by outbound column name."""

    payload = _mapping_payload(
        {
            "col_0000__a": [{"target": "age", "target_cde_id": 900, "similarity": 0.93}],
            "col_0001__b": [{"target": "sex", "target_cde_id": 901, "similarity": 0.91}],
            "col_0002__c": [{"target": "No_Matches_Found", "target_cde_id": None, "similarity": 0.0}],
        }
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_tabular(
        source_path=sample_csv_path,
        target_schema="gc",
        target_version="v1",
    )

    column_mappings = _column_slots(manifest, 3)
    first = column_mappings[0]
    assert first is not None
    assert first["column_name"] == "a"
    assert first["cde_key"] == "age"
    assert first["harmonization"] == "harmonizable"
    assert first["alternatives"][0]["confidence"] == 0.93

    second = column_mappings[1]
    assert second is not None
    assert second["column_name"] == "b"
    assert second["cde_key"] == "sex"

    assert column_mappings[2] is None


# ---- Wire-shape contract tests ----


def test_discovery_entry_uses_column_name(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Mapped entries carry column_name (not the legacy 'name' key)."""

    payload = _array_payload(
        [
            {
                "column_name": "a",
                "matches": [
                    {
                        "target": "A_target",
                        "target_cde_id": 1,
                        "confidence": 0.95,
                        "harmonization": "harmonizable",
                    }
                ],
            },
            {"column_name": "b", "matches": []},
            {"column_name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_tabular(
        source_path=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
    )

    entry = _column_slots(manifest, 3)[0]
    assert entry is not None
    # Renamed from legacy "name" — must use "column_name"
    assert entry["column_name"] == "a"
    assert "name" not in entry


def test_discovery_entry_has_no_target_field(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """targetField must not appear in any mapped entry — it was removed from _make_entry."""

    payload = _array_payload(
        [
            {
                "column_name": "a",
                "matches": [
                    {
                        "target": "A_target",
                        "target_cde_id": 1,
                        "confidence": 0.95,
                        "harmonization": "harmonizable",
                    }
                ],
            },
            {"column_name": "b", "matches": []},
            {"column_name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_tabular(
        source_path=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
    )

    entry = _column_slots(manifest, 3)[0]
    assert entry is not None
    assert "targetField" not in entry


# ---- Positional parity tests (A/B/C) ----


def test_positional_parity_all_columns_matched(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Trivial case: all three tabular columns match above threshold — length equals tabular column count."""

    # Given a tabular file with 3 columns, all with non-empty samples, and a response that
    # covers all three. Before invocation no prior manifest exists.
    assert sample_csv_path.read_text(encoding="utf-8").splitlines()[0] == "a,b,c"

    payload = _array_payload(
        [
            {
                "column_name": "a",
                "matches": [
                    {
                        "target": "A_target",
                        "target_cde_id": 101,
                        "confidence": 0.99,
                        "harmonization": "harmonizable",
                    }
                ],
            },
            {
                "column_name": "b",
                "matches": [
                    {
                        "target": "B_target",
                        "target_cde_id": 102,
                        "confidence": 0.95,
                        "harmonization": "harmonizable",
                    }
                ],
            },
            {
                "column_name": "c",
                "matches": [
                    {
                        "target": "C_target",
                        "target_cde_id": 103,
                        "confidence": 0.9,
                        "harmonization": "harmonizable",
                    }
                ],
            },
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    # When discover_mapping_from_tabular runs
    manifest = configured_client.discover_mapping_from_tabular(
        source_path=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
    )

    # Then request length == 3, manifest length == 3, every entry is non-None with matching column_name
    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    data = cast(dict[str, list[str]], content.get("data", {}))
    assert len(data) == 3
    assert list(data) == ["col_0000__a", "col_0001__b", "col_0002__c"]

    column_mappings = _column_slots(manifest, 3)
    assert len(column_mappings) == 3
    for index, header in enumerate(("a", "b", "c")):
        entry = column_mappings[index]
        assert entry is not None
        assert entry["column_name"] == header


def test_positional_parity_empty_column_preserved_and_mismatch_detected(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    empty_middle_csv_path: Path,
) -> None:
    """Empty-value columns are sent with values=[]; response mismatch fails fast."""

    # Given a tabular file where column 'b' has all-empty values
    csv_path = empty_middle_csv_path

    # Negative assertion: before discovery, column b has zero non-empty samples
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    data_rows = [line.split(",") for line in lines[1:]]
    assert sum(1 for row in data_rows if row[1].strip()) == 0

    # Backend returns only 2 results (drops column 'b') — must raise
    payload = _array_payload(
        [
            {"column_name": "a", "matches": [{"target": "A", "target_cde_id": 1, "confidence": 0.9, "harmonization": "harmonizable"}]},
            {"column_name": "c", "matches": [{"target": "C", "target_cde_id": 3, "confidence": 0.9, "harmonization": "harmonizable"}]},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    # When discovery runs, it must raise rather than silently shift column_ids
    with pytest.raises(MappingDiscoveryError) as exc:
        _ = configured_client.discover_mapping_from_tabular(
            source_path=csv_path,
            target_schema="ccdi",
            target_version="v1",
        )

    message = str(exc.value)
    assert "expected 3" in message
    assert "found 2" in message

    # And the outbound request preserved the empty-value column at position 1
    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    data = cast(dict[str, list[str]], content.get("data", {}))
    assert len(data) == 3
    assert data["col_0001__b"] == []


def test_positional_parity_below_threshold_becomes_none(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Below-threshold columns keep their position with None; manifest length equals tabular column count."""

    # Given a tabular file with 3 columns where column 'b' has a top confidence of 0.2
    # against a threshold of 0.8. Before: backend returns a result for 'b' but
    # no option meets the threshold.
    payload = _array_payload(
        [
            {"column_name": "a", "matches": [{"target": "A", "target_cde_id": 1, "confidence": 0.95, "harmonization": "harmonizable"}]},
            {"column_name": "b", "matches": [{"target": "weak", "target_cde_id": 2, "confidence": 0.2, "harmonization": "harmonizable"}]},
            {"column_name": "c", "matches": [{"target": "C", "target_cde_id": 3, "confidence": 0.9, "harmonization": "harmonizable"}]},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    # When discovery runs with a strict threshold
    manifest = configured_client.discover_mapping_from_tabular(
        source_path=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
        confidence_threshold=0.8,
    )

    # Then column_mappings has length 3 and the below-threshold slot is None
    column_mappings = _column_slots(manifest, 3)
    assert len(column_mappings) == 3
    assert column_mappings[0] is not None
    assert column_mappings[1] is None
    assert column_mappings[2] is not None


def test_parses_real_api_confidence_and_emits_cde_key(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fixture-backed contract test: the SDK reads 'confidence' (the real API key),
    emits 'cde_key' on every non-None entry, and drops slots missing target_cde_id.
    """

    # Given a hand-synthesized recorded API payload using the real key names
    fixture_path = Path(__file__).parent / "fixtures" / "recommend_synthetic_3col.json"
    csv_path = Path(__file__).parent / "fixtures" / "synthetic_3col.csv"
    recorded = cast(dict[str, object], json.loads(fixture_path.read_text(encoding="utf-8")))

    # Wrap the recorded body in the lambda's statusCode/body envelope
    payload = _recorded_payload(recorded)
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    # When discovery runs
    manifest = configured_client.discover_mapping_from_tabular(
        source_path=csv_path,
        target_schema="gc",
        target_version="v1",
        confidence_threshold=0.7,
    )

    # Then the manifest has length 3 and the slots reflect the three scenarios
    column_mappings = _column_slots(manifest, 3)
    assert len(column_mappings) == 3

    # Column 0 — above threshold, has cde_id and cde_key
    diagnosis_entry = column_mappings[0]
    assert diagnosis_entry is not None
    assert diagnosis_entry["column_name"] == "diagnosis"
    assert diagnosis_entry["cde_key"] == "disease_type"
    assert diagnosis_entry["cde_id"] == 323

    # Column 1 — all matches below 0.7 threshold — slot is None
    assert column_mappings[1] is None

    # Column 2 — top match above threshold but missing target_cde_id — slot is None
    assert column_mappings[2] is None


def test_alternative_entries_preserve_confidence_field(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AlternativeEntry values expose 'confidence' (not 'similarity') downstream."""

    fixture_path = Path(__file__).parent / "fixtures" / "recommend_synthetic_3col.json"
    csv_path = Path(__file__).parent / "fixtures" / "synthetic_3col.csv"
    recorded = cast(dict[str, object], json.loads(fixture_path.read_text(encoding="utf-8")))
    payload = _recorded_payload(recorded)
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_tabular(
        source_path=csv_path,
        target_schema="gc",
        target_version="v1",
        confidence_threshold=0.7,
    )

    diagnosis_entry = _column_slots(manifest, 3)[0]
    assert diagnosis_entry is not None
    alternatives = diagnosis_entry["alternatives"]
    assert len(alternatives) >= 1
    top = alternatives[0]
    assert top["target"] == "disease_type"
    assert top["confidence"] == 0.85
    assert "similarity" not in top


# ---- Harmonization field tests ----


_HARMONIZATION_ENUM: set[str] = {"harmonizable", "no_permissible_values", "numeric"}


def _assert_entry_carries_harmonization(entry: object) -> None:
    """Every non-None column entry and each of its alternatives must carry a valid harmonization."""
    assert entry is not None, "all four columns are above threshold in this fixture"
    entry_dict = cast(dict[str, object], entry)
    assert "harmonization" in entry_dict
    assert entry_dict["harmonization"] in _HARMONIZATION_ENUM
    alternatives = cast(list[dict[str, object]], entry_dict["alternatives"])
    for alt in alternatives:
        assert "harmonization" in alt
        assert alt["harmonization"] in _HARMONIZATION_ENUM


def _raw_top_match_harmonizations(recorded: dict[str, object]) -> set[str]:
    """Collect the top-match harmonization value from every result entry in a raw fixture."""
    raw_results = cast(list[dict[str, object]], recorded["results"])
    harmonizations: set[str] = set()
    for result in raw_results:
        matches = cast(list[dict[str, object]], result["matches"])
        if matches:
            harmonizations.add(cast(str, matches[0]["harmonization"]))
    return harmonizations


def test_harmonization_surfaces_on_every_alternative_and_top_level(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every non-None entry carries harmonization at the top level; every alternative carries it too.

    The four-column fixture exercises all three harmonization values end-to-end
    so a regression in any code path (boundary parse, adapter format, entry build)
    surfaces as a failed assertion on the corresponding column.
    """

    fixture_path = Path(__file__).parent / "fixtures" / "recommend_synthetic_4col_harmonization.json"
    csv_path = Path(__file__).parent / "fixtures" / "synthetic_4col_harmonization.csv"
    recorded = cast(dict[str, object], json.loads(fixture_path.read_text(encoding="utf-8")))
    payload = _recorded_payload(recorded)
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    # Precondition: the raw fixture contains the three distinct harmonization values
    # on the four top-match slots so the assertions below are exercising real data.
    assert _raw_top_match_harmonizations(recorded) == _HARMONIZATION_ENUM

    manifest = configured_client.discover_mapping_from_tabular(
        source_path=csv_path,
        target_schema="gc",
        target_version="v1",
        confidence_threshold=0.4,
    )

    column_mappings = _column_slots(manifest, 4)
    assert len(column_mappings) == 4
    for entry in column_mappings:
        _assert_entry_carries_harmonization(entry)

    # Column-specific checks — top-level mirrors the top alternative per plan contract.
    first = column_mappings[0]
    third = column_mappings[2]
    fourth = column_mappings[3]
    assert first is not None
    assert third is not None
    assert fourth is not None
    assert first["harmonization"] == "harmonizable"
    assert third["harmonization"] == "no_permissible_values"
    assert third["cde_key"] == "middle_name"
    assert fourth["harmonization"] == "numeric"


def test_positional_parity_rejects_reordered_response_of_equal_length(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Equal-length but reordered responses must raise, not silently shift column_ids.

    'why': the SDK uses the response array index as column_id. A backend regression
    that sorts results alphabetically (or any other reorder) would keep the length
    invariant intact but bind every CDE to the wrong source column. The parity
    cross-check catches that by comparing response column_name against the
    outbound column_name at each index.
    """

    # Given a tabular file with headers [a, b, c] and a response that returns them in [c, a, b] order
    assert sample_csv_path.read_text(encoding="utf-8").splitlines()[0] == "a,b,c"
    payload = _array_payload(
        [
            {
                "column_name": "c",
                "matches": [
                    {"target": "C", "target_cde_id": 3, "confidence": 0.9, "harmonization": "harmonizable"},
                ],
            },
            {
                "column_name": "a",
                "matches": [
                    {"target": "A", "target_cde_id": 1, "confidence": 0.9, "harmonization": "harmonizable"},
                ],
            },
            {
                "column_name": "b",
                "matches": [
                    {"target": "B", "target_cde_id": 2, "confidence": 0.9, "harmonization": "harmonizable"},
                ],
            },
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    # When / Then — parity cross-check fails at the first mismatched slot (index 0: expected "a", found "c")
    with pytest.raises(MappingDiscoveryError) as exc:
        _ = configured_client.discover_mapping_from_tabular(
            source_path=sample_csv_path,
            target_schema="ccdi",
            target_version="v1",
        )

    message = str(exc.value)
    assert "column_name mismatch" in message
    assert "'col_0000__a'" in message
    assert "'col_0000__c'" in message


def test_zero_column_tabular_file_rejected_at_boundary(
    configured_client: NetriasClient,
    tmp_path: Path,
) -> None:
    """A tabular file with no header row must raise MappingValidationError, not a phantom success.

    'why': without this guard, an empty-header tabular file would send columns=[] upstream,
    receive an empty results array back, and the adapter would return a manifest
    with column_mappings=[] — a trivially "successful" result for a degenerate input.
    """

    # Given a tabular file with zero columns (entirely empty file)
    csv_path = tmp_path / "empty.csv"
    _ = csv_path.write_text("", encoding="utf-8")
    assert csv_path.read_text(encoding="utf-8") == ""

    # When / Then — discovery rejects before any request goes out
    with pytest.raises(MappingValidationError) as exc:
        _ = configured_client.discover_mapping_from_tabular(
            source_path=csv_path,
            target_schema="ccdi",
            target_version="v1",
        )

    message = str(exc.value)
    assert "no header row" in message
    assert "0 columns" in message


def test_discovery_strict_rejects_response_missing_harmonization(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """SDK must fail loudly when the API omits harmonization on a match.

    Rationale: Phase A deploy precedes Phase B release, so by the time SDK 0.5.0
    is pinned the Lambda always emits the field. A missing field therefore signals
    a real Lambda regression, not rolling-deploy skew — must surface, not default.
    """

    payload = _array_payload(
        [
            {
                "column_name": "a",
                "matches": [
                    {"target": "Sample.name", "target_cde_id": 11, "confidence": 0.92},
                ],
            },
            {"column_name": "b", "matches": []},
            {"column_name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(MappingDiscoveryError) as exc:
        _ = configured_client.discover_mapping_from_tabular(
            source_path=sample_csv_path,
            target_schema="ccdi",
            target_version="v1",
        )

    assert "harmonization" in str(exc.value)
