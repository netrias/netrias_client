"""Test mapping discovery workflows.

'why': guarantee recommendation calls integrate cleanly and preserve positional parity
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import httpx
import pytest

from netrias_client import NetriasClient
from netrias_client._errors import MappingDiscoveryError, NetriasAPIUnavailable

from ._utils import install_mock_transport, json_failure, json_success, transport_error


def _array_payload(results: list[dict[str, object]]) -> dict[str, object]:
    """Build an array-format discovery response body."""

    return {
        "statusCode": 200,
        "body": json.dumps({"results": results}),
    }


def test_discover_mapping_from_csv_success(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Return structured recommendations when the API succeeds."""

    payload = _array_payload(
        [
            {
                "name": "a",
                "matches": [
                    {"target": "Sample.name", "target_cde_id": 11, "confidence": 0.92, "harmonization": "harmonizable"},
                    {"target": "Sample.display_name", "target_cde_id": 12, "confidence": 0.5, "harmonization": "harmonizable"},
                ],
            },
            {"name": "b", "matches": []},
            {"name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_csv(
        source_csv=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
    )

    column_mappings = manifest["column_mappings"]
    assert isinstance(column_mappings, list)
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


def test_discover_mapping_from_csv_samples_csv_data(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """CSV convenience wrapper derives samples and forwards to discovery API."""

    payload = _array_payload(
        [
            {"name": "a", "matches": []},
            {"name": "b", "matches": []},
            {"name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_csv(
        source_csv=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
        sample_limit=1,
    )

    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    columns_section = cast(list[dict[str, object]], content.get("columns", []))
    column_names = [entry.get("name") for entry in columns_section]
    assert column_names == ["a", "b", "c"]
    assert isinstance(manifest["column_mappings"], list)


def test_discover_mapping_from_csv_handles_api_error(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Raise MappingDiscoveryError when the API returns a non-2xx status."""

    capture = json_failure({"message": "unsupported schema"}, status_code=400)
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(MappingDiscoveryError) as exc:
        _ = configured_client.discover_mapping_from_csv(
            source_csv=sample_csv_path,
            target_schema="bogus",
            target_version="v1",
        )

    assert "unsupported schema" in str(exc.value)


def test_discover_mapping_from_csv_raises_on_transport_error(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Raise NetriasAPIUnavailable when transport fails."""

    capture = transport_error(httpx.ConnectError("boom"))
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(NetriasAPIUnavailable):
        _ = configured_client.discover_mapping_from_csv(
            source_csv=sample_csv_path,
            target_schema="ccdi",
            target_version="v1",
        )


@pytest.mark.asyncio
async def test_discover_mapping_from_csv_async(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Async variant yields the same structure as the sync function."""

    payload = _array_payload(
        [
            {"name": "a", "matches": []},
            {"name": "b", "matches": []},
            {"name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = await configured_client.discover_mapping_from_csv_async(
        source_csv=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
    )
    assert isinstance(manifest["column_mappings"], list)


def test_discover_mapping_from_csv_sends_top_k_parameter(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Verify top_k parameter is included in the request payload."""

    payload = _array_payload(
        [
            {"name": "a", "matches": []},
            {"name": "b", "matches": []},
            {"name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    _ = configured_client.discover_mapping_from_csv(
        source_csv=sample_csv_path,
        target_schema="gc",
        target_version="v1",
        top_k=5,
    )

    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    assert content.get("top_k") == 5


@pytest.mark.asyncio
async def test_discover_mapping_from_csv_async_includes_version(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Async CSV discovery wrapper includes target_version in request."""

    payload = _array_payload(
        [
            {"name": "a", "matches": []},
            {"name": "b", "matches": []},
            {"name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = await configured_client.discover_mapping_from_csv_async(
        source_csv=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
        sample_limit=1,
    )

    assert isinstance(manifest["column_mappings"], list)
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
                "name": "a",
                "matches": [
                    {"target": "age", "target_cde_id": 900, "confidence": 1.0, "harmonization": "numeric"},
                    {"target": "ageUnit", "target_cde_id": 904, "confidence": 0.1, "harmonization": "harmonizable"},
                ],
            },
            {
                "name": "b",
                "matches": [
                    {"target": "sex", "target_cde_id": 901, "confidence": 0.95, "harmonization": "harmonizable"},
                ],
            },
            {"name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_csv(
        source_csv=sample_csv_path,
        target_schema="gc",
        target_version="v1",
    )

    column_mappings = manifest["column_mappings"]
    assert len(column_mappings) == 3
    first = column_mappings[0]
    assert first is not None
    assert first["column_name"] == "a"
    assert "targetField" not in first

    second = column_mappings[1]
    assert second is not None
    assert second["column_name"] == "b"
    assert "targetField" not in second

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
            {"name": "a", "matches": [{"target": "A_target", "target_cde_id": 1, "confidence": 0.95, "harmonization": "harmonizable"}]},
            {"name": "b", "matches": []},
            {"name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_csv(
        source_csv=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
    )

    entry = manifest["column_mappings"][0]
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
            {"name": "a", "matches": [{"target": "A_target", "target_cde_id": 1, "confidence": 0.95, "harmonization": "harmonizable"}]},
            {"name": "b", "matches": []},
            {"name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_csv(
        source_csv=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
    )

    entry = manifest["column_mappings"][0]
    assert entry is not None
    assert "targetField" not in entry


# ---- Positional parity tests (A/B/C) ----


def test_positional_parity_all_columns_matched(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Trivial case: all three CSV columns match above threshold — length equals CSV column count."""

    # Given a CSV with 3 columns, all with non-empty samples, and a response that
    # covers all three. Before invocation no prior manifest exists.
    assert sample_csv_path.read_text(encoding="utf-8").splitlines()[0] == "a,b,c"

    payload = _array_payload(
        [
            {"name": "a", "matches": [{"target": "A_target", "target_cde_id": 101, "confidence": 0.99, "harmonization": "harmonizable"}]},
            {"name": "b", "matches": [{"target": "B_target", "target_cde_id": 102, "confidence": 0.95, "harmonization": "harmonizable"}]},
            {"name": "c", "matches": [{"target": "C_target", "target_cde_id": 103, "confidence": 0.9, "harmonization": "harmonizable"}]},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    # When discover_mapping_from_csv runs
    manifest = configured_client.discover_mapping_from_csv(
        source_csv=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
    )

    # Then request length == 3, manifest length == 3, every entry is non-None with matching name
    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    columns_section = cast(list[dict[str, object]], content.get("columns", []))
    assert len(columns_section) == 3
    assert [entry["name"] for entry in columns_section] == ["a", "b", "c"]

    column_mappings = manifest["column_mappings"]
    assert len(column_mappings) == 3
    for index, header in enumerate(("a", "b", "c")):
        entry = column_mappings[index]
        assert entry is not None
        assert entry["column_name"] == header


def test_positional_parity_empty_column_preserved_and_mismatch_detected(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Empty-value columns are sent with values=[]; response mismatch fails fast."""

    # Given a CSV where column 'b' has all-empty values
    csv_path = tmp_path / "empty_middle.csv"
    _ = csv_path.write_text("a,b,c\n1,,3\n4,,6\n", encoding="utf-8")

    # Negative assertion: before discovery, column b has zero non-empty samples
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    data_rows = [line.split(",") for line in lines[1:]]
    assert sum(1 for row in data_rows if row[1].strip()) == 0

    # Backend returns only 2 results (drops column 'b') — must raise
    payload = _array_payload(
        [
            {"name": "a", "matches": [{"target": "A", "target_cde_id": 1, "confidence": 0.9, "harmonization": "harmonizable"}]},
            {"name": "c", "matches": [{"target": "C", "target_cde_id": 3, "confidence": 0.9, "harmonization": "harmonizable"}]},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    # When discovery runs, it must raise rather than silently shift column_ids
    with pytest.raises(MappingDiscoveryError) as exc:
        _ = configured_client.discover_mapping_from_csv(
            source_csv=csv_path,
            target_schema="ccdi",
            target_version="v1",
        )

    message = str(exc.value)
    assert "expected 3" in message
    assert "found 2" in message

    # And the outbound request preserved the empty-value column at position 1
    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    columns_section = cast(list[dict[str, object]], content.get("columns", []))
    assert len(columns_section) == 3
    assert columns_section[1] == {"name": "b", "values": []}


def test_positional_parity_below_threshold_becomes_none(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Below-threshold columns keep their position with None; manifest length equals CSV column count."""

    # Given a CSV with 3 columns where column 'b' has a top confidence of 0.2
    # against a threshold of 0.8. Before: backend returns a result for 'b' but
    # no option meets the threshold.
    payload = _array_payload(
        [
            {"name": "a", "matches": [{"target": "A", "target_cde_id": 1, "confidence": 0.95, "harmonization": "harmonizable"}]},
            {"name": "b", "matches": [{"target": "weak", "target_cde_id": 2, "confidence": 0.2, "harmonization": "harmonizable"}]},
            {"name": "c", "matches": [{"target": "C", "target_cde_id": 3, "confidence": 0.9, "harmonization": "harmonizable"}]},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    # When discovery runs with a strict threshold
    manifest = configured_client.discover_mapping_from_csv(
        source_csv=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
        confidence_threshold=0.8,
    )

    # Then column_mappings has length 3 and the below-threshold slot is None
    column_mappings = manifest["column_mappings"]
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
    payload = {"statusCode": 200, "body": json.dumps(recorded)}
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    # When discovery runs
    manifest = configured_client.discover_mapping_from_csv(
        source_csv=csv_path,
        target_schema="gc",
        target_version="v1",
        confidence_threshold=0.7,
    )

    # Then the manifest has length 3 and the slots reflect the three scenarios
    column_mappings = manifest["column_mappings"]
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
    payload = {"statusCode": 200, "body": json.dumps(recorded)}
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_csv(
        source_csv=csv_path,
        target_schema="gc",
        target_version="v1",
        confidence_threshold=0.7,
    )

    diagnosis_entry = manifest["column_mappings"][0]
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
    payload = {"statusCode": 200, "body": json.dumps(recorded)}
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    # Precondition: the raw fixture contains the three distinct harmonization values
    # on the four top-match slots so the assertions below are exercising real data.
    assert _raw_top_match_harmonizations(recorded) == _HARMONIZATION_ENUM

    manifest = configured_client.discover_mapping_from_csv(
        source_csv=csv_path,
        target_schema="gc",
        target_version="v1",
        confidence_threshold=0.4,
    )

    column_mappings = manifest["column_mappings"]
    assert len(column_mappings) == 4
    for entry in column_mappings:
        _assert_entry_carries_harmonization(entry)

    # Column-specific checks — top-level mirrors the top alternative per plan contract.
    first = cast(dict[str, object], cast(object, column_mappings[0]))
    third = cast(dict[str, object], cast(object, column_mappings[2]))
    fourth = cast(dict[str, object], cast(object, column_mappings[3]))
    assert first["harmonization"] == "harmonizable"
    assert third["harmonization"] == "no_permissible_values"
    assert third["cde_key"] == "middle_name"
    assert fourth["harmonization"] == "numeric"


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
                "name": "a",
                "matches": [
                    {"target": "Sample.name", "target_cde_id": 11, "confidence": 0.92},
                ],
            },
            {"name": "b", "matches": []},
            {"name": "c", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(MappingDiscoveryError) as exc:
        _ = configured_client.discover_mapping_from_csv(
            source_csv=sample_csv_path,
            target_schema="ccdi",
            target_version="v1",
        )

    assert "harmonization" in str(exc.value)
