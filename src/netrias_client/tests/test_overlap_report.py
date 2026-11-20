"""Test overlap report generation.

'why': verify overlap analysis produces correct match/unmatch metrics,
handles blanks consistently, and writes expected output files
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from netrias_client._models import ColumnKeyedManifestPayload, DataModelStoreEndpoints, LogLevel, Settings
from netrias_client._tabular import TabularDataset, read_tabular
from netrias_client.overlap_report import run_overlap_analysis

ReportEntry = dict[str, object]


@pytest.fixture
def overlap_dataset() -> TabularDataset:
    return read_tabular(Path(__file__).parent / "fixtures" / "data_overlap.csv")


@pytest.fixture
def overlap_manifest() -> ColumnKeyedManifestPayload:
    manifest_path = Path(__file__).parent / "fixtures" / "sample_manifest.json"
    return cast(ColumnKeyedManifestPayload, json.loads(manifest_path.read_text()))


@pytest.fixture
def mock_settings() -> Settings:
    return Settings(
        api_key="test-api-key",
        discovery_url="https://example.test/discovery",
        harmonization_url="https://example.test/harmonization",
        timeout=10.0,
        log_level=LogLevel.INFO,
        discovery_use_gateway_bypass=False,
        log_directory=None,
        data_model_store_endpoints=DataModelStoreEndpoints(base_url="https://example.test/data-model-store"),
    )


@pytest.fixture
def alpha_pvs() -> frozenset[str]:
    return frozenset({"White", "Black or African American", "Asian"})


@pytest.fixture
def beta_pvs() -> frozenset[str]:
    return frozenset({"Active", "Inactive"})


def _mock_pv_lookup(alpha_pvs: frozenset[str], beta_pvs: frozenset[str]):
    """Return a side_effect that returns the right PV set per cde_key."""
    async def _lookup(
        settings: Settings,
        model_key: str,
        version: str,
        cde_key: str,
        include_inactive: bool = False,
    ) -> frozenset[str]:
        _ = (settings, model_key, version, include_inactive)
        if cde_key == "alpha":
            return alpha_pvs
        if cde_key == "beta":
            return beta_pvs
        return frozenset()
    return _lookup


def _read_report(path: Path) -> list[ReportEntry]:
    return cast(list[ReportEntry], json.loads(path.read_text()))


def _find_report_entry(report: list[ReportEntry], column_name: str) -> ReportEntry:
    return next(entry for entry in report if entry["column_name"] == column_name)


def _list_field(entry: ReportEntry, field: str) -> list[dict[str, object]]:
    value = entry[field]
    assert isinstance(value, list)
    return cast(list[dict[str, object]], value)


def _int_field(entry: ReportEntry, field: str) -> int:
    value = entry[field]
    assert isinstance(value, int)
    return value


def _float_field(entry: ReportEntry, field: str) -> float:
    value = entry[field]
    assert isinstance(value, int | float)
    return float(value)


@pytest.mark.asyncio
async def test_writes_json_and_csv(
    overlap_dataset: TabularDataset,
    overlap_manifest: ColumnKeyedManifestPayload,
    mock_settings: Settings,
    alpha_pvs: frozenset[str],
    beta_pvs: frozenset[str],
    tmp_path: Path,
) -> None:
    """Overlap report writes both JSON and CSV files with expected content."""

    # Given: output directory has no report files yet
    assert not (tmp_path / "overlap_report.json").exists()
    assert not (tmp_path / "overlap_report.csv").exists()

    # When: overlap analysis runs
    with patch(
        "netrias_client.overlap_report.get_pv_set_async",
        new_callable=AsyncMock,
        side_effect=_mock_pv_lookup(alpha_pvs, beta_pvs),
    ):
        await run_overlap_analysis(
            dataset=overlap_dataset,
            manifest=overlap_manifest,
            settings=mock_settings,
            target_schema="ccdi",
            external_version_number="1",
            output_dir=tmp_path,
            logger=logging.getLogger("test"),
        )

    # Then: both files exist with the expected structure
    report = _read_report(tmp_path / "overlap_report.json")
    assert len(report) == 2
    assert all(entry["status"] == "ok" for entry in report)

    df = pd.read_csv(tmp_path / "overlap_report.csv")
    assert len(df) > 0
    assert set(df.columns) == {
        "column_key", "column_name", "cde_key", "value", "normalized_value", "count", "in_pv_set"
    }


@pytest.mark.asyncio
async def test_skips_non_harmonizable_columns(
    overlap_dataset: TabularDataset,
    overlap_manifest: ColumnKeyedManifestPayload,
    mock_settings: Settings,
    alpha_pvs: frozenset[str],
    beta_pvs: frozenset[str],
    tmp_path: Path,
) -> None:
    """Columns with harmonization other than 'harmonizable' are excluded from the report."""

    # Given: manifest has 3 columns — a and b harmonizable, c is not
    assert overlap_manifest["column_mappings"]["col_0002"]["harmonization"] == "no_permissible_values"

    # When: overlap analysis runs
    with patch(
        "netrias_client.overlap_report.get_pv_set_async",
        new_callable=AsyncMock,
        side_effect=_mock_pv_lookup(alpha_pvs, beta_pvs),
    ):
        await run_overlap_analysis(
            dataset=overlap_dataset,
            manifest=overlap_manifest,
            settings=mock_settings,
            target_schema="ccdi",
            external_version_number="1",
            output_dir=tmp_path,
            logger=logging.getLogger("test"),
        )

    # Then: report contains exactly the harmonizable columns from the manifest
    expected_harmonizable_count = sum(
        1 for col in overlap_manifest["column_mappings"].values()
        if col["harmonization"] == "harmonizable"
    )
    report = _read_report(tmp_path / "overlap_report.json")
    assert len(report) == expected_harmonizable_count
    assert all(entry["status"] == "ok" for entry in report)


@pytest.mark.asyncio
async def test_matched_and_unmatched_counts(
    overlap_dataset: TabularDataset,
    overlap_manifest: ColumnKeyedManifestPayload,
    mock_settings: Settings,
    alpha_pvs: frozenset[str],
    beta_pvs: frozenset[str],
    tmp_path: Path,
) -> None:
    """Matched and unmatched values are counted and identified correctly."""

    # Given: column a has White(x2), Black or African American(x1), Asian(x1),
    # Unkown(x1), Not Reported(x1), blank(x1), whitespace(x1); PV set is
    # {White, Black or African American, Asian}, so matched != all distinct values
    assert overlap_manifest["column_mappings"]["col_0000"]["cde_key"] == "alpha"

    # When: overlap analysis runs
    with patch(
        "netrias_client.overlap_report.get_pv_set_async",
        new_callable=AsyncMock,
        side_effect=_mock_pv_lookup(alpha_pvs, beta_pvs),
    ):
        await run_overlap_analysis(
            dataset=overlap_dataset,
            manifest=overlap_manifest,
            settings=mock_settings,
            target_schema="ccdi",
            external_version_number="1",
            output_dir=tmp_path,
            logger=logging.getLogger("test"),
        )

    # Then: matched/unmatched counts and exact values are correct
    report = _read_report(tmp_path / "overlap_report.json")
    col_a = _find_report_entry(report, "a")

    assert col_a["status"] == "ok"
    assert _int_field(col_a, "matched_distinct_raw_values") == 3
    assert _int_field(col_a, "matched_total_raw_values") == 4

    matched_values = {m["value"] for m in _list_field(col_a, "top_raw_matches")}
    assert matched_values == {"White", "Black or African American", "Asian"}

    unmatched_values = {u["value"] for u in _list_field(col_a, "top_raw_unmatched")}
    assert unmatched_values == {"Unkown", "Not Reported"}


@pytest.mark.asyncio
async def test_blank_handling(
    overlap_dataset: TabularDataset,
    overlap_manifest: ColumnKeyedManifestPayload,
    mock_settings: Settings,
    alpha_pvs: frozenset[str],
    beta_pvs: frozenset[str],
    tmp_path: Path,
) -> None:
    """Blank, empty, and whitespace-only values count as missing."""

    # Given: column a data_overlap.csv row 4 is empty (""), row 8 is whitespace ("   ")

    # When: overlap analysis runs
    with patch(
        "netrias_client.overlap_report.get_pv_set_async",
        new_callable=AsyncMock,
        side_effect=_mock_pv_lookup(alpha_pvs, beta_pvs),
    ):
        await run_overlap_analysis(
            dataset=overlap_dataset,
            manifest=overlap_manifest,
            settings=mock_settings,
            target_schema="ccdi",
            external_version_number="1",
            output_dir=tmp_path,
            logger=logging.getLogger("test"),
        )

    # Then: exactly 2 rows count as missing, raising the excluding-nulls rate
    report = _read_report(tmp_path / "overlap_report.json")
    col_a = _find_report_entry(report, "a")

    assert _int_field(col_a, "missing_count") == 2
    assert _float_field(col_a, "match_rate_excluding_nulls") > _float_field(col_a, "match_rate_including_nulls")


@pytest.mark.asyncio
async def test_match_rates(
    overlap_dataset: TabularDataset,
    overlap_manifest: ColumnKeyedManifestPayload,
    mock_settings: Settings,
    alpha_pvs: frozenset[str],
    beta_pvs: frozenset[str],
    tmp_path: Path,
) -> None:
    """Match rates equal exact computed values for each column."""

    # Given: column a — 4 matched / 8 total = 0.5, 4 matched / 6 non-null = 0.67
    # column b — 7 matched / 8 total = 0.88, 7 matched / 7 non-null = 1.0

    # When: overlap analysis runs
    with patch(
        "netrias_client.overlap_report.get_pv_set_async",
        new_callable=AsyncMock,
        side_effect=_mock_pv_lookup(alpha_pvs, beta_pvs),
    ):
        await run_overlap_analysis(
            dataset=overlap_dataset,
            manifest=overlap_manifest,
            settings=mock_settings,
            target_schema="ccdi",
            external_version_number="1",
            output_dir=tmp_path,
            logger=logging.getLogger("test"),
        )

    # Then: exact rates match expected values
    report = _read_report(tmp_path / "overlap_report.json")

    col_a = _find_report_entry(report, "a")
    assert _float_field(col_a, "match_rate_including_nulls") == 0.5
    assert _float_field(col_a, "match_rate_excluding_nulls") == 0.67

    col_b = _find_report_entry(report, "b")
    assert _float_field(col_b, "match_rate_including_nulls") == 0.88
    assert _float_field(col_b, "match_rate_excluding_nulls") == 1.0


@pytest.mark.asyncio
async def test_csv_includes_all_distinct_values(
    overlap_dataset: TabularDataset,
    overlap_manifest: ColumnKeyedManifestPayload,
    mock_settings: Settings,
    alpha_pvs: frozenset[str],
    beta_pvs: frozenset[str],
    tmp_path: Path,
) -> None:
    """CSV output includes both matched and unmatched values with correct in_pv_set flags."""

    # Given: column a has matched values (White, Black or African American, Asian)
    # and unmatched values (Unkown, Not Reported)

    # When: overlap analysis runs
    with patch(
        "netrias_client.overlap_report.get_pv_set_async",
        new_callable=AsyncMock,
        side_effect=_mock_pv_lookup(alpha_pvs, beta_pvs),
    ):
        await run_overlap_analysis(
            dataset=overlap_dataset,
            manifest=overlap_manifest,
            settings=mock_settings,
            target_schema="ccdi",
            external_version_number="1",
            output_dir=tmp_path,
            logger=logging.getLogger("test"),
        )

    # Then: matched and unmatched values are exactly identified
    df = pd.read_csv(tmp_path / "overlap_report.csv")
    col_a_rows = df[df["column_name"] == "a"]

    matched_values = set(col_a_rows[col_a_rows["in_pv_set"] == True]["value"])
    assert matched_values == {"White", "Black or African American", "Asian"}

    unmatched_rows = col_a_rows[col_a_rows["in_pv_set"] == False]
    unmatched_series = unmatched_rows["value"]
    assert isinstance(unmatched_series, pd.Series)
    unmatched_values = set(unmatched_series.dropna())
    assert "Unkown" in unmatched_values
    assert "Not Reported" in unmatched_values


@pytest.mark.asyncio
async def test_status_column_uses_correct_pvs(
    overlap_dataset: TabularDataset,
    overlap_manifest: ColumnKeyedManifestPayload,
    mock_settings: Settings,
    alpha_pvs: frozenset[str],
    beta_pvs: frozenset[str],
    tmp_path: Path,
) -> None:
    """Each column fetches and compares against its own CDE's PV set."""

    # Given: column b has Active(x5), Inactive(x2), blank(x1); PV set for
    # beta is {Active, Inactive}, so all non-blank values should match
    assert overlap_manifest["column_mappings"]["col_0001"]["cde_key"] == "beta"

    # When: overlap analysis runs
    with patch(
        "netrias_client.overlap_report.get_pv_set_async",
        new_callable=AsyncMock,
        side_effect=_mock_pv_lookup(alpha_pvs, beta_pvs),
    ):
        await run_overlap_analysis(
            dataset=overlap_dataset,
            manifest=overlap_manifest,
            settings=mock_settings,
            target_schema="ccdi",
            external_version_number="1",
            output_dir=tmp_path,
            logger=logging.getLogger("test"),
        )

    # Then: all non-blank values match, none unmatched
    report = _read_report(tmp_path / "overlap_report.json")
    col_b = _find_report_entry(report, "b")

    assert col_b["status"] == "ok"
    assert _int_field(col_b, "matched_distinct_raw_values") == 2
    assert _int_field(col_b, "matched_total_raw_values") == 7
    assert _list_field(col_b, "top_raw_unmatched") == []
    matched_values = {m["value"] for m in _list_field(col_b, "top_raw_matches")}
    assert matched_values == {"Active", "Inactive"}
