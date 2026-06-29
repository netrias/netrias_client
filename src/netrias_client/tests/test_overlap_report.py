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
    return {
        "column_mappings": {
            "col_0000": {
                "column_name": "race",
                "cde_key": "race",
                "cde_id": 1,
                "harmonization": "harmonizable",
                "alternatives": [],
            },
            "col_0001": {
                "column_name": "status",
                "cde_key": "status",
                "cde_id": 2,
                "harmonization": "harmonizable",
                "alternatives": [],
            },
            "col_0002": {
                "column_name": "site_id",
                "cde_key": "site",
                "cde_id": 3,
                "harmonization": "no_permissible_values",
                "alternatives": [],
            },
        }
    }


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
def race_pvs() -> frozenset[str]:
    return frozenset({"White", "Black or African American", "Asian"})


@pytest.fixture
def status_pvs() -> frozenset[str]:
    return frozenset({"Active", "Inactive"})


def _mock_pv_lookup(race_pvs: frozenset[str], status_pvs: frozenset[str]):
    """Return a side_effect that returns the right PV set per cde_key."""
    async def _lookup(
        settings: Settings,
        model_key: str,
        version: str,
        cde_key: str,
        include_inactive: bool = False,
    ) -> frozenset[str]:
        _ = (settings, model_key, version, include_inactive)
        if cde_key == "race":
            return race_pvs
        if cde_key == "status":
            return status_pvs
        return frozenset()
    return _lookup


def _read_report(path: Path) -> list[ReportEntry]:
    return cast(list[ReportEntry], json.loads(path.read_text()))


def _find_report_entry(report: list[ReportEntry], column_name: str) -> ReportEntry:
    return next(entry for entry in report if entry["column_name"] == column_name)


def _list_field(entry: ReportEntry, field: str) -> list[object]:
    value = entry[field]
    assert isinstance(value, list)
    return cast(list[object], value)


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
    race_pvs: frozenset[str],
    status_pvs: frozenset[str],
    tmp_path: Path,
) -> None:
    """Overlap report writes both JSON and CSV files to the output directory."""

    with patch(
        "netrias_client.overlap_report.get_pv_set_async",
        new_callable=AsyncMock,
        side_effect=_mock_pv_lookup(race_pvs, status_pvs),
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

    assert (tmp_path / "overlap_report.json").exists()
    assert (tmp_path / "overlap_report.csv").exists()


@pytest.mark.asyncio
async def test_skips_non_harmonizable_columns(
    overlap_dataset: TabularDataset,
    overlap_manifest: ColumnKeyedManifestPayload,
    mock_settings: Settings,
    race_pvs: frozenset[str],
    status_pvs: frozenset[str],
    tmp_path: Path,
) -> None:
    """Columns with harmonization other than 'harmonizable' are excluded from the report."""

    with patch(
        "netrias_client.overlap_report.get_pv_set_async",
        new_callable=AsyncMock,
        side_effect=_mock_pv_lookup(race_pvs, status_pvs),
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

    report = _read_report(tmp_path / "overlap_report.json")
    column_names = [e["column_name"] for e in report]
    assert "site_id" not in column_names
    assert "race" in column_names
    assert "status" in column_names


@pytest.mark.asyncio
async def test_matched_and_unmatched_counts(
    overlap_dataset: TabularDataset,
    overlap_manifest: ColumnKeyedManifestPayload,
    mock_settings: Settings,
    race_pvs: frozenset[str],
    status_pvs: frozenset[str],
    tmp_path: Path,
) -> None:
    """Matched and unmatched values are counted correctly for both distinct and total."""

    with patch(
        "netrias_client.overlap_report.get_pv_set_async",
        new_callable=AsyncMock,
        side_effect=_mock_pv_lookup(race_pvs, status_pvs),
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

    report = _read_report(tmp_path / "overlap_report.json")
    race_entry = _find_report_entry(report, "race")

    assert race_entry["status"] == "ok"
    # White(x2), Black or African American(x1), Asian(x1) = 3 distinct, 4 total
    assert race_entry["matched_distinct_raw_values"] == 3
    assert race_entry["matched_total_raw_values"] == 4
    assert len(_list_field(race_entry, "top_raw_matches")) >= 1
    # Unkown(x1), Not Reported(x1) = 2 unmatched
    assert len(_list_field(race_entry, "top_raw_unmatched")) == 2


@pytest.mark.asyncio
async def test_blank_handling(
    overlap_dataset: TabularDataset,
    overlap_manifest: ColumnKeyedManifestPayload,
    mock_settings: Settings,
    race_pvs: frozenset[str],
    status_pvs: frozenset[str],
    tmp_path: Path,
) -> None:
    """Blank, empty, and whitespace-only values count as missing."""

    with patch(
        "netrias_client.overlap_report.get_pv_set_async",
        new_callable=AsyncMock,
        side_effect=_mock_pv_lookup(race_pvs, status_pvs),
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

    report = _read_report(tmp_path / "overlap_report.json")
    race_entry = _find_report_entry(report, "race")

    # data_overlap.csv race column: row 4 is empty (""), row 8 is whitespace ("   ")
    assert _int_field(race_entry, "missing_count") >= 2
    assert _float_field(race_entry, "match_rate_excluding_nulls") > _float_field(race_entry, "match_rate_including_nulls")


@pytest.mark.asyncio
async def test_match_rates(
    overlap_dataset: TabularDataset,
    overlap_manifest: ColumnKeyedManifestPayload,
    mock_settings: Settings,
    race_pvs: frozenset[str],
    status_pvs: frozenset[str],
    tmp_path: Path,
) -> None:
    """Both match rates are between 0 and 1 and excluding_nulls >= including_nulls."""

    with patch(
        "netrias_client.overlap_report.get_pv_set_async",
        new_callable=AsyncMock,
        side_effect=_mock_pv_lookup(race_pvs, status_pvs),
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

    report = _read_report(tmp_path / "overlap_report.json")
    for entry in report:
        if entry["status"] != "ok":
            continue
        including_nulls = _float_field(entry, "match_rate_including_nulls")
        excluding_nulls = _float_field(entry, "match_rate_excluding_nulls")
        assert 0 <= including_nulls <= 1
        assert 0 <= excluding_nulls <= 1
        assert excluding_nulls >= including_nulls


@pytest.mark.asyncio
async def test_csv_includes_all_distinct_values(
    overlap_dataset: TabularDataset,
    overlap_manifest: ColumnKeyedManifestPayload,
    mock_settings: Settings,
    race_pvs: frozenset[str],
    status_pvs: frozenset[str],
    tmp_path: Path,
) -> None:
    """CSV output includes both matched and unmatched values with in_pv_set flag."""

    with patch(
        "netrias_client.overlap_report.get_pv_set_async",
        new_callable=AsyncMock,
        side_effect=_mock_pv_lookup(race_pvs, status_pvs),
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

    df = pd.read_csv(tmp_path / "overlap_report.csv")
    assert True in df["in_pv_set"].values
    assert False in df["in_pv_set"].values
    assert "normalized_value" in df.columns


@pytest.mark.asyncio
async def test_status_column_uses_correct_pvs(
    overlap_dataset: TabularDataset,
    overlap_manifest: ColumnKeyedManifestPayload,
    mock_settings: Settings,
    race_pvs: frozenset[str],
    status_pvs: frozenset[str],
    tmp_path: Path,
) -> None:
    """Each column fetches and compares against its own CDE's PV set."""

    with patch(
        "netrias_client.overlap_report.get_pv_set_async",
        new_callable=AsyncMock,
        side_effect=_mock_pv_lookup(race_pvs, status_pvs),
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

    report = _read_report(tmp_path / "overlap_report.json")
    status_entry = _find_report_entry(report, "status")

    assert status_entry["status"] == "ok"
    # Active(x5), Inactive(x2) — both are in status_pvs, so all non-blank values match
    assert status_entry["matched_distinct_raw_values"] == 2
    assert len(_list_field(status_entry, "top_raw_unmatched")) == 0
    assert status_entry["matched_total_raw_values"] == 7
    
