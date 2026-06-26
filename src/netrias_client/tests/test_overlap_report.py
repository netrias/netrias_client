"""Test overlap report generation.

'why': verify overlap analysis produces correct match/unmatch metrics,
handles blanks consistently, and writes expected output files
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from netrias_client._tabular import read_tabular
from netrias_client.overlap_report import run_overlap_analysis


@pytest.fixture
def overlap_dataset():
    return read_tabular(Path(__file__).parent / "fixtures" / "data_overlap.csv")


@pytest.fixture
def overlap_manifest():
    return {
        "column_mappings": {
            "col_0000": {
                "column_name": "race",
                "cde_key": "race",
                "harmonization": "harmonizable",
            },
            "col_0001": {
                "column_name": "status",
                "cde_key": "status",
                "harmonization": "harmonizable",
            },
            "col_0002": {
                "column_name": "site_id",
                "cde_key": "site",
                "harmonization": "no_permissible_values",
            },
        }
    }


@pytest.fixture
def mock_settings():
    return type("Settings", (), {})()


@pytest.fixture
def race_pvs():
    return frozenset({"White", "Black or African American", "Asian"})


@pytest.fixture
def status_pvs():
    return frozenset({"Active", "Inactive"})


def _mock_pv_lookup(race_pvs, status_pvs):
    """Return a side_effect that returns the right PV set per cde_key."""
    async def _lookup(**kwargs):
        cde_key = kwargs.get("cde_key")
        if cde_key == "race":
            return race_pvs
        if cde_key == "status":
            return status_pvs
        return frozenset()
    return _lookup


@pytest.mark.asyncio
async def test_writes_json_and_csv(
    overlap_dataset,
    overlap_manifest,
    mock_settings,
    race_pvs,
    status_pvs,
    tmp_path,
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
            target_version="1",
            output_dir=tmp_path,
            logger=__import__("logging").getLogger("test"),
        )

    assert (tmp_path / "overlap_report.json").exists()
    assert (tmp_path / "overlap_report.csv").exists()


@pytest.mark.asyncio
async def test_skips_non_harmonizable_columns(
    overlap_dataset,
    overlap_manifest,
    mock_settings,
    race_pvs,
    status_pvs,
    tmp_path,
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
            target_version="1",
            output_dir=tmp_path,
            logger=__import__("logging").getLogger("test"),
        )

    report = json.loads((tmp_path / "overlap_report.json").read_text())
    column_names = [e["column_name"] for e in report]
    assert "site_id" not in column_names
    assert "race" in column_names
    assert "status" in column_names


@pytest.mark.asyncio
async def test_matched_and_unmatched_counts(
    overlap_dataset,
    overlap_manifest,
    mock_settings,
    race_pvs,
    status_pvs,
    tmp_path,
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
            target_version="1",
            output_dir=tmp_path,
            logger=__import__("logging").getLogger("test"),
        )

    report = json.loads((tmp_path / "overlap_report.json").read_text())
    race_entry = next(e for e in report if e["column_name"] == "race")

    assert race_entry["status"] == "ok"
    # White(x2), Black or African American(x1), Asian(x1) = 3 distinct, 4 total
    assert race_entry["matched_distinct_raw_values"] == 3
    assert race_entry["matched_total_raw_values"] == 4
    assert len(race_entry["top_raw_matches"]) >= 1
    # Unkown(x1), Not Reported(x1) = 2 unmatched
    assert len(race_entry["top_raw_unmatched"]) == 2


@pytest.mark.asyncio
async def test_blank_handling(
    overlap_dataset,
    overlap_manifest,
    mock_settings,
    race_pvs,
    status_pvs,
    tmp_path,
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
            target_version="1",
            output_dir=tmp_path,
            logger=__import__("logging").getLogger("test"),
        )

    report = json.loads((tmp_path / "overlap_report.json").read_text())
    race_entry = next(e for e in report if e["column_name"] == "race")

    # data_overlap.csv race column: row 4 is empty (""), row 8 is whitespace ("   ")
    assert race_entry["missing_count"] >= 2
    assert race_entry["match_rate_excluding_nulls"] > race_entry["match_rate_including_nulls"]


@pytest.mark.asyncio
async def test_match_rates(
    overlap_dataset,
    overlap_manifest,
    mock_settings,
    race_pvs,
    status_pvs,
    tmp_path,
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
            target_version="1",
            output_dir=tmp_path,
            logger=__import__("logging").getLogger("test"),
        )

    report = json.loads((tmp_path / "overlap_report.json").read_text())
    for entry in report:
        if entry["status"] != "ok":
            continue
        assert 0 <= entry["match_rate_including_nulls"] <= 1
        assert 0 <= entry["match_rate_excluding_nulls"] <= 1
        assert entry["match_rate_excluding_nulls"] >= entry["match_rate_including_nulls"]


@pytest.mark.asyncio
async def test_csv_includes_all_distinct_values(
    overlap_dataset,
    overlap_manifest,
    mock_settings,
    race_pvs,
    status_pvs,
    tmp_path,
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
            target_version="1",
            output_dir=tmp_path,
            logger=__import__("logging").getLogger("test"),
        )

    df = pd.read_csv(tmp_path / "overlap_report.csv")
    assert True in df["in_pv_set"].values
    assert False in df["in_pv_set"].values
    assert "normalized_value" in df.columns


@pytest.mark.asyncio
async def test_status_column_uses_correct_pvs(
    overlap_dataset,
    overlap_manifest,
    mock_settings,
    race_pvs,
    status_pvs,
    tmp_path,
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
            target_version="1",
            output_dir=tmp_path,
            logger=__import__("logging").getLogger("test"),
        )

    report = json.loads((tmp_path / "overlap_report.json").read_text())
    status_entry = next(e for e in report if e["column_name"] == "status")

    assert status_entry["status"] == "ok"
    # Active(x5), Inactive(x2) — both are in status_pvs, so all non-blank values match
    assert status_entry["matched_distinct_raw_values"] == 2
    assert len(status_entry["top_raw_unmatched"]) == 0
    assert status_entry["matched_total_raw_values"] == 7
    