"""Verify public model compatibility contracts."""
from __future__ import annotations

from pathlib import Path

from netrias_client._models import HarmonizationResult


def test_harmonization_result_preserves_mapping_id_positional_slot(tmp_path: Path) -> None:
    """Keep existing positional construction behavior for mapping_id."""

    # Given: a caller that constructs the public result model positionally
    output_path = tmp_path / "harmonized.csv"

    # When: the caller supplies the fourth positional argument
    result = HarmonizationResult(output_path, "succeeded", "ok", "mapping-123")

    # Then: that value still belongs to mapping_id, not the newer job_id field
    assert result.mapping_id == "mapping-123"
    assert result.job_id is None
