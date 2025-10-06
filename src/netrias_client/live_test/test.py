"""Minimal live test harness for the Netrias client."""

from __future__ import annotations

from pathlib import Path
from typing import cast
from typing import Final

from dotenv import dotenv_values

from .. import NetriasClient


ROOT: Final[Path] = Path(__file__).resolve().parent
ENV = dotenv_values(ROOT / ".env")
csv_path = ROOT / "data" / "primary_diagnosis_1.csv"


api_key = ENV.get("NETRIAS_API_KEY")

client = NetriasClient()
client.configure(api_key=cast(str, api_key))

cde_mapping = client.discover_cde_mapping(source_csv=csv_path, target_schema="ccdi")

result = client.harmonize(source_path=csv_path, manifest=cde_mapping)

print(f"Harmonize status: {result.status}")
print(f"Harmonized file: {result.file_path}")
