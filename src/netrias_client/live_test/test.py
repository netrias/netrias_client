"""Minimal live test harness for the Netrias client."""

from __future__ import annotations

from pathlib import Path
from typing import cast, Final

from dotenv import dotenv_values

from .. import NetriasClient


ROOT: Final[Path] = Path(__file__).resolve().parent
ENV = dotenv_values(ROOT / ".env")
CSV_PATH: Final[Path] = ROOT / "data" / "primary_diagnosis_1.csv"


def main() -> None:
    api_key = cast(str, ENV.get("NETRIAS_API_KEY"))

    client = NetriasClient(api_key=api_key)

    manifest = client.discover_mapping_from_csv(source_csv=CSV_PATH, target_schema="ccdi", target_version="v1")

    result = client.harmonize(source_path=CSV_PATH, manifest=manifest)

    print(f"Harmonize status: {result.status}")
    print(f"Harmonized file: {result.file_path}")


if __name__ == "__main__":
    main()
