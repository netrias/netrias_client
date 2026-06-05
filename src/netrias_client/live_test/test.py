"""Minimal live test harness for the Netrias client."""

from __future__ import annotations

from typing import cast

from dotenv import dotenv_values

from .. import NetriasClient
from ._constants import CSV_PATH, DISCOVERY_TARGET_VERSION, ENV_PATH, EXTERNAL_VERSION_NUMBER, MODEL_KEY


ENV = dotenv_values(ENV_PATH)


def main() -> None:
    api_key = cast(str, ENV.get("NETRIAS_API_KEY"))

    client = NetriasClient(api_key=api_key)

    manifest = client.discover_mapping_from_tabular(
        source_path=CSV_PATH,
        target_schema=MODEL_KEY,
        target_version=DISCOVERY_TARGET_VERSION,
    )

    result = client.harmonize(
        source_path=CSV_PATH,
        manifest=manifest,
        data_commons_key=MODEL_KEY,
        external_version_number=EXTERNAL_VERSION_NUMBER,
    )

    print(f"Harmonize status: {result.status}")
    print(f"Harmonized file: {result.file_path}")


if __name__ == "__main__":
    main()
