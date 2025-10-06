# Netrias Client

"""Explain how to install and exercise the Netrias harmonization client."""

## Install with `uv`
- Install `uv` once (or update): `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Sync dependencies for a project that consumes the client:
  ```bash
  uv add netrias_client
  uv add python-dotenv  # optional helper for loading .env files
  ```
- Prefer `uv run <command>` for executing scripts so the managed environment is reused automatically.

### Alternative: `pip`
```bash
python -m pip install netrias_client
python -m pip install python-dotenv  # optional
```

## Quickstart Script
Reference script (save as `main.py`) showing a full harmonization round-trip:

```python
#!/usr/bin/env -S uv run python
# /// script
# requires-python = ">=3.13"
# dependencies = ["netrias_client", "python-dotenv"]
# ///

"""Exercise the packaged Netrias client against the live APIs."""

import asyncio
import os
from pathlib import Path
from typing import Final

from dotenv import load_dotenv
from netrias_client import NetriasClient, __version__ as CLIENT_VERSION

load_dotenv(override=True)

CSV_PATH: Final[Path] = Path("data/primary_diagnosis_1.csv")


async def main() -> None:
    client = NetriasClient()
    client.configure(api_key=_resolve_api_key())

    manifest = client.discover_cde_mapping(
        source_csv=CSV_PATH,
        target_schema="ccdi",
    )

    result = await client.harmonize_async(
        source_path=CSV_PATH,
        manifest=manifest,
    )

    print(f"netrias_client version: {CLIENT_VERSION}")
    print(f"Harmonize status: {result.status}")
    print(f"Harmonized file: {result.file_path}")


def _resolve_api_key() -> str:
    api_key = os.getenv("NETRIAS_API_KEY")
    if api_key:
        return api_key
    msg = "Set NETRIAS_API_KEY in your environment or .env file"
    raise RuntimeError(msg)


if __name__ == "__main__":
    asyncio.run(main())
```

### Steps
1. Install or update `uv` (see above).
2. Export `NETRIAS_API_KEY` (or add it to a local `.env`).
3. Adjust `CSV_PATH` to point at the source CSV you want to harmonize.
4. Run `uv run python main.py`.

## `configure()` Options
`NetriasClient.configure(...)` accepts additional tuning knobs. You can mix and match the ones you need:

| Parameter | Type | Purpose |
| --- | --- | --- |
| `api_key` | `str` | **Required.** Bearer token for authenticating with the Netrias services. |
| `timeout` | `float | None` | Override the default 6-hour timeout for long-running harmonization jobs. |
| `log_level` | `LogLevel | str | None` | Control verbosity (`INFO` by default). Accepts enum members or string names. |
| `confidence_threshold` | `float | None` | Minimum score (0–1) for keeping discovery recommendations; lower it to capture more tentative matches. |
| `discovery_use_gateway_bypass` | `bool | None` | Toggle the temporary AWS Lambda bypass path for discovery (defaults to `True`). Set to `False` once API Gateway limits are sufficient. |
| `log_directory` | `Path | str | None` | Directory for per-client log files. When omitted, logs stay on stdout. |

Configure only the options you need; unspecified values fall back to sensible defaults.

## Usage Notes
- `discover_cde_mapping(...)` samples CSV values and returns a manifest-ready payload; use the async variant if you’re already in an event loop.
- Call `harmonize(...)` (sync) or `harmonize_async(...)` (async) with the manifest to download a harmonized CSV. The result object reports status, description, and the output path.
- The package exposes `__version__` so callers can assert the installed release.
- Optional extras (`netrias_client[aws]`) add boto3 helpers for the temporary gateway bypass.
