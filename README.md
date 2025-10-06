# Netrias Client

Python toolkit for working with the Netrias recommendation and harmonization services. The client wraps the HTTP APIs with strong typing, logging, and guard rails so analytics code can focus on describing data rather than orchestrating requests.

## Highlights
- **Single configuration hook** – call `netrias_client.configure(...)` once to centralize API credentials, base URL, logging, and confidence thresholds.
- **Column discovery helpers** – derive column samples from CSV files, invoke the recommendation service, and normalize responses into `MappingDiscoveryResult` models.
- **Adapter utilities** – convert discovery output into harmonization-ready manifest payloads while applying confidence filters and CDE overrides.
- **Asynchronous harmonization loop** – submit jobs, poll for completion, download results, and version output files automatically to avoid accidental overwrites.
- **Extended timing logs** – discovery and harmonization emit duration metrics so you can spot slow calls quickly during live runs.

## Installation (uv)

The project targets Python 3.12+. Use [uv](https://github.com/astral-sh/uv) to manage environments and installations.

```bash
# create or update a project that depends on netrias-client
uv add netrias-client

# install optional AWS helpers (gateway bypass)
uv add netrias-client[aws]
```

For local development within this repository:

```bash
uv sync --group dev            # install development tooling
uv sync --group aws --group dev  # include optional AWS dependencies
```

## Configuration

All client entry points require explicit configuration. Provide the API key; discovery and harmonization endpoints are fixed by the library.

```python
from netrias_client import configure

configure(
    api_key="<netrias api key>",
    # Optional overrides:
    timeout=21600.0,               # seconds (default: 6 hours)
    log_level="INFO",             # CRITICAL|ERROR|WARNING|INFO|DEBUG
    confidence_threshold=0.80,     # discovery adapter filter, 0.0–1.0
)
```

Configuration errors raise `ClientConfigurationError`. Calling `configure` again replaces the active settings atomically and adjusts the global logger to the requested level.

## End-to-End Workflow

The typical harmonization flow contains three steps:

```python
from pathlib import Path

from netrias_client import configure, discover_mapping_from_csv, harmonize

configure(api_key="<netrias api key>")

csv_path = Path("/path/to/source.csv")
schema = "ccdi"

# 1. Ask the recommendation service for potential targets.
discovery = discover_mapping_from_csv(csv_path, target_schema=schema)

# 2. Translate the discovery result into a harmonization manifest.
manifest_payload = build_column_mapping_payload(discovery)

# 3. Kick off harmonization directly with the manifest payload.
result = harmonize(csv_path, manifest_payload)
print(result.status)
print(result.description)
print(result.file_path)
```

- `discover_mapping_from_csv` samples up to 25 values per column (configurable) before making a recommendation request.
- `build_column_mapping_payload` keeps only the highest-confidence target per source column, filters options below the configured threshold, and injects Netrias CDE routing hints when available.
- `harmonize` submits a job and polls `GET /v1/jobs/{jobId}` until the backend returns success or failure. Downloaded CSVs are written next to the source file (versioned as `data.harmonized.v1.csv`, etc.). Pass `manifest_output_path=` if you also want to persist the manifest JSON for inspection.

### Timing Logs

Both discovery and harmonization log elapsed seconds for the full operation and for timeout/transport failures. Sample output:

```
INFO netrias_client: discover mapping start: schema=ccdi columns=12
INFO netrias_client: discover mapping complete: schema=ccdi suggestions=0 duration=47.12s
INFO netrias_client: harmonize start: file=data.csv
INFO netrias_client: harmonize finished: file=data.csv status=succeeded duration=182.45s
```

Use these metrics to separate slow API responses from downstream processing overhead.

## Adapter Notes

Discovery results are normalized to manifest payloads automatically; unmatched columns are logged so you can expand coverage. Confidence thresholds come from `configure(confidence_threshold=...)` and default to 0.8.

## Gateway Bypass (Temporary)

The module `netrias_client._gateway_bypass` exposes `invoke_cde_recommendation_alias(...)`, a stopgap helper that calls the `cde-recommendation` Lambda alias directly. This avoids API Gateway’s short timeout window but requires AWS credentials with `lambda:InvokeFunction` permission and the `boto3` dependency.

```python
from netrias_client._gateway_bypass import invoke_cde_recommendation_alias

result = invoke_cde_recommendation_alias(
    target_schema="ccdi",
    columns={"study_name": ["foo", "bar"]},
    alias="prod",
    region_name="us-east-2",
)
```

Install `boto3` (or `netrias-client[aws]` if provided) before importing the bypass module, and rotate IAM credentials frequently. Once API Gateway limits are raised, prefer the standard discovery flow again.

## Testing & Tooling

The repository ships with pytest-based integration tests plus lint/type tooling.


```bash
uv run pytest
uv run ruff check
uv run basedpyright
uv build                 # produce wheel + sdist
```

Live verification scripts are located under `live_test/` and require a populated `.env` file containing `NETRIAS_API_KEY` (and optionally harmonization overrides while services converge).

## Project Layout

```
src/netrias_client/
    __init__.py          # re-exported public surface
    _adapter.py          # discovery → manifest conversion
    _config.py           # configure()/get_settings()
    _core.py             # harmonization workflow
    _discovery.py        # discovery wrappers and CSV sampling
    _errors.py           # exception taxonomy
    _http.py             # HTTP primitives (submit/poll/download)
    _io.py               # streaming helpers
    _logging.py          # standardized logger setup
    _models.py           # dataclasses for structured responses
    _validators.py       # filesystem and payload validation
```

Tests reside under `src/netrias_client/tests/` and are excluded from the published wheel to keep installs slim; run them locally via `uv run pytest`.

## Contributing

1. `uv sync --group dev` (add `--group aws` if needed) to create the virtual environment.
2. `uv run pytest` to ensure the suite passes prior to committing.
3. Follow the repo conventions: keep functions focused, prefer typed interfaces, and favor logging key transitions over verbose chatter.

Pull requests should include updated documentation or fixtures when they alter API behavior or the manifest contract.
