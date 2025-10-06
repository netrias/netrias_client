# Netrias Client Architecture

## Responsibility & Scope
- Provide a focused Python interface for the Netrias discovery and harmonization services while hiding transport details and enforcing validation guard rails.
- Support CSV inputs today with async and sync entry points; XLSX and broader formats remain future enhancements.
- Maintain transparency on failures by surfacing structured errors, typed results, and detailed logs rather than silent retries.

## Module Overview
- `__init__.py`: surface the public API (`configure`, `discover_mapping*`, `harmonize*`) and version metadata.
- `_adapter.py`: translate discovery responses into harmonization manifest snippets, enforce confidence thresholds, and hydrate static CDE metadata.
- `_config.py`: own the global `Settings` snapshot, validate configuration, normalize gateway-bypass options, and set the shared logger level.
- `_core.py`: contain the harmonization workflow (submit → poll → download) shared by sync/async entry points.
- `_discovery.py`: implement discovery workflows, CSV sampling helpers, and conditional routing between API Gateway and the Lambda alias bypass.
- `_errors.py`: collect the client exception taxonomy (`ClientConfigurationError`, `MappingDiscoveryError`, `NetriasAPIUnavailable`, etc.).
- `_gateway_bypass.py`: temporary helpers that invoke the `cde-recommendation` Lambda alias directly via boto3.
- `_http.py`: build harmonization payloads, submit jobs, fetch job status, and perform discovery requests via HTTPX.
- `_io.py`: stream API responses to disk to avoid loading large files into memory.
- `_logging.py`: create a namespaced logger (`netrias_client`) that honors configured log level.
- `_models.py`: define typed dataclasses (`Settings`, `MappingDiscoveryResult`, `HarmonizationResult`, …).
- `_validators.py`: guard filesystem access, manifest JSON, and discovery samples; raise typed errors early.
- `tests/`: Given/When/Then-style fixtures and utilities for validation, discovery, and harmonization.

## Configuration & Settings
- `configure(...)` must be called before any API invocation; defaults are intentionally minimal.
- Required parameters: `api_key`, `api_url`.
- Optional parameters:
  - `timeout`: defaults to `21600.0` seconds (6 hours) to match long-running harmonization jobs.
  - `log_level`: normalized to standard logging levels (INFO by default).
  - `confidence_threshold`: defaults to `0.8`; used by the adapter to filter discovery candidates.
  - `discovery_use_gateway_bypass`: opt-in flag enabling the Lambda alias dispatcher.
  - `discovery_bypass_function` / `discovery_bypass_alias` / `discovery_bypass_region` / `discovery_bypass_profile`: override values for the bypass when enabled.
- Settings updates are guarded by a module-level lock and produce an immutable snapshot for callers; the logger level updates immediately to avoid stale verbosity.
- Optional AWS dependency (`boto3`) is exposed through the `aws` dependency group for environments that need the bypass.

## Public API
- `configure(*, api_key: str, api_url: str, timeout: float | None = None, log_level: str | None = None, confidence_threshold: float | None = None, discovery_use_gateway_bypass: bool | None = None, discovery_bypass_function: str | None = None, discovery_bypass_alias: str | None = None, discovery_bypass_region: str | None = None, discovery_bypass_profile: str | None = None) -> None`
  - Validates credentials, normalizes optional settings, and mutates the global `Settings` snapshot; raises `ClientConfigurationError` on invalid input.
- Discovery helpers (sync + async):
  - `discover_mapping`, `discover_mapping_async`
  - `discover_mapping_from_csv`, `discover_mapping_from_csv_async`
  - Accept target schema plus either prepared samples or a CSV path; return `MappingDiscoveryResult`; raise `MappingDiscoveryError` (domain failures), `MappingValidationError` (input issues), or `NetriasAPIUnavailable` (transport errors).
- Harmonization helpers (sync + async):
  - `harmonize`, `harmonize_async`
  - Require CSV + manifest paths, optional output path; return `HarmonizationResult` regardless of success/failure; raise `HarmonizationJobError` on submit/poll/timeout failures and `NetriasAPIUnavailable` on transport errors.
- Adapter helper:
  - `build_column_mapping_payload` (exported via `_adapter` for advanced consumers) converts discovery output into the manifest format expected by harmonization.

## Discovery Workflow
1. Validate schema (`validate_target_schema`) and column samples (`validate_column_samples`); CSV helpers (`discover_mapping_from_csv*`) read the header plus up to `sample_limit` rows (default 25) to build samples automatically.
2. Route based on configuration:
   - **Default (API Gateway)**: POST to `{api_url}/cde-recommendation` with payload `{ "body": "{...}" }`, sending the configured API key as `x-api-key`. Responses are parsed via `_interpret_discovery_response` and converted into `MappingDiscoveryResult`.
   - **Gateway Bypass (temporary)**: Call the `cde-recommendation` Lambda alias directly using boto3. Request and response shapes mimic the API Gateway proxy event (`{"body": "...", "isBase64Encoded": false}`); errors surface as `GatewayBypassError`, wrapped into `NetriasAPIUnavailable` for the public surface.
3. Suggestions are represented as `MappingSuggestion` / `MappingRecommendationOption` instances and stored alongside the raw payload for diagnostics.
4. Duration metrics are logged for success, transport errors, and bypass failures.

## Adapter Responsibilities
- `build_column_mapping_payload(result)` extracts the highest-confidence target per source column, filters below the configured threshold, and merges static CDE metadata (route, target field, `cdeId`).
- Unresolved columns (missing CDE metadata) are logged for observability; downstream harmonization can still proceed with passthrough mappings when appropriate.

## Harmonization Workflow
1. Validate inputs (`validate_source_path`, `validate_manifest_path`, `validate_output_path`). Output validation automatically versions existing destinations (`.harmonized.v1.csv`, `.v2`, …) rather than overwriting.
2. Build a gzip-compressed payload containing schema/document data plus the mapping manifest (`_http.build_harmonize_payload`). Hard fail if compression exceeds 10 MiB.
3. Submit the job via `POST {api_url}/v1/jobs/harmonize` with `Bearer` authentication; capture `jobId`.
4. Poll `GET {api_url}/v1/jobs/{jobId}` until the status is `SUCCEEDED` or `FAILED`. INFO logs include elapsed seconds per heartbeat; timeouts emit the accumulated duration before raising `HarmonizationJobError`.
5. Stream the final CSV via the signed `finalUrl`; successful downloads emit `HarmonizationResult(status="succeeded")`, while non-2xx responses return `status="failed"` with parsed error messaging. Transport errors raise `NetriasAPIUnavailable`.

## Data Models & Exceptions
- `Settings`: configuration snapshot, including gateway-bypass decisions.
- `MappingDiscoveryResult` / `MappingSuggestion` / `MappingRecommendationOption`: structured discovery outputs plus raw payload.
- `HarmonizationResult`: communicates file path, status (`succeeded`, `failed`, `timeout`), description, and optional mapping identifier.
- Exceptions inherit from `NetriasClientError`:
  - `ClientConfigurationError`, `FileValidationError`, `MappingDiscoveryError`, `MappingValidationError`, `OutputLocationError`, `NetriasAPIUnavailable`, `HarmonizationJobError`, `GatewayBypassError` (internal).

## Logging Strategy
- Logger name: `netrias_client`.
- INFO logs:
  - Discovery start/finish with duration, bypass invocation start/finish, adapter decisions.
  - Harmonization start, job submission, polling heartbeats (with elapsed seconds), download completion, and total workflow duration.
- ERROR logs precede raised exceptions; unresolved adapter entries are logged at INFO for visibility.

## Output & Filesystem Behavior
- Harmonized CSVs stream to disk via `_io.stream_download_to_file`; partial downloads are avoided through temp writes.
- Output naming: defaults to `<source>.harmonized.csv`; collisions are versioned (`.harmonized.v{n}.csv`).
- Manifest payloads written by the live harness (`live_test/test.py`) aid manual validation but are optional for API consumers.

## Testing & Tooling
- Test suite covers validation, configuration, discovery parsing, and harmonization control flow using fixtures under `src/netrias_client/tests/` with Given/When/Then comments.
- Recommended commands:
  - `uv run pytest`
  - `uv run ruff check`
  - `uv run basedpyright`
- Integration checks leverage `httpx` mock transports; live smoke testing is provided via `live_test/test.py` (requires `.env` values for API key and optional harmonization overrides).

## Extensibility & Roadmap Notes
- Future work: expand `_COLUMN_METADATA`, add XLSX ingestion, codify staged manifest schema, and introduce CLI/telemetry once core flows stabilize.
- Gateway bypass is explicitly temporary; once API Gateway timeouts are alleviated, the direct Lambda module can be retired and the configuration flags removed in a major/minor release.
