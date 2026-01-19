# Netrias Client Architecture

## Responsibility & Scope
- Provide a focused Python interface for the Netrias discovery and harmonization services while hiding transport details and enforcing validation guard rails.
- Support CSV inputs today with async and sync entry points; XLSX and broader formats remain future enhancements.
- Maintain transparency on failures by surfacing structured errors, typed results, and detailed logs rather than silent retries.

## Module Overview
- `__init__.py`: surface the public API (`NetriasClient`) and version metadata.
- `_adapter.py`: internal helpers that translate discovery responses into harmonization manifest snippets, enforcing confidence thresholds and hydrating static CDE metadata.
- `_client.py`: implement the stateful `NetriasClient` facade.
- `_config.py`: validate configuration inputs, normalize gateway-bypass options, and build immutable `Settings` snapshots.
- `_core.py`: contain the harmonization workflow (submit → poll → download) shared by sync/async entry points.
- `_discovery.py`: implement discovery workflows, CSV sampling helpers, and conditional routing between API Gateway and the Lambda alias bypass.
- `_errors.py`: collect the client exception taxonomy (`ClientConfigurationError`, `MappingDiscoveryError`, `NetriasAPIUnavailable`, `DataModelStoreError`, etc.).
- `_gateway_bypass.py`: temporary helpers that invoke the `cde-recommendation` Lambda alias directly via boto3.
- `_http.py`: build harmonization payloads, submit jobs, fetch job status, perform discovery requests, and query the Data Model Store via HTTPX.
- `_data_model_store.py`: business logic for querying data models, CDEs, and permissible values; provides async-first functions with sync wrappers that handle existing event loops via `ThreadPoolExecutor` fallback.
- `_sfn_discovery.py`: Step Functions-based discovery polling using `discover_via_step_functions()` (blocking/synchronous; uses `time.sleep()` for polling).
- `_io.py`: stream API responses to disk to avoid loading large files into memory.
- `_logging.py`: create a namespaced logger (`netrias_client`) that honors configured log level.
- `_models.py`: define typed dataclasses (`Settings`, `MappingDiscoveryResult`, `HarmonizationResult`, `DataModel`, `CDE`, `PermissibleValue`, …).
- `_validators.py`: guard filesystem access, manifest JSON, and discovery samples; raise typed errors early.
- `tests/`: Given/When/Then-style fixtures and utilities for validation, discovery, and harmonization.

## Configuration & Settings
- `NetriasClient(api_key)` initializes the client with an API key and default settings; the client is ready to use immediately after construction.
- `NetriasClient.configure(...)` optionally adjusts non-default settings. Unspecified parameters preserve their current values (incremental configuration). Discovery calls require an explicit target schema per invocation.
- All `configure()` parameters are optional:
  - `timeout`: defaults to `1200.0` seconds (20 minutes).
  - `log_level`: accepts a string value (`"CRITICAL"`, `"ERROR"`, `"WARNING"`, `"INFO"`, `"DEBUG"`); defaults to `"INFO"`.
  - `discovery_use_gateway_bypass`: opt-in flag enabling the Lambda alias dispatcher (other bypass parameters remain fixed by the library).
  - URL overrides (`discovery_url`, `harmonization_url`, `data_model_store_url`): enable testing against staging environments without code changes.
- `confidence_threshold` is a per-call parameter on discovery methods (not on `configure()`), allowing callers to adjust filtering per invocation.
- Settings updates live on each `NetriasClient` instance; `.settings` returns a defensive copy. Logger level updates immediately to avoid stale verbosity.
- Operations snapshot settings and logger atomically via `OperationContext` to ensure thread-safe behavior when `configure()` is called concurrently.
- Optional AWS dependency (`boto3`) is exposed through the `aws` dependency group for environments that need the bypass.

## Public API
- `NetriasClient`
  - `__init__(api_key)` – validates the API key and initializes the client with default settings; raises `ClientConfigurationError` on invalid input.
  - `configure(...)` – optionally adjusts settings; stores an immutable `Settings` snapshot; raises `ClientConfigurationError` on invalid input.
  - `discover_mapping_from_csv`, `discover_mapping_from_csv_async` – derive samples from a CSV and return manifest payloads; accept `confidence_threshold` to filter recommendations; raise `MappingDiscoveryError`, `MappingValidationError`, or `NetriasAPIUnavailable`.
  - `harmonize`, `harmonize_async` – require CSV + manifest (path or mapping) and optional output destinations; return `HarmonizationResult` even on failure; raise `NetriasAPIUnavailable` on transport errors.
- Consumers instantiate `NetriasClient`; configuration snapshots and loggers live on the instance rather than module-wide globals.
- Adapter helpers remain internal; discovery APIs automatically convert responses into harmonization manifest payloads for callers.
- Data Model Store methods:
  - `list_data_models`, `list_data_models_async` – query available data commons; return `tuple[DataModel, ...]`.
  - `list_cdes`, `list_cdes_async` – query CDEs for a model version; return `tuple[CDE, ...]`.
  - `list_pvs`, `list_pvs_async` – query permissible values for a CDE; return `tuple[PermissibleValue, ...]`.
  - `get_pv_set`, `get_pv_set_async` – auto-paginate and return `frozenset[str]` for O(1) membership testing.
  - `validate_value`, `validate_value_async` – convenience methods returning `bool` for single-value validation.

## Discovery Workflow
1. Validate schema (`validate_target_schema`) and column samples (`validate_column_samples`); CSV helpers (`discover_mapping_from_csv*`) read the header plus up to `sample_limit` rows (default 25) to build samples automatically.
2. Route based on configuration:
   - **Default (API Gateway)**: POST to the built-in discovery URL with payload `{ "body": "{...}" }`, sending the configured API key as `x-api-key`. Responses are parsed via `_interpret_discovery_response` and converted into manifest payloads.
   - **Gateway Bypass (temporary)**: Call the `cde-recommendation` Lambda alias directly using boto3. Request and response shapes mimic the API Gateway proxy event (`{"body": "...", "isBase64Encoded": false}`); errors surface as `GatewayBypassError`, wrapped into `NetriasAPIUnavailable` for the public surface.
3. Suggestions are represented as `MappingSuggestion` / `MappingRecommendationOption` instances and stored alongside the raw payload for diagnostics.
4. Duration metrics are logged for success, transport errors, and bypass failures.

## Adapter Responsibilities
- Discovery normalization extracts the highest-confidence target per source column, filters below the configured threshold, and merges static CDE metadata (route, target field, `cdeId`).
- Unresolved columns (missing CDE metadata) are logged for observability; downstream harmonization can still proceed with passthrough mappings when appropriate.

## Harmonization Workflow
1. Validate inputs (`validate_source_path`, `validate_manifest_path`, `validate_output_path`). Output validation automatically versions existing destinations (`.harmonized.v1.csv`, `.v2`, …) rather than overwriting.
2. Build a gzip-compressed payload containing schema/document data plus the mapping manifest (`_http.build_harmonize_payload`). Hard fail if compression exceeds 10 MiB.
3. Submit the job via `POST <harmonization_url>/v1/jobs/harmonize` with `Bearer` authentication; capture `jobId`.
4. Poll `GET <harmonization_url>/v1/jobs/{jobId}` until the status is `SUCCEEDED` or `FAILED`. INFO logs include elapsed seconds per heartbeat; timeouts emit the accumulated duration before raising `HarmonizationJobError`.
5. Stream the final CSV via the signed `finalUrl`; successful downloads emit `HarmonizationResult(status="succeeded")`, while non-2xx responses return `status="failed"` with parsed error messaging. Transport errors raise `NetriasAPIUnavailable`.

## Data Model Store Workflow
The Data Model Store API provides read-only access to reference data for validation use cases.

1. **List data models**: Query available data commons via `GET /data-models`. Returns `DataModel` instances with key, name, and description.
2. **List CDEs**: Query CDEs for a model version via `GET /data-models/{key}/versions/{version}/cdes`. Returns `CDE` instances with key, IDs, and optional description.
3. **List PVs**: Query permissible values for a CDE via `GET /data-models/{key}/versions/{version}/cdes/{cde_key}/pvs`. Returns `PermissibleValue` instances.
4. **Validation helpers**: `get_pv_set()` auto-paginates and returns `frozenset[str]` for O(1) membership testing. `validate_value()` provides a one-liner for checking a single value.

All methods follow the async-first pattern with sync wrappers. Sync wrappers detect existing event loops and use `ThreadPoolExecutor` fallback to avoid `asyncio.run()` conflicts in Jupyter/FastAPI/Django contexts. Path parameters are URL-encoded to prevent injection. Errors surface as `DataModelStoreError` (4xx) or `NetriasAPIUnavailable` (5xx/timeout). Pagination is limited to 100 pages to prevent infinite loops.

## Data Models & Exceptions
- `Settings`: configuration snapshot, including gateway-bypass decisions and optional per-client log directory.
- `MappingDiscoveryResult` / `MappingSuggestion` / `MappingRecommendationOption`: structured discovery outputs plus raw payload.
- `HarmonizationResult`: communicates file path, status (`succeeded`, `failed`, `timeout`), description, and optional mapping identifier.
- `DataModel` / `CDE` / `PermissibleValue`: reference data from the Data Model Store for validation use cases.
- Exceptions inherit from `NetriasClientError`:
  - `ClientConfigurationError`, `FileValidationError`, `MappingDiscoveryError`, `MappingValidationError`, `OutputLocationError`, `NetriasAPIUnavailable`, `HarmonizationJobError`, `DataModelStoreError`, `GatewayBypassError` (internal).

## Logging Strategy
- Logger namespace: `netrias_client` (exported as `LOGGER_NAMESPACE` for programmatic access).
- Each client instance creates a child logger under this namespace (e.g., `netrias_client.instance.a1b2c3d4`).
- External configuration: users can configure the parent logger before creating clients; handlers propagate to all instances.
- `NetriasClient.__init__()` wires stream handlers if the parent has none; subsequent `configure()` calls may add a file handler when `log_directory` is supplied.
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
