# Netrias Client Backend Design

## Module Layout
- `__init__.py`: expose public API (`configure`, `discover_mapping`, `harmonize`, async counterparts) plus version metadata.
- `_config.py`: manage runtime settings (API URL, key, timeout, log level) and header assembly; explicit configuration only, no env reads.
- `_http.py`: house request-building utilities and an `httpx`-backed transport used by sync and async entry points.
- `_validators.py`: provide pure validation helpers for CSV paths, manifest payload paths, output locations, and discovery payloads; raise typed errors on failure.
- `_io.py`: handle streaming uploads and streaming downloads to disk.
- `_core.py`: implement core harmonization workflow functions shared by sync and async facades.
- `_discovery.py`: orchestrate mapping discovery workflows, including CSV sampling helpers and normalized recommendation payloads for sync/async consumers.
- `_models.py`: define dataclasses (`Settings`, `HarmonizationResult`, `MappingDiscoveryResult`, etc.).
- `_logging.py`: centralize logger creation and enforce configured log level.

## Configuration Flow
- Explicit configuration via `configure(api_key=..., api_url=..., ...)` is required before use; no environment reads on import.
- `configure` updates the `Settings` instance; validation catches empty API URL/key, negative timeouts, and unsupported log levels.
- Logging configuration updates the module logger immediately after `Settings` mutation.
- Public calls obtain an immutable snapshot of `Settings`; both sync and async wrappers rely on the same snapshot to avoid concurrent mutation.

## Sync/Async Execution Strategy
- `_core` exposes async function `_harmonize_async` implementing the workflow end-to-end.
- Public async API (`harmonize_async`) awaits the core function directly.
- Public sync API (`harmonize`) runs the core async function inside `asyncio.run` to block the caller while reusing identical logic.
- This structure keeps validation, HTTP interaction, and error handling unified across sync and async paths.

## Validation Responsibilities
- `validate_source_path(path: Path) -> Path`
  - Ensure file exists and is not a directory.
  - Confirm `.csv` extension (case-insensitive) and enforce a hard-coded size limit (250 MB) prior to any network calls.
- `validate_manifest_path(path: Path) -> Path`
  - Ensure manifest JSON exists and is not a directory.
- `validate_output_path(path: Path | None, *, source_name: str) -> Path`
  - Default to `Path.cwd() / f"{source_name}.harmonized.csv"` when `path` is `None` or resolves to a directory.
  - Ensure parent directories exist and are writable; fail fast if the destination file already exists.

## API Interaction
- Use `httpx` to provide a transport layer supporting both sync and async usage across harmonization and discovery flows.
- Harmonization endpoint: `POST /v1/harmonization/run` with multipart CSV upload and JSON manifest payload; server responds with a streaming CSV and status metadata.
- Mapping discovery endpoint: `POST /cde-recommendation` (API Gateway) with JSON payload `{ "body": "{...}" }`; headers include `x-api-key` plus standard JSON content type.
- Discovery helpers accept either pre-built column samples or a CSV path with optional sample limits for quick client integration.
- For now, harmonization returns a `HarmonizationResult` even on non-2xx responses (`status == "failed"`). No exception is raised for domain failures.
- A simple retry/backoff policy can be layered later; the first cut keeps logic minimal and avoids bloat.

## Output Generation
- Harmonization responses stream to temporary files before atomic rename to the final location to avoid partial outputs.
- Output format always matches input (`.csv`), appended with `.harmonized.csv` when defaulting.
- Harmonization status derived from HTTP outcome and payload; `succeeded` for 2xx, `failed` for API domain errors, `timeout` when the client aborts due to deadline.
- `description` populated from API message fields or synthesized from client-side errors.

## Error Reporting Workflow
- Validation failures raise immediately with actionable messages.
- Harmonization API failures are returned as `HarmonizationResult(status="failed")` without raising.
- Mapping discovery API failures raise `MappingDiscoveryError` so callers can decide how to proceed before harmonization.
- Timeouts and connection failures surface as `NetriasAPIUnavailable`.
- All custom exceptions inherit from `NetriasClientError`.

## Logging
- `_logging` builds a `logging.Logger` named `netrias_client` configured according to `Settings.log_level`.
- Each harmonization attempt logs start, completion (with duration), and outcome status.
- WARN logs emitted for recoverable anomalies.

## Thread Safety & Concurrency
- Configuration mutations guarded by a module-level lock to prevent concurrent `configure` calls from racing.
- HTTP clients instantiated per workflow execution; connection pooling handled internally by `httpx`.
- Async pathways run within caller-provided event loops; sync wrappers create isolated loops to avoid interference.

## Testing & Quality Gates
- Validation helpers tested with temporary CSV assets under `src/netrias_client/tests/` following Given/When/Then structure.
- End-to-end smoke tests use `httpx.MockTransport` to simulate API responses for both sync and async code paths.
- `ruff check` and `basedpyright` run against the `netrias_client` package; CI enforces clean reports before merge.

## Roadmap Considerations
- Formalize `ColumnMapping` lookups for harmonization once the manifest schema stabilizes.
- Expand validation and IO layers to support XLSX (including multi-sheet selection).
- Add opt-in metrics/telemetry after core reliability and concurrency stories land.
- Introduce a CLI wrapper that composes the sync API without duplicating logic when user demand surfaces.
