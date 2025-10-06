# Netrias Client Refactor Plan – Stateful NetriasClient (Plan B)

## Overview
Introduce a `NetriasClient` class that encapsulates configuration state and exposes the discovery and harmonization APIs (sync/async). Module-level functions remain as shims during a transition period, delegating to a default client instance.

## Rationale
- **Simplify the public surface:** Today callers must understand the sequencing of `configure`, `discover_mapping_from_csv`, `_adapter.build_column_mapping_payload`, and `harmonize`. The goal is to expose these capabilities through a single client object with clearly documented methods, so new users can discover everything via IDE/tooling without reading internal modules.
- **Eliminate global state coupling:** Current helpers share global configuration stored in `_settings`, making concurrent usage with different credentials awkward and error-prone. A stateful client will allow multiple independent flows (e.g., different tenants) within the same process.
- **Align with common SDK patterns:** Most API SDKs provide an instance-based facade (e.g., `Client().do_something()`), which reduces surprise for integrators and makes the library feel consistent with expectations.
- **Improve testability and future extensibility:** Instance-level state makes it easier to swap transports, inject metrics/caching, and run isolated tests without heavy monkeypatching.

## Current Baseline (October 2025)
- `configure(api_key=..., ...)` is the only configuration entry point; discovery and harmonization URLs are fixed internally (`https://api.netriasbdf.cloud` and `https://tbdxz7nffi.execute-api.us-east-2.amazonaws.com`). Gateway bypass defaults to `True`.
- Discovery helpers (`discover_mapping*`) now return manifest payloads (`{"column_mappings": {...}}`), not the raw `MappingDiscoveryResult` dataclass. `_adapter` is an internal detail.
- `harmonize`/`harmonize_async` accept either a manifest mapping or a `Path`, with optional `manifest_output_path` to persist the JSON.
- Live harness resides under `netrias_client/live_test/` and is runnable via `uv run live-test` (script defined in `[tool.uv.scripts]`).
- Tests expect the single-key flow (bypass is disabled in fixtures) and assert that manifests already contain the resolved `column_mappings` structure.
- Logging, timeout configuration, and gateway bypass options remain centralized in `_config`; settings currently include discovery/harmonization URLs, bypass metadata, confidence threshold, etc.

## Detailed Steps
1. **Client Class & Settings Management**
   - Implement `NetriasClient` (new module or in `__init__.py`) containing:
     - An immutable `Settings` snapshot (API key, discovery URL, harmonization URL, timeout, log level, confidence threshold, gateway bypass options).
     - Methods: `configure`, `discover_mapping`, `discover_mapping_async`, `discover_mapping_from_csv`, `discover_mapping_from_csv_async`, `harmonize`, `harmonize_async`.
   - `configure` validates inputs (route through existing helpers) and stores the normalized `Settings` on the instance. Expose a read-only accessor (`client.settings`) for debugging/tests.
   - Provide convenience constructor/`from_env` if it simplifies consumer code (optional).

2. **Refactor Core Modules to Use Instance State**
   - Update `_discovery`, `_core`, `_adapter`, `_gateway_bypass`, `_http`, `_validators` to accept a `Settings` argument supplied by the client rather than calling `get_settings()`.
   - Ensure async workflows capture the `Settings` snapshot at entry so configuration changes do not affect in-flight calls.
   - Maintain logging calls but ensure the configured log level is applied when instantiating the client (consider whether logger should remain global or become per-client).

3. **Module-Level Compatibility Layer**
   - Create a module-scoped default client (`DEFAULT_CLIENT = NetriasClient()`).
   - Keep existing functions (`configure`, `discover_mapping*`, `harmonize*`) but have them delegate to `DEFAULT_CLIENT`.
   - Document the legacy entry points as supported but encourage new code to instantiate and use `NetriasClient` directly.

4. **Documentation & Discoverability**
   - Update README, architecture notes, and live test examples to showcase the object-oriented workflow:
     ```python
     from netrias_client import NetriasClient

     client = NetriasClient()
     client.configure(api_key="...")
     manifest = client.discover_mapping_from_csv(Path("data.csv"))
     result = client.harmonize(Path("data.csv"), manifest, manifest_output_path=Path("manifest.json"))
     ```
   - Highlight async usage (`await client.harmonize_async(...)`) and mention that multiple clients can operate simultaneously with different credentials.

5. **Testing Adjustments**
   - Replace fixtures that mutate global configuration with fixtures that yield a configured `NetriasClient` instance (disable bypass in tests where deterministic mock transports are used).
   - Add regression tests ensuring the module-level shims still behave as expected (manifest dict returned, harmonize accepts both dict/path).
   - Add tests for multiple clients to verify isolation (e.g., different API keys, toggling bypass) and confirm shared single-key behaviour.
   - Update live-test documentation/runbook to reference `uv run live-test`.

6. **Type Hints & Public Surface**
   - Export `NetriasClient` via `__all__`.
   - Add comprehensive type hints/docstrings for each public method to improve IDE discoverability.
   - Consider a `Protocol` or TypedDict for harmonization/discovery results if it improves clarity.

7. **Future Enhancements (Post-Refactor)**
   - Consider optional constructor arguments (e.g., `NetriasClient(api_key=..., bypass=False)`), per-client transport configuration, and metrics hooks once the facade stabilizes.
   - Evaluate whether to deprecate the module-level functions in a future major release once consumers migrate.

## Additional Notes
- Discovery API unit tests currently simulate HTTP responses via `json_success`; ensure the new return type expectations (manifest dict) remain aligned when refactoring internals.
- Harmonization tests rely on the `job_success` helper to emulate submit/poll/download; the client refactor must continue to allow injection of custom transports for these scenarios.
- Keep the UV scripts (`pytest`, `live-test`) working throughout the refactor so CLI workflows remain stable.
- Watch for logging semantics: the current logger is module-global; if moving to per-client logging, update tests that assert on log content.

## Pros
- **Isolated State:** Multiple clients can run concurrently with different credentials and configuration without touching global mutable state.
- **Improved Ergonomics:** Users interact with a single, discoverable object exposing all capabilities (sync and async).
- **Better Testability:** Tests can instantiate dedicated clients with precise settings, reducing reliance on monkeypatching globals.
- **Extensibility:** Easy to add per-client features (custom transports, metrics, caching) without impacting other users.

## Cons / Risks
- **Refactor Complexity:** Requires threading `Settings` through all key modules; thorough review and regression testing needed to avoid breakage.
- **Transition Overhead:** Need to maintain both object and function APIs until users migrate; documentation must clearly indicate the preferred path.
- **Logging Behavior:** Decide whether log level remains process-wide or becomes per-client; inconsistent handling could surprise users.
- **Backward Compatibility:** Must ensure existing behavior (error types, logging messages, defaults) remains intact through shims.
