"""Blocking CDE discovery via API Gateway + Step Functions polling.

'why': API Gateway returns executionArn immediately (no timeout), then poll
DescribeExecution for results. Avoids API Gateway's 45-second timeout limit.
"""
from __future__ import annotations

import base64
import json
import logging
import time
from collections.abc import Mapping
from typing import Final, Protocol, cast

import boto3  # pyright: ignore[reportMissingTypeStubs]
import httpx

from ._config import API_KEY_HEADER, ASYNC_POLL_INTERVAL_SECONDS, BYPASS_REGION
from ._errors import AsyncDiscoveryError
from ._models import ColumnSamples

TERMINAL_STATES: Final[frozenset[str]] = frozenset({"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"})


class _StepFunctionsClient(Protocol):
    def describe_execution(self, executionArn: str) -> Mapping[str, object]: ...


class _BotoClientFactory(Protocol):
    def __call__(self, service_name: str, *, region_name: str) -> object: ...


def discover_via_step_functions(
    api_url: str,
    target_schema: str,
    external_version_number: str,
    columns: list[ColumnSamples],
    timeout: float,
    logger: logging.Logger,
    top_k: int = 3,
    api_key: str | None = None,
) -> Mapping[str, object]:
    """POST to API Gateway, poll DescribeExecution, return results.

    'why': decouples request from long-running execution via Step Functions

    NOTE: This is a blocking/synchronous function.
    Uses time.sleep for polling and httpx sync client.
    """
    execution_arn = _start_execution(api_url, target_schema, external_version_number, columns, top_k, logger, api_key)
    return _poll_execution(execution_arn, timeout, logger)


def _start_execution(
    api_url: str,
    schema: str,
    external_version_number: str,
    columns: list[ColumnSamples],
    top_k: int,
    logger: logging.Logger,
    api_key: str | None = None,
) -> str:
    """POST request body to API Gateway, get executionArn back."""
    # CDE recommendation currently names this wire field target_version; keep
    # that transport name confined here.
    payload = {
        "target_schema": schema,
        "target_version": external_version_number,
        "columns": columns,
        "top_k": top_k,
    }
    # 'why': base64 encode payload to avoid VTL escaping issues with special characters
    encoded_payload = base64.b64encode(json.dumps(payload).encode()).decode()
    wrapper = {"encoded_payload": encoded_payload}

    url = f"{api_url.rstrip('/')}/recommend"
    logger.debug("async discovery: posting to %s", url)

    # 'why': cache-busting headers prevent API Gateway from returning stale responses
    headers: dict[str, str] = {"Cache-Control": "no-cache", "Pragma": "no-cache"}
    if api_key:
        headers[API_KEY_HEADER] = api_key
    with httpx.Client(timeout=30.0) as client:
        response = client.post(url, json=wrapper, headers=headers)
        _ = response.raise_for_status()
        result = cast(dict[str, object], response.json())

    execution_arn = result.get("executionArn")
    if not isinstance(execution_arn, str) or not execution_arn:
        raise AsyncDiscoveryError(f"API Gateway did not return executionArn: {result}")

    logger.info("async discovery: execution started arn=%s", execution_arn)
    return execution_arn


def _poll_execution(
    execution_arn: str,
    timeout: float,
    logger: logging.Logger,
) -> Mapping[str, object]:
    """Poll DescribeExecution until terminal state.

    'why': Step Functions execution runs asynchronously; we poll until it completes
    """
    region = _extract_region_from_arn(execution_arn)
    boto_client = cast(_BotoClientFactory, boto3.client)
    sfn = cast(_StepFunctionsClient, boto_client("stepfunctions", region_name=region))

    started = time.monotonic()
    deadline = started + timeout

    while time.monotonic() < deadline:
        response = sfn.describe_execution(executionArn=execution_arn)
        status = response["status"]
        elapsed = time.monotonic() - started

        if status == "SUCCEEDED":
            logger.info("async discovery: succeeded elapsed=%.2fs", elapsed)
            return _parse_output(_string_value(response, "output", "{}"))

        if status in TERMINAL_STATES:
            error = _string_value(response, "error", "unknown")
            cause = _string_value(response, "cause", "")
            logger.error("async discovery: %s error=%s cause=%s", status, error, cause)
            raise AsyncDiscoveryError(f"Execution {status}: {error} - {cause}")

        logger.debug("async discovery: polling status=%s elapsed=%.2fs", status, elapsed)
        time.sleep(ASYNC_POLL_INTERVAL_SECONDS)

    total_elapsed = time.monotonic() - started
    logger.error("async discovery: polling timed out elapsed=%.2fs", total_elapsed)
    raise AsyncDiscoveryError(f"Polling timed out after {total_elapsed:.1f}s")


def _extract_region_from_arn(arn: str) -> str:
    """Extract and validate AWS region from Step Functions execution ARN.

    ARN format: arn:aws:states:REGION:ACCOUNT:execution:STATE_MACHINE:EXECUTION_ID

    'why': validate region matches expected deployment to prevent SSRF via malicious ARN
    """

    parts = arn.split(":")
    if len(parts) < 4:
        raise AsyncDiscoveryError(f"Invalid execution ARN format: {arn}")
    region = parts[3]
    if region != BYPASS_REGION:
        raise AsyncDiscoveryError(f"Unexpected region in ARN: {region} (expected {BYPASS_REGION})")
    return region


def _parse_output(output_str: str) -> Mapping[str, object]:
    """Parse Step Functions output JSON.

    The Aggregate Lambda returns: {"statusCode": 200, "body": {...}}
    The body may be a string (JSON) or dict.
    """
    parsed = _safe_json_loads(output_str, "output")
    if not isinstance(parsed, dict):
        raise AsyncDiscoveryError("Output must be a JSON object")
    return _extract_body(cast(dict[str, object], parsed))


def _safe_json_loads(text: str, context: str) -> object:
    """Parse JSON or raise AsyncDiscoveryError with context."""
    try:
        return cast(object, json.loads(text))
    except json.JSONDecodeError as exc:
        raise AsyncDiscoveryError(f"Invalid JSON in {context}: {exc}") from exc


def _extract_body(parsed: dict[str, object]) -> Mapping[str, object]:
    """Extract body from parsed response, handling string-encoded JSON.

    'why': Step Functions wraps Lambda output; body may be string-encoded JSON
    """

    body = parsed.get("body", parsed)
    if isinstance(body, str):
        decoded = _safe_json_loads(body, "body")
        if not isinstance(decoded, dict):
            raise AsyncDiscoveryError("body must be a JSON object")
        return cast(dict[str, object], decoded)
    if isinstance(body, dict):
        return cast(dict[str, object], body)
    return parsed


def _string_value(payload: Mapping[str, object], key: str, default: str) -> str:
    value = payload.get(key, default)
    return value if isinstance(value, str) else default
