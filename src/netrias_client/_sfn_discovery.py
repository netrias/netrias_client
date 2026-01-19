"""Async CDE discovery via API Gateway + Step Functions polling.

'why': API Gateway returns executionArn immediately (no timeout), then poll
DescribeExecution for results. Avoids API Gateway's 45-second timeout limit.
"""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping, Sequence
from typing import Final

import boto3  # pyright: ignore[reportMissingTypeStubs]
import httpx

from ._config import ASYNC_POLL_INTERVAL_SECONDS
from ._errors import AsyncDiscoveryError

TERMINAL_STATES: Final[frozenset[str]] = frozenset({"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"})


def discover_via_step_functions(
    api_url: str,
    target_schema: str,
    target_version: str,
    columns: Mapping[str, Sequence[str]],
    timeout: float,
    logger: logging.Logger,
    top_k: int = 3,
) -> Mapping[str, object]:
    """POST to API Gateway, poll DescribeExecution, return results.

    'why': decouples request from long-running execution via Step Functions

    NOTE: This is a blocking/synchronous function.
    Uses time.sleep for polling and httpx sync client.
    """
    execution_arn = _start_execution(api_url, target_schema, target_version, columns, top_k, logger)
    return _poll_execution(execution_arn, timeout, logger)


def _start_execution(
    api_url: str,
    schema: str,
    version: str,
    columns: Mapping[str, Sequence[str]],
    top_k: int,
    logger: logging.Logger,
) -> str:
    """POST request body to API Gateway, get executionArn back."""
    payload = {
        "target_schema": schema,
        "target_version": version,
        "data": dict(columns),
        "top_k": top_k,
    }
    url = f"{api_url.rstrip('/')}/recommend"
    logger.debug("async discovery: posting to %s", url)

    # 'why': cache-busting headers prevent API Gateway from returning stale responses
    headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"}
    with httpx.Client(timeout=30.0) as client:
        response = client.post(url, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()

    execution_arn = result.get("executionArn")
    if not execution_arn:
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
    sfn = boto3.client("stepfunctions", region_name=region)

    started = time.monotonic()
    deadline = started + timeout

    while time.monotonic() < deadline:
        response = sfn.describe_execution(executionArn=execution_arn)
        status = response["status"]
        elapsed = time.monotonic() - started

        if status == "SUCCEEDED":
            logger.info("async discovery: succeeded elapsed=%.2fs", elapsed)
            return _parse_output(response.get("output", "{}"))

        if status in TERMINAL_STATES:
            error = response.get("error", "unknown")
            cause = response.get("cause", "")
            logger.error("async discovery: %s error=%s cause=%s", status, error, cause)
            raise AsyncDiscoveryError(f"Execution {status}: {error} - {cause}")

        logger.debug("async discovery: polling status=%s elapsed=%.2fs", status, elapsed)
        time.sleep(ASYNC_POLL_INTERVAL_SECONDS)

    total_elapsed = time.monotonic() - started
    logger.error("async discovery: polling timed out elapsed=%.2fs", total_elapsed)
    raise AsyncDiscoveryError(f"Polling timed out after {total_elapsed:.1f}s")


def _extract_region_from_arn(arn: str) -> str:
    """Extract AWS region from Step Functions execution ARN.

    ARN format: arn:aws:states:REGION:ACCOUNT:execution:STATE_MACHINE:EXECUTION_ID

    'why': boto3 client needs region; ARN is authoritative source
    """

    parts = arn.split(":")
    if len(parts) < 4:
        raise AsyncDiscoveryError(f"Invalid execution ARN format: {arn}")
    return parts[3]


def _parse_output(output_str: str) -> Mapping[str, object]:
    """Parse Step Functions output JSON.

    The Aggregate Lambda returns: {"statusCode": 200, "body": {...}}
    The body may be a string (JSON) or dict.
    """
    parsed = _safe_json_loads(output_str, "output")
    if not isinstance(parsed, dict):
        raise AsyncDiscoveryError("Output must be a JSON object")
    return _extract_body(parsed)


def _safe_json_loads(text: str, context: str) -> object:
    """Parse JSON or raise AsyncDiscoveryError with context."""
    try:
        return json.loads(text)
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
        return decoded
    if isinstance(body, dict):
        return body
    return parsed
