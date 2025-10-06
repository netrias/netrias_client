"""Temporary gateway bypass helpers for direct Lambda invocation.

'why': mitigate API Gateway timeouts by calling the CDE recommendation alias directly
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Sequence

from ._logging import get_logger


_logger = get_logger()


class GatewayBypassError(RuntimeError):
    """Raised when the direct Lambda invocation fails."""


def invoke_cde_recommendation_alias(
    *,
    target_schema: str,
    columns: Mapping[str, Sequence[str]],
    function_name: str = "cde-recommendation",
    alias: str = "prod",
    region_name: str = "us-east-2",
    timeout_seconds: float | None = None,
    profile_name: str | None = None,
) -> Mapping[str, Any]:
    """Call the CDE recommendation Lambda alias directly and return its parsed payload.

    NOTE: This bypass is temporary. Prefer the public API once API Gateway limits are addressed.
    """

    client = _build_lambda_client(
        region_name=region_name,
        profile_name=profile_name,
        timeout_seconds=timeout_seconds,
    )
    normalized_columns = _normalized_columns(columns)
    body = json.dumps({"target_schema": target_schema, "data": normalized_columns})
    event = {"body": body, "isBase64Encoded": False}

    _logger.info(
        "gateway bypass invoke start: function=%s alias=%s schema=%s columns=%s",
        function_name,
        alias,
        target_schema,
        len(columns),
    )

    try:
        response = client.invoke(
            FunctionName=function_name,
            Qualifier=alias,
            Payload=json.dumps(event).encode("utf-8"),
        )
    except Exception as exc:  # pragma: no cover - boto3 specific
        _logger.error(
            "gateway bypass invoke failed: function=%s alias=%s err=%s",
            function_name,
            alias,
            exc,
        )
        raise GatewayBypassError(f"lambda invoke failed: {exc}") from exc

    status_code = response.get("StatusCode")
    payload_stream = response.get("Payload")
    raw_payload = payload_stream.read() if payload_stream is not None else b""

    try:
        payload = json.loads(raw_payload.decode("utf-8")) if raw_payload else {}
    except json.JSONDecodeError as exc:  # pragma: no cover - unexpected lambda output
        raise GatewayBypassError(f"lambda returned non-JSON payload: {exc}") from exc

    _logger.info(
        "gateway bypass invoke complete: function=%s alias=%s status=%s",
        function_name,
        alias,
        status_code,
    )

    return _extract_body_mapping(payload)


def _build_lambda_client(
    *,
    region_name: str,
    profile_name: str | None,
    timeout_seconds: float | None,
):
    try:
        import boto3  # type: ignore
        from botocore.config import Config  # type: ignore
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise GatewayBypassError(
            "boto3 is required for the gateway bypass helper; install netrias-client[aws] or boto3 explicitly"
        ) from exc

    client_kwargs: dict[str, object] = {"region_name": region_name}
    if timeout_seconds is not None:
        client_kwargs["config"] = Config(
            read_timeout=timeout_seconds,
            connect_timeout=min(timeout_seconds, 10.0),
        )

    if profile_name:
        session = boto3.Session(profile_name=profile_name, region_name=region_name)
        return session.client("lambda", **client_kwargs)
    return boto3.client("lambda", **client_kwargs)


def _extract_body_mapping(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    body = payload.get("body")
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:  # pragma: no cover - unexpected lambda output
            raise GatewayBypassError(f"lambda body was not valid JSON: {exc}") from exc
    return payload


def _normalized_columns(columns: Mapping[str, Sequence[str]]) -> dict[str, list[str]]:
    normalized: dict[str, list[str]] = {}
    for key, values in columns.items():
        name = _normalized_column_key(key)
        if name is None:
            continue
        cleaned = _normalized_column_values(values)
        if cleaned:
            normalized[name] = cleaned
    return normalized


def _normalized_column_key(raw: str) -> str | None:
    text = raw.strip()
    return text or None


def _normalized_column_values(values: Sequence[object]) -> list[str]:
    return [text for text in (_normalized_column_value(value) for value in values) if text]


def _normalized_column_value(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
