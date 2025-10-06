"""Temporary gateway bypass helpers for direct Lambda invocation.

'why': mitigate API Gateway timeouts by calling the CDE recommendation alias directly

# TODO: remove this module once API Gateway latency is resolved and direct Lambda
# calls are no longer necessary.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from typing import Callable, IO, Protocol, cast


class GatewayBypassError(RuntimeError):
    """Raised when the direct Lambda invocation fails."""


class _LambdaClient(Protocol):
    def invoke(
        self,
        FunctionName: str,
        Qualifier: str,
        Payload: bytes,
    ) -> Mapping[str, object]:
        ...


class _ClientFactory(Protocol):
    def __call__(self, service_name: str, **kwargs: object) -> object:
        ...


class _SessionProtocol(Protocol):
    def client(self, service_name: str, **kwargs: object) -> object:
        ...


def invoke_cde_recommendation_alias(
    target_schema: str,
    columns: Mapping[str, Sequence[object]],
    function_name: str = "cde-recommendation",
    alias: str = "prod",
    region_name: str = "us-east-2",
    timeout_seconds: float | None = None,
    profile_name: str | None = None,
    logger: logging.Logger | None = None,
) -> Mapping[str, object]:
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

    active_logger = logger or logging.getLogger("netrias_client")

    active_logger.info(
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
        active_logger.error(
            "gateway bypass invoke failed: function=%s alias=%s err=%s",
            function_name,
            alias,
            exc,
        )
        raise GatewayBypassError(f"lambda invoke failed: {exc}") from exc

    status_code = response.get("StatusCode")
    payload_stream = cast(IO[bytes] | None, response.get("Payload"))
    raw_payload = _read_lambda_payload(payload_stream)
    payload = _json_payload(raw_payload)

    active_logger.info(
        "gateway bypass invoke complete: function=%s alias=%s status=%s",
        function_name,
        alias,
        status_code,
    )

    return _extract_body_mapping(payload)


def _build_lambda_client(
    region_name: str,
    profile_name: str | None,
    timeout_seconds: float | None,
) -> _LambdaClient:
    boto3, Config = _load_boto_dependencies()
    config = (
        Config(
            read_timeout=timeout_seconds,
            connect_timeout=min(timeout_seconds, 10.0),
        )
        if timeout_seconds is not None
        else None
    )

    if profile_name:
        session_factory = cast(
            Callable[..., object],
            getattr(boto3, "Session"),
        )
        session = cast(
            _SessionProtocol,
            session_factory(profile_name=profile_name, region_name=region_name),
        )
        factory = cast(_ClientFactory, session.client)
    else:
        factory = cast(_ClientFactory, getattr(boto3, "client"))

    return _lambda_client_from_factory(factory, region_name=region_name, config=config)


def _load_boto_dependencies():
    try:
        import boto3  # pyright: ignore[reportMissingTypeStubs]
        from botocore.config import Config  # pyright: ignore[reportMissingTypeStubs]
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise GatewayBypassError(
            "boto3 is required for the gateway bypass helper; install netrias-client[aws] or boto3 explicitly"
        ) from exc
    return boto3, Config


def _lambda_client_from_factory(
    factory: _ClientFactory,
    region_name: str,
    config: object | None,
) -> _LambdaClient:
    kwargs: dict[str, object] = {"region_name": region_name}
    if config is not None:
        kwargs["config"] = config
    client_obj = factory("lambda", **kwargs)
    return cast(_LambdaClient, client_obj)


def _read_lambda_payload(stream: IO[bytes] | None) -> bytes:
    if stream is None:
        return b""
    return stream.read()


def _json_payload(raw_payload: bytes) -> Mapping[str, object]:
    if not raw_payload:
        return {}
    try:
        return cast(Mapping[str, object], json.loads(raw_payload.decode("utf-8")))
    except json.JSONDecodeError as exc:  # pragma: no cover - unexpected lambda output
        raise GatewayBypassError(f"lambda returned non-JSON payload: {exc}") from exc


def _extract_body_mapping(payload: Mapping[str, object]) -> Mapping[str, object]:
    body = payload.get("body")
    if isinstance(body, str):
        try:
            return cast(Mapping[str, object], json.loads(body))
        except json.JSONDecodeError as exc:  # pragma: no cover - unexpected lambda output
            raise GatewayBypassError(f"lambda body was not valid JSON: {exc}") from exc
    return payload


def _normalized_columns(columns: Mapping[str, Sequence[object]]) -> dict[str, list[str]]:
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
