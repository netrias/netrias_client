"""Offer reusable test utilities.

'why': centralize HTTP mocking and request capture for scenario assertions
"""
from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import override

import httpx
from pytest import MonkeyPatch


@dataclass(slots=True)
class MockTransportCapture:
    """Capture requests emitted during a mocked exchange.

    'why': allow tests to assert on request construction without global state
    """

    transport: httpx.MockTransport
    requests: list[httpx.Request]


def streaming_success(
    chunks: Sequence[bytes], *, headers: dict[str, str] | None = None, status_code: int = 200
) -> MockTransportCapture:
    """Return a mock transport streaming CSV bytes.

    'why': reuse identical streaming responses for success scenarios
    """

    recorded: list[httpx.Request] = []

    handler = partial(
        _streaming_handler,
        chunks=tuple(chunks),
        headers=headers,
        status_code=status_code,
        recorded=recorded,
    )

    return MockTransportCapture(httpx.MockTransport(handler), recorded)


def json_failure(payload: dict[str, object], *, status_code: int) -> MockTransportCapture:
    """Return a mock transport returning a JSON failure payload.

    'why': drive domain failure scenarios with realistic API responses
    """

    recorded: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        recorded.append(request)
        body = json.dumps(payload).encode("utf-8")
        return httpx.Response(status_code, headers={"content-type": "application/json"}, content=body, request=request)

    return MockTransportCapture(httpx.MockTransport(handler), recorded)


def json_success(payload: Mapping[str, object], *, status_code: int = 200) -> MockTransportCapture:
    """Return a mock transport yielding a JSON success payload."""

    recorded: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        recorded.append(request)
        return httpx.Response(status_code, json=payload, request=request)

    return MockTransportCapture(httpx.MockTransport(handler), recorded)


def transport_error(exc: Exception) -> MockTransportCapture:
    """Return a mock transport that raises the provided exception.

    'why': simplify negative-path tests covering transport failures
    """

    recorded: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - exception path only
        recorded.append(request)
        raise exc

    return MockTransportCapture(httpx.MockTransport(handler), recorded)


def install_mock_transport(monkeypatch: MonkeyPatch, capture: MockTransportCapture) -> None:
    """Patch `httpx.AsyncClient` within `_http` to use the provided transport.

    'why': ensure the client under test routes through controlled mock transports
    """

    original_async_client = httpx.AsyncClient

    class _PatchedAsyncClient(original_async_client):
        def __init__(self, *args: object, **kwargs: object) -> None:  # type: ignore[override]
            kwdict: dict[str, object] = dict(kwargs)
            kwdict["transport"] = capture.transport
            super().__init__(*args, **kwdict)

    monkeypatch.setattr("netrias_client._http.httpx.AsyncClient", _PatchedAsyncClient)


class _ChunkStream(httpx.AsyncByteStream):
    """Provide an async byte stream backed by an in-memory sequence."""

    def __init__(self, payload: Sequence[bytes]) -> None:
        self._payload: list[bytes] = list(payload)

    async def aiter_bytes(self) -> AsyncIterator[bytes]:
        for chunk in self._payload:
            yield chunk

    @override
    def __aiter__(self) -> AsyncIterator[bytes]:  # pragma: no cover - delegated to aiter_bytes
        return self.aiter_bytes()


async def _streaming_handler(
    request: httpx.Request,
    *,
    chunks: Sequence[bytes],
    headers: dict[str, str] | None,
    status_code: int,
    recorded: list[httpx.Request],
) -> httpx.Response:
    recorded.append(request)
    response_headers = headers or {"Content-Type": "text/csv"}
    return httpx.Response(
        status_code,
        headers=response_headers,
        stream=_ChunkStream(chunks),
        request=request,
    )
