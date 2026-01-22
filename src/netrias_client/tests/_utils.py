"""Offer reusable test utilities.

'why': centralize HTTP mocking and request capture for scenario assertions
"""
from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
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


def job_success(
    chunks: Sequence[bytes],
    job_id: str = "job-123",
    final_url: str = "https://mock.netrias/result.csv",
) -> MockTransportCapture:
    """Return a mock transport that simulates the multi-step harmonization workflow.

    'why': cover submit, poll, and download interactions without touching the network
    """

    recorded: list[httpx.Request] = []
    state = _JobState()

    async def handler(request: httpx.Request) -> httpx.Response:
        recorded.append(request)
        response = _resolve_job_response(request, job_id, final_url, chunks, state)
        if response is None:
            raise AssertionError(f"unexpected request during job_success: {request.method} {request.url}")
        return response

    return MockTransportCapture(httpx.MockTransport(handler), recorded)


def json_failure(payload: dict[str, object], status_code: int) -> MockTransportCapture:
    """Return a mock transport returning a JSON failure payload.

    'why': drive domain failure scenarios with realistic API responses
    """

    recorded: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        recorded.append(request)
        body = json.dumps(payload).encode("utf-8")
        return httpx.Response(status_code, headers={"content-type": "application/json"}, content=body, request=request)

    return MockTransportCapture(httpx.MockTransport(handler), recorded)


def json_success(payload: Mapping[str, object], status_code: int = 200) -> MockTransportCapture:
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
    """Patch httpx clients within modules to use the provided transport.

    'why': ensure the client under test routes through controlled mock transports
    """

    original_async_client = httpx.AsyncClient
    original_sync_client = httpx.Client

    class _PatchedAsyncClient(original_async_client):
        def __init__(self, **kwargs: object) -> None:  # type: ignore[override]
            kwdict: dict[str, object] = dict(kwargs)
            kwdict["transport"] = capture.transport
            super().__init__(**kwdict)  # pyright: ignore[reportArgumentType]

    class _PatchedSyncClient(original_sync_client):
        """Sync client wrapper that delegates to async transport via sync_execute.

        'why': _sfn_discovery uses sync httpx.Client; we need to mock it too
        """

        def __init__(self, **kwargs: object) -> None:  # type: ignore[override]
            kwdict: dict[str, object] = dict(kwargs)
            kwdict["transport"] = capture.transport
            super().__init__(**kwdict)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr("netrias_client._http.httpx.AsyncClient", _PatchedAsyncClient)
    monkeypatch.setattr("netrias_client._core.httpx.AsyncClient", _PatchedAsyncClient)
    monkeypatch.setattr("netrias_client._data_model_store.httpx.AsyncClient", _PatchedAsyncClient)
    monkeypatch.setattr("netrias_client._sfn_discovery.httpx.Client", _PatchedSyncClient)


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



@dataclass(slots=True)
class _JobState:
    status_served: bool = False


def _job_submit_response(request: httpx.Request, job_id: str) -> httpx.Response | None:
    if request.method != "POST":
        return None
    if not request.url.path.endswith("/v1/jobs/harmonize"):
        return None
    return httpx.Response(202, json={"job_id": job_id}, request=request)


def _job_status_response(
    request: httpx.Request,
    job_id: str,
    final_url: str,
    state: _JobState,
) -> httpx.Response | None:
    if request.method != "GET":
        return None
    if not request.url.path.endswith(f"/v1/jobs/{job_id}"):
        return None
    state.status_served = True
    payload = {"status": "SUCCEEDED", "finalUrl": final_url}
    return httpx.Response(200, json=payload, request=request)


def _job_download_response(
    request: httpx.Request,
    final_url: str,
    chunks: Sequence[bytes],
) -> httpx.Response | None:
    if request.method != "GET":
        return None
    if str(request.url) != final_url:
        return None
    return httpx.Response(
        200,
        headers={"Content-Type": "text/csv"},
        stream=_ChunkStream(chunks),
        request=request,
    )


def _resolve_job_response(
    request: httpx.Request,
    job_id: str,
    final_url: str,
    chunks: Sequence[bytes],
    state: _JobState,
) -> httpx.Response | None:
    response = _job_submit_response(request, job_id)
    if response is not None:
        return response
    response = _job_status_response(request, job_id, final_url, state)
    if response is not None:
        return response
    return _job_download_response(request, final_url, chunks)


def paginated_pv_responses(
    pages: Sequence[Sequence[Mapping[str, object]]],
) -> MockTransportCapture:
    """Return a mock transport that returns paginated PV responses.

    'why': test get_pv_set auto-pagination across multiple pages
    """

    recorded: list[httpx.Request] = []
    call_count = [0]

    async def handler(request: httpx.Request) -> httpx.Response:
        recorded.append(request)
        page_index = call_count[0]
        call_count[0] += 1

        if page_index >= len(pages):
            payload: dict[str, object] = {"total": 0, "items": []}
        else:
            page = pages[page_index]
            payload = {"total": len(page), "items": list(page)}

        return httpx.Response(200, json=payload, request=request)

    return MockTransportCapture(httpx.MockTransport(handler), recorded)
