"""Async/sync bridging utilities.

'why': provide consistent sync wrappers that handle nested event loops (Jupyter, FastAPI)
"""
from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Coroutine
from typing import TypeVar

_T = TypeVar("_T")


def run_sync(coro: Coroutine[object, object, _T]) -> _T:
    """Run a coroutine synchronously, handling existing event loops.

    'why': asyncio.run() fails when called from within an existing event loop
    (e.g., Jupyter notebooks, FastAPI). This helper uses a ThreadPoolExecutor
    fallback when an event loop is already running.
    """

    try:
        _ = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(asyncio.run, coro)
        return future.result()
