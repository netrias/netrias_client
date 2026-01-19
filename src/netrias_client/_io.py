"""I/O helpers for streaming responses.

'why': keep file operations small and testable; avoid partial outputs
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import httpx


async def stream_download_to_file(response: httpx.Response, dest_path: Path) -> Path:
    """Stream an HTTP response body to `dest_path` atomically.

    Writes to a temporary file in the destination directory and then renames.
    """

    dest_path = Path(dest_path)
    tmp_dir = dest_path.parent
    tmp_dir.mkdir(parents=True, exist_ok=True)

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(dir=tmp_dir, delete=False, suffix=".partial") as tmp:
            tmp_path = Path(tmp.name)
            async for chunk in response.aiter_bytes():
                _ = tmp.write(chunk)
        _ = tmp_path.replace(dest_path)
        return dest_path
    except BaseException:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)
        raise

