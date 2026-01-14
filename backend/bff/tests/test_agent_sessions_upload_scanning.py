from __future__ import annotations

import asyncio
import struct
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from bff.routers.agent_sessions import _clamav_instream_scan, _scan_upload_bytes


async def _start_fake_clamd(*, response: str):  # noqa: ANN001
    async def _handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        cmd = await reader.readuntil(b"\x00")
        assert cmd == b"zINSTREAM\x00"
        while True:
            size_raw = await reader.readexactly(4)
            size = struct.unpack("!I", size_raw)[0]
            if size == 0:
                break
            await reader.readexactly(size)
        writer.write(response.encode("utf-8"))
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(_handler, "127.0.0.1", 0)
    sock = next(iter(server.sockets or []))
    host, port = sock.getsockname()[:2]
    return server, host, int(port)


@pytest.mark.asyncio
async def test_clamav_instream_scan_ok() -> None:
    server, host, port = await _start_fake_clamd(response="stream: OK\n")
    try:
        signature = await _clamav_instream_scan(blob=b"hello", host=host, port=port, timeout_seconds=1.0)
        assert signature is None
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_clamav_instream_scan_found() -> None:
    server, host, port = await _start_fake_clamd(response="stream: Test-Signature FOUND\n")
    try:
        signature = await _clamav_instream_scan(blob=b"hello", host=host, port=port, timeout_seconds=1.0)
        assert signature == "Test-Signature"
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_agent_session_upload_scan_rejects_when_found() -> None:
    server, host, port = await _start_fake_clamd(response="stream: Test-Signature FOUND\n")
    try:
        settings = SimpleNamespace(
            context_upload_clamav_host=host,
            context_upload_clamav_port=port,
            context_upload_clamav_timeout_seconds=1.0,
            context_upload_clamav_required=True,
        )
        with pytest.raises(HTTPException) as exc:
            await _scan_upload_bytes(blob=b"hello", agent_settings=settings)
        assert exc.value.status_code == 400
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_agent_session_upload_scan_rejects_when_scanner_unavailable_and_required() -> None:
    settings = SimpleNamespace(
        context_upload_clamav_host="127.0.0.1",
        context_upload_clamav_port=65432,
        context_upload_clamav_timeout_seconds=0.1,
        context_upload_clamav_required=True,
    )
    with pytest.raises(HTTPException) as exc:
        await _scan_upload_bytes(blob=b"hello", agent_settings=settings)
    assert exc.value.status_code == 503
