from __future__ import annotations

import logging

import pytest

from shared.utils.worker_runner import run_component_lifecycle


@pytest.mark.asyncio
async def test_run_component_lifecycle_propagates_run_error_without_close() -> None:
    class _Component:
        async def initialize(self) -> None:
            return None

        async def run(self) -> None:
            raise RuntimeError("run-failed")

    with pytest.raises(RuntimeError, match="run-failed"):
        await run_component_lifecycle(_Component())


@pytest.mark.asyncio
async def test_run_component_lifecycle_keeps_primary_error_when_close_also_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _Component:
        async def initialize(self) -> None:
            return None

        async def run(self) -> None:
            raise RuntimeError("run-failed")

        async def close(self) -> None:
            raise RuntimeError("close-failed")

    with caplog.at_level(logging.WARNING, logger="shared.utils.worker_runner"):
        with pytest.raises(RuntimeError, match="run-failed"):
            await run_component_lifecycle(_Component())

    assert any("close failed after primary lifecycle error" in rec.message.lower() for rec in caplog.records)


@pytest.mark.asyncio
async def test_run_component_lifecycle_raises_close_error_when_no_primary_error() -> None:
    class _Component:
        async def initialize(self) -> None:
            return None

        async def run(self) -> None:
            return None

        async def close(self) -> None:
            raise RuntimeError("close-failed")

    with pytest.raises(RuntimeError, match="close-failed"):
        await run_component_lifecycle(_Component())
