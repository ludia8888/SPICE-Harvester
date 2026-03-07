from __future__ import annotations

from types import SimpleNamespace

import pytest

from shared.services.registries.dataset_registry_get_or_create import get_or_create_dataset_record


class _UniqueViolationError(RuntimeError):
    sqlstate = "23505"


@pytest.mark.asyncio
async def test_get_or_create_dataset_record_returns_existing_without_create() -> None:
    dataset = SimpleNamespace(dataset_id="ds-existing")
    calls = {"lookup": 0, "create": 0}

    async def _lookup() -> SimpleNamespace | None:
        calls["lookup"] += 1
        return dataset

    async def _create() -> SimpleNamespace:
        calls["create"] += 1
        return SimpleNamespace(dataset_id="ds-created")

    resolved, created = await get_or_create_dataset_record(
        lookup=_lookup,
        create=_create,
        conflict_context="db/orders@main",
    )

    assert resolved is dataset
    assert created is False
    assert calls == {"lookup": 1, "create": 0}


@pytest.mark.asyncio
async def test_get_or_create_dataset_record_creates_when_missing() -> None:
    created_dataset = SimpleNamespace(dataset_id="ds-created")
    calls = {"lookup": 0, "create": 0}

    async def _lookup() -> SimpleNamespace | None:
        calls["lookup"] += 1
        return None

    async def _create() -> SimpleNamespace:
        calls["create"] += 1
        return created_dataset

    resolved, created = await get_or_create_dataset_record(
        lookup=_lookup,
        create=_create,
        conflict_context="db/orders@main",
    )

    assert resolved is created_dataset
    assert created is True
    assert calls == {"lookup": 1, "create": 1}


@pytest.mark.asyncio
async def test_get_or_create_dataset_record_refetches_after_unique_violation() -> None:
    created_by_other = SimpleNamespace(dataset_id="ds-raced")
    calls = {"lookup": 0, "create": 0}

    async def _lookup() -> SimpleNamespace | None:
        calls["lookup"] += 1
        if calls["lookup"] == 1:
            return None
        return created_by_other

    async def _create() -> SimpleNamespace:
        calls["create"] += 1
        raise _UniqueViolationError("duplicate key value violates unique constraint")

    resolved, created = await get_or_create_dataset_record(
        lookup=_lookup,
        create=_create,
        conflict_context="db/orders@main",
    )

    assert resolved is created_by_other
    assert created is False
    assert calls == {"lookup": 2, "create": 1}


@pytest.mark.asyncio
async def test_get_or_create_dataset_record_reraises_unique_violation_when_lookup_stays_empty() -> None:
    async def _lookup() -> SimpleNamespace | None:
        return None

    async def _create() -> SimpleNamespace:
        raise _UniqueViolationError("duplicate key value violates unique constraint")

    with pytest.raises(_UniqueViolationError):
        await get_or_create_dataset_record(
            lookup=_lookup,
            create=_create,
            conflict_context="db/orders@main",
        )


@pytest.mark.asyncio
async def test_get_or_create_dataset_record_reraises_non_unique_exception() -> None:
    async def _lookup() -> SimpleNamespace | None:
        return None

    async def _create() -> SimpleNamespace:
        raise RuntimeError("connection lost")

    with pytest.raises(RuntimeError, match="connection lost"):
        await get_or_create_dataset_record(
            lookup=_lookup,
            create=_create,
            conflict_context="db/orders@main",
        )
