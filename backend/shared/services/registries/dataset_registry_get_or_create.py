"""Race-safe helpers for dataset registry get-or-create flows."""

from __future__ import annotations

import logging
from typing import Awaitable, Callable, Optional, TypeVar


logger = logging.getLogger(__name__)

_UNIQUE_VIOLATION_SQLSTATE = "23505"
_RecordT = TypeVar("_RecordT")
_LookupFn = Callable[[], Awaitable[Optional[_RecordT]]]
_CreateFn = Callable[[], Awaitable[_RecordT]]


def is_unique_violation_error(exc: Exception) -> bool:
    sqlstate = str(getattr(exc, "sqlstate", "") or getattr(exc, "pgcode", "")).strip()
    if sqlstate == _UNIQUE_VIOLATION_SQLSTATE:
        return True

    exc_name = exc.__class__.__name__.lower()
    if "unique" in exc_name and "violation" in exc_name:
        return True

    message = str(exc).lower()
    return (
        "duplicate key value violates unique constraint" in message
        or "unique constraint failed" in message
    )


async def get_or_create_record(
    *,
    lookup: _LookupFn[_RecordT],
    create: _CreateFn[_RecordT],
    conflict_context: str,
) -> tuple[_RecordT, bool]:
    existing = await lookup()
    if existing is not None:
        return existing, False

    try:
        created = await create()
    except Exception as exc:
        if not is_unique_violation_error(exc):
            raise
        existing = await lookup()
        if existing is None:
            raise
        logger.info("Create raced; reusing existing record (%s)", conflict_context)
        return existing, False

    return created, True


async def get_or_create_dataset_record(
    *,
    lookup: _LookupFn[_RecordT],
    create: _CreateFn[_RecordT],
    conflict_context: str,
) -> tuple[_RecordT, bool]:
    return await get_or_create_record(
        lookup=lookup,
        create=create,
        conflict_context=conflict_context,
    )
