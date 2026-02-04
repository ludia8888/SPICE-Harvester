"""Deterministic ID helpers (uuid5).

Centralizes uuid5-based IDs to avoid duplicated string formats across services.
"""

from __future__ import annotations

from uuid import UUID, NAMESPACE_URL, uuid5


def deterministic_uuid5(key: str) -> UUID:
    return uuid5(NAMESPACE_URL, str(key))


def deterministic_uuid5_str(key: str) -> str:
    return str(deterministic_uuid5(key))


def deterministic_uuid5_hex_prefix(key: str, *, length: int = 12) -> str:
    if length <= 0:
        raise ValueError("length must be positive")
    return deterministic_uuid5(key).hex[:length]

