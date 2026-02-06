from __future__ import annotations

from datetime import datetime, timedelta, timezone


def exponential_backoff_seconds(
    attempts: int,
    *,
    base_seconds: int,
    max_seconds: int,
) -> int:
    attempt_count = max(0, int(attempts))
    base = max(0, int(base_seconds))
    ceiling = max(0, int(max_seconds))
    if base == 0 or ceiling == 0:
        return 0
    multiplier = 2 ** max(0, attempt_count - 1)
    return min(ceiling, base * multiplier)


def next_exponential_backoff_at(
    attempts: int,
    *,
    base_seconds: int,
    max_seconds: int,
) -> datetime:
    delay_seconds = exponential_backoff_seconds(
        attempts,
        base_seconds=base_seconds,
        max_seconds=max_seconds,
    )
    return datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
