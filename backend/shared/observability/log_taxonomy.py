from __future__ import annotations

import logging
from typing import Any, Mapping, Optional


def taxonomy_extra(
    *,
    event_name: str,
    event_family: str,
    failure_class: Optional[str] = None,
    retryable: Optional[bool] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    payload = dict(extra or {})
    payload["_taxonomy_event_name"] = str(event_name or "").strip() or "-"
    payload["_taxonomy_event_family"] = str(event_family or "").strip() or "-"
    payload["_taxonomy_failure_class"] = str(failure_class or "-").strip() or "-"
    if retryable is None:
        payload["_taxonomy_retryable_flag"] = "-"
    else:
        payload["_taxonomy_retryable_flag"] = "true" if bool(retryable) else "false"
    return payload


def log_taxonomy_event(
    logger: logging.Logger,
    level: int,
    message: str,
    *,
    event_name: str,
    event_family: str,
    failure_class: Optional[str] = None,
    retryable: Optional[bool] = None,
    extra: Optional[Mapping[str, Any]] = None,
    exc_info: Any = None,
) -> None:
    logger.log(
        level,
        message,
        extra=taxonomy_extra(
            event_name=event_name,
            event_family=event_family,
            failure_class=failure_class,
            retryable=retryable,
            extra=extra,
        ),
        exc_info=exc_info,
    )
