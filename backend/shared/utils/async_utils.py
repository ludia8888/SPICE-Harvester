from __future__ import annotations

import inspect
from typing import Any


async def await_if_needed(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def raise_for_status_async(response: Any) -> None:
    await await_if_needed(response.raise_for_status())


async def response_json_async(response: Any) -> Any:
    return await await_if_needed(response.json())


async def aclose_if_present(resource: Any) -> None:
    if resource is None:
        return
    closer = getattr(resource, "aclose", None)
    if closer is None:
        return
    await await_if_needed(closer())
