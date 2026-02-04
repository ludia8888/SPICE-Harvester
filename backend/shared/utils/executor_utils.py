from __future__ import annotations

import asyncio
from concurrent.futures import Executor
from functools import partial
from typing import Callable, Optional, TypeVar

T = TypeVar("T")


async def call_in_executor(
    executor: Optional[Executor],
    func: Callable[..., T],
    *args,
    **kwargs,
) -> T:
    loop = asyncio.get_running_loop()
    bound = partial(func, *args, **kwargs)
    return await loop.run_in_executor(executor, bound)

