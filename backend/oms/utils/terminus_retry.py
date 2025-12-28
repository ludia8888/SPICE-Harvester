from __future__ import annotations

import asyncio
import logging
from functools import wraps
from typing import Any, Awaitable, Callable, Iterable, Optional, Tuple, Type

import httpx

RetryExceptions = Tuple[Type[BaseException], ...]
FailureFactory = Callable[[BaseException, int], BaseException]


def build_async_retry(
    *,
    retry_exceptions: Optional[Iterable[Type[BaseException]]] = None,
    backoff: str = "linear",
    logger: Optional[logging.Logger] = None,
    on_failure: Optional[FailureFactory] = None,
) -> Callable[[int, float], Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]]:
    resolved_logger = logger or logging.getLogger(__name__)
    resolved_exceptions: RetryExceptions = tuple(retry_exceptions or (httpx.ConnectError, httpx.TimeoutException))

    def decorator_factory(max_retries: int = 3, delay: float = 1.0):
        def decorator(func: Callable[..., Awaitable[Any]]):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                last_exception: Optional[BaseException] = None
                retries = int(max_retries)
                base_delay = float(delay)

                for attempt in range(retries):
                    try:
                        return await func(*args, **kwargs)
                    except resolved_exceptions as exc:
                        last_exception = exc
                        if attempt < retries - 1:
                            wait_time = base_delay * (2 ** attempt) if backoff == "exponential" else base_delay * (attempt + 1)
                            resolved_logger.warning(
                                "Retry %s/%s for %s: %s",
                                attempt + 1,
                                retries,
                                getattr(func, "__name__", "unknown"),
                                exc,
                            )
                            await asyncio.sleep(wait_time)
                        else:
                            resolved_logger.error(
                                "Max retries reached for %s: %s",
                                getattr(func, "__name__", "unknown"),
                                exc,
                            )
                    except Exception:
                        raise

                if last_exception is None:
                    last_exception = RuntimeError("Retry failed without exception")
                if on_failure:
                    raise on_failure(last_exception, retries)
                raise last_exception

            return wrapper

        return decorator

    return decorator_factory
