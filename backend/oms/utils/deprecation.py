from __future__ import annotations

import functools
import inspect
import warnings
from typing import Any, Awaitable, Callable, Optional, TypeVar, cast


_F = TypeVar("_F", bound=Callable[..., Any])


def _append_doc(fn: _F, extra: str) -> None:
    doc = fn.__doc__ or ""
    doc = doc.rstrip()
    if doc:
        doc += "\n\n"
    doc += extra.strip() + "\n"
    fn.__doc__ = doc


def deprecated(
    *,
    reason: str,
    version: str,
    alternative: Optional[str] = None,
    removal_version: Optional[str] = None,
) -> Callable[[_F], _F]:
    """Decorator to mark a function as deprecated.

    Adds a ``DeprecationWarning`` on call, marks ``__deprecated__`` on the
    wrapped callable, and annotates the docstring.
    """

    def decorator(fn: _F) -> _F:
        message = f"{fn.__name__} is deprecated since {version}: {reason}"
        if alternative:
            message += f". Use {alternative} instead"
        if removal_version:
            message += f". Will be removed in {removal_version}"

        note = f"deprecated API (since {version}). {reason}".strip()
        if alternative:
            note += f" Alternative: {alternative}."
        if removal_version:
            note += f" Removal: {removal_version}."

        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def _async_wrapper(*args: Any, **kwargs: Any) -> Any:
                warnings.warn(message, category=DeprecationWarning, stacklevel=2)
                return await cast(Callable[..., Awaitable[Any]], fn)(*args, **kwargs)

            wrapped = cast(_F, _async_wrapper)
        else:

            @functools.wraps(fn)
            def _wrapper(*args: Any, **kwargs: Any) -> Any:
                warnings.warn(message, category=DeprecationWarning, stacklevel=2)
                return fn(*args, **kwargs)

            wrapped = cast(_F, _wrapper)

        setattr(wrapped, "__deprecated__", True)
        _append_doc(wrapped, note)
        return wrapped

    return decorator


def legacy_api(reason: str) -> Callable[[_F], _F]:
    """Decorator to mark a callable as a legacy API.

    Does not emit a runtime warning by default; it exists primarily for
    surfacing legacy status in docs and introspection.
    """

    def decorator(fn: _F) -> _F:
        setattr(fn, "__legacy__", True)
        _append_doc(fn, f"Legacy API. {reason}".strip())
        return fn

    return decorator


def experimental(feature: str) -> Callable[[_F], _F]:
    """Decorator to mark a callable as experimental."""

    def decorator(fn: _F) -> _F:
        setattr(fn, "__experimental__", True)
        _append_doc(fn, f"Experimental API ({feature}).".strip())
        return fn

    return decorator
