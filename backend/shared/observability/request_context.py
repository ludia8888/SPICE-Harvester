"""
Request/operation context for debugging.

Why this exists:
- Trace IDs are great, but operators and users often share a simple token like
  `request_id` / `correlation_id` when reporting incidents.
- Kafka workers don't naturally have HTTP request objects, so we need a
  thread-safe/async-safe way to carry these values through code paths.

Design:
- ContextVars for: request_id, correlation_id, db_name, principal.
- Best-effort W3C baggage integration when OpenTelemetry is available.
  (This helps propagate correlation_id across HTTP/Kafka without custom plumbing.)
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Dict, Iterator, Mapping, Optional
from urllib.parse import unquote


_REQUEST_ID: ContextVar[Optional[str]] = ContextVar("spice.request_id", default=None)
_CORRELATION_ID: ContextVar[Optional[str]] = ContextVar("spice.correlation_id", default=None)
_DB_NAME: ContextVar[Optional[str]] = ContextVar("spice.db_name", default=None)
_PRINCIPAL: ContextVar[Optional[str]] = ContextVar("spice.principal", default=None)


try:  # optional OpenTelemetry API
    from opentelemetry import baggage as otel_baggage
    from opentelemetry import context as otel_context

    _HAS_OTEL = True
except Exception:  # pragma: no cover - env dependent
    otel_baggage = None
    otel_context = None
    _HAS_OTEL = False


def _norm(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def generate_request_id() -> str:
    return uuid.uuid4().hex


def get_request_id() -> Optional[str]:
    return _REQUEST_ID.get()


def get_correlation_id() -> Optional[str]:
    return _CORRELATION_ID.get()


def get_db_name() -> Optional[str]:
    return _DB_NAME.get()


def get_principal() -> Optional[str]:
    return _PRINCIPAL.get()


def parse_baggage_header(header_value: Optional[str]) -> Dict[str, str]:
    """
    Best-effort parser for W3C `baggage` header.

    Example:
      baggage: correlation_id=abc,request_id=req-1;prop=1,foo=bar
    """
    raw = _norm(header_value)
    if not raw:
        return {}

    parsed: Dict[str, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        # Drop optional properties after ';'
        kv = item.split(";", 1)[0].strip()
        if "=" not in kv:
            continue
        k, v = kv.split("=", 1)
        key = _norm(k)
        if not key:
            continue
        try:
            value = unquote(v.strip())
        except Exception:
            value = v.strip()
        if value:
            parsed[key] = value
    return parsed


def context_from_headers(headers: Mapping[str, Any]) -> Dict[str, Optional[str]]:
    # Header keys can be case-insensitive in FastAPI/Starlette, but keep defensive.
    def _h(name: str) -> Optional[str]:
        return _norm(headers.get(name) or headers.get(name.lower()) or headers.get(name.upper()))

    request_id = _h("X-Request-Id") or _h("X-Request-ID")
    correlation_id = _h("X-Correlation-Id") or _h("X-Correlation-ID")
    db_name = _h("X-DB-Name") or _h("X-Project")
    principal = _h("X-Principal")  # optional internal header
    return {
        "request_id": request_id,
        "correlation_id": correlation_id,
        "db_name": db_name,
        "principal": principal,
    }


def context_from_metadata(metadata: Optional[Mapping[str, Any]]) -> Dict[str, Optional[str]]:
    if not isinstance(metadata, Mapping):
        return {}

    request_id = _norm(metadata.get("request_id"))
    correlation_id = _norm(metadata.get("correlation_id"))
    db_name = _norm(metadata.get("db_name") or metadata.get("project") or metadata.get("db"))
    principal = _norm(metadata.get("principal"))

    if not (request_id and correlation_id):
        baggage_items = parse_baggage_header(_norm(metadata.get("baggage")))
        request_id = request_id or _norm(baggage_items.get("request_id"))
        correlation_id = correlation_id or _norm(baggage_items.get("correlation_id"))

    return {
        "request_id": request_id,
        "correlation_id": correlation_id,
        "db_name": db_name,
        "principal": principal,
    }


@contextmanager
def request_context(
    *,
    request_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    db_name: Optional[str] = None,
    principal: Optional[str] = None,
    inject_baggage: bool = True,
) -> Iterator[None]:
    """
    Attach request/debug context for the duration of a `with` block.

    When OpenTelemetry is available, also inject these values into the OTel baggage
    so they can propagate across HTTP/Kafka hops.
    """
    request_id = _norm(request_id)
    correlation_id = _norm(correlation_id)
    db_name = _norm(db_name)
    principal = _norm(principal)

    token_request = _REQUEST_ID.set(request_id)
    token_corr = _CORRELATION_ID.set(correlation_id)
    token_db = _DB_NAME.set(db_name)
    token_principal = _PRINCIPAL.set(principal)

    otel_token = None
    if inject_baggage and _HAS_OTEL and otel_baggage is not None and otel_context is not None:
        try:
            ctx = otel_context.get_current()
            if request_id and not otel_baggage.get_baggage("request_id", context=ctx):
                ctx = otel_baggage.set_baggage("request_id", request_id, context=ctx)
            if correlation_id and not otel_baggage.get_baggage("correlation_id", context=ctx):
                ctx = otel_baggage.set_baggage("correlation_id", correlation_id, context=ctx)
            if db_name and not otel_baggage.get_baggage("db_name", context=ctx):
                ctx = otel_baggage.set_baggage("db_name", db_name, context=ctx)
            otel_token = otel_context.attach(ctx)
        except Exception:
            otel_token = None

    try:
        yield
    finally:
        if otel_token is not None and _HAS_OTEL and otel_context is not None:
            try:
                otel_context.detach(otel_token)
            except Exception:
                pass
        _REQUEST_ID.reset(token_request)
        _CORRELATION_ID.reset(token_corr)
        _DB_NAME.reset(token_db)
        _PRINCIPAL.reset(token_principal)
