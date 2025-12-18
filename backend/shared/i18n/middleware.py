from __future__ import annotations

import json
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from starlette.responses import JSONResponse, Response, StreamingResponse

from shared.i18n.context import reset_language, set_language
from shared.i18n.translator import localize_free_text
from shared.utils.language import get_accept_language


def install_i18n_middleware(app: FastAPI, *, max_body_bytes: int = 1_000_000) -> None:
    """
    Install request-scoped language + best-effort response localization.

    This middleware guarantees:
    - request language is available via ContextVar (shared.i18n.get_language)
    - `message` / `detail` / `errors[]` strings are localized best-effort
    """

    @app.middleware("http")
    async def _i18n_middleware(request: Request, call_next):
        lang = get_accept_language(request)
        token = set_language(lang)
        try:
            response = await call_next(request)
        finally:
            # We reset after response is created, but before returning we might still rewrite body.
            reset_language(token)

        content_type = (response.headers.get("content-type") or "").lower()
        if "application/json" not in content_type:
            return response

        content_length = response.headers.get("content-length")
        if content_length is not None:
            try:
                if int(content_length) > max_body_bytes:
                    return response
            except ValueError:
                pass

        # Avoid huge bodies or streaming; if too large, pass through without rewrite.
        body_parts = []
        body_size = 0
        iterator = response.body_iterator.__aiter__()
        while True:
            try:
                chunk = await iterator.__anext__()
            except StopAsyncIteration:
                break
            body_parts.append(chunk)
            body_size += len(chunk)
            if body_size > max_body_bytes:
                async def _stream_body():
                    for part in body_parts:
                        yield part
                    async for rest in iterator:
                        yield rest

                return StreamingResponse(
                    _stream_body(),
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type="application/json",
                    background=response.background,
                )

        body = b"".join(body_parts)

        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            return Response(
                content=body,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type="application/json",
                background=response.background,
            )

        rewritten = _rewrite_payload(payload, target_lang=lang, status_code=response.status_code)
        headers = dict(response.headers)
        # Body may have changed (translations), so we must not reuse the old content-length.
        headers.pop("content-length", None)
        new_response = JSONResponse(
            content=rewritten,
            status_code=response.status_code,
            headers=headers,
        )
        new_response.background = response.background
        return new_response


def _rewrite_payload(
    payload: Any,
    *,
    target_lang: str,
    status_code: int,
    api_status: Optional[str] = None,
    is_root: bool = True,
) -> Any:
    if isinstance(payload, dict):
        if is_root and api_status is None:
            api_status = payload.get("status") if isinstance(payload.get("status"), str) else None

        for key, value in list(payload.items()):
            if is_root and key == "message" and isinstance(value, str):
                # Only apply generic fallbacks when the response looks like our ApiResponse shape.
                payload[key] = localize_free_text(
                    value,
                    target_lang=target_lang,
                    status_code=status_code if api_status else None,
                    api_status=api_status if api_status else None,
                )
                continue

            if is_root and key == "detail" and isinstance(value, str):
                # FastAPI default error responses use {"detail": "..."}
                payload[key] = localize_free_text(
                    value,
                    target_lang=target_lang,
                    status_code=status_code,
                    api_status=api_status,
                )
                continue

            if key == "description" and isinstance(value, str):
                payload[key] = localize_free_text(
                    value,
                    target_lang=target_lang,
                    status_code=None,
                    api_status=None,
                )
                continue

            if key == "error" and isinstance(value, str):
                payload[key] = localize_free_text(
                    value,
                    target_lang=target_lang,
                    status_code=None,
                    api_status=None,
                )
                continue

            if is_root and key == "errors" and isinstance(value, list):
                errors_out = []
                for item in value:
                    if isinstance(item, str):
                        errors_out.append(
                            localize_free_text(
                                item,
                                target_lang=target_lang,
                                status_code=status_code if api_status else None,
                                api_status=api_status if api_status else None,
                            )
                        )
                    else:
                        errors_out.append(
                            _rewrite_payload(
                                item,
                                target_lang=target_lang,
                                status_code=status_code,
                                api_status=api_status,
                                is_root=False,
                            )
                        )
                payload[key] = errors_out
                continue

            if isinstance(value, dict) or isinstance(value, list):
                payload[key] = _rewrite_payload(
                    value,
                    target_lang=target_lang,
                    status_code=status_code,
                    api_status=api_status,
                    is_root=False,
                )

        return payload

    if isinstance(payload, list):
        return [
            _rewrite_payload(
                item,
                target_lang=target_lang,
                status_code=status_code,
                api_status=api_status,
                is_root=False,
            )
            for item in payload
        ]

    return payload
