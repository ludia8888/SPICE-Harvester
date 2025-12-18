from __future__ import annotations

import json
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from starlette.responses import JSONResponse, Response

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

        # Avoid huge bodies or streaming.
        body = b""
        async for chunk in response.body_iterator:
            body += chunk
            if len(body) > max_body_bytes:
                # Re-create original response with consumed body.
                return Response(
                    content=body,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type="application/json",
                    background=response.background,
                )

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


def _rewrite_payload(payload: Any, *, target_lang: str, status_code: int) -> Any:
    if isinstance(payload, dict):
        api_status = payload.get("status") if isinstance(payload.get("status"), str) else None

        if isinstance(payload.get("message"), str):
            # Only apply generic fallbacks when the response looks like our ApiResponse shape.
            # For arbitrary payloads (e.g., root endpoints), we keep the original message unless
            # we can translate it via known mappings/patterns.
            payload["message"] = localize_free_text(
                payload["message"],
                target_lang=target_lang,
                status_code=status_code if api_status else None,
                api_status=api_status if api_status else None,
            )

        if isinstance(payload.get("detail"), str):
            # FastAPI default error responses use {"detail": "..."}
            payload["detail"] = localize_free_text(
                payload["detail"],
                target_lang=target_lang,
                status_code=status_code,
                api_status=api_status,
            )

        if isinstance(payload.get("errors"), list):
            errors_out = []
            for item in payload["errors"]:
                errors_out.append(
                    localize_free_text(
                        item,
                        target_lang=target_lang,
                        status_code=status_code if api_status else None,
                        api_status=api_status if api_status else None,
                    )
                    if isinstance(item, str)
                    else item
                )
            payload["errors"] = errors_out

        return payload

    if isinstance(payload, list):
        return payload

    return payload
