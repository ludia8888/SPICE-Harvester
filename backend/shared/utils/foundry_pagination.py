from __future__ import annotations

from shared.utils.foundry_page_token import decode_offset_page_token, encode_offset_page_token

FOUNDRY_PAGE_TOKEN_TTL_SECONDS = 60 * 60 * 24


def decode_foundry_page_token(page_token: str | None, *, scope: str | None = None) -> int:
    return decode_offset_page_token(
        page_token,
        ttl_seconds=FOUNDRY_PAGE_TOKEN_TTL_SECONDS,
        expected_scope=scope,
    )


def encode_foundry_page_token(offset: int, *, scope: str | None = None) -> str:
    return encode_offset_page_token(offset, scope=scope)
