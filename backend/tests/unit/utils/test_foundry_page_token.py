from __future__ import annotations

import time

import pytest

from shared.utils.foundry_page_token import decode_offset_page_token, encode_offset_page_token


def test_foundry_page_token_round_trip() -> None:
    token = encode_offset_page_token(123)
    assert isinstance(token, str)
    assert decode_offset_page_token(token, ttl_seconds=60) == 123


def test_foundry_page_token_rejects_expired_token() -> None:
    issued_at = int(time.time()) - 120
    token = encode_offset_page_token(10, issued_at=issued_at)
    with pytest.raises(ValueError, match="expired"):
        decode_offset_page_token(token, ttl_seconds=60)


def test_foundry_page_token_empty_defaults_to_zero_offset() -> None:
    assert decode_offset_page_token(None, ttl_seconds=60) == 0
    assert decode_offset_page_token("", ttl_seconds=60) == 0


def test_foundry_page_token_scope_mismatch_rejected() -> None:
    token = encode_offset_page_token(5, scope="scope-a")
    with pytest.raises(ValueError, match="invalid for this request"):
        decode_offset_page_token(token, ttl_seconds=60, expected_scope="scope-b")
