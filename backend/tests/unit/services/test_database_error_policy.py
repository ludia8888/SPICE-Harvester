from __future__ import annotations

import logging

import pytest
from fastapi import HTTPException

from bff.services.database_error_policy import MessageErrorPolicy, apply_message_error_policies


def test_apply_message_error_policies_raises_policy_http_exception() -> None:
    with pytest.raises(HTTPException) as raised:
        apply_message_error_policies(
            exc=RuntimeError("database not found"),
            logger=logging.getLogger(__name__),
            log_message="failed: %s",
            policies=(
                MessageErrorPolicy(
                    patterns=("database not found",),
                    status_code=404,
                    detail="db missing",
                ),
            ),
            default_status_code=500,
            default_detail="default",
        )
    assert raised.value.status_code == 404
    assert raised.value.detail["message"] == "db missing"


def test_apply_message_error_policies_returns_fallback_when_configured() -> None:
    fallback = {"versions": [], "count": 0}
    resolved = apply_message_error_policies(
        exc=RuntimeError("empty history"),
        logger=logging.getLogger(__name__),
        log_message="failed: %s",
        policies=(
            MessageErrorPolicy(
                patterns=("empty history",),
                status_code=200,
                detail="",
                fallback_return=fallback,
            ),
        ),
        default_status_code=500,
        default_detail="default",
    )
    assert resolved == fallback
