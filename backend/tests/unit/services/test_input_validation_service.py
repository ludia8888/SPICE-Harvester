from __future__ import annotations

import pytest
from fastapi import HTTPException

from bff.services import input_validation_service
from shared.security.input_sanitizer import SecurityViolationError


def test_validated_db_name_returns_valid_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(input_validation_service, "validate_db_name", lambda value: f"db:{value}")
    assert input_validation_service.validated_db_name("test") == "db:test"


def test_validated_db_name_converts_value_error_to_http_400(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(_: str) -> str:
        raise ValueError("invalid db")

    monkeypatch.setattr(input_validation_service, "validate_db_name", _raise)
    with pytest.raises(HTTPException) as exc_info:
        input_validation_service.validated_db_name("bad")
    assert exc_info.value.status_code == 400
    assert "invalid db" in str(exc_info.value.detail)


def test_validated_branch_name_converts_security_violation(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(_: str) -> str:
        raise SecurityViolationError("blocked branch")

    monkeypatch.setattr(input_validation_service, "validate_branch_name", _raise)
    with pytest.raises(HTTPException) as exc_info:
        input_validation_service.validated_branch_name("feature")
    assert exc_info.value.status_code == 400
    assert "blocked branch" in str(exc_info.value.detail)


def test_sanitized_payload_returns_sanitized_data(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(input_validation_service, "sanitize_input", lambda payload: {"ok": payload})
    assert input_validation_service.sanitized_payload({"a": 1}) == {"ok": {"a": 1}}


def test_sanitized_payload_converts_security_violation(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(_: object) -> object:
        raise SecurityViolationError("payload blocked")

    monkeypatch.setattr(input_validation_service, "sanitize_input", _raise)
    with pytest.raises(HTTPException) as exc_info:
        input_validation_service.sanitized_payload({"bad": True})
    assert exc_info.value.status_code == 400
    assert "payload blocked" in str(exc_info.value.detail)
