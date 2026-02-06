from __future__ import annotations

import pytest
from fastapi import HTTPException

from bff.services import ontology_class_id_service
from shared.security.input_sanitizer import SecurityViolationError


def test_resolve_or_generate_class_id_returns_validated_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ontology_class_id_service, "validate_class_id", lambda value: f"class:{value}")
    payload = {"id": "Account"}
    assert ontology_class_id_service.resolve_or_generate_class_id(payload) == "class:Account"


def test_resolve_or_generate_class_id_converts_validation_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(_: str) -> str:
        raise ValueError("invalid class id")

    monkeypatch.setattr(ontology_class_id_service, "validate_class_id", _raise)
    with pytest.raises(HTTPException) as exc_info:
        ontology_class_id_service.resolve_or_generate_class_id({"id": "bad"})
    assert exc_info.value.status_code == 400
    assert "invalid class id" in str(exc_info.value.detail)


def test_resolve_or_generate_class_id_converts_security_violation(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(_: str) -> str:
        raise SecurityViolationError("blocked")

    monkeypatch.setattr(ontology_class_id_service, "validate_class_id", _raise)
    with pytest.raises(HTTPException) as exc_info:
        ontology_class_id_service.resolve_or_generate_class_id({"id": "bad"})
    assert exc_info.value.status_code == 400
    assert "blocked" in str(exc_info.value.detail)


def test_resolve_or_generate_class_id_generates_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ontology_class_id_service, "_localized_to_string", lambda _: "Localized Name")
    monkeypatch.setattr(ontology_class_id_service, "generate_simple_id", lambda **kwargs: f"gen:{kwargs['label']}")

    resolved = ontology_class_id_service.resolve_or_generate_class_id({"label": {"en": "Localized Name"}})
    assert resolved == "gen:Localized Name"
