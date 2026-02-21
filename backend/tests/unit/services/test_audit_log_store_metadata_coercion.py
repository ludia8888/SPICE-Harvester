from __future__ import annotations

from shared.services.core.audit_log_store import AuditLogStore


def test_coerce_metadata_accepts_dict() -> None:
    assert AuditLogStore._coerce_metadata({"a": 1}) == {"a": 1}


def test_coerce_metadata_parses_json_string() -> None:
    assert AuditLogStore._coerce_metadata('{"event":"created","ok":true}') == {
        "event": "created",
        "ok": True,
    }


def test_coerce_metadata_returns_empty_for_invalid_string() -> None:
    assert AuditLogStore._coerce_metadata("not-json") == {}


def test_coerce_metadata_returns_empty_for_broken_mapping() -> None:
    class _BrokenMapping(dict):
        def items(self):  # type: ignore[override]
            raise ValueError("broken mapping")

    assert AuditLogStore._coerce_metadata(_BrokenMapping({"a": 1})) == {}
