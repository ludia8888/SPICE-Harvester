from __future__ import annotations

from datetime import datetime, timezone

import pytest

from shared.security.data_encryption import (
    DataEncryptor,
    encryptor_from_keys,
    is_encrypted_bytes,
    is_encrypted_json,
    is_encrypted_text,
)
from shared.services.agent_session_registry import AgentSessionRegistry


@pytest.mark.unit
def test_data_encryptor_round_trip_text(monkeypatch: pytest.MonkeyPatch) -> None:
    # 32-byte key (urlsafe base64)
    monkeypatch.setenv("DATA_ENCRYPTION_KEYS", "base64:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    encryptor = encryptor_from_keys("base64:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    assert encryptor is not None

    aad = b"session:test"
    ciphertext = encryptor.encrypt_text("hello", aad=aad)
    assert is_encrypted_text(ciphertext)
    assert encryptor.decrypt_text(ciphertext, aad=aad) == "hello"


@pytest.mark.unit
def test_data_encryptor_round_trip_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_ENCRYPTION_KEYS", "base64:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    encryptor = encryptor_from_keys("base64:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    assert encryptor is not None

    aad = b"session:test"
    payload = {"x": 1, "y": "z"}
    wrapped = encryptor.encrypt_json(payload, aad=aad)
    assert is_encrypted_json(wrapped)
    assert encryptor.decrypt_json(wrapped, aad=aad) == payload


@pytest.mark.unit
def test_data_encryptor_round_trip_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_ENCRYPTION_KEYS", "base64:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    encryptor = encryptor_from_keys("base64:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    assert encryptor is not None

    aad = b"session:test"
    blob = b"\x00\x01hello\xff"
    ciphertext = encryptor.encrypt_bytes(blob, aad=aad)
    assert is_encrypted_bytes(ciphertext)
    assert encryptor.decrypt_bytes(ciphertext, aad=aad) == blob


@pytest.mark.unit
def test_agent_session_registry_decrypts_message_content(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_ENCRYPTION_KEYS", "base64:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    encryptor = DataEncryptor(keys=[b"\x00" * 32])
    session_id = "11111111-1111-1111-1111-111111111111"
    aad = f"session:{session_id}".encode("utf-8")
    ciphertext = encryptor.encrypt_text("secret", aad=aad)

    registry = AgentSessionRegistry(dsn="postgresql://example.invalid")  # not connecting in this unit test
    record = registry._row_to_message(  # type: ignore[arg-type]
        {
            "message_id": "22222222-2222-2222-2222-222222222222",
            "session_id": session_id,
            "role": "user",
            "content": ciphertext,
            "content_digest": "sha256:test",
            "is_removed": False,
            "removed_at": None,
            "removed_by": None,
            "removed_reason": None,
            "token_count": 1,
            "cost_estimate": None,
            "latency_ms": None,
            "metadata": {},
            "created_at": datetime.now(timezone.utc),
        }
    )
    assert record.content == "secret"


@pytest.mark.unit
def test_agent_session_registry_decrypts_tool_call_payloads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_ENCRYPTION_KEYS", "base64:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    encryptor = DataEncryptor(keys=[b"\x00" * 32])
    session_id = "11111111-1111-1111-1111-111111111111"
    aad = f"session:{session_id}".encode("utf-8")
    wrapped_req = encryptor.encrypt_json({"body": {"x": 1}}, aad=aad)
    wrapped_resp = encryptor.encrypt_json({"data": {"ok": True}}, aad=aad)

    registry = AgentSessionRegistry(dsn="postgresql://example.invalid")  # not connecting in this unit test
    record = registry._row_to_tool_call(  # type: ignore[arg-type]
        {
            "tool_run_id": "33333333-3333-3333-3333-333333333333",
            "session_id": session_id,
            "tenant_id": "tenant-1",
            "job_id": None,
            "plan_id": None,
            "run_id": None,
            "step_id": None,
            "tool_id": "tool.x",
            "method": "POST",
            "path": "/api/v1/test",
            "query": {},
            "request_body": wrapped_req,
            "request_digest": "sha256:req",
            "request_token_count": None,
            "idempotency_key": None,
            "status": "COMPLETED",
            "response_status": 200,
            "response_body": wrapped_resp,
            "response_digest": "sha256:resp",
            "response_token_count": None,
            "error_code": None,
            "error_message": None,
            "side_effect_summary": {},
            "latency_ms": 10,
            "started_at": datetime.now(timezone.utc),
            "finished_at": datetime.now(timezone.utc),
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
    )
    assert record.request_body == {"body": {"x": 1}}
    assert record.response_body == {"data": {"ok": True}}
