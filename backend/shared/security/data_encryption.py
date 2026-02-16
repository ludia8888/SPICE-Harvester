from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from typing import Any, Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import logging

_TEXT_PREFIX = "enc:v1:"
_BYTES_PREFIX = b"encb:v1:"
_JSON_MARKER = "__encrypted__"


def _strip_prefix(value: str, prefix: str) -> str:
    text = str(value or "").strip()
    if text.lower().startswith(prefix.lower()):
        return text[len(prefix) :]
    return text


def _b64decode(value: str) -> bytes:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("empty base64 value")
    padded = raw + "=" * ((4 - (len(raw) % 4)) % 4)
    return base64.urlsafe_b64decode(padded.encode("utf-8"))


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("utf-8").rstrip("=")


def parse_encryption_keys(raw: Optional[str]) -> list[bytes]:
    """
    Parse a comma-separated list of keys from env/settings.

    Accepts:
      - base64 (urlsafe) values, optionally prefixed with `base64:`
      - hex values, optionally prefixed with `hex:`

    AESGCM supports 16/24/32 byte keys.
    """
    if raw is None:
        return []
    text = str(raw).strip()
    if not text:
        return []

    keys: list[bytes] = []
    for part in [p.strip() for p in text.split(",") if p.strip()]:
        normalized = part
        try:
            if normalized.lower().startswith("hex:"):
                candidate = bytes.fromhex(_strip_prefix(normalized, "hex:"))
            else:
                candidate = _b64decode(_strip_prefix(normalized, "base64:"))
        except Exception as exc:
            raise ValueError("Invalid DATA_ENCRYPTION_KEYS entry") from exc
        if len(candidate) not in {16, 24, 32}:
            raise ValueError("Encryption key must be 16/24/32 bytes (AESGCM)")
        keys.append(candidate)
    return keys


@dataclass(frozen=True)
class DataEncryptor:
    keys: list[bytes]

    def _aesgcm(self, key: bytes) -> AESGCM:
        return AESGCM(key)

    def encrypt_text(self, plaintext: str, *, aad: Optional[bytes] = None) -> str:
        if not self.keys:
            return str(plaintext)
        key = self.keys[0]
        nonce = os.urandom(12)
        aesgcm = self._aesgcm(key)
        pt = str(plaintext).encode("utf-8")
        ct = aesgcm.encrypt(nonce, pt, aad)
        return f"{_TEXT_PREFIX}{_b64encode(nonce)}:{_b64encode(ct)}"

    def decrypt_text(self, ciphertext: str, *, aad: Optional[bytes] = None) -> str:
        text = str(ciphertext or "")
        if not text.startswith(_TEXT_PREFIX):
            return text
        payload = text[len(_TEXT_PREFIX) :]
        if ":" not in payload:
            raise ValueError("Invalid encrypted payload")
        nonce_b64, ct_b64 = payload.split(":", 1)
        nonce = _b64decode(nonce_b64)
        ct = _b64decode(ct_b64)
        last_exc: Optional[Exception] = None
        for key in self.keys:
            try:
                aesgcm = self._aesgcm(key)
                pt = aesgcm.decrypt(nonce, ct, aad)
                return pt.decode("utf-8")
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception fallback at shared/security/data_encryption.py:100", exc_info=True)
                last_exc = exc
                continue
        raise ValueError("Unable to decrypt payload") from last_exc

    def encrypt_bytes(self, plaintext: bytes, *, aad: Optional[bytes] = None) -> bytes:
        if not self.keys:
            return bytes(plaintext)
        key = self.keys[0]
        nonce = os.urandom(12)
        aesgcm = self._aesgcm(key)
        ct = aesgcm.encrypt(nonce, bytes(plaintext), aad)
        return _BYTES_PREFIX + nonce + ct

    def decrypt_bytes(self, ciphertext: bytes, *, aad: Optional[bytes] = None) -> bytes:
        blob = bytes(ciphertext)
        if not blob.startswith(_BYTES_PREFIX):
            return blob
        payload = blob[len(_BYTES_PREFIX) :]
        if len(payload) < 12:
            raise ValueError("Invalid encrypted bytes payload")
        nonce = payload[:12]
        ct = payload[12:]
        last_exc: Optional[Exception] = None
        for key in self.keys:
            try:
                aesgcm = self._aesgcm(key)
                return aesgcm.decrypt(nonce, ct, aad)
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception fallback at shared/security/data_encryption.py:128", exc_info=True)
                last_exc = exc
                continue
        raise ValueError("Unable to decrypt payload") from last_exc

    def encrypt_json(self, value: Any, *, aad: Optional[bytes] = None) -> Any:
        if not self.keys:
            return value
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
        return {
            _JSON_MARKER: True,
            "v": 1,
            "ciphertext": self.encrypt_text(encoded, aad=aad),
        }

    def decrypt_json(self, value: Any, *, aad: Optional[bytes] = None) -> Any:
        if not isinstance(value, dict) or not value.get(_JSON_MARKER):
            return value
        ciphertext = value.get("ciphertext")
        if not isinstance(ciphertext, str):
            raise ValueError("Invalid encrypted json payload")
        decoded = self.decrypt_text(ciphertext, aad=aad)
        return json.loads(decoded)


def is_encrypted_text(value: Any) -> bool:
    return isinstance(value, str) and value.startswith(_TEXT_PREFIX)


def is_encrypted_json(value: Any) -> bool:
    return isinstance(value, dict) and bool(value.get(_JSON_MARKER))


def is_encrypted_bytes(value: Any) -> bool:
    return isinstance(value, (bytes, bytearray)) and bytes(value).startswith(_BYTES_PREFIX)


def encryptor_from_keys(raw_keys: Optional[str]) -> Optional[DataEncryptor]:
    keys = parse_encryption_keys(raw_keys)
    return DataEncryptor(keys=keys) if keys else None
