"""
LLM safety utilities (domain-neutral).

Goals:
- Minimize data sent to LLM (truncate / sample).
- Mask likely PII before leaving the system.
- Provide stable digests (hashes) for audit/caching without storing raw payloads.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union


_EMAIL_RE = re.compile(r"(?i)\\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}\\b")
# Loose phone matcher (international/local); we mask long digit runs to reduce PII leakage.
_LONG_DIGIT_RE = re.compile(r"\\b\\+?\\d[\\d\\s().-]{7,}\\d\\b")


def sha256_hex(value: Union[str, bytes]) -> str:
    data = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(data).hexdigest()


def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True, default=str)


def digest_for_audit(obj: Any) -> str:
    return sha256_hex(stable_json_dumps(obj))


def truncate_text(text: str, *, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 1)] + "…"


def _mask_email(text: str) -> str:
    def _repl(match: re.Match[str]) -> str:
        raw = match.group(0)
        digest = sha256_hex(raw)[:10]
        return f"<email:{digest}>"

    return _EMAIL_RE.sub(_repl, text)


def _mask_long_digits(text: str) -> str:
    def _repl(match: re.Match[str]) -> str:
        raw = match.group(0)
        digits = re.sub(r"\\D+", "", raw)
        if len(digits) < 8:
            return raw
        digest = sha256_hex(digits)[:10]
        return f"<number:{digest}>"

    return _LONG_DIGIT_RE.sub(_repl, text)


def mask_pii_text(text: str, *, max_chars: Optional[int] = None) -> str:
    out = text
    out = _mask_email(out)
    out = _mask_long_digits(out)
    if max_chars is not None:
        out = truncate_text(out, max_chars=max_chars)
    return out


def mask_pii(obj: Any, *, max_string_chars: int = 200) -> Any:
    """
    Recursively mask likely PII in a JSON-like structure.

    Notes:
    - This is intentionally conservative (masking *likely* PII).
    - Do not treat this as a formal DLP system.
    """

    if obj is None:
        return None
    if isinstance(obj, (bool, int, float)):
        return obj
    if isinstance(obj, str):
        return mask_pii_text(obj, max_chars=max_string_chars)
    if isinstance(obj, (list, tuple)):
        return [mask_pii(v, max_string_chars=max_string_chars) for v in obj]
    if isinstance(obj, dict):
        return {str(k): mask_pii(v, max_string_chars=max_string_chars) for k, v in obj.items()}
    # Fallback: stringize
    return mask_pii_text(str(obj), max_chars=max_string_chars)


def sample_items(items: List[Any], *, max_items: int) -> List[Any]:
    if max_items <= 0:
        return []
    return items[:max_items]

