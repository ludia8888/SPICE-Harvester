from __future__ import annotations

import re
from typing import Any, Dict, Optional

from shared.utils.language import detect_language_from_text, normalize_language


def m(*, en: str, ko: str, lang: Optional[str] = None, **params: Any) -> str:
    """
    Inline bilingual message helper.

    Uses the request-scoped language set by middleware.
    """
    from shared.i18n.context import get_language

    selected = normalize_language(lang) if lang else get_language()
    template = en if selected == "en" else ko
    if not params:
        return template
    try:
        return template.format(**params)
    except Exception:
        return template


def _generic_http_detail(status_code: int, *, lang: str) -> str:
    lang = normalize_language(lang)
    if status_code == 400:
        return m(en="Invalid request.", ko="잘못된 요청입니다.", lang=lang)
    if status_code == 401:
        return m(en="Authentication required.", ko="인증이 필요합니다.", lang=lang)
    if status_code == 403:
        return m(en="You do not have permission.", ko="권한이 없습니다.", lang=lang)
    if status_code == 404:
        return m(en="Not found.", ko="찾을 수 없습니다.", lang=lang)
    if status_code == 409:
        return m(en="Conflict.", ko="충돌이 발생했습니다.", lang=lang)
    if status_code == 422:
        return m(en="Validation failed.", ko="입력값 검증에 실패했습니다.", lang=lang)
    if status_code == 429:
        return m(en="Too many requests.", ko="요청이 너무 많습니다.", lang=lang)
    if status_code >= 500:
        return m(en="Internal server error.", ko="서버 오류가 발생했습니다.", lang=lang)
    return m(en="Request failed.", ko="요청에 실패했습니다.", lang=lang)


def _generic_api_message(api_status: str, *, lang: str) -> str:
    lang = normalize_language(lang)
    status = (api_status or "").lower()
    if status == "success":
        return m(en="Success.", ko="성공했습니다.", lang=lang)
    if status == "created":
        return m(en="Created.", ko="생성되었습니다.", lang=lang)
    if status == "accepted":
        return m(en="Accepted.", ko="접수되었습니다.", lang=lang)
    if status == "warning":
        return m(en="Warning.", ko="경고가 있습니다.", lang=lang)
    if status == "partial":
        return m(en="Partially succeeded.", ko="부분 성공했습니다.", lang=lang)
    if status == "error":
        return m(en="Error.", ko="오류가 발생했습니다.", lang=lang)
    return m(en="OK.", ko="확인되었습니다.", lang=lang)


_KO_NOT_FOUND_RE = re.compile(
    r"^(?P<resource>[^']+?)\s*'(?P<id>[^']+)'\s*를 찾을 수 없습니다\s*$"
)
_KO_FAILED_RE = re.compile(
    r"^(?P<prefix>.+?)\s*(?P<verb>조회|생성|업데이트|삭제)\s*실패\s*:\s*(?P<reason>.+)$"
)


_KO_RESOURCE_TRANSLATIONS: Dict[str, str] = {
    "인스턴스": "Instance",
    "온톨로지": "Ontology",
    "클래스": "Class",
    "데이터베이스": "Database",
    "브랜치": "Branch",
    "커밋": "Commit",
    "명령": "Command",
}

_KO_VERB_TRANSLATIONS: Dict[str, str] = {
    "조회": "fetch",
    "생성": "create",
    "업데이트": "update",
    "삭제": "delete",
}


def _translate_known(text: str, *, target_lang: str) -> Optional[str]:
    """
    Small curated dictionary for common phrases.
    """
    target_lang = normalize_language(target_lang)
    src_lang = detect_language_from_text(text)
    if src_lang == target_lang:
        return text

    table: Dict[str, Dict[str, str]] = {
        "잘못된 JSON 형식입니다": {"en": "Invalid JSON format.", "ko": "잘못된 JSON 형식입니다"},
        "Invalid JSON format": {"en": "Invalid JSON format.", "ko": "잘못된 JSON 형식입니다"},
        "Invalid JSON format.": {"en": "Invalid JSON format.", "ko": "잘못된 JSON 형식입니다"},
        "입력 데이터 검증 실패": {"en": "Input validation failed.", "ko": "입력 데이터 검증 실패"},
        "Input validation failed.": {"en": "Input validation failed.", "ko": "입력 데이터 검증 실패"},
        "찾을 수 없습니다": {"en": "Not found.", "ko": "찾을 수 없습니다"},
        "Not found.": {"en": "Not found.", "ko": "찾을 수 없습니다"},
        "권한이 없습니다": {"en": "You do not have permission.", "ko": "권한이 없습니다"},
        "You do not have permission.": {"en": "You do not have permission.", "ko": "권한이 없습니다"},
        "인증이 필요합니다": {"en": "Authentication required.", "ko": "인증이 필요합니다"},
        "Authentication required.": {"en": "Authentication required.", "ko": "인증이 필요합니다"},
        "Service is healthy": {"en": "Service is healthy.", "ko": "서비스가 정상입니다."},
        "Service is healthy.": {"en": "Service is healthy.", "ko": "서비스가 정상입니다."},
        "백엔드 포 프론트엔드 서비스": {
            "en": "Backend for Frontend service.",
            "ko": "백엔드 포 프론트엔드 서비스",
        },
        "도메인 독립적인 온톨로지 관리 서비스": {
            "en": "Domain-agnostic ontology management service.",
            "ko": "도메인 독립적인 온톨로지 관리 서비스",
        },
    }

    normalized = text.strip()
    if normalized in table:
        return table[normalized].get(target_lang)
    return None


def _translate_ko_to_en(text: str) -> Optional[str]:
    match = _KO_NOT_FOUND_RE.match(text.strip())
    if match:
        resource_ko = match.group("resource").strip()
        resource_en = _KO_RESOURCE_TRANSLATIONS.get(resource_ko, "Resource")
        resource_en = resource_en[0].upper() + resource_en[1:]
        return f"{resource_en} '{match.group('id')}' was not found."

    match = _KO_FAILED_RE.match(text.strip())
    if match:
        verb_ko = match.group("verb")
        verb_en = _KO_VERB_TRANSLATIONS.get(verb_ko, "process")
        reason = match.group("reason").strip()
        return f"Failed to {verb_en}: {reason}"

    if "보안 위반" in text:
        return "Security violation detected in input."

    return None


def localize_free_text(
    text: Any,
    *,
    target_lang: str,
    status_code: Optional[int] = None,
    api_status: Optional[str] = None,
) -> Any:
    """
    Best-effort localization for existing free-text messages.

    - If the text already matches the requested language, keep it.
    - Otherwise try curated dictionary + small pattern-based translations.
    - If still unknown, fall back to a generic message (to avoid KR-only/EN-only leaks).
    """
    if not isinstance(text, str):
        return text

    target = normalize_language(target_lang)
    stripped = text.strip()
    if not stripped:
        return text

    current = detect_language_from_text(stripped)
    if current == target:
        return text

    known = _translate_known(stripped, target_lang=target)
    if known:
        return known

    if target == "en" and current == "ko":
        translated = _translate_ko_to_en(stripped)
        if translated:
            return translated

    # Fallbacks (generic but localized)
    if api_status:
        return _generic_api_message(api_status, lang=target)
    if status_code is not None:
        return _generic_http_detail(int(status_code), lang=target)

    return text
