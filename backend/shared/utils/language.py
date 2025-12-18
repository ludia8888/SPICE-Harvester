"""
Language utilities for SPICE HARVESTER
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import Request


SUPPORTED_LANGUAGES = ["en", "ko"]


def get_supported_languages() -> List[str]:
    """
    Get list of supported languages.

    Returns:
        List of supported language codes
    """
    return list(SUPPORTED_LANGUAGES)


def get_default_language() -> str:
    """
    Get default language.

    Returns:
        Default language code
    """
    return "en"


def is_supported_language(lang: str) -> bool:
    """
    Check if language is supported.

    Args:
        lang: Language code

    Returns:
        True if supported, False otherwise
    """
    return normalize_language(lang) in SUPPORTED_LANGUAGES


def normalize_language(lang: Optional[str]) -> str:
    """
    Normalize language code.

    Supports:
    - region codes (en-US -> en, ko-KR -> ko)
    - common synonyms (eng/english, kor/korean/kr)
    """
    if not lang:
        return get_default_language()

    raw = str(lang).strip().lower()
    if not raw:
        return get_default_language()

    # Strip quality values: "en;q=0.9"
    if ";" in raw:
        raw = raw.split(";", 1)[0].strip()

    # Take primary subtag: "en-us" -> "en"
    if "-" in raw:
        raw = raw.split("-", 1)[0].strip()
    if "_" in raw:
        raw = raw.split("_", 1)[0].strip()

    if raw in ("english", "eng"):
        return "en"
    if raw in ("korean", "kor", "kr"):
        return "ko"

    if raw in SUPPORTED_LANGUAGES:
        return raw

    return get_default_language()


def _parse_accept_language_header(value: str) -> List[str]:
    """
    Parse Accept-Language into a list of language codes ordered by preference.

    Very small parser; we don't implement full RFC behavior, but we respect q=.
    """
    if not value:
        return []

    parts = [p.strip() for p in value.split(",") if p.strip()]
    weighted: List[tuple[float, str]] = []
    for part in parts:
        lang = part
        q = 1.0
        if ";" in part:
            lang, params = part.split(";", 1)
            lang = lang.strip()
            params = params.strip()
            if params.startswith("q="):
                try:
                    q = float(params[2:])
                except Exception:
                    q = 1.0
        weighted.append((q, normalize_language(lang)))

    # Sort by q desc, stable otherwise
    weighted.sort(key=lambda item: item[0], reverse=True)
    return [lang for _, lang in weighted]


def _normalize_language_map_key(key: str) -> Optional[str]:
    """
    Strict normalization for language-map keys.

    Unlike `normalize_language()`, unknown keys are rejected (return None) instead of defaulting to "en",
    to avoid accidentally clobbering translations when clients send unsupported languages.
    """
    raw = str(key).strip().lower()
    if not raw:
        return None
    if ";" in raw:
        raw = raw.split(";", 1)[0].strip()
    if raw in ("english", "eng") or raw.startswith("en"):
        return "en"
    if raw in ("korean", "kor", "kr") or raw.startswith("ko"):
        return "ko"
    return None


def get_accept_language(request: Request) -> str:
    """
    Get the preferred language from the request.

    Args:
        request: FastAPI request object

    Returns:
        Language code (e.g., 'en', 'ko')
    """
    # Explicit override (query param) beats header. FE can use this instead of setting headers.
    query_lang = request.query_params.get("lang") or request.query_params.get("language")
    if query_lang:
        return normalize_language(query_lang)

    accept_language = request.headers.get("Accept-Language", "")
    for candidate in _parse_accept_language_header(accept_language):
        if candidate in SUPPORTED_LANGUAGES:
            return candidate

    return get_default_language()


def get_language_name(lang: str) -> str:
    """
    Get human-readable name for language code.

    Args:
        lang: Language code

    Returns:
        Human-readable language name
    """
    language_names = {"en": "English", "ko": "Korean"}

    return language_names.get(lang.lower(), lang)


def detect_language_from_text(text: str) -> str:
    """
    Simple language detection from text.

    Args:
        text: Text to analyze

    Returns:
        Detected language code
    """
    if not text:
        return get_default_language()

    # Simple heuristic: check for Korean characters
    korean_chars = sum(1 for char in text if "\uac00" <= char <= "\ud7af")
    total_chars = len(text)

    if korean_chars > 0 and korean_chars / total_chars > 0.1:
        return "ko"

    return "en"


def fallback_languages(lang: str) -> List[str]:
    """
    Languages to try in order when a translation is missing.
    """
    primary = normalize_language(lang)
    out: List[str] = []
    for candidate in (primary, "en", "ko"):
        if candidate not in out:
            out.append(candidate)
    return out


def coerce_localized_text(value: Any, *, default_lang: Optional[str] = None) -> Dict[str, str]:
    """
    Coerce a LocalizedText-like value into a normalized language map.

    - If value is a string, detect language (ko if contains Hangul) and store under that key.
    - If value is a dict, normalize keys to en/ko (region codes supported).
    - Drops empty strings and None values.
    """
    if value is None:
        return {}

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        lang = normalize_language(default_lang) if default_lang else detect_language_from_text(text)
        return {lang: text}

    if isinstance(value, dict):
        out: Dict[str, str] = {}
        for k, v in value.items():
            if v is None:
                continue
            text = str(v).strip()
            if not text:
                continue
            key = _normalize_language_map_key(str(k))
            if not key:
                continue
            out[key] = text
        return out

    # Pydantic models or other objects (best-effort)
    try:
        text = str(value).strip()
    except Exception:
        return {}
    if not text:
        return {}
    lang = normalize_language(default_lang) if default_lang else detect_language_from_text(text)
    return {lang: text}


def select_localized_text(value: Any, *, lang: str) -> str:
    """
    Choose the best string for the requested language from a LocalizedText-like input.
    """
    mapping = coerce_localized_text(value)
    for candidate in fallback_languages(lang):
        if candidate in mapping:
            return mapping[candidate]
    # Last resort: any available string
    return next(iter(mapping.values()), "")


class MultilingualText:
    """
    Utility class for handling multilingual text.
    """

    def __init__(self, **kwargs):
        """
        Initialize with language-specific text.

        Args:
            **kwargs: Language code as key, text as value
        """
        self.texts = kwargs

    def get(self, language: str, fallback: Optional[str] = None) -> Optional[str]:
        """
        Get text for specific language.

        Args:
            language: Language code
            fallback: Fallback language if primary not found

        Returns:
            Text in requested language or fallback
        """
        # Try primary language
        if language in self.texts:
            return self.texts[language]

        # Try fallback language
        if fallback and fallback in self.texts:
            return self.texts[fallback]

        # Try default language
        default_lang = get_default_language()
        if default_lang in self.texts:
            return self.texts[default_lang]

        # Return any available text
        if self.texts:
            return next(iter(self.texts.values()))

        return None

    def set(self, language: str, text: str) -> None:
        """
        Set text for specific language.

        Args:
            language: Language code
            text: Text to set
        """
        self.texts[language] = text

    def has_language(self, language: str) -> bool:
        """
        Check if text exists for language.

        Args:
            language: Language code

        Returns:
            True if text exists for language
        """
        return language in self.texts

    def get_languages(self) -> List[str]:
        """
        Get list of available languages.

        Returns:
            List of language codes
        """
        return list(self.texts.keys())

    def to_dict(self) -> dict:
        """
        Convert to dictionary.

        Returns:
            Dictionary with language codes as keys
        """
        return dict(self.texts)


if __name__ == "__main__":
    # Test the utilities
    print("Supported languages:", get_supported_languages())
    print("Default language:", get_default_language())

    # Test language detection
    english_text = "Hello, this is English text"
    korean_text = "안녕하세요, 한국어 텍스트입니다"

    print(f"'{english_text}' -> {detect_language_from_text(english_text)}")
    print(f"'{korean_text}' -> {detect_language_from_text(korean_text)}")

    # Test multilingual text
    multi_text = MultilingualText(en="Hello World", ko="안녕 세계")

    print(f"English: {multi_text.get('en')}")
    print(f"Korean: {multi_text.get('ko')}")
    print(f"Available languages: {multi_text.get_languages()}")
