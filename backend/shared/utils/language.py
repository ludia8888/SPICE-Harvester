"""
Language utilities for SPICE HARVESTER
"""

from typing import List, Optional

from fastapi import Request


def get_accept_language(request: Request) -> str:
    """
    Get the preferred language from the request.

    Args:
        request: FastAPI request object

    Returns:
        Language code (e.g., 'en', 'ko')
    """
    # Get Accept-Language header
    accept_language = request.headers.get("Accept-Language", "en")

    # Parse the header and get the first language
    if accept_language:
        # Simple parsing: "en-US,en;q=0.9,ko;q=0.8" -> "en"
        languages = accept_language.split(",")
        if languages:
            primary_lang = languages[0].strip()
            # Extract language code: "en-US" -> "en"
            if "-" in primary_lang:
                primary_lang = primary_lang.split("-")[0]
            # Remove quality values: "en;q=0.9" -> "en"
            if ";" in primary_lang:
                primary_lang = primary_lang.split(";")[0]
            return primary_lang.lower()

    return "en"  # Default to English


def get_supported_languages() -> List[str]:
    """
    Get list of supported languages.

    Returns:
        List of supported language codes
    """
    return ["en", "ko"]


def is_supported_language(lang: str) -> bool:
    """
    Check if language is supported.

    Args:
        lang: Language code

    Returns:
        True if supported, False otherwise
    """
    return lang.lower() in get_supported_languages()


def get_default_language() -> str:
    """
    Get default language.

    Returns:
        Default language code
    """
    return "en"


def normalize_language(lang: Optional[str]) -> str:
    """
    Normalize language code.

    Args:
        lang: Language code to normalize

    Returns:
        Normalized language code
    """
    if not lang:
        return get_default_language()

    # Convert to lowercase
    lang = lang.lower()

    # Handle common variations
    if lang in ["english", "eng"]:
        return "en"
    elif lang in ["korean", "kor"]:
        return "ko"

    # Check if supported
    if is_supported_language(lang):
        return lang

    # Default fallback
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
