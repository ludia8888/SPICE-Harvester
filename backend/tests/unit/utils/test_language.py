"""
Comprehensive unit tests for language utilities module
Tests all language handling functions and classes
"""

import pytest
from unittest.mock import Mock

from shared.utils.language import (
    get_accept_language,
    get_supported_languages,
    is_supported_language,
    get_default_language,
    normalize_language,
    get_language_name,
    detect_language_from_text,
    MultilingualText,
)


class TestLanguageUtilities:
    """Test language utility functions"""

    # =============================================================================
    # Accept Language Parsing Tests
    # =============================================================================

    def test_get_accept_language_simple(self):
        """Test simple Accept-Language header parsing"""
        # Mock request with simple language
        request = Mock()
        request.headers = {"Accept-Language": "en"}
        
        result = get_accept_language(request)
        assert result == "en"

    def test_get_accept_language_with_region(self):
        """Test Accept-Language header with region"""
        request = Mock()
        request.headers = {"Accept-Language": "en-US"}
        
        result = get_accept_language(request)
        assert result == "en"

    def test_get_accept_language_with_quality(self):
        """Test Accept-Language header with quality values"""
        request = Mock()
        request.headers = {"Accept-Language": "en;q=0.9"}
        
        result = get_accept_language(request)
        assert result == "en"

    def test_get_accept_language_complex(self):
        """Test complex Accept-Language header"""
        request = Mock()
        request.headers = {"Accept-Language": "en-US,en;q=0.9,ko;q=0.8"}
        
        result = get_accept_language(request)
        assert result == "en"

    def test_get_accept_language_korean_first(self):
        """Test Accept-Language header with Korean first"""
        request = Mock()
        request.headers = {"Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8"}
        
        result = get_accept_language(request)
        assert result == "ko"

    def test_get_accept_language_missing_header(self):
        """Test missing Accept-Language header"""
        request = Mock()
        request.headers = {}
        
        result = get_accept_language(request)
        assert result == "en"  # Default

    def test_get_accept_language_empty_header(self):
        """Test empty Accept-Language header"""
        request = Mock()
        request.headers = {"Accept-Language": ""}
        
        result = get_accept_language(request)
        assert result == "en"  # Default

    def test_get_accept_language_whitespace(self):
        """Test Accept-Language header with whitespace"""
        request = Mock()
        request.headers = {"Accept-Language": "  ko-KR  ,  en  "}
        
        result = get_accept_language(request)
        assert result == "ko"

    def test_get_accept_language_case_insensitive(self):
        """Test Accept-Language header case handling"""
        request = Mock()
        request.headers = {"Accept-Language": "EN-US"}
        
        result = get_accept_language(request)
        assert result == "en"

    # =============================================================================
    # Supported Languages Tests
    # =============================================================================

    def test_get_supported_languages(self):
        """Test getting supported languages list"""
        languages = get_supported_languages()
        assert isinstance(languages, list)
        assert "en" in languages
        assert "ko" in languages
        assert len(languages) >= 2

    def test_is_supported_language_valid(self):
        """Test checking valid supported languages"""
        assert is_supported_language("en") is True
        assert is_supported_language("ko") is True

    def test_is_supported_language_invalid(self):
        """Test checking invalid languages"""
        assert is_supported_language("fr") is False
        assert is_supported_language("de") is False
        assert is_supported_language("ja") is False

    def test_is_supported_language_case_insensitive(self):
        """Test case-insensitive language support check"""
        assert is_supported_language("EN") is True
        assert is_supported_language("Ko") is True
        assert is_supported_language("KO") is True

    def test_get_default_language(self):
        """Test getting default language"""
        default = get_default_language()
        assert default == "en"
        assert isinstance(default, str)

    # =============================================================================
    # Language Normalization Tests
    # =============================================================================

    def test_normalize_language_none(self):
        """Test normalizing None language"""
        result = normalize_language(None)
        assert result == "en"

    def test_normalize_language_empty(self):
        """Test normalizing empty string"""
        result = normalize_language("")
        assert result == "en"

    def test_normalize_language_valid(self):
        """Test normalizing valid languages"""
        assert normalize_language("en") == "en"
        assert normalize_language("ko") == "ko"

    def test_normalize_language_case_conversion(self):
        """Test normalizing case variations"""
        assert normalize_language("EN") == "en"
        assert normalize_language("Ko") == "ko"
        assert normalize_language("KO") == "ko"

    def test_normalize_language_full_names(self):
        """Test normalizing full language names"""
        assert normalize_language("english") == "en"
        assert normalize_language("English") == "en"
        assert normalize_language("ENGLISH") == "en"
        
        assert normalize_language("korean") == "ko"
        assert normalize_language("Korean") == "ko"
        assert normalize_language("KOREAN") == "ko"

    def test_normalize_language_iso_codes(self):
        """Test normalizing ISO language codes"""
        assert normalize_language("eng") == "en"
        assert normalize_language("kor") == "ko"

    def test_normalize_language_unsupported(self):
        """Test normalizing unsupported languages"""
        assert normalize_language("french") == "en"  # Falls back to default
        assert normalize_language("xyz") == "en"
        assert normalize_language("123") == "en"

    # =============================================================================
    # Language Name Tests
    # =============================================================================

    def test_get_language_name_supported(self):
        """Test getting names for supported languages"""
        assert get_language_name("en") == "English"
        assert get_language_name("ko") == "Korean"

    def test_get_language_name_case_insensitive(self):
        """Test language name lookup is case insensitive"""
        assert get_language_name("EN") == "English"
        assert get_language_name("Ko") == "Korean"
        assert get_language_name("KO") == "Korean"

    def test_get_language_name_unsupported(self):
        """Test getting names for unsupported languages"""
        # Should return the input language code as fallback
        assert get_language_name("fr") == "fr"
        assert get_language_name("de") == "de"
        assert get_language_name("xyz") == "xyz"

    # =============================================================================
    # Language Detection Tests
    # =============================================================================

    def test_detect_language_from_text_empty(self):
        """Test language detection with empty text"""
        assert detect_language_from_text("") == "en"
        assert detect_language_from_text(None) == "en"

    def test_detect_language_from_text_english(self):
        """Test language detection with English text"""
        english_texts = [
            "Hello, this is English text",
            "The quick brown fox jumps over the lazy dog",
            "Machine learning and artificial intelligence",
            "1234567890 numbers and symbols !@#$%",
        ]
        
        for text in english_texts:
            result = detect_language_from_text(text)
            assert result == "en", f"Failed to detect English in: {text}"

    def test_detect_language_from_text_korean(self):
        """Test language detection with Korean text"""
        korean_texts = [
            "안녕하세요, 한국어 텍스트입니다",
            "한글은 세종대왕이 만든 문자입니다",
            "머신러닝과 인공지능",
            "가나다라마바사아자차카타파하",
        ]
        
        for text in korean_texts:
            result = detect_language_from_text(text)
            assert result == "ko", f"Failed to detect Korean in: {text}"

    def test_detect_language_from_text_mixed(self):
        """Test language detection with mixed text"""
        # Text with more Korean should be detected as Korean
        mixed_korean = "Hello 안녕하세요 세계 world 한국어가 더 많습니다"
        result = detect_language_from_text(mixed_korean)
        assert result == "ko"
        
        # Text with more English should be detected as English
        mixed_english = "안녕 Hello world this is mostly English text with some Korean"
        result = detect_language_from_text(mixed_english)
        assert result == "en"

    def test_detect_language_from_text_minimal_korean(self):
        """Test language detection with minimal Korean characters"""
        # Less than 10% Korean characters should be detected as English
        minimal_korean = "This is mostly English text with one 한 character"
        result = detect_language_from_text(minimal_korean)
        assert result == "en"


class TestMultilingualText:
    """Test MultilingualText class"""

    def test_init_empty(self):
        """Test creating empty MultilingualText"""
        multi_text = MultilingualText()
        assert len(multi_text.texts) == 0

    def test_init_with_kwargs(self):
        """Test creating MultilingualText with initial values"""
        multi_text = MultilingualText(en="Hello", ko="안녕")
        assert multi_text.texts["en"] == "Hello"
        assert multi_text.texts["ko"] == "안녕"

    def test_get_existing_language(self):
        """Test getting text for existing language"""
        multi_text = MultilingualText(en="Hello", ko="안녕")
        
        assert multi_text.get("en") == "Hello"
        assert multi_text.get("ko") == "안녕"

    def test_get_nonexistent_language(self):
        """Test getting text for non-existent language"""
        multi_text = MultilingualText(en="Hello")
        
        # Should fall back to default language ("en") when requested language doesn't exist
        assert multi_text.get("fr") == "Hello"

    def test_get_with_fallback(self):
        """Test getting text with fallback language"""
        multi_text = MultilingualText(en="Hello", ko="안녕")
        
        # Request French with English fallback
        result = multi_text.get("fr", fallback="en")
        assert result == "Hello"
        
        # Request Spanish with Korean fallback
        result = multi_text.get("es", fallback="ko")
        assert result == "안녕"

    def test_get_with_invalid_fallback(self):
        """Test getting text with invalid fallback"""
        multi_text = MultilingualText(en="Hello")
        
        # Request with invalid fallback should fall back to default language
        result = multi_text.get("fr", fallback="de")
        assert result == "Hello"  # Falls back to default "en"

    def test_get_with_default_fallback(self):
        """Test getting text falls back to default language"""
        multi_text = MultilingualText(en="Hello", ko="안녕")
        
        # Request unavailable language should fall back to default "en"
        result = multi_text.get("fr")
        assert result == "Hello"

    def test_get_any_available(self):
        """Test getting any available text when default not available"""
        multi_text = MultilingualText(ko="안녕", fr="Bonjour")
        
        # No English (default), should return any available
        result = multi_text.get("de")
        assert result in ["안녕", "Bonjour"]

    def test_get_completely_empty(self):
        """Test getting text from empty MultilingualText"""
        multi_text = MultilingualText()
        
        result = multi_text.get("en")
        assert result is None

    def test_set_new_language(self):
        """Test setting text for new language"""
        multi_text = MultilingualText()
        
        multi_text.set("en", "Hello")
        assert multi_text.texts["en"] == "Hello"

    def test_set_update_existing(self):
        """Test updating text for existing language"""
        multi_text = MultilingualText(en="Hello")
        
        multi_text.set("en", "Hi there")
        assert multi_text.texts["en"] == "Hi there"

    def test_has_language_existing(self):
        """Test checking for existing language"""
        multi_text = MultilingualText(en="Hello", ko="안녕")
        
        assert multi_text.has_language("en") is True
        assert multi_text.has_language("ko") is True

    def test_has_language_nonexistent(self):
        """Test checking for non-existent language"""
        multi_text = MultilingualText(en="Hello")
        
        assert multi_text.has_language("fr") is False
        assert multi_text.has_language("de") is False

    def test_get_languages(self):
        """Test getting list of available languages"""
        multi_text = MultilingualText(en="Hello", ko="안녕", fr="Bonjour")
        
        languages = multi_text.get_languages()
        assert isinstance(languages, list)
        assert "en" in languages
        assert "ko" in languages
        assert "fr" in languages
        assert len(languages) == 3

    def test_get_languages_empty(self):
        """Test getting languages from empty MultilingualText"""
        multi_text = MultilingualText()
        
        languages = multi_text.get_languages()
        assert languages == []

    def test_to_dict(self):
        """Test converting to dictionary"""
        multi_text = MultilingualText(en="Hello", ko="안녕")
        
        result = multi_text.to_dict()
        assert isinstance(result, dict)
        assert result["en"] == "Hello"
        assert result["ko"] == "안녕"
        assert len(result) == 2

    def test_to_dict_empty(self):
        """Test converting empty MultilingualText to dictionary"""
        multi_text = MultilingualText()
        
        result = multi_text.to_dict()
        assert result == {}


class TestLanguageIntegration:
    """Test integration scenarios with language utilities"""

    def test_request_to_multilingual_workflow(self):
        """Test complete workflow from request to multilingual text"""
        # Simulate request with Korean preference
        request = Mock()
        request.headers = {"Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8"}
        
        # Get preferred language
        preferred_lang = get_accept_language(request)
        assert preferred_lang == "ko"
        
        # Normalize the language
        normalized_lang = normalize_language(preferred_lang)
        assert normalized_lang == "ko"
        
        # Check if supported
        assert is_supported_language(normalized_lang) is True
        
        # Get multilingual text
        multi_text = MultilingualText(
            en="Welcome to SPICE HARVESTER",
            ko="SPICE HARVESTER에 오신 것을 환영합니다"
        )
        
        # Get text in preferred language
        result = multi_text.get(normalized_lang, fallback="en")
        assert result == "SPICE HARVESTER에 오신 것을 환영합니다"

    def test_text_detection_workflow(self):
        """Test workflow with automatic language detection"""
        # Detect language from user input
        user_input = "안녕하세요, 도움이 필요합니다"
        detected_lang = detect_language_from_text(user_input)
        assert detected_lang == "ko"
        
        # Normalize and validate
        normalized_lang = normalize_language(detected_lang)
        assert normalized_lang == "ko"
        assert is_supported_language(normalized_lang) is True
        
        # Get language name for logging
        lang_name = get_language_name(normalized_lang)
        assert lang_name == "Korean"

    def test_fallback_chain(self):
        """Test complete fallback chain"""
        # Unsupported language should fall back correctly
        unsupported_lang = "fr"
        
        # Check it's not supported
        assert is_supported_language(unsupported_lang) is False
        
        # Normalize should fall back to default
        normalized = normalize_language(unsupported_lang)
        assert normalized == "en"
        
        # Should work with multilingual text
        multi_text = MultilingualText(en="Hello", ko="안녕")
        result = multi_text.get(unsupported_lang, fallback=normalized)
        assert result == "Hello"


if __name__ == "__main__":
    pytest.main([__file__])