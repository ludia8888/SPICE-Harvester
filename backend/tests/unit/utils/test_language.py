from shared.utils.language import first_localized_text


def test_first_localized_text_prefers_english_then_korean() -> None:
    assert first_localized_text({"ko": "이름", "en": "Name"}) == "Name"


def test_first_localized_text_falls_back_to_available_language() -> None:
    assert first_localized_text({"ko": "이름"}) == "이름"


def test_first_localized_text_ignores_non_string_non_mapping_inputs() -> None:
    assert first_localized_text(123) is None
