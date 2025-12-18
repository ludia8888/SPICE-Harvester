import pytest

from shared.utils.label_mapper import LabelMapper


@pytest.mark.asyncio
async def test_label_mapper_detects_language_for_string_and_falls_back(tmp_path):
    db_path = tmp_path / "label_mappings.db"
    mapper = LabelMapper(db_path=str(db_path))

    await mapper.register_class("testdb", "Product", "Product", "A product")

    # Stored as English (auto-detected) but retrievable from both languages via fallback.
    assert await mapper.get_class_label("testdb", "Product", "en") == "Product"
    assert await mapper.get_class_label("testdb", "Product", "ko") == "Product"

    assert await mapper.get_class_id("testdb", "Product", "en") == "Product"
    assert await mapper.get_class_id("testdb", "Product", "ko") == "Product"


@pytest.mark.asyncio
async def test_label_mapper_supports_language_map_and_reverse_lookup(tmp_path):
    db_path = tmp_path / "label_mappings.db"
    mapper = LabelMapper(db_path=str(db_path))

    await mapper.register_class(
        "testdb",
        "Product",
        {"en": "Product", "ko": "제품"},
        {"en": "A product", "ko": "제품 설명"},
    )

    assert await mapper.get_class_label("testdb", "Product", "en") == "Product"
    assert await mapper.get_class_label("testdb", "Product", "ko") == "제품"

    # Reverse lookup works regardless of requested language.
    assert await mapper.get_class_id("testdb", "Product", "ko") == "Product"
    assert await mapper.get_class_id("testdb", "제품", "en") == "Product"


@pytest.mark.asyncio
async def test_label_mapper_batch_fallback_returns_best_available(tmp_path):
    db_path = tmp_path / "label_mappings.db"
    mapper = LabelMapper(db_path=str(db_path))

    await mapper.register_class("testdb", "EnglishOnly", "Product", None)
    await mapper.register_class("testdb", "KoreanOnly", "제품", None)
    await mapper.register_class("testdb", "Both", {"en": "Order", "ko": "주문"}, None)

    labels_en = await mapper.get_class_labels_in_batch(
        "testdb", ["EnglishOnly", "KoreanOnly", "Both"], lang="en"
    )

    assert labels_en["EnglishOnly"] == "Product"
    assert labels_en["KoreanOnly"] == "제품"  # fallback
    assert labels_en["Both"] == "Order"

    labels_ko = await mapper.get_class_labels_in_batch(
        "testdb", ["EnglishOnly", "KoreanOnly", "Both"], lang="ko"
    )

    assert labels_ko["EnglishOnly"] == "Product"  # fallback
    assert labels_ko["KoreanOnly"] == "제품"
    assert labels_ko["Both"] == "주문"

