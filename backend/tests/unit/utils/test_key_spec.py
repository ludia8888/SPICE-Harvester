from shared.utils.key_spec import derive_key_spec_from_properties, extract_payload_key_spec, normalize_object_type_key_spec


def test_derive_key_spec_from_properties_uses_property_flags() -> None:
    derived = derive_key_spec_from_properties(
        [
            {"name": "customer_id", "primary_key": True, "title_key": True},
            {"name": "tenant_id", "primary_key": True},
            {"name": "display_name", "title_key": True},
            {"name": "ignored", "primary_key": False},
        ]
    )

    assert derived == {
        "primary_key": ["customer_id", "tenant_id"],
        "title_key": ["customer_id", "display_name"],
    }


def test_normalize_object_type_key_spec_falls_back_to_properties_when_pk_spec_missing() -> None:
    normalized = normalize_object_type_key_spec(
        {
            "properties": [
                {"name": "customer_id", "primary_key": True, "title_key": True},
                {"name": "tenant_id", "primary_key": True},
                {"name": "display_name", "title_key": True},
            ]
        },
        columns=["customer_id", "tenant_id", "display_name"],
    )

    assert normalized["primary_key"] == ["customer_id", "tenant_id"]
    assert normalized["title_key"] == ["customer_id", "display_name"]


def test_normalize_object_type_key_spec_supports_camel_case_pk_spec() -> None:
    normalized = normalize_object_type_key_spec(
        {
            "pkSpec": {
                "primaryKey": ["customer_id", "tenant_id"],
                "titleKey": ["customer_id"],
            }
        },
        columns=["customer_id", "tenant_id", "display_name"],
    )

    assert normalized["primary_key"] == ["customer_id", "tenant_id"]
    assert normalized["title_key"] == ["customer_id"]


def test_derive_key_spec_from_properties_supports_camel_case_flags() -> None:
    derived = derive_key_spec_from_properties(
        [
            {"name": "customer_id", "primaryKey": True, "titleKey": True},
            {"name": "tenant_id", "primaryKey": True},
            {"name": "display_name", "titleKey": True},
        ]
    )

    assert derived == {
        "primary_key": ["customer_id", "tenant_id"],
        "title_key": ["customer_id", "display_name"],
    }


def test_derive_key_spec_from_properties_uses_id_when_name_missing() -> None:
    derived = derive_key_spec_from_properties(
        [
            {"id": "customer_id", "primaryKey": True, "titleKey": True},
            {"name": "tenant_id", "primary_key": True},
            {"id": "display_name", "titleKey": True},
        ]
    )

    assert derived == {
        "primary_key": ["customer_id", "tenant_id"],
        "title_key": ["customer_id", "display_name"],
    }


def test_extract_payload_key_spec_prefers_property_flags_before_metadata_key_spec() -> None:
    primary_key, title_key = extract_payload_key_spec(
        {
            "properties": [
                {"name": "customer_id", "primaryKey": True, "titleKey": True},
                {"name": "tenant_id", "primary_key": True},
            ],
            "metadata": {
                "keySpec": {
                    "primaryKey": ["ignored_metadata_id"],
                    "titleKey": ["ignored_metadata_title"],
                }
            },
        }
    )

    assert primary_key == ["customer_id", "tenant_id"]
    assert title_key == ["customer_id"]


def test_extract_payload_key_spec_falls_back_to_metadata_when_property_flags_missing() -> None:
    primary_key, title_key = extract_payload_key_spec(
        {
            "properties": [
                {"name": "customer_id"},
                {"name": "display_name"},
            ],
            "metadata": {
                "keySpec": {
                    "primaryKey": ["customer_id"],
                    "titleKey": ["display_name"],
                }
            },
        }
    )

    assert primary_key == ["customer_id"]
    assert title_key == ["display_name"]


def test_extract_payload_key_spec_uses_id_when_property_flags_omit_name() -> None:
    primary_key, title_key = extract_payload_key_spec(
        {
            "properties": [
                {"id": "customer_id", "primaryKey": True, "titleKey": True},
                {"name": "tenant_id", "primary_key": True},
            ]
        }
    )

    assert primary_key == ["customer_id", "tenant_id"]
    assert title_key == ["customer_id"]


def test_normalize_object_type_key_spec_uses_id_when_property_flags_omit_name() -> None:
    normalized = normalize_object_type_key_spec(
        {
            "properties": [
                {"id": "customer_id", "primaryKey": True, "titleKey": True},
                {"name": "tenant_id", "primary_key": True},
                {"id": "display_name", "titleKey": True},
            ]
        },
        columns=["customer_id", "tenant_id", "display_name"],
    )

    assert normalized["primary_key"] == ["customer_id", "tenant_id"]
    assert normalized["title_key"] == ["customer_id", "display_name"]
