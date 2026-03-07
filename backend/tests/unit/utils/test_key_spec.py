from shared.utils.key_spec import derive_key_spec_from_properties, normalize_object_type_key_spec


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
