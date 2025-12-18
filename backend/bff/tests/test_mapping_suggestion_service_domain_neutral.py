from bff.services.mapping_suggestion_service import MappingSuggestionService


def test_semantic_match_disabled_by_default():
    service = MappingSuggestionService()
    assert service.semantic_match_enabled is False


def test_semantic_match_is_opt_in_and_domain_neutral():
    source_schema = [{"name": "telephone", "type": "xsd:string"}]
    target_schema = [{"name": "phone", "type": "xsd:string"}]

    disabled = MappingSuggestionService(config={"features": {"semantic_match": False}})
    suggestion_disabled = disabled.suggest_mappings(source_schema=source_schema, target_schema=target_schema)
    assert suggestion_disabled.mappings == []

    enabled = MappingSuggestionService(config={"features": {"semantic_match": True}})
    suggestion_enabled = enabled.suggest_mappings(source_schema=source_schema, target_schema=target_schema)
    assert len(suggestion_enabled.mappings) == 1
    assert suggestion_enabled.mappings[0].source_field == "telephone"
    assert suggestion_enabled.mappings[0].target_field == "phone"


def test_label_is_used_for_matching_but_id_is_returned():
    service = MappingSuggestionService(config={"features": {"semantic_match": False}})

    source_schema = [{"name": "전화번호", "type": "xsd:string"}]
    target_schema = [{"name": "phone_number", "label": "전화번호", "type": "xsd:string"}]

    suggestion = service.suggest_mappings(source_schema=source_schema, target_schema=target_schema)
    assert len(suggestion.mappings) == 1
    assert suggestion.mappings[0].source_field == "전화번호"
    assert suggestion.mappings[0].target_field == "phone_number"
