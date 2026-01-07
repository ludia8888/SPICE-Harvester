from bff.services.mapping_suggestion_service import MappingSuggestionService


def _pairs(suggestion):
    return sorted((m.source_field, m.target_field) for m in suggestion.mappings)


def test_mapping_suggestion_is_deterministic():
    source_schema = [
        {"name": "full_name", "type": "xsd:string"},
        {"name": "id", "type": "xsd:string"},
        {"name": "email", "type": "xsd:string"},
    ]
    target_schema = [
        {"name": "id", "type": "xsd:string"},
        {"name": "name", "type": "xsd:string"},
        {"name": "email", "type": "xsd:string"},
    ]

    service = MappingSuggestionService()
    first = service.suggest_mappings(source_schema, target_schema)
    second = service.suggest_mappings(list(reversed(source_schema)), list(reversed(target_schema)))

    assert _pairs(first) == _pairs(second)
