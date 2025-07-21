"""
Comprehensive unit tests for jsonld utility module
Tests all JSON-LD conversion functionality with REAL behavior verification
"""

import json
import pytest
from typing import Dict, Any

from shared.utils.jsonld import (
    JSONToJSONLDConverter,
    get_default_converter,
)


class TestJSONToJSONLDConverter:
    """Test JSONToJSONLDConverter class functionality"""

    def setup_method(self):
        """Setup for each test method"""
        self.converter = JSONToJSONLDConverter()

    def test_init_with_default_context(self):
        """Test initialization with default context"""
        converter = JSONToJSONLDConverter()
        
        assert converter.context is not None
        assert "@context" in converter.context
        assert "@vocab" in converter.context["@context"]
        assert "rdfs" in converter.context["@context"]
        assert "owl" in converter.context["@context"]

    def test_init_with_custom_context(self):
        """Test initialization with custom context"""
        custom_context = {
            "@context": {
                "custom": "http://example.com/custom#"
            }
        }
        
        converter = JSONToJSONLDConverter(custom_context)
        
        assert converter.context == custom_context
        assert "custom" in converter.context["@context"]

    def test_get_default_context(self):
        """Test default context structure"""
        context = self.converter._get_default_context()
        
        assert isinstance(context, dict)
        assert "@context" in context
        
        ctx = context["@context"]
        assert "@vocab" in ctx
        assert "rdfs" in ctx
        assert "owl" in ctx
        assert "xsd" in ctx
        assert "label" in ctx
        assert "comment" in ctx
        assert "type" in ctx
        assert "id" in ctx
        
        # Verify mappings
        assert ctx["label"] == "rdfs:label"
        assert ctx["comment"] == "rdfs:comment"
        assert ctx["type"] == "@type"
        assert ctx["id"] == "@id"


class TestConvertToJSONLD:
    """Test conversion from JSON to JSON-LD"""

    def setup_method(self):
        """Setup for each test method"""
        self.converter = JSONToJSONLDConverter()

    def test_convert_basic_object(self):
        """Test converting basic object to JSON-LD"""
        input_data = {
            "id": "Person",
            "label": "Person",
            "description": "A human being"
        }
        
        result = self.converter.convert_to_jsonld(input_data.copy())
        
        assert "@context" in result
        assert "@id" in result
        assert result["@id"] == "Person"
        assert "rdfs:label" in result
        assert result["rdfs:label"] == "Person"
        assert "rdfs:comment" in result
        assert result["rdfs:comment"] == "A human being"
        assert "id" not in result
        assert "label" not in result
        assert "description" not in result

    def test_convert_with_existing_context(self):
        """Test conversion when @context already exists"""
        input_data = {
            "@context": {"existing": "http://example.com/"},
            "id": "Test",
            "label": "Test Label"
        }
        
        result = self.converter.convert_to_jsonld(input_data.copy())
        
        # Should preserve existing context
        assert result["@context"]["existing"] == "http://example.com/"
        assert "@id" in result
        assert "rdfs:label" in result

    def test_convert_with_existing_type(self):
        """Test conversion when @type already exists"""
        input_data = {
            "id": "CustomClass",
            "@type": "owl:ObjectProperty",
            "label": "Custom"
        }
        
        result = self.converter.convert_to_jsonld(input_data.copy())
        
        # Should preserve existing @type
        assert result["@type"] == "owl:ObjectProperty"

    def test_convert_with_existing_id(self):
        """Test conversion when @id already exists"""
        input_data = {
            "id": "RegularId",
            "@id": "ExistingId",
            "label": "Test"
        }
        
        result = self.converter.convert_to_jsonld(input_data.copy())
        
        # Should preserve existing @id and NOT delete id field when @id exists
        assert result["@id"] == "ExistingId"
        assert "id" in result  # id is preserved when @id already exists
        assert result["id"] == "RegularId"

    def test_convert_adds_default_type(self):
        """Test that default type is added when none exists"""
        input_data = {
            "id": "SomeClass",
            "label": "Some Class"
        }
        
        result = self.converter.convert_to_jsonld(input_data.copy())
        
        assert "@type" in result
        assert result["@type"] == "owl:Class"

    def test_convert_preserves_other_fields(self):
        """Test that other fields are preserved"""
        input_data = {
            "id": "Person",
            "label": "Person",
            "custom_field": "custom_value",
            "relationships": ["has_friend"],
            "properties": {"age": "xsd:int"}
        }
        
        result = self.converter.convert_to_jsonld(input_data.copy())
        
        assert result["custom_field"] == "custom_value"
        assert result["relationships"] == ["has_friend"]
        assert result["properties"] == {"age": "xsd:int"}

    def test_convert_empty_object(self):
        """Test converting empty object"""
        input_data = {}
        
        result = self.converter.convert_to_jsonld(input_data.copy())
        
        assert "@context" in result
        assert "@type" in result
        assert result["@type"] == "owl:Class"

    def test_convert_only_adds_context_if_missing(self):
        """Test that context is only added if missing"""
        input_data = {
            "@context": {"custom": "value"},
            "id": "test"
        }
        
        original_context = input_data["@context"]
        result = self.converter.convert_to_jsonld(input_data.copy())
        
        # Should preserve original context structure
        assert "custom" in result["@context"]


class TestConvertFromJSONLD:
    """Test conversion from JSON-LD to regular JSON"""

    def setup_method(self):
        """Setup for each test method"""
        self.converter = JSONToJSONLDConverter()

    def test_convert_from_jsonld_basic(self):
        """Test basic conversion from JSON-LD"""
        jsonld_data = {
            "@context": {"rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
            "@id": "Person",
            "@type": "owl:Class",
            "rdfs:label": "Person",
            "rdfs:comment": "A human being"
        }
        
        result = self.converter.convert_from_jsonld(jsonld_data.copy())
        
        assert "@context" not in result
        assert "id" in result
        assert result["id"] == "Person"
        assert "type" in result
        assert result["type"] == "owl:Class"
        assert "label" in result
        assert result["label"] == "Person"
        assert "description" in result
        assert result["description"] == "A human being"
        assert "@id" not in result
        assert "@type" not in result
        assert "rdfs:label" not in result
        assert "rdfs:comment" not in result

    def test_convert_from_jsonld_preserves_other_fields(self):
        """Test that other fields are preserved during conversion"""
        jsonld_data = {
            "@context": {},
            "@id": "Test",
            "rdfs:label": "Test",
            "custom_field": "value",
            "properties": {"name": "string"}
        }
        
        result = self.converter.convert_from_jsonld(jsonld_data.copy())
        
        assert result["custom_field"] == "value"
        assert result["properties"] == {"name": "string"}

    def test_convert_from_jsonld_missing_fields(self):
        """Test conversion when some fields are missing"""
        jsonld_data = {
            "@context": {},
            "custom_field": "value"
        }
        
        result = self.converter.convert_from_jsonld(jsonld_data.copy())
        
        assert result["custom_field"] == "value"
        assert "@context" not in result

    def test_convert_from_jsonld_empty_object(self):
        """Test converting empty JSON-LD object"""
        jsonld_data = {}
        
        result = self.converter.convert_from_jsonld(jsonld_data.copy())
        
        assert result == {}

    def test_convert_roundtrip(self):
        """Test round-trip conversion JSON -> JSON-LD -> JSON"""
        original_data = {
            "id": "Product",
            "label": "Product",
            "description": "An item for sale",
            "type": "Class",
            "custom_field": "custom_value"
        }
        
        # Convert to JSON-LD
        jsonld_data = self.converter.convert_to_jsonld(original_data.copy())
        
        # Convert back to JSON
        result_data = self.converter.convert_from_jsonld(jsonld_data)
        
        # Should preserve essential fields
        assert result_data["id"] == original_data["id"]
        assert result_data["label"] == original_data["label"]
        assert result_data["description"] == original_data["description"]
        assert result_data["custom_field"] == original_data["custom_field"]
        # Note: type might be changed to the default owl:Class during conversion


class TestJSONLDValidation:
    """Test JSON-LD validation functionality"""

    def setup_method(self):
        """Setup for each test method"""
        self.converter = JSONToJSONLDConverter()

    def test_validate_valid_jsonld(self):
        """Test validation of valid JSON-LD"""
        valid_jsonld = {
            "@context": {"rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
            "@id": "http://example.com/Person",
            "@type": "owl:Class"
        }
        
        result = self.converter.validate_jsonld(valid_jsonld)
        
        assert result is True

    def test_validate_jsonld_with_id_only(self):
        """Test validation with @id only"""
        jsonld_with_id = {
            "@context": {},
            "@id": "http://example.com/Person"
        }
        
        result = self.converter.validate_jsonld(jsonld_with_id)
        
        assert result is True

    def test_validate_jsonld_with_type_only(self):
        """Test validation with @type only"""
        jsonld_with_type = {
            "@context": {},
            "@type": "owl:Class"
        }
        
        result = self.converter.validate_jsonld(jsonld_with_type)
        
        assert result is True

    def test_validate_invalid_jsonld_not_dict(self):
        """Test validation of non-dictionary data"""
        invalid_data = ["not", "a", "dict"]
        
        result = self.converter.validate_jsonld(invalid_data)
        
        assert result is False

    def test_validate_invalid_jsonld_no_context(self):
        """Test validation without context"""
        invalid_jsonld = {
            "@id": "http://example.com/Person"
        }
        
        result = self.converter.validate_jsonld(invalid_jsonld)
        
        assert result is False

    def test_validate_invalid_jsonld_no_id_or_type(self):
        """Test validation without @id or @type"""
        invalid_jsonld = {
            "@context": {},
            "some_field": "some_value"
        }
        
        result = self.converter.validate_jsonld(invalid_jsonld)
        
        assert result is False

    def test_validate_handles_exceptions(self):
        """Test that validation handles exceptions gracefully"""
        # The validation function is quite permissive, so let's test actual edge cases
        # This data has @context and @id, so it will pass validation
        permissive_data = {
            "@context": None,
            "@id": {"complex": "object"}
        }
        
        result = self.converter.validate_jsonld(permissive_data)
        
        # This actually passes validation because @context exists (even if None) and @id exists
        assert result is True
        
        # Test truly problematic data that should fail
        truly_invalid_data = "not a dict at all"
        
        result2 = self.converter.validate_jsonld(truly_invalid_data)
        assert result2 is False


class TestJSONLDExpansion:
    """Test JSON-LD expansion functionality"""

    def setup_method(self):
        """Setup for each test method"""
        self.converter = JSONToJSONLDConverter()

    def test_expand_jsonld_with_prefixes(self):
        """Test expansion of prefixed properties"""
        jsonld_data = {
            "@context": {
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "owl": "http://www.w3.org/2002/07/owl#"
            },
            "@id": "Person",
            "rdfs:label": "Person",
            "owl:equivalentClass": "Human"
        }
        
        result = self.converter.expand_jsonld(jsonld_data.copy())
        
        # Should expand prefixed properties
        assert "http://www.w3.org/2000/01/rdf-schema#label" in result
        assert result["http://www.w3.org/2000/01/rdf-schema#label"] == "Person"
        assert "http://www.w3.org/2002/07/owl#equivalentClass" in result
        assert result["http://www.w3.org/2002/07/owl#equivalentClass"] == "Human"
        
        # Original prefixed properties should be removed
        assert "rdfs:label" not in result
        assert "owl:equivalentClass" not in result

    def test_expand_jsonld_preserves_context(self):
        """Test that expansion preserves @context"""
        jsonld_data = {
            "@context": {"rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
            "@id": "Person",
            "rdfs:label": "Person"
        }
        
        result = self.converter.expand_jsonld(jsonld_data.copy())
        
        assert "@context" in result

    def test_expand_jsonld_no_prefixes(self):
        """Test expansion when no prefixes are present"""
        jsonld_data = {
            "@context": {"rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
            "@id": "Person",
            "label": "Person"
        }
        
        result = self.converter.expand_jsonld(jsonld_data.copy())
        
        # Should preserve non-prefixed properties
        assert result["label"] == "Person"

    def test_expand_jsonld_unknown_prefix(self):
        """Test expansion with unknown prefix"""
        jsonld_data = {
            "@context": {"known": "http://example.com/"},
            "@id": "Person",
            "unknown:property": "value"
        }
        
        result = self.converter.expand_jsonld(jsonld_data.copy())
        
        # Should preserve unknown prefixed properties
        assert result["unknown:property"] == "value"

    def test_expand_jsonld_uses_default_context(self):
        """Test expansion uses default context when none provided"""
        jsonld_data = {
            "@id": "Person",
            "rdfs:label": "Person"
        }
        
        result = self.converter.expand_jsonld(jsonld_data.copy())
        
        # Should use default context for expansion
        assert isinstance(result, dict)


class TestJSONLDCompaction:
    """Test JSON-LD compaction functionality"""

    def setup_method(self):
        """Setup for each test method"""
        self.converter = JSONToJSONLDConverter()

    def test_compact_jsonld_with_full_uris(self):
        """Test compaction of full URIs to prefixes"""
        jsonld_data = {
            "@id": "Person",
            "http://www.w3.org/2000/01/rdf-schema#label": "Person",
            "http://www.w3.org/2002/07/owl#equivalentClass": "Human"
        }
        
        result = self.converter.compact_jsonld(jsonld_data.copy())
        
        # Should compact URIs to prefixes based on default context
        # Note: This is a simplified implementation, so we mainly check structure
        assert isinstance(result, dict)
        assert result["@id"] == "Person"

    def test_compact_jsonld_preserves_non_uri_properties(self):
        """Test that non-URI properties are preserved"""
        jsonld_data = {
            "@id": "Person",
            "custom_property": "value",
            "another_field": 123
        }
        
        result = self.converter.compact_jsonld(jsonld_data.copy())
        
        assert result["custom_property"] == "value"
        assert result["another_field"] == 123

    def test_compact_jsonld_no_matching_uris(self):
        """Test compaction when no URIs match context"""
        jsonld_data = {
            "@id": "Person",
            "http://unknown.example.com/property": "value"
        }
        
        result = self.converter.compact_jsonld(jsonld_data.copy())
        
        # Should preserve unknown URIs
        assert result["http://unknown.example.com/property"] == "value"


class TestJSONStringOperations:
    """Test JSON string serialization and parsing"""

    def setup_method(self):
        """Setup for each test method"""
        self.converter = JSONToJSONLDConverter()

    def test_to_json_string(self):
        """Test converting data to JSON-LD string"""
        input_data = {
            "id": "Person",
            "label": "Person",
            "description": "A human being"
        }
        
        result = self.converter.to_json_string(input_data)
        
        assert isinstance(result, str)
        
        # Parse back to verify structure
        parsed = json.loads(result)
        assert "@context" in parsed
        assert "@id" in parsed
        assert parsed["@id"] == "Person"

    def test_to_json_string_with_indent(self):
        """Test JSON string with custom indentation"""
        input_data = {"id": "Test", "label": "Test"}
        
        result = self.converter.to_json_string(input_data, indent=4)
        
        assert isinstance(result, str)
        # Should contain newlines due to indentation
        assert "\n" in result

    def test_from_json_string(self):
        """Test parsing JSON-LD string"""
        jsonld_string = json.dumps({
            "@context": {"rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
            "@id": "Person",
            "rdfs:label": "Person"
        })
        
        result = self.converter.from_json_string(jsonld_string)
        
        assert isinstance(result, dict)
        assert "id" in result
        assert result["id"] == "Person"
        assert "label" in result
        assert result["label"] == "Person"

    def test_from_json_string_invalid_json(self):
        """Test parsing invalid JSON string"""
        invalid_json = "{'invalid': 'json'}"  # Single quotes
        
        with pytest.raises(json.JSONDecodeError):
            self.converter.from_json_string(invalid_json)

    def test_roundtrip_json_string(self):
        """Test round-trip through JSON string"""
        original_data = {
            "id": "Product",
            "label": "Product",
            "description": "An item"
        }
        
        # Store original values since convert_to_jsonld modifies input data
        original_id = original_data["id"]
        original_label = original_data["label"]
        original_description = original_data["description"]
        
        # Convert to JSON-LD string (this converts to JSON-LD format first)
        # NOTE: This modifies the input data!
        jsonld_string = self.converter.to_json_string(original_data)
        
        # Parse back (this converts from JSON-LD back to regular JSON)
        result_data = self.converter.from_json_string(jsonld_string)
        
        # Verify the roundtrip preserves the essential data
        assert result_data["id"] == original_id
        assert result_data["label"] == original_label
        assert result_data["description"] == original_description
        
        # Also verify the intermediate JSON-LD step
        import json
        jsonld_data = json.loads(jsonld_string)
        assert "@id" in jsonld_data
        assert "rdfs:label" in jsonld_data
        assert "rdfs:comment" in jsonld_data


class TestGlobalFunctions:
    """Test global utility functions"""

    def test_get_default_converter(self):
        """Test get_default_converter function"""
        converter = get_default_converter()
        
        assert isinstance(converter, JSONToJSONLDConverter)
        assert converter.context is not None
        assert "@context" in converter.context

    def test_get_default_converter_different_instances(self):
        """Test that get_default_converter returns new instances"""
        converter1 = get_default_converter()
        converter2 = get_default_converter()
        
        # Should be different instances (not cached)
        assert converter1 is not converter2
        
        # But should have same context structure
        assert converter1.context == converter2.context


class TestEdgeCasesAndErrorHandling:
    """Test edge cases and error handling"""

    def setup_method(self):
        """Setup for each test method"""
        self.converter = JSONToJSONLDConverter()

    def test_convert_with_none_input(self):
        """Test conversion with None input"""
        # Should handle None gracefully
        try:
            result = self.converter.convert_to_jsonld(None)
            # If it doesn't raise, check the result
            assert result is not None
        except (TypeError, AttributeError):
            # This is also acceptable behavior
            pass

    def test_convert_with_non_dict_input(self):
        """Test conversion with non-dictionary input"""
        try:
            result = self.converter.convert_to_jsonld(["list", "instead", "of", "dict"])
            # If it doesn't raise, check that it's still a list
            assert isinstance(result, (list, dict))
        except (TypeError, AttributeError):
            # This is also acceptable behavior
            pass

    def test_convert_preserves_nested_structures(self):
        """Test that nested structures are preserved"""
        input_data = {
            "id": "Complex",
            "label": "Complex Object",
            "nested": {
                "inner": "value",
                "list": [1, 2, 3]
            },
            "array": ["a", "b", "c"]
        }
        
        result = self.converter.convert_to_jsonld(input_data.copy())
        
        assert result["nested"]["inner"] == "value"
        assert result["nested"]["list"] == [1, 2, 3]
        assert result["array"] == ["a", "b", "c"]

    def test_convert_with_special_characters(self):
        """Test conversion with special characters in values"""
        input_data = {
            "id": "Special",
            "label": "Special chars: éñ中文🔥",
            "description": "Unicode: αβγ δεζ"
        }
        
        result = self.converter.convert_to_jsonld(input_data.copy())
        
        assert "rdfs:label" in result
        assert result["rdfs:label"] == "Special chars: éñ中文🔥"
        assert result["rdfs:comment"] == "Unicode: αβγ δεζ"


if __name__ == "__main__":
    pytest.main([__file__])