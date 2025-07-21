"""
Comprehensive unit tests for ObjectValidator
Tests all object/JSON validation functionality with REAL behavior verification
NO MOCKS - Tests verify actual code behavior
"""

import json
import pytest
from typing import Dict, Any, Optional

from shared.validators.object_validator import ObjectValidator
from shared.models.common import DataType


class TestObjectValidator:
    """Test ObjectValidator with real behavior verification"""

    def setup_method(self):
        """Setup for each test"""
        self.validator = ObjectValidator()

    def test_validate_basic_dict(self):
        """Test validation of basic dictionary"""
        result = self.validator.validate({"name": "test", "value": 123})
        assert result.is_valid is True
        assert result.message == "Object validation passed"
        assert result.normalized_value == {"name": "test", "value": 123}
        assert result.metadata["type"] == "object"
        assert result.metadata["field_count"] == 2

    def test_validate_json_string(self):
        """Test validation of JSON string"""
        json_str = '{"name": "test", "value": 123}'
        result = self.validator.validate(json_str)
        assert result.is_valid is True
        assert result.normalized_value == {"name": "test", "value": 123}

    def test_validate_invalid_json_string(self):
        """Test validation of invalid JSON string"""
        result = self.validator.validate("{invalid json}")
        assert result.is_valid is False
        assert result.message == "Invalid JSON object format"

    def test_validate_non_dict_type(self):
        """Test validation fails for non-dict types"""
        # Test with list
        result = self.validator.validate([1, 2, 3])
        assert result.is_valid is False
        assert "Expected object/dict, got list" in result.message

        # Test with string that's not JSON
        result = self.validator.validate("not json")
        assert result.is_valid is False
        assert result.message == "Invalid JSON object format"

        # Test with number
        result = self.validator.validate(123)
        assert result.is_valid is False
        assert "Expected object/dict, got int" in result.message

    def test_validate_required_fields(self):
        """Test validation with required fields constraint"""
        constraints = {
            "required": ["name", "email"]
        }
        
        # Test with all required fields present
        result = self.validator.validate(
            {"name": "John", "email": "john@example.com", "age": 25},
            constraints
        )
        assert result.is_valid is True

        # Test with missing required field
        result = self.validator.validate(
            {"name": "John", "age": 25},
            constraints
        )
        assert result.is_valid is False
        assert "Required field 'email' is missing" in result.message

        # Test with empty object
        result = self.validator.validate({}, constraints)
        assert result.is_valid is False
        assert "Required field 'name' is missing" in result.message

    def test_validate_additional_properties_not_allowed(self):
        """Test validation when additional properties are not allowed"""
        constraints = {
            "additionalProperties": False,
            "schema": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            }
        }
        
        # Test with only allowed fields
        result = self.validator.validate(
            {"name": "John", "age": 25},
            constraints
        )
        assert result.is_valid is True

        # Test with additional field
        result = self.validator.validate(
            {"name": "John", "age": 25, "email": "john@example.com"},
            constraints
        )
        assert result.is_valid is False
        assert "Additional properties not allowed: ['email']" in result.message

    def test_validate_schema_field_types(self):
        """Test validation with schema field type constraints"""
        constraints = {
            "schema": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "salary": {"type": "float"},
                "is_active": {"type": "boolean"}
            }
        }
        
        # Test with correct types
        result = self.validator.validate(
            {
                "name": "John",
                "age": 25,
                "salary": 50000.50,
                "is_active": True
            },
            constraints
        )
        assert result.is_valid is True

        # Test with wrong type for string field
        result = self.validator.validate(
            {"name": 123, "age": 25},
            constraints
        )
        assert result.is_valid is False
        assert "Field 'name' validation failed" in result.message

        # Test with wrong type for integer field
        result = self.validator.validate(
            {"name": "John", "age": "25"},
            constraints
        )
        assert result.is_valid is False
        assert "Field 'age' validation failed" in result.message

        # Test with wrong type for boolean field
        result = self.validator.validate(
            {"name": "John", "is_active": "yes"},
            constraints
        )
        assert result.is_valid is False
        assert "Field 'is_active' validation failed" in result.message

    def test_validate_nested_constraints(self):
        """Test validation with nested field constraints"""
        constraints = {
            "schema": {
                "email": {
                    "type": "email",
                    "constraints": {"required": True}
                },
                "age": {
                    "type": "integer",
                    "constraints": {"min": 18, "max": 100}
                }
            }
        }
        
        # Note: This test depends on whether email validator is available
        # For now, test basic field validation
        result = self.validator.validate(
            {"age": 25},
            constraints
        )
        # Should pass as fields in schema are not required unless specified
        assert result.is_valid is True

    def test_validate_empty_object(self):
        """Test validation of empty object"""
        result = self.validator.validate({})
        assert result.is_valid is True
        assert result.metadata["field_count"] == 0

    def test_validate_with_no_constraints(self):
        """Test validation with no constraints"""
        result = self.validator.validate(
            {"any": "value", "number": 123, "nested": {"key": "value"}}
        )
        assert result.is_valid is True
        assert result.normalized_value["any"] == "value"
        assert result.normalized_value["number"] == 123
        assert result.normalized_value["nested"] == {"key": "value"}

    def test_normalize_json_string(self):
        """Test normalize method with JSON string"""
        json_str = '{"key": "value"}'
        normalized = self.validator.normalize(json_str)
        assert normalized == {"key": "value"}

    def test_normalize_dict(self):
        """Test normalize method with dictionary"""
        original = {"key": "value"}
        normalized = self.validator.normalize(original)
        assert normalized == {"key": "value"}
        assert normalized is not original  # Should be a copy

    def test_normalize_invalid_json(self):
        """Test normalize method with invalid JSON string"""
        normalized = self.validator.normalize("{invalid}")
        assert normalized == "{invalid}"  # Returns original on error

    def test_normalize_non_dict(self):
        """Test normalize method with non-dict value"""
        assert self.validator.normalize(123) == 123
        assert self.validator.normalize([1, 2, 3]) == [1, 2, 3]
        assert self.validator.normalize(None) is None

    def test_get_supported_types(self):
        """Test get_supported_types method"""
        supported_types = self.validator.get_supported_types()
        assert DataType.OBJECT.value in supported_types
        assert "object" in supported_types
        assert "json" in supported_types
        assert "dict" in supported_types
        assert len(supported_types) == 4

    def test_validate_field_type_fallback(self):
        """Test field type validation fallback for basic types"""
        # Test various numeric type names
        constraints = {
            "schema": {
                "float_field": {"type": "float"},
                "double_field": {"type": "double"},
                "number_field": {"type": "number"}
            }
        }
        
        result = self.validator.validate(
            {
                "float_field": 1.5,
                "double_field": 2.5,
                "number_field": 3.5
            },
            constraints
        )
        assert result.is_valid is True

        # Test integer accepted as float
        result = self.validator.validate(
            {"float_field": 1},
            constraints
        )
        assert result.is_valid is True

        # Test boolean not accepted as number
        result = self.validator.validate(
            {"float_field": True},
            constraints
        )
        assert result.is_valid is False
        assert "Field 'float_field' validation failed" in result.message

    def test_validate_complex_object(self):
        """Test validation of complex nested object"""
        complex_obj = {
            "user": {
                "name": "John Doe",
                "age": 30,
                "contacts": {
                    "email": "john@example.com",
                    "phone": "+1234567890"
                }
            },
            "settings": {
                "notifications": True,
                "theme": "dark"
            },
            "data": [1, 2, 3]
        }
        
        result = self.validator.validate(complex_obj)
        assert result.is_valid is True
        assert result.metadata["field_count"] == 3

    def test_validate_json_string_with_special_chars(self):
        """Test validation of JSON string with special characters"""
        json_str = '{"message": "Hello\\nWorld", "path": "C:\\\\Users\\\\test"}'
        result = self.validator.validate(json_str)
        assert result.is_valid is True
        assert result.normalized_value["message"] == "Hello\nWorld"
        assert result.normalized_value["path"] == "C:\\Users\\test"

    def test_validate_required_and_schema_combined(self):
        """Test validation with both required fields and schema"""
        constraints = {
            "required": ["name", "type"],
            "schema": {
                "name": {"type": "string"},
                "type": {"type": "string"},
                "count": {"type": "integer"}
            }
        }
        
        # Test with valid data
        result = self.validator.validate(
            {"name": "test", "type": "example", "count": 5},
            constraints
        )
        assert result.is_valid is True

        # Test missing required field
        result = self.validator.validate(
            {"name": "test", "count": 5},
            constraints
        )
        assert result.is_valid is False
        assert "Required field 'type' is missing" in result.message

        # Test wrong type for optional field
        result = self.validator.validate(
            {"name": "test", "type": "example", "count": "five"},
            constraints
        )
        assert result.is_valid is False
        assert "Field 'count' validation failed" in result.message

    def test_validate_field_normalization(self):
        """Test that field values are normalized when validators provide normalized values"""
        # This would require email/url validators to be available
        # For now, test that normalized values are preserved
        constraints = {
            "schema": {
                "value": {"type": "string"}
            }
        }
        
        result = self.validator.validate(
            {"value": "  test  "},  # Spaces that might be normalized
            constraints
        )
        assert result.is_valid is True
        # Without string validator, value should be unchanged
        assert result.normalized_value["value"] == "  test  "

    def test_edge_case_null_values(self):
        """Test handling of null values in object"""
        result = self.validator.validate({"key": None})
        assert result.is_valid is True
        assert result.normalized_value["key"] is None

    def test_edge_case_numeric_string_keys(self):
        """Test object with numeric string keys"""
        result = self.validator.validate({"123": "value", "456": "another"})
        assert result.is_valid is True
        assert result.normalized_value["123"] == "value"

    def test_edge_case_empty_string_key(self):
        """Test object with empty string key"""
        result = self.validator.validate({"": "value"})
        assert result.is_valid is True
        assert result.normalized_value[""] == "value"

    def test_boolean_type_strict_validation(self):
        """Test that boolean validation is strict (not accepting 1/0)"""
        constraints = {
            "schema": {
                "flag": {"type": "bool"}
            }
        }
        
        # True boolean should pass
        result = self.validator.validate({"flag": True}, constraints)
        assert result.is_valid is True

        # Integer should fail
        result = self.validator.validate({"flag": 1}, constraints)
        assert result.is_valid is False
        assert "Expected boolean" in result.message

    def test_integer_type_strict_validation(self):
        """Test that integer validation doesn't accept boolean"""
        constraints = {
            "schema": {
                "count": {"type": "int"}
            }
        }
        
        # Integer should pass
        result = self.validator.validate({"count": 42}, constraints)
        assert result.is_valid is True

        # Boolean should fail (even though True == 1 in Python)
        result = self.validator.validate({"count": True}, constraints)
        assert result.is_valid is False
        assert "Expected integer" in result.message


if __name__ == "__main__":
    pytest.main([__file__, "-v"])