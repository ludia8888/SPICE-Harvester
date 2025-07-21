"""
Test constraint validator functionality
"""

import pytest
from decimal import Decimal

from shared.validators.constraint_validator import ConstraintValidator
from shared.validators.base_validator import ValidationResult
from tests.utils.assertions import assert_equal, assert_dict_contains, assert_contains


class TestConstraintValidator:
    """Test constraint validation logic"""
    
    def test_string_length_constraints(self):
        """Test string length validation"""
        # Min length
        result = ConstraintValidator.validate_constraints(
            "hello", "string", {"minLength": 3}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="string_minLength_validation",
            context={"value": "hello", "constraint": {"minLength": 3}}
        )
        
        result = ConstraintValidator.validate_constraints(
            "hi", "string", {"minLength": 3}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="string_minLength_validation",
            context={"value": "hi", "constraint": {"minLength": 3}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="too short",
            field_name="validation_message"
        )
        
        # Max length
        result = ConstraintValidator.validate_constraints(
            "hello", "string", {"maxLength": 10}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="string_maxLength_validation",
            context={"value": "hello", "constraint": {"maxLength": 10}}
        )
        
        result = ConstraintValidator.validate_constraints(
            "hello world!", "string", {"maxLength": 10}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="string_maxLength_validation",
            context={"value": "hello world!", "constraint": {"maxLength": 10}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="too long",
            field_name="validation_message"
        )
    
    def test_numeric_range_constraints(self):
        """Test numeric range validation"""
        # Minimum
        result = ConstraintValidator.validate_constraints(
            42, "integer", {"minimum": 0}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="numeric_minimum_validation",
            context={"value": 42, "constraint": {"minimum": 0}}
        )
        
        result = ConstraintValidator.validate_constraints(
            -5, "integer", {"minimum": 0}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="numeric_minimum_validation",
            context={"value": -5, "constraint": {"minimum": 0}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="too small",
            field_name="validation_message"
        )
        
        # Maximum
        result = ConstraintValidator.validate_constraints(
            50, "integer", {"maximum": 100}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="numeric_maximum_validation",
            context={"value": 50, "constraint": {"maximum": 100}}
        )
        
        result = ConstraintValidator.validate_constraints(
            150, "integer", {"maximum": 100}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="numeric_maximum_validation",
            context={"value": 150, "constraint": {"maximum": 100}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="too large",
            field_name="validation_message"
        )
        
        # Exclusive ranges
        result = ConstraintValidator.validate_constraints(
            10, "integer", {"exclusiveMinimum": 9}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="numeric_exclusiveMinimum_validation",
            context={"value": 10, "constraint": {"exclusiveMinimum": 9}}
        )
        
        result = ConstraintValidator.validate_constraints(
            10, "integer", {"exclusiveMinimum": 10}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="numeric_exclusiveMinimum_validation",
            context={"value": 10, "constraint": {"exclusiveMinimum": 10}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="greater than",
            field_name="validation_message"
        )
    
    def test_collection_constraints(self):
        """Test collection size and uniqueness"""
        # Min items
        result = ConstraintValidator.validate_constraints(
            [1, 2, 3], "array", {"minItems": 2}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="array_minItems_validation",
            context={"value": [1, 2, 3], "constraint": {"minItems": 2}}
        )
        
        result = ConstraintValidator.validate_constraints(
            [1], "array", {"minItems": 2}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="array_minItems_validation",
            context={"value": [1], "constraint": {"minItems": 2}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="too few",
            field_name="validation_message"
        )
        
        # Unique items
        result = ConstraintValidator.validate_constraints(
            [1, 2, 3], "array", {"uniqueItems": True}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="array_uniqueItems_validation",
            context={"value": [1, 2, 3], "constraint": {"uniqueItems": True}}
        )
        
        result = ConstraintValidator.validate_constraints(
            [1, 2, 2, 3], "array", {"uniqueItems": True}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="array_uniqueItems_validation",
            context={"value": [1, 2, 2, 3], "constraint": {"uniqueItems": True}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="duplicate",
            field_name="validation_message"
        )
    
    def test_format_constraints(self):
        """Test format validation"""
        # Email format
        result = ConstraintValidator.validate_constraints(
            "user@example.com", "string", {"format": "email"}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="format_email_validation",
            context={"value": "user@example.com", "constraint": {"format": "email"}}
        )
        
        result = ConstraintValidator.validate_constraints(
            "invalid-email", "string", {"format": "email"}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="format_email_validation",
            context={"value": "invalid-email", "constraint": {"format": "email"}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="email format",
            field_name="validation_message"
        )
        
        # Date format
        result = ConstraintValidator.validate_constraints(
            "2023-12-25", "string", {"format": "date"}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="format_date_validation",
            context={"value": "2023-12-25", "constraint": {"format": "date"}}
        )
        
        result = ConstraintValidator.validate_constraints(
            "25/12/2023", "string", {"format": "date"}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="format_date_validation",
            context={"value": "25/12/2023", "constraint": {"format": "date"}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="date format",
            field_name="validation_message"
        )
        
        # UUID format
        result = ConstraintValidator.validate_constraints(
            "550e8400-e29b-41d4-a716-446655440000", "string", {"format": "uuid"}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="format_uuid_validation",
            context={"value": "550e8400-e29b-41d4-a716-446655440000", "constraint": {"format": "uuid"}}
        )
        
        result = ConstraintValidator.validate_constraints(
            "not-a-uuid", "string", {"format": "uuid"}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="format_uuid_validation",
            context={"value": "not-a-uuid", "constraint": {"format": "uuid"}, "message": result.message}
        )
    
    def test_pattern_constraints(self):
        """Test regex pattern validation"""
        # Valid pattern
        result = ConstraintValidator.validate_constraints(
            "ABC123", "string", {"pattern": "^[A-Z]+[0-9]+$"}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="pattern_validation",
            context={"value": "ABC123", "constraint": {"pattern": "^[A-Z]+[0-9]+$"}}
        )
        
        result = ConstraintValidator.validate_constraints(
            "abc123", "string", {"pattern": "^[A-Z]+[0-9]+$"}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="pattern_validation",
            context={"value": "abc123", "constraint": {"pattern": "^[A-Z]+[0-9]+$"}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="pattern",
            field_name="validation_message"
        )
        
        # Invalid regex
        result = ConstraintValidator.validate_constraints(
            "test", "string", {"pattern": "["}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="invalid_pattern_validation",
            context={"value": "test", "constraint": {"pattern": "["}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="invalid regex",
            field_name="validation_message"
        )
    
    def test_custom_validator(self):
        """Test custom validation function"""
        # Simple boolean return
        def is_even(value):
            return value % 2 == 0
        
        result = ConstraintValidator.validate_constraints(
            4, "integer", {"validator": is_even}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="custom_validator_even",
            context={"value": 4, "validator": "is_even"}
        )
        
        result = ConstraintValidator.validate_constraints(
            5, "integer", {"validator": is_even}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="custom_validator_even",
            context={"value": 5, "validator": "is_even", "message": result.message}
        )
        
        # Tuple return with message
        def validate_range(value):
            if 0 <= value <= 100:
                return True, "Valid range"
            return False, f"Value {value} is out of range [0, 100]"
        
        result = ConstraintValidator.validate_constraints(
            50, "integer", {"validator": validate_range}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="custom_validator_range",
            context={"value": 50, "validator": "validate_range"}
        )
        
        result = ConstraintValidator.validate_constraints(
            150, "integer", {"validator": validate_range}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="custom_validator_range",
            context={"value": 150, "validator": "validate_range", "message": result.message}
        )
        assert_contains(
            text=result.message,
            substring="out of range",
            field_name="validation_message"
        )
    
    def test_multiple_constraints(self):
        """Test multiple constraints together"""
        constraints = {
            "minLength": 5,
            "maxLength": 10,
            "pattern": "^[a-z]+$",
            "lowercase": True
        }
        
        # Valid value
        result = ConstraintValidator.validate_constraints(
            "hello", "string", constraints
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="multiple_constraints_valid",
            context={"value": "hello", "constraints": constraints}
        )
        
        # Too short
        result = ConstraintValidator.validate_constraints(
            "hi", "string", constraints
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="multiple_constraints_too_short",
            context={"value": "hi", "constraints": constraints, "message": result.message}
        )
        
        # Too long
        result = ConstraintValidator.validate_constraints(
            "helloworld!", "string", constraints
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="multiple_constraints_too_long",
            context={"value": "helloworld!", "constraints": constraints, "message": result.message}
        )
        
        # Wrong case
        result = ConstraintValidator.validate_constraints(
            "HELLO", "string", constraints
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="multiple_constraints_wrong_case",
            context={"value": "HELLO", "constraints": constraints, "message": result.message}
        )
        
        # Invalid pattern
        result = ConstraintValidator.validate_constraints(
            "hello123", "string", constraints
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="multiple_constraints_invalid_pattern",
            context={"value": "hello123", "constraints": constraints, "message": result.message}
        )
    
    def test_constraint_compatibility(self):
        """Test constraint compatibility checking"""
        # Compatible constraints
        result = ConstraintValidator.validate_constraint_compatibility({
            "minLength": 5,
            "maxLength": 10
        })
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="constraint_compatibility_valid",
            context={"constraints": {"minLength": 5, "maxLength": 10}}
        )
        
        # Incompatible string length
        result = ConstraintValidator.validate_constraint_compatibility({
            "minLength": 10,
            "maxLength": 5
        })
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="constraint_compatibility_string_length",
            context={"constraints": {"minLength": 10, "maxLength": 5}, "message": result.message}
        )
        assert_contains(
            text=result.message,
            substring="minLength",
            field_name="validation_message"
        )
        assert_contains(
            text=result.message,
            substring="maxLength",
            field_name="validation_message"
        )
        
        # Incompatible numeric range
        result = ConstraintValidator.validate_constraint_compatibility({
            "minimum": 100,
            "maximum": 50
        })
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="constraint_compatibility_numeric_range",
            context={"constraints": {"minimum": 100, "maximum": 50}, "message": result.message}
        )
        assert_contains(
            text=result.message,
            substring="minimum",
            field_name="validation_message"
        )
        assert_contains(
            text=result.message,
            substring="maximum",
            field_name="validation_message"
        )
    
    def test_merge_constraints(self):
        """Test constraint merging"""
        base_constraints = {
            "minLength": 5,
            "maxLength": 20,
            "pattern": "^[a-z]+$"
        }
        
        override_constraints = {
            "maxLength": 15,  # Override
            "lowercase": True  # Add new
        }
        
        merged = ConstraintValidator.merge_constraints(
            base_constraints, override_constraints
        )
        
        assert_equal(
            actual=merged["minLength"],
            expected=5,
            field_name="merged_constraints.minLength",
            context={"description": "should be unchanged"}
        )
        assert_equal(
            actual=merged["maxLength"],
            expected=15,
            field_name="merged_constraints.maxLength",
            context={"description": "should be overridden"}
        )
        assert_equal(
            actual=merged["pattern"],
            expected="^[a-z]+$",
            field_name="merged_constraints.pattern",
            context={"description": "should be unchanged"}
        )
        assert_equal(
            actual=merged["lowercase"],
            expected=True,
            field_name="merged_constraints.lowercase",
            context={"description": "should be added"}
        )
    
    def test_precision_constraints(self):
        """Test decimal precision validation"""
        # Valid precision
        result = ConstraintValidator.validate_constraints(
            123.45, "decimal", {"precision": 2}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="precision_validation_valid",
            context={"value": 123.45, "constraint": {"precision": 2}}
        )
        
        # Too many decimal places
        result = ConstraintValidator.validate_constraints(
            123.456, "decimal", {"precision": 2}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="precision_validation_too_many_places",
            context={"value": 123.456, "constraint": {"precision": 2}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="decimal places",
            field_name="validation_message"
        )
        
        # Works with Decimal type
        result = ConstraintValidator.validate_constraints(
            Decimal("123.45"), "decimal", {"precision": 2}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="precision_validation_decimal_type",
            context={"value": "Decimal('123.45')", "constraint": {"precision": 2}}
        )
    
    def test_multiple_of_constraint(self):
        """Test multiple of validation"""
        # Valid multiple
        result = ConstraintValidator.validate_constraints(
            15, "integer", {"multipleOf": 5}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="multipleOf_validation_valid",
            context={"value": 15, "constraint": {"multipleOf": 5}}
        )
        
        # Not a multiple
        result = ConstraintValidator.validate_constraints(
            17, "integer", {"multipleOf": 5}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="multipleOf_validation_invalid",
            context={"value": 17, "constraint": {"multipleOf": 5}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="multiple of",
            field_name="validation_message"
        )
    
    def test_contains_constraints(self):
        """Test collection contains validation"""
        # Contains
        result = ConstraintValidator.validate_constraints(
            ["apple", "banana", "orange"], "array", {"contains": "banana"}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="contains_validation_valid",
            context={"value": ["apple", "banana", "orange"], "constraint": {"contains": "banana"}}
        )
        
        result = ConstraintValidator.validate_constraints(
            ["apple", "orange"], "array", {"contains": "banana"}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="contains_validation_missing",
            context={"value": ["apple", "orange"], "constraint": {"contains": "banana"}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="must contain",
            field_name="validation_message"
        )
        
        # Not contains
        result = ConstraintValidator.validate_constraints(
            ["apple", "orange"], "array", {"notContains": "banana"}
        )
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="notContains_validation_valid",
            context={"value": ["apple", "orange"], "constraint": {"notContains": "banana"}}
        )
        
        result = ConstraintValidator.validate_constraints(
            ["apple", "banana", "orange"], "array", {"notContains": "banana"}
        )
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="notContains_validation_invalid",
            context={"value": ["apple", "banana", "orange"], "constraint": {"notContains": "banana"}, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="must not contain",
            field_name="validation_message"
        )


    def test_allowed_chars_constraint(self):
        """Test allowedChars constraint edge cases"""
        # Test with invalid characters (line 91-94)
        constraints = {"allowedChars": "abc"}
        result = ConstraintValidator.validate_constraints("abcxyz", "string", constraints)
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="allowedChars_validation_with_invalid_chars",
            context={"value": "abcxyz", "constraint": constraints, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="invalid characters found",
            field_name="validation_message"
        )

    def test_case_constraints_edge_cases(self):
        """Test uppercase/lowercase constraint edge cases"""
        # Test uppercase constraint (line 100)
        constraints = {"uppercase": False}  # Test falsy condition
        result = ConstraintValidator.validate_constraints("Hello", "string", constraints)
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="uppercase_constraint_false",
            context={"value": "Hello", "constraint": constraints}
        )

    def test_exclusive_maximum_edge_case(self):
        """Test exclusiveMaximum edge case (line 133)"""
        constraints = {"exclusiveMaximum": 10}
        result = ConstraintValidator.validate_constraints(10, "number", constraints)
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="exclusiveMaximum_equal_value",
            context={"value": 10, "constraint": constraints, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="must be less than",
            field_name="validation_message"
        )

    def test_collection_constraints_edge_cases(self):
        """Test collection constraint edge cases"""
        # Test maxItems with exact limit (line 176)
        constraints = {"maxItems": 3}
        result = ConstraintValidator.validate_constraints([1, 2, 3, 4], "array", constraints)
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="maxItems_over_limit",
            context={"value": [1, 2, 3, 4], "constraint": constraints, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="too many items",
            field_name="validation_message"
        )

    def test_format_non_string_input(self):
        """Test format validation with non-string input (line 212)"""
        constraints = {"format": "email"}
        result = ConstraintValidator.validate_constraints(123, "number", constraints)
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="format_validation_non_string",
            context={"value": 123, "constraint": constraints, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="format validation requires string",
            field_name="validation_message"
        )

    def test_unknown_format(self):
        """Test unknown format constraint (line 238)"""
        constraints = {"format": "nonexistent-format"}
        result = ConstraintValidator.validate_constraints("test", "string", constraints)
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="unknown_format_validation",
            context={"value": "test", "constraint": constraints, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="unknown format",
            field_name="validation_message"
        )

    def test_custom_validator_edge_cases(self):
        """Test custom validator edge cases (lines 272-279)"""
        # Test ValidationResult return type
        def validation_result_validator(value):
            from shared.validators.base_validator import ValidationResult
            return ValidationResult(is_valid=True, message="Custom ValidationResult")
        
        constraints = {"validator": validation_result_validator}
        result = ConstraintValidator.validate_constraints("test", "string", constraints)
        assert_equal(
            actual=result.is_valid,
            expected=True,
            field_name="custom_validator_validation_result",
            context={"value": "test"}
        )
        assert_equal(
            actual=result.message,
            expected="All constraints validated",
            field_name="custom_validator_validation_result_message",
            context={"value": "test", "note": "Main method overrides custom validation message"}
        )

        # Test invalid return type
        def invalid_return_validator(value):
            return "invalid return"
        
        constraints = {"validator": invalid_return_validator}
        result = ConstraintValidator.validate_constraints("test", "string", constraints)
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="custom_validator_invalid_return",
            context={"value": "test", "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="invalid return from custom validator",
            field_name="validation_message"
        )

        # Test exception handling
        def exception_validator(value):
            raise ValueError("Test exception")
        
        constraints = {"validator": exception_validator}
        result = ConstraintValidator.validate_constraints("test", "string", constraints)
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="custom_validator_exception",
            context={"value": "test", "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="custom validation error",
            field_name="validation_message"
        )

    def test_constraint_compatibility_collection_mixed_names(self):
        """Test constraint compatibility with mixed collection constraint names (lines 323-327)"""
        # Test mixed minItems and maxLength
        constraints = {"minItems": 5, "maxLength": 2}
        result = ConstraintValidator.validate_constraint_compatibility(constraints)
        assert_equal(
            actual=result.is_valid,
            expected=False,
            field_name="constraint_compatibility_mixed_collection_names",
            context={"constraints": constraints, "message": result.message}
        )
        assert_contains(
            text=result.message.lower(),
            substring="incompatible constraints",
            field_name="validation_message"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])