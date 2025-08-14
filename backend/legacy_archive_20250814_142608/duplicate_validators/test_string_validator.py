"""
Comprehensive unit tests for StringValidator
Tests all string validation functionality with REAL behavior verification
NO MOCKS - Tests verify actual code behavior
"""

import pytest
from typing import Dict, Any, Optional

from shared.validators.string_validator import StringValidator
from shared.models.common import DataType


class TestStringValidator:
    """Test StringValidator with real behavior verification"""

    def setup_method(self):
        """Setup for each test"""
        self.validator = StringValidator()

    def test_validate_basic_string(self):
        """Test validation of basic string"""
        result = self.validator.validate("hello world")
        assert result.is_valid is True
        assert result.message == "String validation passed"
        assert result.normalized_value == "hello world"
        assert result.metadata["type"] == "string"
        assert result.metadata["length"] == 11

    def test_validate_non_string_types(self):
        """Test validation fails for non-string types"""
        # Number
        result = self.validator.validate(123)
        assert result.is_valid is False
        assert "Expected string, got int" in result.message

        # List
        result = self.validator.validate([1, 2, 3])
        assert result.is_valid is False
        assert "Expected string, got list" in result.message

        # None
        result = self.validator.validate(None)
        assert result.is_valid is False
        assert "Expected string, got NoneType" in result.message

        # Dict
        result = self.validator.validate({"key": "value"})
        assert result.is_valid is False
        assert "Expected string, got dict" in result.message

    def test_validate_format_alphanumeric(self):
        """Test validation with alphanumeric format"""
        constraints = {"format": "alphanumeric"}
        
        # Valid alphanumeric (ConstraintValidator: strict, no spaces)
        result = self.validator.validate("Hello123", constraints)
        assert result.is_valid is True

        # Invalid - spaces not allowed in ConstraintValidator
        result = self.validator.validate("Hello 123", constraints)
        assert result.is_valid is False
        assert "Invalid alphanumeric format" in result.message

        # Invalid - special characters
        result = self.validator.validate("Hello@123", constraints)
        assert result.is_valid is False
        assert "Invalid alphanumeric format" in result.message

        # Invalid - punctuation
        result = self.validator.validate("Hello.123", constraints)
        assert result.is_valid is False
        assert "Invalid alphanumeric format" in result.message

    def test_validate_format_alpha(self):
        """Test validation with alpha format"""
        constraints = {"format": "alpha"}
        
        # Valid alpha (ConstraintValidator: strict, no spaces)
        result = self.validator.validate("HelloWorld", constraints)
        assert result.is_valid is True

        # Invalid - spaces not allowed in ConstraintValidator
        result = self.validator.validate("Hello World", constraints)
        assert result.is_valid is False
        assert "Invalid alpha format" in result.message

        # Invalid - contains numbers
        result = self.validator.validate("Hello123", constraints)
        assert result.is_valid is False
        assert "Invalid alpha format" in result.message

        # Invalid - special characters
        result = self.validator.validate("Hello!", constraints)
        assert result.is_valid is False
        assert "Invalid alpha format" in result.message

    def test_validate_format_numeric(self):
        """Test validation with numeric format"""
        constraints = {"format": "numeric"}
        
        # Valid numeric (ConstraintValidator: strict, no spaces)
        result = self.validator.validate("12345", constraints)
        assert result.is_valid is True

        # Invalid - contains letters
        result = self.validator.validate("123abc", constraints)
        assert result.is_valid is False
        assert "Invalid numeric format" in result.message

        # Invalid - contains spaces (ConstraintValidator doesn't allow spaces)
        result = self.validator.validate("123 456", constraints)
        assert result.is_valid is False
        assert "Invalid numeric format" in result.message

        # Invalid - decimal point
        result = self.validator.validate("123.45", constraints)
        assert result.is_valid is False
        assert "Invalid numeric format" in result.message

    def test_validate_format_lowercase(self):
        """Test validation with lowercase format"""
        constraints = {"format": "lowercase"}
        
        # Invalid - ConstraintValidator doesn't support lowercase format
        result = self.validator.validate("hello world", constraints)
        assert result.is_valid is False
        assert "Unknown format: lowercase" in result.message

        # Invalid - ConstraintValidator doesn't support lowercase format
        result = self.validator.validate("Hello World", constraints)
        assert result.is_valid is False
        assert "Unknown format: lowercase" in result.message

        # Invalid - ConstraintValidator doesn't support lowercase format
        result = self.validator.validate("hello123!", constraints)
        assert result.is_valid is False
        assert "Unknown format: lowercase" in result.message

    def test_validate_format_uppercase(self):
        """Test validation with uppercase format"""
        constraints = {"format": "uppercase"}
        
        # Invalid - ConstraintValidator doesn't support uppercase format
        result = self.validator.validate("HELLO WORLD", constraints)
        assert result.is_valid is False
        assert "Unknown format: uppercase" in result.message

        # Invalid - ConstraintValidator doesn't support uppercase format
        result = self.validator.validate("Hello World", constraints)
        assert result.is_valid is False
        assert "Unknown format: uppercase" in result.message

        # Invalid - ConstraintValidator doesn't support uppercase format
        result = self.validator.validate("HELLO123!", constraints)
        assert result.is_valid is False
        assert "Unknown format: uppercase" in result.message

    def test_validate_trim_constraint(self):
        """Test validation with trim constraint"""
        constraints = {"trim": True}
        
        # String with leading/trailing spaces
        result = self.validator.validate("  hello world  ", constraints)
        assert result.is_valid is True
        assert result.normalized_value == "hello world"
        assert result.metadata["length"] == 11  # Length after trimming

        # String without spaces
        result = self.validator.validate("hello", constraints)
        assert result.is_valid is True
        assert result.normalized_value == "hello"

    def test_validate_blacklist_constraint(self):
        """Test validation with blacklist constraint"""
        constraints = {"blacklist": ["bad", "forbidden", "banned"]}
        
        # Not in blacklist
        result = self.validator.validate("good", constraints)
        assert result.is_valid is True

        # In blacklist
        result = self.validator.validate("bad", constraints)
        assert result.is_valid is False
        assert "String is blacklisted: bad" in result.message

        result = self.validator.validate("forbidden", constraints)
        assert result.is_valid is False
        assert "String is blacklisted: forbidden" in result.message

    def test_validate_whitelist_constraint(self):
        """Test validation with whitelist constraint"""
        constraints = {"whitelist": ["allowed", "permitted", "valid"]}
        
        # In whitelist
        result = self.validator.validate("allowed", constraints)
        assert result.is_valid is True

        result = self.validator.validate("valid", constraints)
        assert result.is_valid is True

        # Not in whitelist
        result = self.validator.validate("invalid", constraints)
        assert result.is_valid is False
        assert "String must be one of: ['allowed', 'permitted', 'valid']" in result.message

    def test_validate_transformation_lowercase(self):
        """Test validation with toLowerCase transformation"""
        constraints = {"toLowerCase": True}
        
        result = self.validator.validate("Hello WORLD", constraints)
        assert result.is_valid is True
        assert result.normalized_value == "hello world"

        # Already lowercase
        result = self.validator.validate("hello", constraints)
        assert result.is_valid is True
        assert result.normalized_value == "hello"

    def test_validate_transformation_uppercase(self):
        """Test validation with toUpperCase transformation"""
        constraints = {"toUpperCase": True}
        
        result = self.validator.validate("Hello world", constraints)
        assert result.is_valid is True
        assert result.normalized_value == "HELLO WORLD"

        # Already uppercase
        result = self.validator.validate("HELLO", constraints)
        assert result.is_valid is True
        assert result.normalized_value == "HELLO"

    def test_validate_combined_constraints(self):
        """Test validation with multiple constraints"""
        # Trim and lowercase
        constraints = {"trim": True, "toLowerCase": True}
        result = self.validator.validate("  HELLO World  ", constraints)
        assert result.is_valid is True
        assert result.normalized_value == "hello world"

        # Format and whitelist
        constraints = {"format": "alpha", "whitelist": ["hello", "world"]}
        result = self.validator.validate("hello", constraints)
        assert result.is_valid is True

        result = self.validator.validate("hello123", constraints)
        assert result.is_valid is False  # Fails ConstraintValidator format check first
        assert "Invalid alpha format" in result.message

        # Lowercase transformation takes precedence over uppercase (elif logic)
        constraints = {"toLowerCase": True, "toUpperCase": True}
        result = self.validator.validate("Hello", constraints)
        assert result.is_valid is True
        assert result.normalized_value == "hello"  # toLowerCase wins due to elif

    def test_validate_with_constraint_validator_integration(self):
        """Test that ConstraintValidator integration works"""
        # These constraints should be handled by ConstraintValidator
        constraints = {
            "minLength": 5,
            "maxLength": 10,
            "pattern": "^[A-Z].*"
        }
        
        # Valid according to all constraints
        result = self.validator.validate("Hello", constraints)
        assert result.is_valid is True

        # Too short
        result = self.validator.validate("Hi", constraints)
        assert result.is_valid is False

        # Too long
        result = self.validator.validate("Hello World!", constraints)
        assert result.is_valid is False

        # Doesn't match pattern (doesn't start with uppercase)
        result = self.validator.validate("hello", constraints)
        assert result.is_valid is False

    def test_normalize_method(self):
        """Test normalize method"""
        # String with spaces
        normalized = self.validator.normalize("  hello world  ")
        assert normalized == "hello world"

        # String without spaces
        normalized = self.validator.normalize("hello")
        assert normalized == "hello"

        # Non-string returns as-is
        assert self.validator.normalize(123) == 123
        assert self.validator.normalize(None) is None
        assert self.validator.normalize([1, 2, 3]) == [1, 2, 3]

    def test_get_supported_types(self):
        """Test get_supported_types method"""
        supported_types = self.validator.get_supported_types()
        assert "string" in supported_types
        assert "text" in supported_types
        assert "str" in supported_types
        assert len(supported_types) == 3

    def test_edge_case_empty_string(self):
        """Test validation of empty string"""
        result = self.validator.validate("")
        assert result.is_valid is True
        assert result.normalized_value == ""
        assert result.metadata["length"] == 0

    def test_edge_case_whitespace_only(self):
        """Test validation of whitespace-only string"""
        # Without trim
        result = self.validator.validate("   ")
        assert result.is_valid is True
        assert result.normalized_value == "   "
        assert result.metadata["length"] == 3

        # With trim
        constraints = {"trim": True}
        result = self.validator.validate("   ", constraints)
        assert result.is_valid is True
        assert result.normalized_value == ""
        assert result.metadata["length"] == 0

    def test_edge_case_special_characters(self):
        """Test validation of strings with special characters"""
        special_strings = [
            "hello\nworld",  # Newline
            "hello\tworld",  # Tab
            "hello\\world",  # Backslash
            "hello\"world",  # Quote
            "hello'world",   # Single quote
            "héllo wörld",   # Unicode
            "🌍🌎🌏",        # Emoji
        ]
        
        for s in special_strings:
            result = self.validator.validate(s)
            assert result.is_valid is True
            assert result.normalized_value == s

    def test_format_with_empty_string(self):
        """Test format validation with empty string"""
        # Empty string fails all ConstraintValidator format checks
        constraints = {"format": "alphanumeric"}
        result = self.validator.validate("", constraints)
        assert result.is_valid is False
        assert "Invalid alphanumeric format" in result.message

        constraints = {"format": "alpha"}
        result = self.validator.validate("", constraints)
        assert result.is_valid is False
        assert "Invalid alpha format" in result.message

        # But not numeric (ConstraintValidator rejects empty string)
        constraints = {"format": "numeric"}
        result = self.validator.validate("", constraints)
        assert result.is_valid is False
        assert "Invalid numeric format" in result.message

    def test_blacklist_whitelist_interaction(self):
        """Test when both blacklist and whitelist are present"""
        # If both are present, both should be checked
        constraints = {
            "whitelist": ["good", "bad", "ugly"],
            "blacklist": ["bad"]
        }
        
        # In whitelist but not blacklist
        result = self.validator.validate("good", constraints)
        assert result.is_valid is True

        # In both whitelist and blacklist (blacklist takes precedence)
        result = self.validator.validate("bad", constraints)
        assert result.is_valid is False
        assert "String is blacklisted" in result.message

        # Not in whitelist
        result = self.validator.validate("other", constraints)
        assert result.is_valid is False
        assert "String must be one of" in result.message

    def test_trim_applied_before_other_checks(self):
        """Test that trim is applied before other validations"""
        # Trim should happen before blacklist check
        constraints = {"trim": True, "blacklist": ["hello"]}
        result = self.validator.validate("  hello  ", constraints)
        assert result.is_valid is False  # Should fail because trimmed value is "hello"

        # Trim should happen before whitelist check
        constraints = {"trim": True, "whitelist": ["hello"]}
        result = self.validator.validate("  hello  ", constraints)
        assert result.is_valid is True  # Should pass because trimmed value is "hello"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])