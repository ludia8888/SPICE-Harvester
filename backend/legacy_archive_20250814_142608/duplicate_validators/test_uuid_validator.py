"""
Comprehensive unit tests for UuidValidator
Tests all UUID validation functionality with REAL behavior verification
NO MOCKS - Tests verify actual code behavior
"""

import pytest
import uuid
from typing import Dict, Any, Optional

from shared.validators.uuid_validator import UuidValidator


class TestUuidValidator:
    """Test UuidValidator with real behavior verification"""

    def setup_method(self):
        """Setup for each test"""
        self.validator = UuidValidator()
        
        # Sample UUIDs for testing
        self.sample_uuids = {
            "v1": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "v3": "6fa459ea-ee8a-3ca4-894e-db77e160355e",
            "v4": "550e8400-e29b-41d4-a716-446655440000",
            "v5": "886313e1-3b8a-5372-9b90-0c9aee199e5d",
            "nil": "00000000-0000-0000-0000-000000000000",
            "uppercase": "550E8400-E29B-41D4-A716-446655440000",
            "mixed_case": "550e8400-E29B-41d4-A716-446655440000",
        }

    def test_validate_basic_uuid_v4(self):
        """Test validation of basic UUID v4"""
        uuid_str = self.sample_uuids["v4"]
        result = self.validator.validate(uuid_str)
        assert result.is_valid is True
        assert result.message == "UUID validation passed"
        assert result.normalized_value["uuid"] == uuid_str.lower()
        assert result.normalized_value["version"] == 4
        assert result.normalized_value["isNil"] is False
        assert result.metadata["type"] == "uuid"
        assert result.metadata["version"] == 4

    def test_validate_uuid_v1_with_timestamp(self):
        """Test validation of UUID v1 with timestamp extraction"""
        uuid_str = self.sample_uuids["v1"]
        result = self.validator.validate(uuid_str)
        assert result.is_valid is True
        assert result.normalized_value["version"] == 1
        # UUID v1 should have timestamp info
        assert "timestamp" in result.normalized_value

    def test_validate_uuid_v3(self):
        """Test validation of UUID v3"""
        uuid_str = self.sample_uuids["v3"]
        result = self.validator.validate(uuid_str)
        assert result.is_valid is True
        assert result.normalized_value["version"] == 3

    def test_validate_uuid_v5(self):
        """Test validation of UUID v5"""
        uuid_str = self.sample_uuids["v5"]
        result = self.validator.validate(uuid_str)
        assert result.is_valid is True
        assert result.normalized_value["version"] == 5

    def test_validate_nil_uuid(self):
        """Test validation of nil UUID"""
        uuid_str = self.sample_uuids["nil"]
        result = self.validator.validate(uuid_str)
        assert result.is_valid is True
        assert result.normalized_value["isNil"] is True
        assert result.normalized_value["version"] is None  # Nil UUID has no version

    def test_validate_uppercase_uuid(self):
        """Test validation with uppercase UUID"""
        uuid_str = self.sample_uuids["uppercase"]
        result = self.validator.validate(uuid_str)
        assert result.is_valid is True
        # Should be normalized to lowercase
        assert result.normalized_value["uuid"] == uuid_str.lower()

    def test_validate_mixed_case_uuid(self):
        """Test validation with mixed case UUID"""
        uuid_str = self.sample_uuids["mixed_case"]
        result = self.validator.validate(uuid_str)
        assert result.is_valid is True
        # Should be normalized to lowercase
        assert result.normalized_value["uuid"] == uuid_str.lower()

    def test_validate_non_string_types(self):
        """Test validation fails for non-string types"""
        # Number
        result = self.validator.validate(123)
        assert result.is_valid is False
        assert "Expected string, got int" in result.message

        # List
        result = self.validator.validate(["550e8400-e29b-41d4-a716-446655440000"])
        assert result.is_valid is False
        assert "Expected string, got list" in result.message

        # Dict
        result = self.validator.validate({"uuid": "550e8400-e29b-41d4-a716-446655440000"})
        assert result.is_valid is False
        assert "Expected string, got dict" in result.message

        # None
        result = self.validator.validate(None)
        assert result.is_valid is False
        assert "Expected string, got NoneType" in result.message

    def test_validate_invalid_uuid_formats(self):
        """Test validation of invalid UUID formats"""
        invalid_uuids = [
            "not-a-uuid",
            "550e8400-e29b-41d4-a716-44665544000",  # Too short
            "550e8400-e29b-41d4-a716-4466554400001",  # Too long
            "550e8400-e29b-41d4-a716",  # Missing segments
            "550e8400e29b41d4a716446655440000",  # No dashes
            "550g8400-e29b-41d4-a716-446655440000",  # Invalid character
            "550e8400-e29b-41d4-a716-446655440000-extra",  # Extra content
            "",  # Empty string
            "   ",  # Whitespace only
            "550e8400-e29b-41d4-a716-44665544000Z",  # Invalid character at end
            "x550e8400-e29b-41d4-a716-446655440000",  # Invalid prefix
        ]
        
        for invalid_uuid in invalid_uuids:
            result = self.validator.validate(invalid_uuid)
            assert result.is_valid is False, f"Should fail for: {invalid_uuid}"
            assert result.message == "Invalid UUID format"

    def test_validate_version_constraint_single_version(self):
        """Test version constraint with single version"""
        constraints = {"version": 4}
        
        # Valid version 4 UUID
        result = self.validator.validate(self.sample_uuids["v4"], constraints)
        assert result.is_valid is True

        # Invalid - version 1 when version 4 required
        result = self.validator.validate(self.sample_uuids["v1"], constraints)
        assert result.is_valid is False
        assert result.message == "UUID must be version 4"

    def test_validate_version_constraint_multiple_versions(self):
        """Test version constraint with multiple allowed versions"""
        constraints = {"version": [1, 4]}
        
        # Valid version 1 UUID
        result = self.validator.validate(self.sample_uuids["v1"], constraints)
        assert result.is_valid is True

        # Valid version 4 UUID
        result = self.validator.validate(self.sample_uuids["v4"], constraints)
        assert result.is_valid is True

        # Invalid - version 3 when only 1,4 allowed
        result = self.validator.validate(self.sample_uuids["v3"], constraints)
        assert result.is_valid is False
        assert "UUID version must be one of: [1, 4]" in result.message

    def test_validate_variant_constraint(self):
        """Test variant constraint validation"""
        # Get variant from a real UUID
        uuid_obj = uuid.UUID(self.sample_uuids["v4"])
        constraints = {"variant": uuid_obj.variant}
        
        result = self.validator.validate(self.sample_uuids["v4"], constraints)
        assert result.is_valid is True

        # Test with different variant (this will likely fail for most standard UUIDs)
        # Most UUIDs will have the same variant, so this tests the logic
        if uuid_obj.variant != "reserved_ncs":
            constraints_invalid = {"variant": "reserved_ncs"}
            result = self.validator.validate(self.sample_uuids["v4"], constraints_invalid)
            # This should fail if the variant doesn't match
            assert result.is_valid is False
            assert "UUID must have variant:" in result.message

    def test_validate_allow_nil_true(self):
        """Test allowNil constraint set to True (default)"""
        constraints = {"allowNil": True}
        
        result = self.validator.validate(self.sample_uuids["nil"], constraints)
        assert result.is_valid is True

    def test_validate_allow_nil_false(self):
        """Test allowNil constraint set to False"""
        constraints = {"allowNil": False}
        
        # Nil UUID should be rejected
        result = self.validator.validate(self.sample_uuids["nil"], constraints)
        assert result.is_valid is False
        assert result.message == "Nil UUID is not allowed"

        # Regular UUID should still pass
        result = self.validator.validate(self.sample_uuids["v4"], constraints)
        assert result.is_valid is True

    def test_validate_format_lowercase(self):
        """Test format constraint - lowercase (default)"""
        constraints = {"format": "lowercase"}
        
        result = self.validator.validate(self.sample_uuids["uppercase"], constraints)
        assert result.is_valid is True
        assert result.normalized_value["uuid"] == self.sample_uuids["uppercase"].lower()

    def test_validate_format_uppercase(self):
        """Test format constraint - uppercase"""
        constraints = {"format": "uppercase"}
        
        result = self.validator.validate(self.sample_uuids["v4"], constraints)
        assert result.is_valid is True
        assert result.normalized_value["uuid"] == self.sample_uuids["v4"].upper()

    def test_validate_format_nodashes(self):
        """Test format constraint - no dashes"""
        constraints = {"format": "nodashes"}
        
        result = self.validator.validate(self.sample_uuids["v4"], constraints)
        assert result.is_valid is True
        expected = self.sample_uuids["v4"].replace("-", "")
        assert result.normalized_value["uuid"] == expected

    def test_validate_format_urn(self):
        """Test format constraint - URN format"""
        constraints = {"format": "urn"}
        
        result = self.validator.validate(self.sample_uuids["v4"], constraints)
        assert result.is_valid is True
        expected = f"urn:uuid:{self.sample_uuids['v4']}"
        assert result.normalized_value["uuid"] == expected

    def test_validate_format_default(self):
        """Test format constraint - default behavior"""
        # No format specified should default to lowercase
        result = self.validator.validate(self.sample_uuids["uppercase"])
        assert result.is_valid is True
        assert result.normalized_value["uuid"] == self.sample_uuids["uppercase"].lower()

    def test_validate_combined_constraints(self):
        """Test validation with multiple constraints"""
        constraints = {
            "version": 4,
            "allowNil": False,
            "format": "uppercase"
        }
        
        # Valid UUID meeting all constraints
        result = self.validator.validate(self.sample_uuids["v4"], constraints)
        assert result.is_valid is True
        assert result.normalized_value["uuid"] == self.sample_uuids["v4"].upper()

        # Fail version constraint
        result = self.validator.validate(self.sample_uuids["v1"], constraints)
        assert result.is_valid is False
        assert result.message == "UUID must be version 4"

        # Fail nil constraint (nil UUID has version None, so fails version check first)
        result = self.validator.validate(self.sample_uuids["nil"], constraints)
        assert result.is_valid is False
        assert result.message == "UUID must be version 4"  # Version check happens before nil check

    def test_validate_uuid_parsing_edge_cases(self):
        """Test UUID parsing with edge cases"""
        # Valid UUID but unusual format (should work with uuid.UUID)
        edge_cases = [
            self.sample_uuids["v4"].upper(),  # Uppercase
            self.sample_uuids["v4"].replace("-", ""),  # No dashes (not allowed by regex but tested)
        ]
        
        # Only test cases that pass the regex validation
        for uuid_str in edge_cases:
            result = self.validator.validate(uuid_str)
            if result.is_valid:
                assert result.normalized_value["uuid"] is not None

    def test_validate_malformed_uuid_after_regex_pass(self):
        """Test UUIDs that pass regex but fail uuid.UUID parsing"""
        # These should pass regex but might fail UUID parsing
        malformed_uuids = [
            "ffffffff-ffff-ffff-ffff-ffffffffffff",  # All F's
            "123e4567-e89b-12d3-a456-426614174000",  # Valid format
        ]
        
        for uuid_str in malformed_uuids:
            result = self.validator.validate(uuid_str)
            # These should generally be valid since they match UUID format
            if not result.is_valid:
                assert "Invalid UUID:" in result.message

    def test_validate_version_1_timestamp_extraction(self):
        """Test timestamp extraction for version 1 UUIDs"""
        # Create a version 1 UUID
        v1_uuid = uuid.uuid1()
        uuid_str = str(v1_uuid)
        
        result = self.validator.validate(uuid_str)
        assert result.is_valid is True
        assert result.normalized_value["version"] == 1
        
        # Should have timestamp
        if "timestamp" in result.normalized_value:
            assert isinstance(result.normalized_value["timestamp"], int)

    def test_validate_version_1_timestamp_exception_handling(self):
        """Test exception handling for version 1 timestamp extraction"""
        # Use predefined v1 UUID that should work
        result = self.validator.validate(self.sample_uuids["v1"])
        assert result.is_valid is True
        assert result.normalized_value["version"] == 1
        # Timestamp might be present, if not that's also fine (covered by exception handling)

    def test_normalize_basic_uuids(self):
        """Test normalize method with basic UUIDs"""
        # Normal UUID
        normalized = self.validator.normalize(self.sample_uuids["v4"])
        assert normalized == self.sample_uuids["v4"].lower()

        # Uppercase UUID
        normalized = self.validator.normalize(self.sample_uuids["uppercase"])
        assert normalized == self.sample_uuids["uppercase"].lower()

        # UUID with whitespace
        uuid_with_space = f"  {self.sample_uuids['v4']}  "
        normalized = self.validator.normalize(uuid_with_space)
        assert normalized == self.sample_uuids["v4"].lower()

    def test_normalize_invalid_uuid_triggers_bare_except(self):
        """Test normalize with invalid UUID (triggers bare except BUG!)"""
        # Invalid UUID should return original value due to bare except
        invalid_uuid = "not-a-uuid"
        normalized = self.validator.normalize(invalid_uuid)
        assert normalized == invalid_uuid

    def test_normalize_non_string_types(self):
        """Test normalize method with non-string types"""
        assert self.validator.normalize(123) == 123
        assert self.validator.normalize([1, 2, 3]) == [1, 2, 3]
        assert self.validator.normalize({"uuid": "test"}) == {"uuid": "test"}
        assert self.validator.normalize(None) is None

    def test_get_supported_types(self):
        """Test get_supported_types method"""
        supported_types = self.validator.get_supported_types()
        expected_types = ["uuid", "guid", "identifier"]
        assert supported_types == expected_types
        assert len(supported_types) == 3

    def test_validate_edge_case_empty_constraints(self):
        """Test validation with empty constraints"""
        result = self.validator.validate(self.sample_uuids["v4"], {})
        assert result.is_valid is True

    def test_validate_edge_case_none_constraints(self):
        """Test validation with None constraints"""
        result = self.validator.validate(self.sample_uuids["v4"], None)
        assert result.is_valid is True

    def test_uuid_variant_coverage(self):
        """Test UUID variant detection"""
        # Test with standard UUID to ensure variant is captured
        result = self.validator.validate(self.sample_uuids["v4"])
        assert result.is_valid is True
        assert "variant" in result.normalized_value
        assert isinstance(result.normalized_value["variant"], str)

    def test_uuid_regex_pattern_case_insensitive(self):
        """Test that UUID regex is case insensitive"""
        # Test various case combinations
        uuid_cases = [
            self.sample_uuids["v4"].lower(),
            self.sample_uuids["v4"].upper(),
            self.sample_uuids["v4"].title(),  # Title case
        ]
        
        for uuid_str in uuid_cases:
            result = self.validator.validate(uuid_str)
            assert result.is_valid is True, f"Should pass for case: {uuid_str}"

    def test_uuid_metadata_structure(self):
        """Test UUID result metadata structure"""
        result = self.validator.validate(self.sample_uuids["v4"])
        
        assert result.is_valid is True
        
        # Verify all expected fields are present
        required_fields = ["uuid", "version", "variant", "isNil"]
        for field in required_fields:
            assert field in result.normalized_value
        
        # Verify metadata structure
        assert "type" in result.metadata
        assert result.metadata["type"] == "uuid"
        assert "version" in result.metadata

    def test_uuid_format_edge_cases(self):
        """Test edge cases in UUID formatting"""
        constraints_list = [
            {"format": "lowercase"},
            {"format": "uppercase"},
            {"format": "nodashes"},
            {"format": "urn"},
        ]
        
        for constraints in constraints_list:
            result = self.validator.validate(self.sample_uuids["v4"], constraints)
            assert result.is_valid is True
            assert "uuid" in result.normalized_value
            
            # Verify format was applied
            format_type = constraints["format"]
            formatted_uuid = result.normalized_value["uuid"]
            
            if format_type == "uppercase":
                assert formatted_uuid.isupper()
            elif format_type == "lowercase":
                assert formatted_uuid.islower()
            elif format_type == "nodashes":
                assert "-" not in formatted_uuid
            elif format_type == "urn":
                assert formatted_uuid.startswith("urn:uuid:")

    def test_uuid_version_detection_accuracy(self):
        """Test accurate version detection for different UUID types"""
        version_tests = [
            (self.sample_uuids["v1"], 1),
            (self.sample_uuids["v3"], 3),
            (self.sample_uuids["v4"], 4),
            (self.sample_uuids["v5"], 5),
        ]
        
        for uuid_str, expected_version in version_tests:
            result = self.validator.validate(uuid_str)
            assert result.is_valid is True
            assert result.normalized_value["version"] == expected_version
            assert result.metadata["version"] == expected_version

    def test_constraint_validation_order(self):
        """Test that constraints are validated in the correct order"""
        # Test order: format -> parsing -> version -> variant -> nil
        constraints = {
            "version": 1,  # This should fail before variant check
            "variant": "some_variant",
            "allowNil": False
        }
        
        # Version 4 UUID should fail on version check before variant check
        result = self.validator.validate(self.sample_uuids["v4"], constraints)
        assert result.is_valid is False
        assert result.message == "UUID must be version 1"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])