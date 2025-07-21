"""
Example of using enhanced assertions for better error messages
"""

import pytest
from tests.utils.assertions import (
    assert_equal, assert_dict_equal, assert_list_equal,
    assert_api_response, assert_dict_contains, assert_type,
    assert_in_range, assert_matches_pattern
)


class TestEnhancedAssertionsExample:
    """Example test class showing enhanced assertions usage"""
    
    def test_basic_equality(self):
        """Example: Basic equality with context"""
        user_id = 123
        expected_name = "John Doe"
        actual_name = "Jane Doe"
        
        # This will produce a detailed error message
        assert_equal(
            actual=actual_name,
            expected=expected_name,
            field_name="user.name",
            context={
                "user_id": user_id,
                "test_scenario": "User name validation"
            }
        )
    
    def test_dictionary_comparison(self):
        """Example: Dictionary comparison with detailed diff"""
        expected_user = {
            "id": 1,
            "name": "John",
            "email": "john@example.com",
            "roles": ["admin", "user"],
            "active": True
        }
        
        actual_user = {
            "id": 1,
            "name": "John",
            "email": "john@test.com",  # Different email
            "roles": ["user"],         # Missing 'admin' role
            "active": True,
            "created_at": "2024-01-20"  # Extra field
        }
        
        # This will show exactly what's different
        assert_dict_equal(
            actual=actual_user,
            expected=expected_user,
            ignore_keys=["created_at"]  # Can ignore certain keys
        )
    
    def test_list_comparison(self):
        """Example: List comparison with ordering"""
        expected_items = ["apple", "banana", "cherry"]
        actual_items = ["apple", "cherry", "banana", "date"]
        
        # Ordered comparison (default)
        assert_list_equal(
            actual=actual_items,
            expected=expected_items,
            ordered=True
        )
        
        # Unordered comparison
        assert_list_equal(
            actual=actual_items,
            expected=expected_items,
            ordered=False
        )
    
    def test_api_response_validation(self):
        """Example: API response validation"""
        # Mock response object
        class MockResponse:
            status_code = 400
            url = "/api/v1/users"
            method = "POST"
            
            def json(self):
                return {
                    "error": "Validation failed",
                    "details": {
                        "email": "Invalid email format",
                        "age": "Must be a positive integer"
                    }
                }
        
        response = MockResponse()
        
        # This will show full API error details
        assert_api_response(
            response=response,
            expected_status=200,
            expected_fields={
                "id": 1,
                "created": True
            },
            check_response_time=1.0
        )
    
    def test_type_validation(self):
        """Example: Type validation with context"""
        user_age = "25"  # String instead of int
        
        assert_type(
            value=user_age,
            expected_type=int,
            field_name="user.age"
        )
    
    def test_range_validation(self):
        """Example: Numeric range validation"""
        temperature = 150
        
        assert_in_range(
            value=temperature,
            min_value=-50,
            max_value=50,
            field_name="room_temperature_celsius"
        )
    
    def test_pattern_matching(self):
        """Example: Regex pattern matching"""
        email = "invalid-email"
        
        assert_matches_pattern(
            value=email,
            pattern=r'^[\w\.-]+@[\w\.-]+\.\w+$',
            field_name="user_email"
        )
    
    def test_complex_validation_scenario(self):
        """Example: Complex validation with multiple checks"""
        response_data = {
            "users": [
                {"id": 1, "name": "Alice", "score": 85},
                {"id": 2, "name": "Bob", "score": 120},  # Invalid score
            ],
            "total": 3,  # Wrong total
            "page": 1
        }
        
        # Multiple validations that will all show detailed errors
        
        # Check total count
        assert_equal(
            actual=response_data["total"],
            expected=2,
            field_name="response.total",
            context={"actual_users_count": len(response_data["users"])}
        )
        
        # Check user scores are in valid range
        for user in response_data["users"]:
            assert_in_range(
                value=user["score"],
                min_value=0,
                max_value=100,
                field_name=f"user[{user['id']}].score"
            )
        
        # Check required fields
        assert_dict_contains(
            actual=response_data,
            expected_subset={
                "users": list,
                "total": int,
                "page": int,
                "has_more": bool  # Missing field
            }
        )


# Note: These tests are designed to fail to demonstrate the error messages.
# In real tests, you would have assertions that pass when the code is correct.