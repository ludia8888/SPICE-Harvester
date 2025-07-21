"""
Enhanced assertion helpers for tests with detailed error messages
"""

from typing import Any, Dict, List, Optional, Union, Callable
import json
import logging
from .error_formatter import TestErrorFormatter

logger = logging.getLogger(__name__)


class EnhancedAssertions:
    """Enhanced assertions with detailed error messages"""
    
    @staticmethod
    def assert_equal(actual: Any, expected: Any, field_name: str = "value", context: Optional[Dict[str, Any]] = None):
        """Assert equality with detailed error message"""
        if actual != expected:
            error_msg = TestErrorFormatter.format_validation_error(
                field=field_name,
                expected=expected,
                actual=actual,
                context=context
            )
            raise AssertionError(error_msg)
    
    @staticmethod
    def assert_dict_equal(actual: dict, expected: dict, ignore_keys: Optional[List[str]] = None):
        """Assert dictionaries are equal with detailed diff"""
        differences = []
        
        # Check for missing keys
        for key in expected:
            if ignore_keys and key in ignore_keys:
                continue
            if key not in actual:
                differences.append({
                    'path': key,
                    'expected': expected[key],
                    'actual': '<missing>',
                    'reason': 'Key not found in actual'
                })
        
        # Check for extra keys
        for key in actual:
            if ignore_keys and key in ignore_keys:
                continue
            if key not in expected:
                differences.append({
                    'path': key,
                    'expected': '<missing>',
                    'actual': actual[key],
                    'reason': 'Unexpected key in actual'
                })
        
        # Check values for common keys
        for key in expected:
            if ignore_keys and key in ignore_keys:
                continue
            if key in actual and actual[key] != expected[key]:
                differences.append({
                    'path': key,
                    'expected': expected[key],
                    'actual': actual[key],
                    'reason': 'Values differ'
                })
        
        if differences:
            error_msg = TestErrorFormatter.format_comparison_error(
                field="dictionaries",
                differences=differences
            )
            raise AssertionError(error_msg)
    
    @staticmethod
    def assert_list_equal(actual: list, expected: list, ordered: bool = True):
        """Assert lists are equal with detailed diff"""
        differences = []
        
        if ordered:
            # Compare ordered lists
            for i, (actual_item, expected_item) in enumerate(zip(actual, expected)):
                if actual_item != expected_item:
                    differences.append({
                        'path': f'index[{i}]',
                        'expected': expected_item,
                        'actual': actual_item,
                        'reason': 'Values differ at index'
                    })
            
            # Check length differences
            if len(actual) < len(expected):
                for i in range(len(actual), len(expected)):
                    differences.append({
                        'path': f'index[{i}]',
                        'expected': expected[i],
                        'actual': '<missing>',
                        'reason': 'Missing item in actual'
                    })
            elif len(actual) > len(expected):
                for i in range(len(expected), len(actual)):
                    differences.append({
                        'path': f'index[{i}]',
                        'expected': '<missing>',
                        'actual': actual[i],
                        'reason': 'Extra item in actual'
                    })
        else:
            # Compare unordered lists
            missing_in_actual = []
            extra_in_actual = []
            
            for item in expected:
                if item not in actual:
                    missing_in_actual.append(item)
            
            for item in actual:
                if item not in expected:
                    extra_in_actual.append(item)
            
            if missing_in_actual:
                differences.append({
                    'path': 'items',
                    'expected': missing_in_actual,
                    'actual': '<missing>',
                    'reason': 'Items missing in actual'
                })
            
            if extra_in_actual:
                differences.append({
                    'path': 'items',
                    'expected': '<missing>',
                    'actual': extra_in_actual,
                    'reason': 'Extra items in actual'
                })
        
        if differences:
            error_msg = TestErrorFormatter.format_comparison_error(
                field="lists",
                differences=differences
            )
            raise AssertionError(error_msg)
    
    @staticmethod
    def assert_api_response(
        response: Any,
        expected_status: int,
        expected_fields: Optional[Dict[str, Any]] = None,
        check_response_time: Optional[float] = None
    ):
        """Assert API response with detailed error info"""
        # Check status code
        if hasattr(response, 'status_code'):
            actual_status = response.status_code
        elif hasattr(response, 'status'):
            actual_status = response.status
        else:
            raise ValueError("Response object doesn't have status_code or status attribute")
        
        if actual_status != expected_status:
            # Get response body
            if hasattr(response, 'json') and callable(response.json):
                try:
                    response_body = response.json()
                except (json.JSONDecodeError, ValueError, TypeError) as e:
                    logger.debug(f"Failed to parse response as JSON: {e}")
                    response_body = getattr(response, 'text', str(response))
            elif hasattr(response, 'data'):
                response_body = response.data
            else:
                response_body = str(response)
            
            error_msg = TestErrorFormatter.format_api_error(
                endpoint=getattr(response, 'url', 'unknown'),
                method=getattr(response, 'method', 'unknown'),
                status_code=actual_status,
                response_body=response_body
            )
            raise AssertionError(error_msg)
        
        # Check response fields if specified
        if expected_fields:
            if hasattr(response, 'json') and callable(response.json):
                actual_data = response.json()
            elif hasattr(response, 'data'):
                actual_data = response.data
            else:
                actual_data = {}
            
            EnhancedAssertions.assert_dict_contains(actual_data, expected_fields)
        
        # Check response time if specified
        if check_response_time and hasattr(response, 'elapsed'):
            actual_time = response.elapsed.total_seconds()
            if actual_time > check_response_time:
                raise AssertionError(
                    f"\n⏱️  Response Time Exceeded\n"
                    f"   Expected: < {check_response_time}s\n"
                    f"   Actual:   {actual_time:.3f}s"
                )
    
    @staticmethod
    def assert_dict_contains(actual: dict, expected_subset: dict):
        """Assert dictionary contains expected subset"""
        differences = []
        
        for key, expected_value in expected_subset.items():
            if key not in actual:
                differences.append({
                    'path': key,
                    'expected': expected_value,
                    'actual': '<missing>',
                    'reason': 'Key not found'
                })
            elif actual[key] != expected_value:
                differences.append({
                    'path': key,
                    'expected': expected_value,
                    'actual': actual[key],
                    'reason': 'Values differ'
                })
        
        if differences:
            error_msg = TestErrorFormatter.format_comparison_error(
                field="dictionary subset",
                differences=differences
            )
            raise AssertionError(error_msg)
    
    @staticmethod
    def assert_type(value: Any, expected_type: type, field_name: str = "value"):
        """Assert value is of expected type"""
        if not isinstance(value, expected_type):
            error_msg = TestErrorFormatter.format_validation_error(
                field=field_name,
                expected=f"type: {expected_type.__name__}",
                actual=f"type: {type(value).__name__}",
                context={"value": value}
            )
            raise AssertionError(error_msg)
    
    @staticmethod
    def assert_in_range(value: Union[int, float], min_value: Optional[Union[int, float]] = None, 
                       max_value: Optional[Union[int, float]] = None, field_name: str = "value"):
        """Assert numeric value is within range"""
        if min_value is not None and value < min_value:
            error_msg = TestErrorFormatter.format_validation_error(
                field=field_name,
                expected=f">= {min_value}",
                actual=value,
                context={"violation": "below minimum"}
            )
            raise AssertionError(error_msg)
        
        if max_value is not None and value > max_value:
            error_msg = TestErrorFormatter.format_validation_error(
                field=field_name,
                expected=f"<= {max_value}",
                actual=value,
                context={"violation": "above maximum"}
            )
            raise AssertionError(error_msg)
    
    @staticmethod
    def assert_matches_pattern(value: str, pattern: str, field_name: str = "value"):
        """Assert string matches regex pattern"""
        import re
        if not re.match(pattern, value):
            error_msg = TestErrorFormatter.format_validation_error(
                field=field_name,
                expected=f"matches pattern: {pattern}",
                actual=value,
                context={"pattern_type": "regex"}
            )
            raise AssertionError(error_msg)
    
    @staticmethod
    def assert_contains(text: str, substring: str, field_name: str = "text"):
        """Assert string contains substring"""
        if substring not in text:
            error_msg = TestErrorFormatter.format_validation_error(
                field=field_name,
                expected=f"contains substring: '{substring}'",
                actual=text,
                context={"operation": "substring_search"}
            )
            raise AssertionError(error_msg)


# Convenience shortcuts
assert_equal = EnhancedAssertions.assert_equal
assert_dict_equal = EnhancedAssertions.assert_dict_equal
assert_list_equal = EnhancedAssertions.assert_list_equal
assert_api_response = EnhancedAssertions.assert_api_response
assert_dict_contains = EnhancedAssertions.assert_dict_contains
assert_type = EnhancedAssertions.assert_type
assert_in_range = EnhancedAssertions.assert_in_range
assert_matches_pattern = EnhancedAssertions.assert_matches_pattern
assert_contains = EnhancedAssertions.assert_contains