"""
Error formatter for improved test error messages
"""

import traceback
import json
from typing import Any, Dict, List, Optional, Union
import re


class TestErrorFormatter:
    """Format test errors with detailed context"""
    
    @staticmethod
    def format_validation_error(
        field: str,
        expected: Any,
        actual: Any,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Format validation errors with clear comparisons"""
        error_parts = [
            f"\n❌ Validation Error for field: '{field}'",
            f"   Expected: {TestErrorFormatter._format_value(expected)}",
            f"   Actual:   {TestErrorFormatter._format_value(actual)}"
        ]
        
        if context:
            error_parts.append("\n   Context:")
            for key, value in context.items():
                error_parts.append(f"     - {key}: {TestErrorFormatter._format_value(value)}")
        
        return "\n".join(error_parts)
    
    @staticmethod
    def format_api_error(
        endpoint: str,
        method: str,
        status_code: int,
        response_body: Any,
        request_data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> str:
        """Format API errors with request/response details"""
        error_parts = [
            f"\n❌ API Error: {method} {endpoint}",
            f"   Status Code: {status_code}"
        ]
        
        if request_data:
            error_parts.append(f"\n   Request Data:")
            error_parts.append(TestErrorFormatter._format_json(request_data, indent=6))
        
        if headers:
            error_parts.append(f"\n   Request Headers:")
            for key, value in headers.items():
                if key.lower() not in ['authorization', 'api-key', 'x-api-key']:
                    error_parts.append(f"     {key}: {value}")
        
        error_parts.append(f"\n   Response Body:")
        if isinstance(response_body, (dict, list)):
            error_parts.append(TestErrorFormatter._format_json(response_body, indent=6))
        else:
            error_parts.append(f"     {response_body}")
        
        return "\n".join(error_parts)
    
    @staticmethod
    def format_assertion_error(
        test_name: str,
        assertion: str,
        actual_value: Any,
        expected_value: Any,
        additional_info: Optional[Dict[str, Any]] = None
    ) -> str:
        """Format assertion errors with detailed information"""
        error_parts = [
            f"\n❌ Assertion Failed in test: {test_name}",
            f"   Assertion: {assertion}",
            f"   Expected: {TestErrorFormatter._format_value(expected_value)}",
            f"   Actual:   {TestErrorFormatter._format_value(actual_value)}"
        ]
        
        if additional_info:
            error_parts.append("\n   Additional Information:")
            for key, value in additional_info.items():
                error_parts.append(f"     - {key}: {TestErrorFormatter._format_value(value)}")
        
        # Add type information if types differ
        if type(expected_value) != type(actual_value):
            error_parts.append(f"\n   Type Mismatch:")
            error_parts.append(f"     Expected type: {type(expected_value).__name__}")
            error_parts.append(f"     Actual type:   {type(actual_value).__name__}")
        
        return "\n".join(error_parts)
    
    @staticmethod
    def format_exception_error(
        exception: Exception,
        test_context: Optional[Dict[str, Any]] = None,
        include_traceback: bool = True
    ) -> str:
        """Format exceptions with context and traceback"""
        error_parts = [
            f"\n❌ Exception: {type(exception).__name__}",
            f"   Message: {str(exception)}"
        ]
        
        if test_context:
            error_parts.append("\n   Test Context:")
            for key, value in test_context.items():
                error_parts.append(f"     - {key}: {TestErrorFormatter._format_value(value)}")
        
        if include_traceback:
            tb_lines = traceback.format_exception(type(exception), exception, exception.__traceback__)
            # Filter out test framework noise
            filtered_tb = TestErrorFormatter._filter_traceback(tb_lines)
            if filtered_tb:
                error_parts.append("\n   Traceback (most recent call last):")
                error_parts.append("".join(filtered_tb).rstrip())
        
        return "\n".join(error_parts)
    
    @staticmethod
    def format_timeout_error(
        test_name: str,
        timeout_seconds: float,
        operation: str,
        partial_output: Optional[str] = None
    ) -> str:
        """Format timeout errors with context"""
        error_parts = [
            f"\n⏱️  Timeout Error in test: {test_name}",
            f"   Operation: {operation}",
            f"   Timeout: {timeout_seconds} seconds"
        ]
        
        if partial_output:
            error_parts.append("\n   Partial Output (last 500 chars):")
            error_parts.append(f"   {partial_output[-500:]}")
        
        error_parts.append("\n   Suggestions:")
        error_parts.append("     - Check if services are running properly")
        error_parts.append("     - Verify network connectivity")
        error_parts.append(f"     - Consider increasing timeout (current: {timeout_seconds}s)")
        
        return "\n".join(error_parts)
    
    @staticmethod
    def format_comparison_error(
        field: str,
        differences: List[Dict[str, Any]]
    ) -> str:
        """Format comparison errors for complex objects"""
        error_parts = [
            f"\n❌ Comparison Error for: {field}",
            f"   Found {len(differences)} difference(s):"
        ]
        
        for i, diff in enumerate(differences, 1):
            error_parts.append(f"\n   Difference #{i}:")
            error_parts.append(f"     Path: {diff.get('path', 'root')}")
            error_parts.append(f"     Expected: {TestErrorFormatter._format_value(diff.get('expected'))}")
            error_parts.append(f"     Actual:   {TestErrorFormatter._format_value(diff.get('actual'))}")
            if 'reason' in diff:
                error_parts.append(f"     Reason: {diff['reason']}")
        
        return "\n".join(error_parts)
    
    @staticmethod
    def _format_value(value: Any, max_length: int = 100) -> str:
        """Format values for display"""
        if value is None:
            return "None"
        elif isinstance(value, str):
            if len(value) > max_length:
                return f'"{value[:max_length]}..." (truncated, length: {len(value)})'
            return f'"{value}"'
        elif isinstance(value, (dict, list)):
            formatted = json.dumps(value, ensure_ascii=False)
            if len(formatted) > max_length:
                return f"{formatted[:max_length]}... (truncated)"
            return formatted
        else:
            return str(value)
    
    @staticmethod
    def _format_json(data: Union[dict, list], indent: int = 4) -> str:
        """Format JSON data with proper indentation"""
        lines = json.dumps(data, ensure_ascii=False, indent=2).split('\n')
        return '\n'.join(' ' * indent + line for line in lines)
    
    @staticmethod
    def _filter_traceback(tb_lines: List[str]) -> List[str]:
        """Filter out test framework noise from traceback"""
        filtered = []
        skip_patterns = [
            r"pytest.*/_pytest/",
            r"unittest/",
            r"asyncio/",
            r"concurrent/futures/",
        ]
        
        for line in tb_lines:
            # Skip lines that match noise patterns
            if any(re.search(pattern, line) for pattern in skip_patterns):
                continue
            filtered.append(line)
        
        return filtered


# Convenience functions for common error types
def format_validation_error(field: str, expected: Any, actual: Any, **kwargs) -> str:
    """Convenience function for validation errors"""
    return TestErrorFormatter.format_validation_error(field, expected, actual, kwargs)


def format_api_error(endpoint: str, method: str, status_code: int, response_body: Any, **kwargs) -> str:
    """Convenience function for API errors"""
    return TestErrorFormatter.format_api_error(endpoint, method, status_code, response_body, **kwargs)


def format_assertion_error(test_name: str, assertion: str, actual: Any, expected: Any, **kwargs) -> str:
    """Convenience function for assertion errors"""
    return TestErrorFormatter.format_assertion_error(test_name, assertion, actual, expected, kwargs)


def format_exception_error(exception: Exception, **kwargs) -> str:
    """Convenience function for exception errors"""
    return TestErrorFormatter.format_exception_error(exception, **kwargs)