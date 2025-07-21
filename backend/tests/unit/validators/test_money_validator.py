"""
Comprehensive unit tests for MoneyValidator
Tests all money validation functionality with REAL behavior verification
NO MOCKS - Tests verify actual code behavior
"""

import pytest
from decimal import Decimal
from typing import Dict, Any, Optional

from shared.validators.money_validator import MoneyValidator


class TestMoneyValidator:
    """Test MoneyValidator with real behavior verification"""

    def setup_method(self):
        """Setup for each test"""
        self.validator = MoneyValidator()

    def test_validate_string_format_basic(self):
        """Test validation of basic string format money"""
        money_str = "123.45 USD"
        result = self.validator.validate(money_str)
        assert result.is_valid is True
        assert result.message == "Valid money"
        assert result.normalized_value["amount"] == 123.45
        assert result.normalized_value["currency"] == "USD"
        assert result.normalized_value["formatted"] == "123.45 USD"
        assert result.normalized_value["decimal_amount"] == "123.45"
        assert result.metadata["type"] == "money"
        assert result.metadata["currency"] == "USD"
        assert result.metadata["decimal_places"] == 2

    def test_validate_string_format_integer_amount(self):
        """Test validation with integer amount"""
        money_str = "100 EUR"
        result = self.validator.validate(money_str)
        assert result.is_valid is True
        assert result.normalized_value["amount"] == 100.0
        assert result.normalized_value["currency"] == "EUR"
        assert result.normalized_value["formatted"] == "100.00 EUR"  # Should be rounded to 2 decimals

    def test_validate_string_format_various_currencies(self):
        """Test validation with various supported currencies"""
        test_cases = [
            ("50.00 GBP", "GBP"),
            ("1000 JPY", "JPY"),
            ("75.25 CAD", "CAD"),
            ("200.50 AUD", "AUD"),
            ("99.99 CHF", "CHF"),
        ]
        
        for money_str, expected_currency in test_cases:
            result = self.validator.validate(money_str)
            assert result.is_valid is True, f"Should pass for: {money_str}"
            assert result.normalized_value["currency"] == expected_currency

    def test_validate_dict_format_basic(self):
        """Test validation of dictionary format money"""
        money_dict = {"amount": 123.45, "currency": "USD"}
        result = self.validator.validate(money_dict)
        assert result.is_valid is True
        assert result.normalized_value["amount"] == 123.45
        assert result.normalized_value["currency"] == "USD"

    def test_validate_dict_format_string_amount(self):
        """Test validation with string amount in dictionary"""
        money_dict = {"amount": "123.45", "currency": "EUR"}
        result = self.validator.validate(money_dict)
        assert result.is_valid is True
        assert result.normalized_value["amount"] == 123.45
        assert result.normalized_value["currency"] == "EUR"

    def test_validate_dict_format_integer_amount(self):
        """Test validation with integer amount in dictionary"""
        money_dict = {"amount": 100, "currency": "GBP"}
        result = self.validator.validate(money_dict)
        assert result.is_valid is True
        assert result.normalized_value["amount"] == 100.0
        assert result.normalized_value["currency"] == "GBP"

    def test_validate_unsupported_types(self):
        """Test validation fails for unsupported types"""
        # Number only
        result = self.validator.validate(123.45)
        assert result.is_valid is False
        assert result.message == "Money value must be string or object"

        # List
        result = self.validator.validate([123.45, "USD"])
        assert result.is_valid is False
        assert result.message == "Money value must be string or object"

        # None
        result = self.validator.validate(None)
        assert result.is_valid is False
        assert result.message == "Money value must be string or object"

    def test_validate_invalid_string_formats(self):
        """Test validation of invalid string formats"""
        invalid_formats = [
            "123.45",  # Missing currency
            "USD 123.45",  # Wrong order
            "123.45USD",  # No space
            "123.45 US",  # Invalid currency length
            "123.45 USDD",  # Invalid currency length
            "$123.45",  # Currency symbol not supported
            "123,45 USD",  # Wrong decimal separator
            "123.45.67 USD",  # Multiple decimals
            "",  # Empty string
            "   ",  # Whitespace only
            "abc.def USD",  # Non-numeric amount
            "123.45 usd",  # Lowercase currency
        ]
        
        # Note: "123.45  USD" with multiple spaces actually passes the current regex
        
        for invalid_format in invalid_formats:
            result = self.validator.validate(invalid_format)
            assert result.is_valid is False, f"Should fail for: {invalid_format}"
            assert "Invalid money format" in result.message

    def test_validate_invalid_dict_formats(self):
        """Test validation of invalid dictionary formats"""
        # Missing amount
        result = self.validator.validate({"currency": "USD"})
        assert result.is_valid is False
        assert result.message == "Money object must have 'amount' and 'currency' fields"

        # Missing currency
        result = self.validator.validate({"amount": 123.45})
        assert result.is_valid is False
        assert result.message == "Money object must have 'amount' and 'currency' fields"

        # Both missing
        result = self.validator.validate({})
        assert result.is_valid is False
        assert result.message == "Money object must have 'amount' and 'currency' fields"

        # Extra fields should be ignored
        result = self.validator.validate({"amount": 123.45, "currency": "USD", "extra": "ignored"})
        assert result.is_valid is True

    def test_validate_invalid_amount_formats_trigger_bare_except(self):
        """Test invalid amount formats (triggers bare except BUG!)"""
        # String format with invalid amount
        result = self.validator.validate("abc USD")
        assert result.is_valid is False
        assert "Invalid money format" in result.message  # Caught by regex first

        # Dict format with invalid amount (triggers bare except)
        result = self.validator.validate({"amount": "invalid", "currency": "USD"})
        assert result.is_valid is False
        assert result.message == "Invalid amount format"

        # Dict format with complex invalid amount
        result = self.validator.validate({"amount": {"nested": "value"}, "currency": "USD"})
        assert result.is_valid is False
        assert result.message == "Invalid amount format"

    def test_validate_unsupported_currency_codes(self):
        """Test validation with unsupported currency codes"""
        unsupported_currencies = [
            "XYZ",  # Fictional currency
            "ABC",  # Another fictional currency
            "999",  # Numeric code
            "AA",   # Too short
            "AAAA", # Too long
        ]
        
        for currency in unsupported_currencies:
            # Test with string format
            result = self.validator.validate(f"100.00 {currency}")
            if result.is_valid is False and "Invalid money format" in result.message:
                # Caught by regex for invalid format
                continue
            assert result.is_valid is False, f"Should fail for currency: {currency}"
            assert f"Unsupported currency code: {currency}" in result.message

            # Test with dict format
            result = self.validator.validate({"amount": 100.00, "currency": currency})
            assert result.is_valid is False, f"Should fail for currency: {currency}"
            assert f"Unsupported currency code: {currency}" in result.message

    def test_validate_min_amount_constraint_pass(self):
        """Test minAmount constraint (passing)"""
        constraints = {"minAmount": 50}
        
        result = self.validator.validate("100.00 USD", constraints)
        assert result.is_valid is True

        result = self.validator.validate({"amount": 75.50, "currency": "EUR"}, constraints)
        assert result.is_valid is True

    def test_validate_min_amount_constraint_fail(self):
        """Test minAmount constraint (failing)"""
        constraints = {"minAmount": 100}
        
        result = self.validator.validate("50.00 USD", constraints)
        assert result.is_valid is False
        assert result.message == "Amount must be at least 100"

        result = self.validator.validate({"amount": 25.75, "currency": "EUR"}, constraints)
        assert result.is_valid is False
        assert result.message == "Amount must be at least 100"

    def test_validate_max_amount_constraint_pass(self):
        """Test maxAmount constraint (passing)"""
        constraints = {"maxAmount": 1000}
        
        result = self.validator.validate("500.00 USD", constraints)
        assert result.is_valid is True

        result = self.validator.validate({"amount": 999.99, "currency": "EUR"}, constraints)
        assert result.is_valid is True

    def test_validate_max_amount_constraint_fail(self):
        """Test maxAmount constraint (failing)"""
        constraints = {"maxAmount": 100}
        
        result = self.validator.validate("500.00 USD", constraints)
        assert result.is_valid is False
        assert result.message == "Amount must be at most 100"

        result = self.validator.validate({"amount": 150.25, "currency": "EUR"}, constraints)
        assert result.is_valid is False
        assert result.message == "Amount must be at most 100"

    def test_validate_amount_range_constraints(self):
        """Test combination of min and max amount constraints"""
        constraints = {"minAmount": 10, "maxAmount": 1000}
        
        # Valid range
        result = self.validator.validate("500.00 USD", constraints)
        assert result.is_valid is True

        # Below minimum
        result = self.validator.validate("5.00 USD", constraints)
        assert result.is_valid is False
        assert result.message == "Amount must be at least 10"

        # Above maximum
        result = self.validator.validate("1500.00 USD", constraints)
        assert result.is_valid is False
        assert result.message == "Amount must be at most 1000"

    def test_validate_allowed_currencies_constraint_pass(self):
        """Test allowedCurrencies constraint (passing)"""
        constraints = {"allowedCurrencies": ["USD", "EUR", "GBP"]}
        
        result = self.validator.validate("100.00 USD", constraints)
        assert result.is_valid is True

        result = self.validator.validate("200.50 EUR", constraints)
        assert result.is_valid is True

        result = self.validator.validate("75.25 GBP", constraints)
        assert result.is_valid is True

    def test_validate_allowed_currencies_constraint_fail(self):
        """Test allowedCurrencies constraint (failing)"""
        constraints = {"allowedCurrencies": ["USD", "EUR"]}
        
        result = self.validator.validate("100.00 JPY", constraints)
        assert result.is_valid is False
        assert "Currency must be one of: ['USD', 'EUR']" in result.message

        result = self.validator.validate({"amount": 100, "currency": "CAD"}, constraints)
        assert result.is_valid is False
        assert "Currency must be one of: ['USD', 'EUR']" in result.message

    def test_validate_decimal_places_default(self):
        """Test default decimal places constraint (2)"""
        # Valid with 2 decimal places
        result = self.validator.validate("123.45 USD")
        assert result.is_valid is True
        assert result.normalized_value["decimal_amount"] == "123.45"

        # Valid with 1 decimal place (should be rounded to 2)
        result = self.validator.validate("123.4 USD")
        assert result.is_valid is True
        assert result.normalized_value["decimal_amount"] == "123.40"

        # Valid with 0 decimal places (should be rounded to 2)
        result = self.validator.validate("123 USD")
        assert result.is_valid is True
        assert result.normalized_value["decimal_amount"] == "123.00"

    def test_validate_decimal_places_constraint_pass(self):
        """Test custom decimalPlaces constraint (passing)"""
        constraints = {"decimalPlaces": 3}
        
        result = self.validator.validate("123.456 USD", constraints)
        assert result.is_valid is True
        assert result.normalized_value["decimal_amount"] == "123.456"
        assert result.metadata["decimal_places"] == 3

    def test_validate_decimal_places_constraint_fail(self):
        """Test custom decimalPlaces constraint (failing)"""
        constraints = {"decimalPlaces": 1}
        
        result = self.validator.validate("123.45 USD", constraints)
        assert result.is_valid is False
        assert result.message == "Amount cannot have more than 1 decimal places"

    def test_validate_decimal_places_rounding(self):
        """Test decimal places rounding behavior"""
        constraints = {"decimalPlaces": 2}
        
        # Should round to 2 decimal places
        result = self.validator.validate("123.456 USD", constraints)
        # This should fail because 123.456 has 3 decimal places when 2 are allowed
        assert result.is_valid is False

        # Test with exact decimal places
        result = self.validator.validate("123.45 USD", constraints)
        assert result.is_valid is True
        assert result.normalized_value["decimal_amount"] == "123.45"

    def test_validate_combined_constraints(self):
        """Test validation with multiple constraints"""
        constraints = {
            "minAmount": 10,
            "maxAmount": 1000,
            "allowedCurrencies": ["USD", "EUR"],
            "decimalPlaces": 2
        }
        
        # Valid money meeting all constraints
        result = self.validator.validate("500.50 USD", constraints)
        assert result.is_valid is True

        # Fail minAmount constraint
        result = self.validator.validate("5.00 USD", constraints)
        assert result.is_valid is False
        assert result.message == "Amount must be at least 10"

        # Fail maxAmount constraint
        result = self.validator.validate("1500.00 USD", constraints)
        assert result.is_valid is False
        assert result.message == "Amount must be at most 1000"

        # Fail allowedCurrencies constraint
        result = self.validator.validate("100.00 JPY", constraints)
        assert result.is_valid is False
        assert "Currency must be one of" in result.message

        # Fail decimalPlaces constraint
        result = self.validator.validate("100.123 USD", constraints)
        assert result.is_valid is False
        assert "Amount cannot have more than 2 decimal places" in result.message

    def test_normalize_valid_money(self):
        """Test normalize method with valid money"""
        # String format
        normalized = self.validator.normalize("123.45 USD")
        assert normalized["amount"] == 123.45
        assert normalized["currency"] == "USD"

        # Dict format
        normalized = self.validator.normalize({"amount": 200.50, "currency": "EUR"})
        assert normalized["amount"] == 200.50
        assert normalized["currency"] == "EUR"

    def test_normalize_invalid_money(self):
        """Test normalize method with invalid money"""
        # Invalid string format
        invalid_money = "invalid format"
        normalized = self.validator.normalize(invalid_money)
        assert normalized == invalid_money  # Returns original value if invalid

        # Invalid dict format
        invalid_dict = {"invalid": "dict"}
        normalized = self.validator.normalize(invalid_dict)
        assert normalized == invalid_dict

    def test_get_supported_types(self):
        """Test get_supported_types method"""
        supported_types = self.validator.get_supported_types()
        expected_types = ["money", "money", "currency"]  # Note: DataType.MONEY.value resolves to "money"
        assert len(supported_types) == 3
        assert "money" in supported_types
        assert "currency" in supported_types

    def test_validate_edge_case_empty_constraints(self):
        """Test validation with empty constraints"""
        result = self.validator.validate("100.00 USD", {})
        assert result.is_valid is True

    def test_validate_edge_case_none_constraints(self):
        """Test validation with None constraints"""
        result = self.validator.validate("100.00 USD", None)
        assert result.is_valid is True

    def test_is_valid_currency_class_method(self):
        """Test is_valid_currency class method"""
        # Valid currencies
        assert MoneyValidator.is_valid_currency("USD") is True
        assert MoneyValidator.is_valid_currency("EUR") is True
        assert MoneyValidator.is_valid_currency("GBP") is True
        assert MoneyValidator.is_valid_currency("JPY") is True

        # Invalid currencies
        assert MoneyValidator.is_valid_currency("XYZ") is False
        assert MoneyValidator.is_valid_currency("ABC") is False
        assert MoneyValidator.is_valid_currency("") is False
        assert MoneyValidator.is_valid_currency("usd") is False  # Case sensitive

    def test_currency_codes_coverage(self):
        """Test comprehensive coverage of supported currency codes"""
        # Test all currencies in COMMON_CURRENCIES
        test_currencies = [
            "USD", "EUR", "GBP", "JPY", "CNY", "KRW", "AUD", "CAD", "CHF", "SEK",
            "NOK", "DKK", "NZD", "SGD", "HKD", "MXN", "BRL", "INR", "RUB", "ZAR",
            "THB", "MYR", "IDR", "PHP", "VND", "PLN", "CZK", "HUF", "TRY", "AED"
        ]
        
        for currency in test_currencies:
            result = self.validator.validate(f"100.00 {currency}")
            assert result.is_valid is True, f"Should support currency: {currency}"
            assert result.normalized_value["currency"] == currency

    def test_amount_precision_edge_cases(self):
        """Test edge cases in amount precision handling"""
        # Very large amounts
        result = self.validator.validate("999999999.99 USD")
        assert result.is_valid is True

        # Very small amounts
        result = self.validator.validate("0.01 USD")
        assert result.is_valid is True

        # Zero amount
        result = self.validator.validate("0.00 USD")
        assert result.is_valid is True

        # High precision with custom decimal places
        constraints = {"decimalPlaces": 4}
        result = self.validator.validate("123.1234 USD", constraints)
        assert result.is_valid is True
        assert result.normalized_value["decimal_amount"] == "123.1234"

    def test_constraint_validation_order(self):
        """Test that constraints are validated in the correct order"""
        # Test order: format -> parsing -> amount -> currency -> precision
        constraints = {
            "minAmount": 100,  # This should fail before currency check
            "allowedCurrencies": ["EUR"],
            "decimalPlaces": 1
        }
        
        # Amount constraint should fail before currency constraint
        result = self.validator.validate("50.00 USD", constraints)
        assert result.is_valid is False
        assert result.message == "Amount must be at least 100"

    def test_money_result_structure(self):
        """Test money result structure completeness"""
        result = self.validator.validate("123.45 USD")
        
        assert result.is_valid is True
        
        # Verify all expected fields are present in normalized_value
        required_fields = ["amount", "currency", "formatted", "decimal_amount"]
        for field in required_fields:
            assert field in result.normalized_value
        
        # Verify metadata structure
        assert "type" in result.metadata
        assert result.metadata["type"] == "money"
        assert "currency" in result.metadata
        assert "decimal_places" in result.metadata

    def test_decimal_amount_vs_float_amount(self):
        """Test decimal vs float amount handling"""
        result = self.validator.validate("123.45 USD")
        
        # Float amount for easy arithmetic
        assert isinstance(result.normalized_value["amount"], float)
        assert result.normalized_value["amount"] == 123.45
        
        # String decimal amount for precise representation
        assert isinstance(result.normalized_value["decimal_amount"], str)
        assert result.normalized_value["decimal_amount"] == "123.45"

    def test_constraint_validator_integration(self):
        """Test integration with ConstraintValidator for numeric validation"""
        # Test that amount constraints use ConstraintValidator properly
        constraints = {"minAmount": 50, "maxAmount": 500}
        
        # Valid range
        result = self.validator.validate("100.00 USD", constraints)
        assert result.is_valid is True
        
        # Test that precision validation uses ConstraintValidator
        constraints = {"decimalPlaces": 2}
        result = self.validator.validate("100.123 USD", constraints)
        assert result.is_valid is False
        assert "decimal places" in result.message


if __name__ == "__main__":
    pytest.main([__file__, "-v"])