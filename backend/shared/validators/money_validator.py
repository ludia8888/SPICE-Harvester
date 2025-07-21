"""
Money validator for SPICE HARVESTER
"""

import re
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional
import logging

from ..models.common import DataType
from .base_validator import BaseValidator, ValidationResult
from .constraint_validator import ConstraintValidator

logger = logging.getLogger(__name__)


class MoneyValidator(BaseValidator):
    """Validator for money/currency data types"""

    # Common ISO 4217 currency codes
    COMMON_CURRENCIES = {
        "USD",
        "EUR",
        "GBP",
        "JPY",
        "CNY",
        "KRW",
        "AUD",
        "CAD",
        "CHF",
        "SEK",
        "NOK",
        "DKK",
        "NZD",
        "SGD",
        "HKD",
        "MXN",
        "BRL",
        "INR",
        "RUB",
        "ZAR",
        "THB",
        "MYR",
        "IDR",
        "PHP",
        "VND",
        "PLN",
        "CZK",
        "HUF",
        "TRY",
        "AED",
    }

    def validate(
        self, value: Any, constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """Validate money value"""
        if constraints is None:
            constraints = {}

        # Parse money value
        amount = None
        currency = None

        if isinstance(value, str):
            # Format: "123.45 USD"
            match = re.match(r"^(\d+(?:\.\d+)?)\s+([A-Z]{3})$", value.strip())
            if not match:
                return ValidationResult(
                    is_valid=False, message="Invalid money format. Use '123.45 USD'"
                )
            amount_str, currency = match.groups()
            try:
                amount = Decimal(amount_str)
            except (ValueError, InvalidOperation) as e:
                logger.warning(f"Failed to parse amount '{amount_str}': {e}")
                return ValidationResult(is_valid=False, message="Invalid amount format")
        elif isinstance(value, dict):
            amount = value.get("amount")
            currency = value.get("currency")
            if amount is None or currency is None:
                return ValidationResult(
                    is_valid=False, message="Money object must have 'amount' and 'currency' fields"
                )
            try:
                amount = Decimal(str(amount))
            except (ValueError, InvalidOperation) as e:
                logger.warning(f"Failed to parse amount '{amount}': {e}")
                return ValidationResult(is_valid=False, message="Invalid amount format")
        else:
            return ValidationResult(is_valid=False, message="Money value must be string or object")

        # Validate amount using generic numeric constraints
        # Map money-specific constraints to generic ones
        numeric_constraints = {}
        if "minAmount" in constraints:
            numeric_constraints["minimum"] = constraints["minAmount"]
        if "maxAmount" in constraints:
            numeric_constraints["maximum"] = constraints["maxAmount"]

        if numeric_constraints:
            result = ConstraintValidator.validate_constraints(
                amount, "decimal", numeric_constraints
            )
            if not result.is_valid:
                # Customize error message for money context
                if "minimum" in numeric_constraints and "too small" in result.message:
                    return ValidationResult(
                        is_valid=False,
                        message=f"Amount must be at least {numeric_constraints['minimum']}",
                    )
                elif "maximum" in numeric_constraints and "too large" in result.message:
                    return ValidationResult(
                        is_valid=False,
                        message=f"Amount must be at most {numeric_constraints['maximum']}",
                    )
                return result

        # Validate currency
        if currency not in self.COMMON_CURRENCIES:
            return ValidationResult(
                is_valid=False, message=f"Unsupported currency code: {currency}"
            )

        # Check allowed currencies constraint
        if "allowedCurrencies" in constraints:
            if currency not in constraints["allowedCurrencies"]:
                return ValidationResult(
                    is_valid=False,
                    message=f"Currency must be one of: {constraints['allowedCurrencies']}",
                )

        # Validate decimal places using precision constraint
        decimal_places = constraints.get("decimalPlaces", 2)
        precision_result = ConstraintValidator.validate_constraints(
            amount, "decimal", {"precision": decimal_places}
        )
        if not precision_result.is_valid:
            return ValidationResult(
                is_valid=False,
                message=f"Amount cannot have more than {decimal_places} decimal places",
            )

        # Round to specified decimal places
        amount = amount.quantize(Decimal(f"0.{'0' * decimal_places}"))

        result = {
            "amount": float(amount),
            "currency": currency,
            "formatted": f"{amount} {currency}",
            "decimal_amount": str(amount),
        }

        return ValidationResult(
            is_valid=True,
            message="Valid money",
            normalized_value=result,
            metadata={"type": "money", "currency": currency, "decimal_places": decimal_places},
        )

    def normalize(self, value: Any) -> Any:
        """Normalize money value"""
        result = self.validate(value)
        if result.is_valid:
            return result.normalized_value
        return value

    def get_supported_types(self) -> List[str]:
        """Get supported types"""
        return [DataType.MONEY.value, "money", "currency"]

    @classmethod
    def is_valid_currency(cls, currency: str) -> bool:
        """Check if currency code is valid"""
        return currency in cls.COMMON_CURRENCIES
