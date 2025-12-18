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

    # Common currency symbols mapped to ISO 4217 codes (best-effort)
    CURRENCY_SYMBOLS = {
        "$": "USD",
        "€": "EUR",
        "£": "GBP",
        "₩": "KRW",
        "₹": "INR",
        "₽": "RUB",
        "₺": "TRY",
        "฿": "THB",
        "₫": "VND",
        "₱": "PHP",
        "₪": "ILS",
    }

    # Symbols that are inherently ambiguous without locale/context (best-effort default is CNY)
    AMBIGUOUS_SYMBOLS = {"¥", "￥"}

    # Common currency unit words / aliases mapped to ISO 4217 codes (best-effort)
    CURRENCY_WORD_ALIASES = {
        # Korean
        "원": "KRW",
        "원화": "KRW",
        # Chinese
        "元": "CNY",
        "人民币": "CNY",
        "人民幣": "CNY",
        # Japanese
        "円": "JPY",
    }

    # Non-ISO but common currency codes mapped to ISO 4217 codes (best-effort)
    CURRENCY_CODE_ALIASES = {
        "RMB": "CNY",
        "CNH": "CNY",
    }

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

    @classmethod
    def _normalize_currency_token(cls, token: str, constraints: Dict[str, Any]) -> Optional[str]:
        """Normalize a currency token (symbol/code/unit) into an ISO 4217 code."""
        if not token:
            return None

        # Exact symbol mapping
        if token in cls.CURRENCY_SYMBOLS:
            return cls.CURRENCY_SYMBOLS[token]

        # Ambiguous symbols (¥/￥): pick best-effort default or honor constraints
        if token in cls.AMBIGUOUS_SYMBOLS:
            allowed = constraints.get("allowedCurrencies")
            if isinstance(allowed, list):
                allowed_set = {str(a).upper() for a in allowed}
                candidates = [c for c in ("CNY", "JPY") if c in allowed_set]
                if len(candidates) == 1:
                    return candidates[0]

            default_currency = constraints.get("defaultCurrency")
            if isinstance(default_currency, str):
                default_currency = default_currency.upper()
                if default_currency in {"CNY", "JPY"}:
                    return default_currency

            # Domain-neutral fallback: treat ¥ as "JPY" unless context (allowed/default) says otherwise.
            # Callers can disambiguate by passing constraints (allowedCurrencies/defaultCurrency).
            return "JPY"

        # Unit words / localized aliases
        if token in cls.CURRENCY_WORD_ALIASES:
            return cls.CURRENCY_WORD_ALIASES[token]

        # Currency codes (ISO-ish) + alias normalization
        code = str(token).upper()
        return cls.CURRENCY_CODE_ALIASES.get(code, code)

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
            s = value.strip()
            if not s:
                return ValidationResult(is_valid=False, message="Money value cannot be empty")

            # Handle parentheses for negatives: (123.45 USD)
            negative = False
            if s.startswith("(") and s.endswith(")"):
                negative = True
                s = s[1:-1].strip()

            # Extract leading sign
            sign = ""
            if s.startswith(("+", "-")):
                sign = s[0]
                s = s[1:].strip()
            if negative and sign != "-":
                sign = "-"

            currency_token: Optional[str] = None
            amount_str: Optional[str] = None

            # 0) Local unit suffix/prefix: 15,000원 / 150元 / 200円
            m = re.match(r"^(.+?)\s*(원화|원|元|円)$", s)
            if m and any(ch.isdigit() for ch in m.group(1)):
                amount_str, currency_token = m.group(1).strip(), m.group(2)

            if currency_token is None:
                m = re.match(r"^(원화|원|元|円)\s*(.+)$", s)
                if m and any(ch.isdigit() for ch in m.group(2)):
                    currency_token, amount_str = m.group(1), m.group(2).strip()

            # 1) "123.45 USD" / "123.45USD"
            if currency_token is None:
                m = re.match(r"^(.+?)\s*([A-Za-z]{3})$", s)
                if m and any(ch.isdigit() for ch in m.group(1)):
                    amount_str, currency_token = m.group(1).strip(), m.group(2).upper()

            # 2) "USD 123.45"
            if currency_token is None:
                m = re.match(r"^([A-Za-z]{3})\s+(.+)$", s)
                if m:
                    currency_token, amount_str = m.group(1).upper(), m.group(2).strip()

            # 3) "$123.45" / "€ 1.234,56"
            if currency_token is None and s and (s[0] in self.CURRENCY_SYMBOLS or s[0] in self.AMBIGUOUS_SYMBOLS):
                currency_token = s[0]
                amount_str = s[1:].strip()

            # 4) "123.45$"
            if currency_token is None and s and (s[-1] in self.CURRENCY_SYMBOLS or s[-1] in self.AMBIGUOUS_SYMBOLS):
                currency_token = s[-1]
                amount_str = s[:-1].strip()

            if currency_token is None or amount_str is None:
                return ValidationResult(
                    is_valid=False,
                    message="Invalid money format. Examples: '123.45 USD', 'USD 123.45', '$123.45', '€1.234,56'",
                )

            currency = self._normalize_currency_token(currency_token, constraints)
            if currency is None:
                return ValidationResult(
                    is_valid=False,
                    message="Invalid money format: currency not recognized",
                )

            # Normalize numeric string (supports US/EU thousands/decimal separators)
            normalized_number = self._normalize_number_string(amount_str)
            if normalized_number is None:
                return ValidationResult(is_valid=False, message="Invalid amount format")

            try:
                amount = Decimal(f"{sign}{normalized_number}")
            except (ValueError, InvalidOperation) as e:
                logger.warning(f"Failed to parse amount '{amount_str}' -> '{normalized_number}': {e}")
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

    @classmethod
    def _normalize_number_string(cls, raw: str) -> Optional[str]:
        """
        Normalize a locale-variant numeric string into a plain decimal representation.

        Supports:
        - 1,234.56 (US)
        - 1.234,56 (EU)
        - 1234,56 (comma decimal)
        - 1 234 567.89 (space thousands)
        """
        s = raw.strip()
        if not s:
            return None

        # Drop spaces/underscores used as grouping separators
        s = s.replace(" ", "").replace("_", "")

        if "," in s and "." in s:
            # Decide decimal separator by last occurrence
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            parts = s.split(",")
            # If all groups after the first are 3 digits -> thousands separators
            if len(parts) >= 2 and all(len(p) == 3 for p in parts[1:]):
                s = "".join(parts)
            else:
                s = s.replace(",", ".")
        elif "." in s:
            # If multiple dots, treat as thousands separators
            if s.count(".") > 1:
                s = s.replace(".", "")

        if not re.fullmatch(r"\d+(?:\.\d+)?", s):
            return None
        return s

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
