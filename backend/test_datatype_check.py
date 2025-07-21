#!/usr/bin/env python3
"""
Check DataType.ENUM.value
"""

from shared.models.common import DataType

print(f"DataType.ENUM.value = '{DataType.ENUM.value}'")
print(f"DataType.ENUM = {DataType.ENUM}")

# Test what the enum validator returns
from shared.validators.enum_validator import EnumValidator

validator = EnumValidator()
supported = validator.get_supported_types()
print(f"EnumValidator.get_supported_types() = {supported}")