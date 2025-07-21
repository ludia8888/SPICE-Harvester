"""
Common data types and enums for SPICE HARVESTER
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


class DataType(Enum):
    """Data type enumeration"""

    STRING = "xsd:string"
    INTEGER = "xsd:integer"
    FLOAT = "xsd:float"
    DOUBLE = "xsd:double"
    BOOLEAN = "xsd:boolean"
    DATE = "xsd:date"
    DATETIME = "xsd:dateTime"
    URI = "xsd:anyURI"
    DECIMAL = "xsd:decimal"
    LONG = "xsd:long"
    SHORT = "xsd:short"
    BYTE = "xsd:byte"
    UNSIGNED_INT = "xsd:unsignedInt"
    UNSIGNED_LONG = "xsd:unsignedLong"
    UNSIGNED_SHORT = "xsd:unsignedShort"
    UNSIGNED_BYTE = "xsd:unsignedByte"
    BASE64_BINARY = "xsd:base64Binary"
    HEX_BINARY = "xsd:hexBinary"
    DURATION = "xsd:duration"
    TIME = "xsd:time"
    GYEAR = "xsd:gYear"
    GMONTH = "xsd:gMonth"
    GDAY = "xsd:gDay"
    GYEAR_MONTH = "xsd:gYearMonth"
    GMONTH_DAY = "xsd:gMonthDay"

    # Complex types
    ARRAY = "array"
    OBJECT = "object"
    ENUM = "enum"
    MONEY = "money"
    PHONE = "phone"
    EMAIL = "email"
    COORDINATE = "coordinate"
    ADDRESS = "address"
    IMAGE = "image"
    FILE = "file"

    @classmethod
    def from_python_type(cls, py_type: type) -> "DataType":
        """Convert Python type to DataType"""
        type_mapping = {
            str: cls.STRING,
            int: cls.INTEGER,
            float: cls.FLOAT,
            bool: cls.BOOLEAN,
        }
        return type_mapping.get(py_type, cls.STRING)

    @classmethod
    def is_numeric(cls, data_type: "DataType") -> bool:
        """Check if data type is numeric"""
        numeric_types = {
            cls.INTEGER,
            cls.FLOAT,
            cls.DOUBLE,
            cls.DECIMAL,
            cls.LONG,
            cls.SHORT,
            cls.BYTE,
            cls.UNSIGNED_INT,
            cls.UNSIGNED_LONG,
            cls.UNSIGNED_SHORT,
            cls.UNSIGNED_BYTE,
        }
        return data_type in numeric_types

    @classmethod
    def is_temporal(cls, data_type: "DataType") -> bool:
        """Check if data type is temporal"""
        temporal_types = {
            cls.DATE,
            cls.DATETIME,
            cls.TIME,
            cls.DURATION,
            cls.GYEAR,
            cls.GMONTH,
            cls.GDAY,
            cls.GYEAR_MONTH,
            cls.GMONTH_DAY,
        }
        return data_type in temporal_types

    def validate_value(self, value: Any) -> bool:
        """Validate if value matches this data type"""
        if value is None:
            return True

        try:
            if self == DataType.STRING:
                return isinstance(value, str)
            elif self == DataType.INTEGER:
                return isinstance(value, int)
            elif self == DataType.FLOAT or self == DataType.DOUBLE:
                return isinstance(value, (int, float))
            elif self == DataType.BOOLEAN:
                return isinstance(value, bool)
            elif self == DataType.DECIMAL:
                return isinstance(value, (int, float))
            else:
                # For other types, convert to string and check
                return isinstance(str(value), str)
        except Exception as e:
            logger.debug(f"Failed to validate {self} for value {value}: {e}")
            return False

    @classmethod
    def is_complex_type(cls, data_type: str) -> bool:
        """Check if data type is complex"""
        complex_types = {
            cls.ARRAY.value,
            cls.OBJECT.value,
            cls.ENUM.value,
            cls.MONEY.value,
            cls.PHONE.value,
            cls.EMAIL.value,
            cls.COORDINATE.value,
            cls.ADDRESS.value,
            cls.IMAGE.value,
            cls.FILE.value,
        }
        return data_type.lower() in complex_types

    @classmethod
    def get_base_type(cls, data_type: str) -> str:
        """Get base type for complex types"""
        data_type_lower = data_type.lower()

        if "string" in data_type_lower or "text" in data_type_lower:
            return cls.STRING.value
        elif "int" in data_type_lower or "number" in data_type_lower:
            return cls.INTEGER.value
        elif "float" in data_type_lower or "decimal" in data_type_lower:
            return cls.FLOAT.value
        elif "bool" in data_type_lower:
            return cls.BOOLEAN.value
        elif "date" in data_type_lower:
            return cls.DATE.value
        else:
            return cls.STRING.value  # Default to string


class Cardinality(Enum):
    """Cardinality enumeration"""

    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:n"
    MANY_TO_ONE = "n:1"
    MANY_TO_MANY = "n:n"
    ONE = "one"
    MANY = "many"

    @classmethod
    def is_valid(cls, value: str) -> bool:
        """Check if value is a valid cardinality"""
        return value in [c.value for c in cls]


@dataclass
class QueryOperator:
    """Query operator definition"""

    name: str
    symbol: str
    description: str
    applies_to: List[DataType]

    def can_apply_to(self, data_type: DataType) -> bool:
        """Check if operator can apply to data type"""
        return data_type in self.applies_to


# Common query operators
QUERY_OPERATORS = {
    "eq": QueryOperator(
        name="equals", symbol="=", description="Equal to", applies_to=list(DataType)
    ),
    "ne": QueryOperator(
        name="not_equals", symbol="!=", description="Not equal to", applies_to=list(DataType)
    ),
    "gt": QueryOperator(
        name="greater_than",
        symbol=">",
        description="Greater than",
        applies_to=[dt for dt in DataType if DataType.is_numeric(dt) or DataType.is_temporal(dt)],
    ),
    "gte": QueryOperator(
        name="greater_than_or_equal",
        symbol=">=",
        description="Greater than or equal to",
        applies_to=[dt for dt in DataType if DataType.is_numeric(dt) or DataType.is_temporal(dt)],
    ),
    "lt": QueryOperator(
        name="less_than",
        symbol="<",
        description="Less than",
        applies_to=[dt for dt in DataType if DataType.is_numeric(dt) or DataType.is_temporal(dt)],
    ),
    "lte": QueryOperator(
        name="less_than_or_equal",
        symbol="<=",
        description="Less than or equal to",
        applies_to=[dt for dt in DataType if DataType.is_numeric(dt) or DataType.is_temporal(dt)],
    ),
    "contains": QueryOperator(
        name="contains",
        symbol="contains",
        description="Contains substring",
        applies_to=[DataType.STRING],
    ),
    "starts_with": QueryOperator(
        name="starts_with",
        symbol="starts_with",
        description="Starts with substring",
        applies_to=[DataType.STRING],
    ),
    "ends_with": QueryOperator(
        name="ends_with",
        symbol="ends_with",
        description="Ends with substring",
        applies_to=[DataType.STRING],
    ),
    "in": QueryOperator(
        name="in", symbol="in", description="In list of values", applies_to=list(DataType)
    ),
    "not_in": QueryOperator(
        name="not_in",
        symbol="not_in",
        description="Not in list of values",
        applies_to=list(DataType),
    ),
}


# Import the standardized ApiResponse
from .requests import ApiResponse

# Backward compatibility alias for BaseResponse
# TODO: Remove this alias after all services migrate to ApiResponse
BaseResponse = ApiResponse
