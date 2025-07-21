"""
Complex type serializer for SPICE HARVESTER
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
import logging

from ..models.common import DataType

logger = logging.getLogger(__name__)


class ComplexTypeSerializer:
    """Complex type serializer for converting between internal and external representations"""

    @staticmethod
    def serialize(
        value: Any, data_type: str, constraints: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Serialize a complex type value to string representation

        Args:
            value: The value to serialize
            data_type: The data type
            constraints: Optional constraints

        Returns:
            Tuple of (serialized_string, metadata)
        """
        if constraints is None:
            constraints = {}

        try:
            if data_type == DataType.ARRAY.value:
                return ComplexTypeSerializer._serialize_array(value, constraints)
            elif data_type == DataType.OBJECT.value:
                return ComplexTypeSerializer._serialize_object(value, constraints)
            elif data_type == DataType.ENUM.value:
                return ComplexTypeSerializer._serialize_enum(value, constraints)
            elif data_type == DataType.MONEY.value:
                return ComplexTypeSerializer._serialize_money(value, constraints)
            elif data_type == DataType.PHONE.value:
                return ComplexTypeSerializer._serialize_phone(value, constraints)
            elif data_type == DataType.EMAIL.value:
                return ComplexTypeSerializer._serialize_email(value, constraints)
            elif data_type == DataType.COORDINATE.value:
                return ComplexTypeSerializer._serialize_coordinate(value, constraints)
            elif data_type == DataType.ADDRESS.value:
                return ComplexTypeSerializer._serialize_address(value, constraints)
            elif data_type == DataType.IMAGE.value:
                return ComplexTypeSerializer._serialize_image(value, constraints)
            elif data_type == DataType.FILE.value:
                return ComplexTypeSerializer._serialize_file(value, constraints)
            else:
                # Default serialization
                return str(value), {"type": data_type}
        except Exception as e:
            return str(value), {"type": data_type, "error": str(e)}

    @staticmethod
    def deserialize(serialized_value: str, data_type: str, metadata: Dict[str, Any]) -> Any:
        """
        Deserialize a string representation back to complex type value

        Args:
            serialized_value: The serialized string
            data_type: The data type
            metadata: Metadata from serialization

        Returns:
            Deserialized value
        """
        try:
            if data_type == DataType.ARRAY.value:
                return ComplexTypeSerializer._deserialize_array(serialized_value, metadata)
            elif data_type == DataType.OBJECT.value:
                return ComplexTypeSerializer._deserialize_object(serialized_value, metadata)
            elif data_type == DataType.ENUM.value:
                return ComplexTypeSerializer._deserialize_enum(serialized_value, metadata)
            elif data_type == DataType.MONEY.value:
                return ComplexTypeSerializer._deserialize_money(serialized_value, metadata)
            elif data_type == DataType.PHONE.value:
                return ComplexTypeSerializer._deserialize_phone(serialized_value, metadata)
            elif data_type == DataType.EMAIL.value:
                return ComplexTypeSerializer._deserialize_email(serialized_value, metadata)
            elif data_type == DataType.COORDINATE.value:
                return ComplexTypeSerializer._deserialize_coordinate(serialized_value, metadata)
            elif data_type == DataType.ADDRESS.value:
                return ComplexTypeSerializer._deserialize_address(serialized_value, metadata)
            elif data_type == DataType.IMAGE.value:
                return ComplexTypeSerializer._deserialize_image(serialized_value, metadata)
            elif data_type == DataType.FILE.value:
                return ComplexTypeSerializer._deserialize_file(serialized_value, metadata)
            else:
                # Default deserialization
                return serialized_value
        except Exception as e:
            logger.warning(f"Failed to deserialize {data_type} value: {e}")
            return serialized_value

    @staticmethod
    def _serialize_array(value: Any, constraints: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Serialize array value"""
        serialized = json.dumps(value, ensure_ascii=False)
        metadata = {
            "type": DataType.ARRAY.value,
            "length": len(value) if isinstance(value, list) else 0,
            "itemType": constraints.get("itemType"),
            "constraints": constraints,
        }
        return serialized, metadata

    @staticmethod
    def _serialize_object(value: Any, constraints: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Serialize object value"""
        serialized = json.dumps(value, ensure_ascii=False)
        metadata = {
            "type": DataType.OBJECT.value,
            "keys": list(value.keys()) if isinstance(value, dict) else [],
            "constraints": constraints,
        }
        return serialized, metadata

    @staticmethod
    def _serialize_enum(value: Any, constraints: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Serialize enum value"""
        serialized = str(value)
        metadata = {
            "type": DataType.ENUM.value,
            "allowedValues": constraints.get("enum", []),
            "constraints": constraints,
        }
        return serialized, metadata

    @staticmethod
    def _serialize_money(value: Any, constraints: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Serialize money value"""
        if isinstance(value, dict):
            serialized = json.dumps(value, ensure_ascii=False)
            metadata = {
                "type": DataType.MONEY.value,
                "amount": value.get("amount"),
                "currency": value.get("currency"),
                "format": "object",
                "constraints": constraints,
            }
        else:
            serialized = str(value)
            metadata = {
                "type": DataType.MONEY.value,
                "format": "string",
                "constraints": constraints,
            }
        return serialized, metadata

    @staticmethod
    def _serialize_phone(value: Any, constraints: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Serialize phone value"""
        if isinstance(value, dict):
            serialized = json.dumps(value, ensure_ascii=False)
            metadata = {
                "type": DataType.PHONE.value,
                "format": "object",
                "original": value.get("original"),
                "e164": value.get("e164"),
                "constraints": constraints,
            }
        else:
            serialized = str(value)
            metadata = {
                "type": DataType.PHONE.value,
                "format": "string",
                "constraints": constraints,
            }
        return serialized, metadata

    @staticmethod
    def _serialize_email(value: Any, constraints: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Serialize email value"""
        if isinstance(value, dict):
            serialized = json.dumps(value, ensure_ascii=False)
            metadata = {
                "type": DataType.EMAIL.value,
                "format": "object",
                "email": value.get("email"),
                "domain": value.get("domain"),
                "constraints": constraints,
            }
        else:
            serialized = str(value)
            metadata = {
                "type": DataType.EMAIL.value,
                "format": "string",
                "constraints": constraints,
            }
        return serialized, metadata

    @staticmethod
    def _serialize_coordinate(
        value: Any, constraints: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Serialize coordinate value"""
        if isinstance(value, dict):
            serialized = json.dumps(value, ensure_ascii=False)
            metadata = {
                "type": DataType.COORDINATE.value,
                "format": "object",
                "latitude": value.get("latitude"),
                "longitude": value.get("longitude"),
                "constraints": constraints,
            }
        else:
            serialized = str(value)
            metadata = {
                "type": DataType.COORDINATE.value,
                "format": "string",
                "constraints": constraints,
            }
        return serialized, metadata

    @staticmethod
    def _serialize_address(value: Any, constraints: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Serialize address value"""
        if isinstance(value, dict):
            serialized = json.dumps(value, ensure_ascii=False)
            metadata = {
                "type": DataType.ADDRESS.value,
                "format": "object",
                "formatted": value.get("formatted"),
                "constraints": constraints,
            }
        else:
            serialized = str(value)
            metadata = {
                "type": DataType.ADDRESS.value,
                "format": "string",
                "constraints": constraints,
            }
        return serialized, metadata

    @staticmethod
    def _serialize_image(value: Any, constraints: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Serialize image value"""
        if isinstance(value, dict):
            serialized = json.dumps(value, ensure_ascii=False)
            metadata = {
                "type": DataType.IMAGE.value,
                "format": "object",
                "url": value.get("url"),
                "extension": value.get("extension"),
                "constraints": constraints,
            }
        else:
            serialized = str(value)
            metadata = {
                "type": DataType.IMAGE.value,
                "format": "string",
                "constraints": constraints,
            }
        return serialized, metadata

    @staticmethod
    def _serialize_file(value: Any, constraints: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Serialize file value"""
        if isinstance(value, dict):
            serialized = json.dumps(value, ensure_ascii=False)
            metadata = {
                "type": DataType.FILE.value,
                "format": "object",
                "url": value.get("url"),
                "name": value.get("name"),
                "size": value.get("size"),
                "mimeType": value.get("mimeType"),
                "constraints": constraints,
            }
        else:
            serialized = str(value)
            metadata = {"type": DataType.FILE.value, "format": "string", "constraints": constraints}
        return serialized, metadata

    @staticmethod
    def _deserialize_array(serialized_value: str, metadata: Dict[str, Any]) -> List[Any]:
        """Deserialize array value"""
        try:
            return json.loads(serialized_value)
        except json.JSONDecodeError:
            return []

    @staticmethod
    def _deserialize_object(serialized_value: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize object value"""
        try:
            return json.loads(serialized_value)
        except json.JSONDecodeError:
            return {}

    @staticmethod
    def _deserialize_enum(serialized_value: str, metadata: Dict[str, Any]) -> Any:
        """Deserialize enum value"""
        # Try to convert to appropriate type
        try:
            # Try int first
            return int(serialized_value)
        except ValueError:
            try:
                # Try float
                return float(serialized_value)
            except ValueError:
                # Try boolean
                if serialized_value.lower() in ["true", "false"]:
                    return serialized_value.lower() == "true"
                # Default to string
                return serialized_value

    @staticmethod
    def _deserialize_money(serialized_value: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize money value"""
        if metadata.get("format") == "object":
            try:
                return json.loads(serialized_value)
            except json.JSONDecodeError:
                return {"amount": 0, "currency": "USD"}
        else:
            # Parse string format like "123.45 USD"
            parts = serialized_value.split()
            if len(parts) >= 2:
                try:
                    amount = float(parts[0])
                    currency = parts[1]
                    return {"amount": amount, "currency": currency}
                except ValueError:
                    # Expected when parsing money string fails
                    pass
            return {"amount": 0, "currency": "USD"}

    @staticmethod
    def _deserialize_phone(serialized_value: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize phone value"""
        if metadata.get("format") == "object":
            try:
                return json.loads(serialized_value)
            except json.JSONDecodeError:
                return {"original": serialized_value}
        else:
            return {"original": serialized_value}

    @staticmethod
    def _deserialize_email(serialized_value: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize email value"""
        if metadata.get("format") == "object":
            try:
                return json.loads(serialized_value)
            except json.JSONDecodeError:
                return {"email": serialized_value}
        else:
            return {"email": serialized_value}

    @staticmethod
    def _deserialize_coordinate(serialized_value: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize coordinate value"""
        if metadata.get("format") == "object":
            try:
                return json.loads(serialized_value)
            except json.JSONDecodeError:
                return {"latitude": 0, "longitude": 0}
        else:
            # Parse string format like "37.7749,-122.4194"
            parts = serialized_value.split(",")
            if len(parts) == 2:
                try:
                    lat = float(parts[0])
                    lon = float(parts[1])
                    return {"latitude": lat, "longitude": lon}
                except ValueError:
                    # Expected when parsing money string fails
                    pass
            return {"latitude": 0, "longitude": 0}

    @staticmethod
    def _deserialize_address(serialized_value: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize address value"""
        if metadata.get("format") == "object":
            try:
                return json.loads(serialized_value)
            except json.JSONDecodeError:
                return {"formatted": serialized_value}
        else:
            return {"formatted": serialized_value}

    @staticmethod
    def _deserialize_image(serialized_value: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize image value"""
        if metadata.get("format") == "object":
            try:
                return json.loads(serialized_value)
            except json.JSONDecodeError:
                return {"url": serialized_value}
        else:
            return {"url": serialized_value}

    @staticmethod
    def _deserialize_file(serialized_value: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize file value"""
        if metadata.get("format") == "object":
            try:
                return json.loads(serialized_value)
            except json.JSONDecodeError:
                return {"url": serialized_value, "name": "unknown"}
        else:
            return {"url": serialized_value, "name": "unknown"}


if __name__ == "__main__":
    # Test the serializer
    print("🔥 THINK ULTRA! ComplexTypeSerializer Testing")
    print("=" * 50)

    # Test array serialization
    array_value = [1, 2, 3, 4, 5]
    serialized, metadata = ComplexTypeSerializer.serialize(array_value, DataType.ARRAY.value)
    print(f"Array serialized: {serialized}")
    print(f"Array metadata: {metadata}")

    deserialized = ComplexTypeSerializer.deserialize(serialized, DataType.ARRAY.value, metadata)
    print(f"Array deserialized: {deserialized}")

    # Test money serialization
    money_value = {"amount": 123.45, "currency": "USD", "formatted": "123.45 USD"}
    serialized, metadata = ComplexTypeSerializer.serialize(money_value, DataType.MONEY.value)
    print(f"Money serialized: {serialized}")
    print(f"Money metadata: {metadata}")

    deserialized = ComplexTypeSerializer.deserialize(serialized, DataType.MONEY.value, metadata)
    print(f"Money deserialized: {deserialized}")

    # Test coordinate serialization
    coord_value = {"latitude": 37.7749, "longitude": -122.4194, "formatted": "37.7749,-122.4194"}
    serialized, metadata = ComplexTypeSerializer.serialize(coord_value, DataType.COORDINATE.value)
    print(f"Coordinate serialized: {serialized}")
    print(f"Coordinate metadata: {metadata}")

    deserialized = ComplexTypeSerializer.deserialize(
        serialized, DataType.COORDINATE.value, metadata
    )
    print(f"Coordinate deserialized: {deserialized}")

    print("\n✅ ComplexTypeSerializer tests completed!")
