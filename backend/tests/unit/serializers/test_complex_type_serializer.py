from __future__ import annotations

from shared.models.common import DataType
from shared.serializers.complex_type_serializer import ComplexTypeSerializer


def test_array_roundtrip() -> None:
    value = [1, 2, 3]
    serialized, metadata = ComplexTypeSerializer.serialize(value, DataType.ARRAY.value)
    assert metadata["length"] == 3

    deserialized = ComplexTypeSerializer.deserialize(serialized, DataType.ARRAY.value, metadata)
    assert deserialized == value


def test_object_roundtrip() -> None:
    value = {"name": "Alice", "age": 30}
    serialized, metadata = ComplexTypeSerializer.serialize(value, DataType.OBJECT.value)
    assert "name" in metadata["keys"]

    deserialized = ComplexTypeSerializer.deserialize(serialized, DataType.OBJECT.value, metadata)
    assert deserialized == value


def test_enum_serialization() -> None:
    serialized, metadata = ComplexTypeSerializer.serialize("ACTIVE", DataType.ENUM.value, {"enum": ["ACTIVE"]})
    assert metadata["allowedValues"] == ["ACTIVE"]

    deserialized = ComplexTypeSerializer.deserialize(serialized, DataType.ENUM.value, metadata)
    assert deserialized == "ACTIVE"


def test_money_serialization_object() -> None:
    value = {"amount": 10.5, "currency": "USD"}
    serialized, metadata = ComplexTypeSerializer.serialize(value, DataType.MONEY.value)
    assert metadata["format"] == "object"

    deserialized = ComplexTypeSerializer.deserialize(serialized, DataType.MONEY.value, metadata)
    assert deserialized["currency"] == "USD"


def test_coordinate_string_deserialization() -> None:
    serialized, metadata = ComplexTypeSerializer.serialize("37.0,-122.0", DataType.COORDINATE.value)
    deserialized = ComplexTypeSerializer.deserialize(serialized, DataType.COORDINATE.value, metadata)
    assert deserialized["latitude"] == 37.0


def test_image_and_file_serialization() -> None:
    image_value = {"url": "https://example.com/a.png", "extension": "png"}
    serialized, metadata = ComplexTypeSerializer.serialize(image_value, DataType.IMAGE.value)
    deserialized = ComplexTypeSerializer.deserialize(serialized, DataType.IMAGE.value, metadata)
    assert deserialized["url"] == image_value["url"]

    file_value = {"url": "https://example.com/a.pdf", "name": "a.pdf"}
    serialized, metadata = ComplexTypeSerializer.serialize(file_value, DataType.FILE.value)
    deserialized = ComplexTypeSerializer.deserialize(serialized, DataType.FILE.value, metadata)
    assert deserialized["name"] == file_value["name"]
