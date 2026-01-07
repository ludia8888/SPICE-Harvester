from shared.validators import (
    ArrayValidator,
    CipherValidator,
    GeoPointValidator,
    GeoShapeValidator,
    MarkingValidator,
    StructValidator,
    StringValidator,
    VectorValidator,
    get_validator,
)


def test_array_validator_rejects_null_items():
    validator = ArrayValidator()
    result = validator.validate([1, None, 3], {"noNullItems": True})
    assert not result.is_valid


def test_array_validator_rejects_nested_arrays():
    validator = ArrayValidator()
    result = validator.validate([1, [2, 3]], {"noNestedArrays": True})
    assert not result.is_valid


def test_struct_validator_rejects_nested_struct():
    validator = StructValidator()
    result = validator.validate({"a": {"b": 1}}, {})
    assert not result.is_valid


def test_struct_validator_rejects_array_field():
    validator = StructValidator()
    result = validator.validate({"a": [1, 2]}, {})
    assert not result.is_valid


def test_geopoint_validator_accepts_latlon():
    validator = GeoPointValidator()
    result = validator.validate("37.5,127.0", {})
    assert result.is_valid


def test_geopoint_validator_rejects_out_of_range():
    validator = GeoPointValidator()
    result = validator.validate("200,200", {})
    assert not result.is_valid


def test_geoshape_validator_accepts_point():
    validator = GeoShapeValidator()
    result = validator.validate({"type": "Point", "coordinates": [127.0, 37.5]}, {})
    assert result.is_valid


def test_geoshape_validator_rejects_invalid_type():
    validator = GeoShapeValidator()
    result = validator.validate({"type": "BadType", "coordinates": [127.0, 37.5]}, {})
    assert not result.is_valid


def test_vector_validator_accepts_numeric_list():
    validator = VectorValidator()
    result = validator.validate([0.1, 0.2, 0.3], {"dimensions": 3})
    assert result.is_valid


def test_vector_validator_rejects_wrong_dimensions():
    validator = VectorValidator()
    result = validator.validate([0.1, 0.2], {"dimensions": 3})
    assert not result.is_valid


def test_marking_validator_rejects_non_string():
    validator = MarkingValidator()
    result = validator.validate(123, {})
    assert not result.is_valid


def test_cipher_validator_rejects_non_string():
    validator = CipherValidator()
    result = validator.validate(123, {})
    assert not result.is_valid


def test_media_uses_string_validator():
    validator = get_validator("media")
    assert isinstance(validator, StringValidator)
    result = validator.validate(123, {})
    assert not result.is_valid


def test_attachment_uses_string_validator():
    validator = get_validator("attachment")
    assert isinstance(validator, StringValidator)
    result = validator.validate(123, {})
    assert not result.is_valid


def test_time_series_uses_string_validator():
    validator = get_validator("time_series")
    assert isinstance(validator, StringValidator)
    result = validator.validate(123, {})
    assert not result.is_valid
