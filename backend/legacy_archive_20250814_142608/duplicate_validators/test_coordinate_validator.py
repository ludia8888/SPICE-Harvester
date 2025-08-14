"""
Comprehensive unit tests for CoordinateValidator
Tests all coordinate validation functionality with REAL behavior verification
NO MOCKS - Tests verify actual code behavior
"""

import pytest
from typing import Dict, Any, Optional
import math

from shared.validators.coordinate_validator import CoordinateValidator
from shared.models.common import DataType


class TestCoordinateValidator:
    """Test CoordinateValidator with real behavior verification"""

    def setup_method(self):
        """Setup for each test"""
        self.validator = CoordinateValidator()

    def test_validate_string_format_valid(self):
        """Test validation of valid string format coordinates"""
        # Basic format
        result = self.validator.validate("40.7128,-74.0060")
        assert result.is_valid is True
        assert result.message == "Valid coordinate"
        assert result.normalized_value["latitude"] == 40.7128
        assert result.normalized_value["longitude"] == -74.0060
        assert result.normalized_value["lat"] == 40.7128
        assert result.normalized_value["lon"] == -74.0060
        assert result.normalized_value["formatted"] == "40.7128,-74.006"
        assert result.normalized_value["geojson"]["type"] == "Point"
        assert result.normalized_value["geojson"]["coordinates"] == [-74.0060, 40.7128]
        assert result.metadata["type"] == "coordinate"
        assert result.metadata["precision"] == 6

        # With spaces
        result = self.validator.validate(" 40.7128 , -74.0060 ")
        assert result.is_valid is True
        assert result.normalized_value["latitude"] == 40.7128

    def test_validate_string_format_invalid(self):
        """Test validation of invalid string format coordinates"""
        # Missing longitude
        result = self.validator.validate("40.7128")
        assert result.is_valid is False
        assert result.message == "Invalid coordinate format. Use 'lat,lon'"

        # Too many parts
        result = self.validator.validate("40.7128,-74.0060,100")
        assert result.is_valid is False
        assert result.message == "Invalid coordinate format. Use 'lat,lon'"

        # Invalid numbers
        result = self.validator.validate("abc,def")
        assert result.is_valid is False
        assert result.message == "Invalid coordinate numbers"

        # Empty string
        result = self.validator.validate("")
        assert result.is_valid is False

    def test_validate_dict_format_valid(self):
        """Test validation of valid dict format coordinates"""
        # Full keys
        result = self.validator.validate({"latitude": 40.7128, "longitude": -74.0060})
        assert result.is_valid is True
        assert result.normalized_value["latitude"] == 40.7128
        assert result.normalized_value["longitude"] == -74.0060

        # Short keys - lat/lon
        result = self.validator.validate({"lat": 40.7128, "lon": -74.0060})
        assert result.is_valid is True
        assert result.normalized_value["latitude"] == 40.7128

        # Short keys - lat/lng
        result = self.validator.validate({"lat": 40.7128, "lng": -74.0060})
        assert result.is_valid is True
        assert result.normalized_value["longitude"] == -74.0060

        # String values that can be converted
        result = self.validator.validate({"lat": "40.7128", "lon": "-74.0060"})
        assert result.is_valid is True
        assert result.normalized_value["latitude"] == 40.7128

    def test_validate_dict_format_invalid(self):
        """Test validation of invalid dict format coordinates"""
        # Missing longitude
        result = self.validator.validate({"latitude": 40.7128})
        assert result.is_valid is False
        assert result.message == "Coordinate object must have latitude and longitude"

        # Missing latitude
        result = self.validator.validate({"longitude": -74.0060})
        assert result.is_valid is False
        assert result.message == "Coordinate object must have latitude and longitude"

        # Invalid values
        result = self.validator.validate({"lat": "abc", "lon": "def"})
        assert result.is_valid is False
        assert result.message == "Invalid coordinate numbers"

        # None values
        result = self.validator.validate({"lat": None, "lon": None})
        assert result.is_valid is False
        assert result.message == "Coordinate object must have latitude and longitude"

    def test_validate_array_format_valid(self):
        """Test validation of valid array format coordinates"""
        # List format
        result = self.validator.validate([40.7128, -74.0060])
        assert result.is_valid is True
        assert result.normalized_value["latitude"] == 40.7128
        assert result.normalized_value["longitude"] == -74.0060

        # Tuple format
        result = self.validator.validate((40.7128, -74.0060))
        assert result.is_valid is True
        assert result.normalized_value["latitude"] == 40.7128

        # String values that can be converted
        result = self.validator.validate(["40.7128", "-74.0060"])
        assert result.is_valid is True

    def test_validate_array_format_invalid(self):
        """Test validation of invalid array format coordinates"""
        # Wrong length
        result = self.validator.validate([40.7128])
        assert result.is_valid is False
        assert result.message == "Coordinate must be string, object, or array"

        result = self.validator.validate([40.7128, -74.0060, 100])
        assert result.is_valid is False
        assert result.message == "Coordinate must be string, object, or array"

        # Invalid values
        result = self.validator.validate(["abc", "def"])
        assert result.is_valid is False
        assert result.message == "Invalid coordinate numbers"

        # None values
        result = self.validator.validate([None, None])
        assert result.is_valid is False
        assert result.message == "Invalid coordinate numbers"

    def test_validate_invalid_types(self):
        """Test validation fails for unsupported types"""
        # Integer
        result = self.validator.validate(123)
        assert result.is_valid is False
        assert result.message == "Coordinate must be string, object, or array"

        # Boolean
        result = self.validator.validate(True)
        assert result.is_valid is False

        # None
        result = self.validator.validate(None)
        assert result.is_valid is False

    def test_validate_latitude_range(self):
        """Test validation of latitude range constraints"""
        # Valid range
        result = self.validator.validate("90,-74.0060")
        assert result.is_valid is True

        result = self.validator.validate("-90,-74.0060")
        assert result.is_valid is True

        # Out of range - too high
        result = self.validator.validate("91,-74.0060")
        assert result.is_valid is False
        assert result.message == "Latitude must be between -90 and 90"

        # Out of range - too low
        result = self.validator.validate("-91,-74.0060")
        assert result.is_valid is False
        assert result.message == "Latitude must be between -90 and 90"

    def test_validate_longitude_range(self):
        """Test validation of longitude range constraints"""
        # Valid range
        result = self.validator.validate("40.7128,180")
        assert result.is_valid is True

        result = self.validator.validate("40.7128,-180")
        assert result.is_valid is True

        # Out of range - too high
        result = self.validator.validate("40.7128,181")
        assert result.is_valid is False
        assert result.message == "Longitude must be between -180 and 180"

        # Out of range - too low
        result = self.validator.validate("40.7128,-181")
        assert result.is_valid is False
        assert result.message == "Longitude must be between -180 and 180"

    def test_validate_bounding_box_constraint(self):
        """Test validation with bounding box constraints"""
        constraints = {
            "boundingBox": [40.0, -75.0, 41.0, -73.0]  # min_lat, min_lon, max_lat, max_lon
        }

        # Inside bounding box
        result = self.validator.validate("40.7128,-74.0060", constraints)
        assert result.is_valid is True

        # Outside bounding box - latitude too low
        result = self.validator.validate("39.5,-74.0060", constraints)
        assert result.is_valid is False
        assert result.message == "Coordinate is outside allowed bounding box"

        # Outside bounding box - latitude too high
        result = self.validator.validate("41.5,-74.0060", constraints)
        assert result.is_valid is False
        assert result.message == "Coordinate is outside allowed bounding box"

        # Outside bounding box - longitude too low
        result = self.validator.validate("40.5,-76.0", constraints)
        assert result.is_valid is False
        assert result.message == "Coordinate is outside allowed bounding box"

        # Outside bounding box - longitude too high
        result = self.validator.validate("40.5,-72.0", constraints)
        assert result.is_valid is False
        assert result.message == "Coordinate is outside allowed bounding box"

    def test_validate_precision_constraint(self):
        """Test validation with precision constraints"""
        # Default precision (6)
        result = self.validator.validate("40.7128456789,-74.0060123456")
        assert result.is_valid is True
        assert result.normalized_value["latitude"] == 40.712846
        assert result.normalized_value["longitude"] == -74.006012
        assert result.metadata["precision"] == 6

        # Custom precision
        constraints = {"precision": 3}
        result = self.validator.validate("40.7128456789,-74.0060123456", constraints)
        assert result.is_valid is True
        assert result.normalized_value["latitude"] == 40.713
        assert result.normalized_value["longitude"] == -74.006
        assert result.metadata["precision"] == 3

        # Zero precision
        constraints = {"precision": 0}
        result = self.validator.validate("40.7128,-74.0060", constraints)
        assert result.is_valid is True
        assert result.normalized_value["latitude"] == 41.0
        assert result.normalized_value["longitude"] == -74.0

    def test_normalize_valid_coordinate(self):
        """Test normalize method with valid coordinates"""
        normalized = self.validator.normalize("40.7128,-74.0060")
        assert normalized["latitude"] == 40.7128
        assert normalized["longitude"] == -74.0060
        assert normalized["formatted"] == "40.7128,-74.006"

    def test_normalize_invalid_coordinate(self):
        """Test normalize method with invalid coordinates"""
        # Should return original value if invalid
        assert self.validator.normalize("invalid") == "invalid"
        assert self.validator.normalize(123) == 123
        assert self.validator.normalize(None) is None

    def test_get_supported_types(self):
        """Test get_supported_types method"""
        supported_types = self.validator.get_supported_types()
        assert DataType.COORDINATE.value in supported_types
        assert "coordinate" in supported_types
        assert "coordinates" in supported_types
        assert "geo" in supported_types
        assert "location" in supported_types
        assert "latlng" in supported_types
        assert len(supported_types) == 6

    def test_calculate_distance_basic(self):
        """Test calculate_distance class method with basic cases"""
        # New York to Los Angeles (approximately 3944 km)
        ny = (40.7128, -74.0060)
        la = (34.0522, -118.2437)
        distance = CoordinateValidator.calculate_distance(ny, la)
        assert 3900 < distance < 4000  # Allow some margin for calculation

        # Same location (0 km)
        distance = CoordinateValidator.calculate_distance(ny, ny)
        assert distance == 0

        # London to Paris (approximately 344 km)
        london = (51.5074, -0.1278)
        paris = (48.8566, 2.3522)
        distance = CoordinateValidator.calculate_distance(london, paris)
        assert 340 < distance < 350

    def test_calculate_distance_edge_cases(self):
        """Test calculate_distance with edge cases"""
        # Antipodal points (opposite sides of Earth)
        point1 = (0, 0)
        point2 = (0, 180)
        distance = CoordinateValidator.calculate_distance(point1, point2)
        # Should be approximately half Earth's circumference (~20,000 km)
        assert 19000 < distance < 21000

        # North to South pole
        north_pole = (90, 0)
        south_pole = (-90, 0)
        distance = CoordinateValidator.calculate_distance(north_pole, south_pole)
        # Should be approximately half Earth's circumference
        assert 19000 < distance < 21000

    def test_edge_case_exact_boundaries(self):
        """Test coordinates at exact boundary values"""
        # Exact boundaries should be valid
        boundaries = [
            ("90,0", 90, 0),      # North pole
            ("-90,0", -90, 0),    # South pole
            ("0,180", 0, 180),    # International date line
            ("0,-180", 0, -180),  # International date line (other side)
        ]
        
        for coord_str, expected_lat, expected_lon in boundaries:
            result = self.validator.validate(coord_str)
            assert result.is_valid is True
            assert result.normalized_value["latitude"] == expected_lat
            assert result.normalized_value["longitude"] == expected_lon

    def test_geojson_format(self):
        """Test that GeoJSON format is correct (lon, lat order)"""
        result = self.validator.validate("40.7128,-74.0060")
        assert result.is_valid is True
        
        geojson = result.normalized_value["geojson"]
        assert geojson["type"] == "Point"
        # GeoJSON uses [longitude, latitude] order (opposite of typical)
        assert geojson["coordinates"] == [-74.0060, 40.7128]

    def test_formatted_output_precision(self):
        """Test formatted output respects precision"""
        # Test that formatted string uses the precision
        constraints = {"precision": 2}
        result = self.validator.validate("40.123456,-74.987654", constraints)
        assert result.is_valid is True
        assert result.normalized_value["formatted"] == "40.12,-74.99"

    def test_mixed_format_keys_in_dict(self):
        """Test dict with mixed format keys (should use first found)"""
        # Has both latitude and lat - should use latitude
        result = self.validator.validate({
            "latitude": 40.7128,
            "lat": 50.0,  # This should be ignored
            "longitude": -74.0060,
            "lon": -80.0  # This should be ignored
        })
        assert result.is_valid is True
        assert result.normalized_value["latitude"] == 40.7128
        assert result.normalized_value["longitude"] == -74.0060

    def test_empty_list(self):
        """Test empty list is invalid"""
        result = self.validator.validate([])
        assert result.is_valid is False
        assert result.message == "Coordinate must be string, object, or array"

    def test_bounding_box_wrong_length(self):
        """Test bounding box with wrong number of elements"""
        # Bounding box with only 3 elements (should be ignored)
        constraints = {"boundingBox": [40.0, -75.0, 41.0]}
        result = self.validator.validate("40.5,-74.5", constraints)
        # Should pass because invalid bounding box is ignored
        assert result.is_valid is True

        # Bounding box with 5 elements (should be ignored)
        constraints = {"boundingBox": [40.0, -75.0, 41.0, -73.0, 100]}
        result = self.validator.validate("40.5,-74.5", constraints)
        assert result.is_valid is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])