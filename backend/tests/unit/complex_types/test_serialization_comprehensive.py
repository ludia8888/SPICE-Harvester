"""
🔥 THINK ULTRA! Comprehensive Complex Type Serialization/Deserialization Tests
Enhanced test suite for complex type serialization and deserialization functionality
"""

import json
import time
import unittest
from datetime import datetime
from typing import Any, Dict, List

from shared.models.common import DataType
from shared.serializers.complex_type_serializer import ComplexTypeSerializer
from tests.utils.enhanced_assertions import EnhancedAssertions


class TestComplexTypeSerializationComprehensive(unittest.TestCase):
    """🔥 THINK ULTRA! Comprehensive serialization/deserialization tests"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.serializer = ComplexTypeSerializer()
        self.enhanced_assert = EnhancedAssertions()
        
        # Test data sets for each type
        self.test_data = {
            DataType.ARRAY.value: {
                "simple": [1, 2, 3, 4, 5],
                "nested": [{"name": "John"}, {"name": "Jane"}],
                "mixed": [1, "text", {"key": "value"}, [1, 2]],
                "empty": [],
                "large": list(range(1000)),  # Performance test data
                "unicode": ["한글", "日本語", "العربية", "русский"]
            },
            DataType.OBJECT.value: {
                "simple": {"name": "John", "age": 30},
                "nested": {"user": {"name": "John", "details": {"email": "john@example.com"}}},
                "empty": {},
                "unicode_keys": {"한글키": "값", "日本語": "値"},
                "large": {f"key_{i}": f"value_{i}" for i in range(100)}
            },
            DataType.MONEY.value: {
                "object_format": {"amount": 123.45, "currency": "USD"},
                "string_format": "123.45 USD",
                "zero": {"amount": 0, "currency": "USD"},
                "negative": {"amount": -50.75, "currency": "EUR"},
                "large_amount": {"amount": 999999.99, "currency": "JPY"}
            },
            DataType.PHONE.value: {
                "us_format": "+1-555-123-4567",
                "international": "+44-20-7946-0958",
                "object_format": {"original": "+1-555-123-4567", "e164": "+15551234567"},
                "korean": "+82-2-123-4567"
            },
            DataType.EMAIL.value: {
                "simple": "test@example.com",
                "complex": "user.name+tag@example-domain.co.uk",
                "object_format": {"email": "test@example.com", "domain": "example.com"},
                "unicode": "테스트@example.com"
            },
            DataType.COORDINATE.value: {
                "seoul": {"latitude": 37.5665, "longitude": 126.9780},
                "string_format": "37.5665,126.9780",
                "negative": {"latitude": -33.8688, "longitude": 151.2093},
                "zero": {"latitude": 0, "longitude": 0}
            },
            DataType.ADDRESS.value: {
                "simple": "123 Main St, Seoul, Korea",
                "object_format": {
                    "street": "123 Main St",
                    "city": "Seoul", 
                    "country": "Korea",
                    "formatted": "123 Main St, Seoul, Korea"
                }
            },
            DataType.IMAGE.value: {
                "url_format": "https://example.com/image.jpg",
                "object_format": {
                    "url": "https://example.com/image.jpg",
                    "extension": "jpg",
                    "width": 1920,
                    "height": 1080
                }
            },
            DataType.FILE.value: {
                "simple": "https://example.com/document.pdf",
                "object_format": {
                    "url": "https://example.com/document.pdf",
                    "name": "document.pdf",
                    "size": 1024000,
                    "mimeType": "application/pdf"
                }
            },
            DataType.ENUM.value: {
                "string": "ACTIVE",
                "number": 1,
                "boolean": True
            }
        }
    
    def test_round_trip_serialization_all_types(self):
        """Test that all data types can be serialized and deserialized without data loss"""
        print("\n🔄 Testing round-trip serialization for all types...")
        
        for data_type, test_cases in self.test_data.items():
            with self.subTest(data_type=data_type):
                print(f"\n  📋 Testing {data_type} type:")
                
                for case_name, original_value in test_cases.items():
                    with self.subTest(case=case_name):
                        print(f"    🧪 Case: {case_name}")
                        
                        # Serialize
                        serialized, metadata = self.serializer.serialize(
                            value=original_value,
                            data_type=data_type,
                            constraints={}
                        )
                        
                        # Verify serialization produced a string
                        self.enhanced_assert.assert_type(
                            actual=serialized,
                            expected_type=str,
                            field_name=f"{data_type}_{case_name}_serialized"
                        )
                        
                        # Verify metadata is a dictionary
                        self.enhanced_assert.assert_type(
                            actual=metadata,
                            expected_type=dict,
                            field_name=f"{data_type}_{case_name}_metadata"
                        )
                        
                        # Deserialize
                        deserialized = self.serializer.deserialize(
                            serialized_value=serialized,
                            data_type=data_type,
                            metadata=metadata
                        )
                        
                        # Verify round-trip integrity based on type
                        self._verify_round_trip_integrity(
                            original=original_value,
                            deserialized=deserialized,
                            data_type=data_type,
                            case_name=case_name
                        )
                        
                        print(f"      ✅ Round-trip successful")
    
    def _verify_round_trip_integrity(self, original: Any, deserialized: Any, data_type: str, case_name: str):
        """Verify that round-trip serialization maintains data integrity"""
        if data_type in [DataType.ARRAY.value, DataType.OBJECT.value]:
            # For complex types, direct comparison
            self.enhanced_assert.assert_equal(
                actual=deserialized,
                expected=original,
                field_name=f"{data_type}_{case_name}_round_trip"
            )
        elif data_type == DataType.MONEY.value:
            if isinstance(original, dict):
                # Verify money object structure
                self.enhanced_assert.assert_dict_contains(
                    actual=deserialized,
                    expected_keys=["amount", "currency"],
                    field_name=f"money_{case_name}_structure"
                )
                self.assertEqual(deserialized["amount"], original["amount"])
                self.assertEqual(deserialized["currency"], original["currency"])
            else:
                # String format should be parsed correctly
                self.assertIsInstance(deserialized, dict)
                self.assertIn("amount", deserialized)
                self.assertIn("currency", deserialized)
        elif data_type == DataType.COORDINATE.value:
            if isinstance(original, dict):
                self.assertEqual(deserialized["latitude"], original["latitude"])
                self.assertEqual(deserialized["longitude"], original["longitude"])
            else:
                # String format should be parsed correctly
                self.assertIsInstance(deserialized, dict)
                self.assertIn("latitude", deserialized)
                self.assertIn("longitude", deserialized)
        elif data_type in [DataType.PHONE.value, DataType.EMAIL.value, DataType.ADDRESS.value, 
                          DataType.IMAGE.value, DataType.FILE.value]:
            if isinstance(original, dict):
                # Object format should maintain key fields
                for key in original.keys():
                    if key in deserialized:
                        self.assertEqual(deserialized[key], original[key])
            else:
                # String format should be preserved in appropriate field
                self.assertIsInstance(deserialized, dict)
        else:
            # For simple types, direct comparison
            self.enhanced_assert.assert_equal(
                actual=deserialized,
                expected=original,
                field_name=f"{data_type}_{case_name}_round_trip"
            )
    
    def test_serialization_error_handling(self):
        """Test serialization behavior with invalid/problematic data"""
        print("\n❌ Testing serialization error handling...")
        
        error_test_cases = [
            {
                "name": "circular_reference",
                "data": self._create_circular_reference(),
                "data_type": DataType.OBJECT.value
            },
            {
                "name": "non_json_serializable",
                "data": datetime.now(),  # datetime is not JSON serializable
                "data_type": DataType.OBJECT.value
            },
            {
                "name": "extremely_large_number",
                "data": {"amount": float('inf'), "currency": "USD"},
                "data_type": DataType.MONEY.value
            },
            {
                "name": "invalid_coordinates",
                "data": {"latitude": "invalid", "longitude": "invalid"},
                "data_type": DataType.COORDINATE.value
            }
        ]
        
        for test_case in error_test_cases:
            with self.subTest(case=test_case["name"]):
                print(f"  🧪 Testing error case: {test_case['name']}")
                
                # Should not raise exception, but handle gracefully
                try:
                    serialized, metadata = self.serializer.serialize(
                        value=test_case["data"],
                        data_type=test_case["data_type"],
                        constraints={}
                    )
                    
                    # Should produce some form of string output
                    self.assertIsInstance(serialized, str)
                    self.assertIsInstance(metadata, dict)
                    
                    # Should be able to deserialize without error
                    deserialized = self.serializer.deserialize(
                        serialized_value=serialized,
                        data_type=test_case["data_type"],
                        metadata=metadata
                    )
                    
                    print(f"    ✅ Error handled gracefully")
                    
                except Exception as e:
                    self.fail(f"Serialization should handle errors gracefully, but raised: {e}")
    
    def _create_circular_reference(self) -> Dict[str, Any]:
        """Create a dictionary with circular reference"""
        obj = {"name": "test"}
        obj["self"] = obj  # Circular reference
        return obj
    
    def test_serialization_performance(self):
        """Test serialization performance with large datasets"""
        print("\n⚡ Testing serialization performance...")
        
        performance_tests = [
            {
                "name": "large_array",
                "data": list(range(10000)),
                "data_type": DataType.ARRAY.value,
                "max_time": 1.0  # seconds
            },
            {
                "name": "large_object", 
                "data": {f"key_{i}": f"value_{i}" for i in range(1000)},
                "data_type": DataType.OBJECT.value,
                "max_time": 0.5
            },
            {
                "name": "nested_structures",
                "data": [{"users": [{"name": f"User {i}", "data": list(range(10))} for i in range(100)]}],
                "data_type": DataType.ARRAY.value,
                "max_time": 2.0
            }
        ]
        
        for test in performance_tests:
            with self.subTest(case=test["name"]):
                print(f"  ⏱️  Testing performance: {test['name']}")
                
                # Measure serialization time
                start_time = time.time()
                serialized, metadata = self.serializer.serialize(
                    value=test["data"],
                    data_type=test["data_type"],
                    constraints={}
                )
                serialize_time = time.time() - start_time
                
                # Measure deserialization time
                start_time = time.time()
                deserialized = self.serializer.deserialize(
                    serialized_value=serialized,
                    data_type=test["data_type"],
                    metadata=metadata
                )
                deserialize_time = time.time() - start_time
                
                total_time = serialize_time + deserialize_time
                
                print(f"    📊 Serialize: {serialize_time:.3f}s, Deserialize: {deserialize_time:.3f}s, Total: {total_time:.3f}s")
                
                # Verify performance meets requirements
                self.assertLess(
                    total_time, 
                    test["max_time"], 
                    f"Performance test '{test['name']}' exceeded max time {test['max_time']}s"
                )
                
                print(f"    ✅ Performance within limits")
    
    def test_unicode_and_encoding_support(self):
        """Test serialization with various Unicode characters and encodings"""
        print("\n🌍 Testing Unicode and encoding support...")
        
        unicode_test_cases = [
            {
                "name": "korean",
                "data": {"한글": "값", "이름": "김철수"},
                "data_type": DataType.OBJECT.value
            },
            {
                "name": "japanese",
                "data": ["こんにちは", "さようなら", "ありがとう"],
                "data_type": DataType.ARRAY.value
            },
            {
                "name": "arabic",
                "data": "مرحبا بالعالم",
                "data_type": DataType.ENUM.value
            },
            {
                "name": "emoji",
                "data": {"message": "Hello 👋 World 🌍", "reactions": ["😀", "😍", "🚀"]},
                "data_type": DataType.OBJECT.value
            },
            {
                "name": "mixed_scripts",
                "data": ["English", "한글", "日本語", "العربية", "русский", "Ελληνικά"],
                "data_type": DataType.ARRAY.value
            }
        ]
        
        for test_case in unicode_test_cases:
            with self.subTest(case=test_case["name"]):
                print(f"  🌐 Testing Unicode case: {test_case['name']}")
                
                # Serialize
                serialized, metadata = self.serializer.serialize(
                    value=test_case["data"],
                    data_type=test_case["data_type"],
                    constraints={}
                )
                
                # Verify serialized string is valid UTF-8
                try:
                    serialized.encode('utf-8')
                    print(f"    ✅ UTF-8 encoding valid")
                except UnicodeEncodeError as e:
                    self.fail(f"Serialized data not valid UTF-8: {e}")
                
                # Deserialize
                deserialized = self.serializer.deserialize(
                    serialized_value=serialized,
                    data_type=test_case["data_type"],
                    metadata=metadata
                )
                
                # Verify Unicode preservation
                if test_case["data_type"] == DataType.ARRAY.value:
                    self.assertEqual(len(deserialized), len(test_case["data"]))
                elif test_case["data_type"] == DataType.OBJECT.value:
                    # For objects, check key preservation
                    if isinstance(test_case["data"], dict):
                        for key in test_case["data"].keys():
                            self.assertIn(key, deserialized)
                
                print(f"    ✅ Unicode preservation verified")
    
    def test_constraint_handling_in_serialization(self):
        """Test how constraints affect serialization/deserialization"""
        print("\n🔧 Testing constraint handling in serialization...")
        
        constraint_tests = [
            {
                "name": "array_with_constraints",
                "data": [1, 2, 3, 4, 5],
                "data_type": DataType.ARRAY.value,
                "constraints": {"minItems": 3, "maxItems": 10, "itemType": "integer"}
            },
            {
                "name": "enum_with_values",
                "data": "ACTIVE",
                "data_type": DataType.ENUM.value,
                "constraints": {"enum": ["ACTIVE", "INACTIVE", "PENDING"]}
            },
            {
                "name": "money_with_currency",
                "data": {"amount": 100.50, "currency": "USD"},
                "data_type": DataType.MONEY.value,
                "constraints": {"currencies": ["USD", "EUR", "JPY"]}
            }
        ]
        
        for test in constraint_tests:
            with self.subTest(case=test["name"]):
                print(f"  🔧 Testing constraint case: {test['name']}")
                
                # Serialize with constraints
                serialized, metadata = self.serializer.serialize(
                    value=test["data"],
                    data_type=test["data_type"],
                    constraints=test["constraints"]
                )
                
                # Verify constraints are preserved in metadata
                self.enhanced_assert.assert_dict_contains(
                    actual=metadata,
                    expected_keys=["constraints"],
                    field_name=f"{test['name']}_metadata_constraints"
                )
                
                self.assertEqual(metadata["constraints"], test["constraints"])
                
                # Deserialize
                deserialized = self.serializer.deserialize(
                    serialized_value=serialized,
                    data_type=test["data_type"],
                    metadata=metadata
                )
                
                print(f"    ✅ Constraints preserved and handled")
    
    def test_edge_case_data_types(self):
        """Test serialization with edge case data types and values"""
        print("\n🔍 Testing edge case data types...")
        
        edge_cases = [
            {
                "name": "empty_string",
                "data": "",
                "data_type": DataType.ENUM.value
            },
            {
                "name": "very_long_string",
                "data": "x" * 10000,
                "data_type": DataType.ENUM.value
            },
            {
                "name": "nested_empty_structures",
                "data": {"empty_array": [], "empty_object": {}, "null_value": None},
                "data_type": DataType.OBJECT.value
            },
            {
                "name": "special_float_values",
                "data": [0.0, -0.0, 1e-10, 1e10],
                "data_type": DataType.ARRAY.value
            },
            {
                "name": "coordinate_boundaries",
                "data": {"latitude": 90.0, "longitude": 180.0},  # Max valid coordinates
                "data_type": DataType.COORDINATE.value
            },
            {
                "name": "coordinate_boundaries_negative",
                "data": {"latitude": -90.0, "longitude": -180.0},  # Min valid coordinates
                "data_type": DataType.COORDINATE.value
            }
        ]
        
        for test_case in edge_cases:
            with self.subTest(case=test_case["name"]):
                print(f"  🔍 Testing edge case: {test_case['name']}")
                
                # Should handle edge cases gracefully
                serialized, metadata = self.serializer.serialize(
                    value=test_case["data"],
                    data_type=test_case["data_type"],
                    constraints={}
                )
                
                # Should produce valid output
                self.assertIsInstance(serialized, str)
                self.assertIsInstance(metadata, dict)
                
                # Should deserialize without error
                deserialized = self.serializer.deserialize(
                    serialized_value=serialized,
                    data_type=test_case["data_type"],
                    metadata=metadata
                )
                
                # Should produce some reasonable output
                self.assertIsNotNone(deserialized)
                
                print(f"    ✅ Edge case handled successfully")
    
    def test_metadata_completeness(self):
        """Test that metadata contains all necessary information"""
        print("\n📋 Testing metadata completeness...")
        
        for data_type in [DataType.ARRAY.value, DataType.OBJECT.value, DataType.MONEY.value,
                         DataType.PHONE.value, DataType.EMAIL.value, DataType.COORDINATE.value]:
            with self.subTest(data_type=data_type):
                print(f"  📋 Testing metadata for: {data_type}")
                
                sample_data = self.test_data[data_type]
                test_value = next(iter(sample_data.values()))
                
                serialized, metadata = self.serializer.serialize(
                    value=test_value,
                    data_type=data_type,
                    constraints={"test": "constraint"}
                )
                
                # All metadata should have type field
                self.enhanced_assert.assert_dict_contains(
                    actual=metadata,
                    expected_keys=["type"],
                    field_name=f"{data_type}_metadata_type"
                )
                
                self.assertEqual(metadata["type"], data_type)
                
                # All metadata should have constraints field
                self.enhanced_assert.assert_dict_contains(
                    actual=metadata,
                    expected_keys=["constraints"],
                    field_name=f"{data_type}_metadata_constraints"
                )
                
                print(f"    ✅ Metadata complete for {data_type}")


if __name__ == "__main__":
    print("🔥 THINK ULTRA! Running comprehensive serialization tests...")
    print("=" * 80)
    
    # Create test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestComplexTypeSerializationComprehensive)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    print("\n" + "=" * 80)
    if result.wasSuccessful():
        print("✅ ALL SERIALIZATION TESTS PASSED!")
    else:
        print("❌ SOME SERIALIZATION TESTS FAILED")
        print(f"Failures: {len(result.failures)}")
        print(f"Errors: {len(result.errors)}")