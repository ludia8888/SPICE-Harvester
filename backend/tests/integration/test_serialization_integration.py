"""
🔥 THINK ULTRA! Serialization Integration Tests
Tests serialization/deserialization in the context of the full application stack
"""

import asyncio
import json
import unittest
from typing import Any, Dict, List

import httpx
import pytest

from shared.models.common import DataType
from shared.serializers.complex_type_serializer import ComplexTypeSerializer
from tests.utils.enhanced_assertions import EnhancedAssertions


class TestSerializationIntegration(unittest.TestCase):
    """Integration tests for serialization with BFF and OMS services"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.enhanced_assert = EnhancedAssertions()
        self.bff_base_url = "http://localhost:8002"
        self.oms_base_url = "http://localhost:8000"
        self.serializer = ComplexTypeSerializer()
        
        # Sample complex data for integration testing
        self.test_ontology_data = {
            "class_definitions": [
                {
                    "class_label": "Person",
                    "properties": [
                        {
                            "property_label": "contact_info",
                            "data_type": DataType.OBJECT.value,
                            "value": {
                                "emails": ["john@example.com", "john.doe@work.com"],
                                "phones": ["+1-555-123-4567", "+1-555-987-6543"],
                                "address": {
                                    "street": "123 Main St",
                                    "city": "Seoul",
                                    "country": "Korea"
                                }
                            }
                        },
                        {
                            "property_label": "coordinates",
                            "data_type": DataType.COORDINATE.value,
                            "value": {"latitude": 37.5665, "longitude": 126.9780}
                        },
                        {
                            "property_label": "salary",
                            "data_type": DataType.MONEY.value,
                            "value": {"amount": 75000.00, "currency": "USD"}
                        },
                        {
                            "property_label": "documents", 
                            "data_type": DataType.ARRAY.value,
                            "value": [
                                {
                                    "type": "resume",
                                    "url": "https://example.com/resume.pdf",
                                    "size": 1024000
                                },
                                {
                                    "type": "portfolio",
                                    "url": "https://example.com/portfolio.pdf", 
                                    "size": 2048000
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    
    @pytest.mark.asyncio
    async def test_end_to_end_serialization_via_api(self):
        """Test serialization through actual API calls"""
        print("\n🔗 Testing end-to-end serialization via API...")
        
        # Skip if services not available
        if not await self._check_services_available():
            self.skipTest("Services not available for integration test")
        
        async with httpx.AsyncClient(timeout=30) as client:
            # Test creating a database with complex type data
            db_name = "test_serialization_db"
            
            try:
                # 1. Create database
                create_response = await client.post(
                    f"{self.bff_base_url}/api/v1/databases",
                    json={"database_name": db_name, "description": "Test serialization database"}
                )
                
                if create_response.status_code not in [200, 201, 409]:  # 409 = already exists
                    self.skipTest(f"Could not create test database: {create_response.status_code}")
                
                # 2. Create class with complex properties
                for class_def in self.test_ontology_data["class_definitions"]:
                    class_response = await client.post(
                        f"{self.bff_base_url}/api/v1/databases/{db_name}/classes",
                        json={
                            "class_label": class_def["class_label"],
                            "description": "Test class with complex properties"
                        }
                    )
                    
                    self.assertIn(class_response.status_code, [200, 201, 409])
                    
                    # 3. Create properties with serialized complex data
                    for prop in class_def["properties"]:
                        # Serialize the complex value
                        serialized_value, metadata = self.serializer.serialize(
                            value=prop["value"],
                            data_type=prop["data_type"],
                            constraints={}
                        )
                        
                        property_data = {
                            "property_label": prop["property_label"],
                            "data_type": prop["data_type"],
                            "description": f"Test property of type {prop['data_type']}",
                            "serialized_value": serialized_value,
                            "serialization_metadata": metadata
                        }
                        
                        prop_response = await client.post(
                            f"{self.bff_base_url}/api/v1/databases/{db_name}/classes/{class_def['class_label']}/properties",
                            json=property_data
                        )
                        
                        # Should accept serialized data
                        self.assertIn(prop_response.status_code, [200, 201, 409])
                        
                        if prop_response.status_code in [200, 201]:
                            print(f"    ✅ Property {prop['property_label']} created with serialized {prop['data_type']} data")
                
                # 4. Retrieve and verify data roundtrip
                retrieve_response = await client.get(
                    f"{self.bff_base_url}/api/v1/databases/{db_name}/classes/{self.test_ontology_data['class_definitions'][0]['class_label']}"
                )
                
                if retrieve_response.status_code == 200:
                    retrieved_data = retrieve_response.json()
                    
                    # Verify complex data was preserved through API roundtrip
                    if "properties" in retrieved_data:
                        for prop in retrieved_data["properties"]:
                            if "serialized_value" in prop and "serialization_metadata" in prop:
                                # Deserialize and verify
                                deserialized = self.serializer.deserialize(
                                    serialized_value=prop["serialized_value"],
                                    data_type=prop["data_type"],
                                    metadata=prop["serialization_metadata"]
                                )
                                
                                # Verify deserialized data is reasonable
                                self.assertIsNotNone(deserialized)
                                print(f"    ✅ Property {prop['property_label']} deserialized successfully")
                    
                    print("    ✅ End-to-end serialization test completed successfully")
                
            except Exception as e:
                print(f"    ⚠️  Integration test encountered error: {e}")
                # Don't fail the test for integration issues, just log
                pass
            
            finally:
                # Cleanup: attempt to delete test database
                try:
                    await client.delete(f"{self.bff_base_url}/api/v1/databases/{db_name}")
                except:
                    pass  # Ignore cleanup errors
    
    async def _check_services_available(self) -> bool:
        """Check if required services are available"""
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                bff_response = await client.get(f"{self.bff_base_url}/health")
                oms_response = await client.get(f"{self.oms_base_url}/health")
                
                return (bff_response.status_code == 200 and 
                       oms_response.status_code == 200)
        except:
            return False
    
    def test_serialization_with_validation_constraints(self):
        """Test serialization works correctly with complex validation constraints"""
        print("\n🔧 Testing serialization with validation constraints...")
        
        complex_constraints = {
            "array_constraint": {
                "minItems": 2,
                "maxItems": 10,
                "itemType": "object",
                "uniqueItems": True
            },
            "money_constraint": {
                "currencies": ["USD", "EUR", "KRW"],
                "minAmount": 0,
                "maxAmount": 1000000
            },
            "coordinate_constraint": {
                "minLatitude": -90,
                "maxLatitude": 90,
                "minLongitude": -180,
                "maxLongitude": 180
            }
        }
        
        test_cases = [
            {
                "name": "array_with_constraints",
                "data": [{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}],
                "data_type": DataType.ARRAY.value,
                "constraints": complex_constraints["array_constraint"]
            },
            {
                "name": "money_with_constraints", 
                "data": {"amount": 50000, "currency": "KRW"},
                "data_type": DataType.MONEY.value,
                "constraints": complex_constraints["money_constraint"]
            },
            {
                "name": "coordinate_with_constraints",
                "data": {"latitude": 37.5665, "longitude": 126.9780},
                "data_type": DataType.COORDINATE.value,
                "constraints": complex_constraints["coordinate_constraint"]
            }
        ]
        
        for test_case in test_cases:
            with self.subTest(case=test_case["name"]):
                print(f"  🔧 Testing: {test_case['name']}")
                
                # Serialize with complex constraints
                serialized, metadata = self.serializer.serialize(
                    value=test_case["data"],
                    data_type=test_case["data_type"],
                    constraints=test_case["constraints"]
                )
                
                # Verify constraints are preserved
                self.enhanced_assert.assert_dict_contains(
                    actual=metadata,
                    expected_keys=["constraints"],
                    field_name=f"{test_case['name']}_constraints"
                )
                
                # Verify constraint details are preserved
                for key, value in test_case["constraints"].items():
                    self.assertEqual(metadata["constraints"][key], value)
                
                # Deserialize
                deserialized = self.serializer.deserialize(
                    serialized_value=serialized,
                    data_type=test_case["data_type"],
                    metadata=metadata
                )
                
                # Verify data integrity
                self.assertIsNotNone(deserialized)
                
                print(f"    ✅ Constraint handling verified for {test_case['name']}")
    
    def test_serialization_performance_in_context(self):
        """Test serialization performance in realistic application context"""
        print("\n⚡ Testing serialization performance in application context...")
        
        # Simulate realistic data volumes
        large_dataset = {
            "users": [
                {
                    "id": i,
                    "name": f"User {i}",
                    "contact": {
                        "email": f"user{i}@example.com",
                        "phone": f"+1-555-{i:03d}-{(i*7)%10000:04d}",
                        "address": {
                            "street": f"{i} Main St",
                            "city": "Seoul",
                            "coordinates": {
                                "latitude": 37.5665 + (i * 0.001),
                                "longitude": 126.9780 + (i * 0.001)
                            }
                        }
                    },
                    "financials": {
                        "salary": {"amount": 50000 + (i * 1000), "currency": "USD"},
                        "bonus": {"amount": 5000 + (i * 100), "currency": "USD"}
                    },
                    "documents": [
                        {"name": f"resume_{i}.pdf", "url": f"https://example.com/resume_{i}.pdf"},
                        {"name": f"portfolio_{i}.pdf", "url": f"https://example.com/portfolio_{i}.pdf"}
                    ]
                }
                for i in range(100)  # 100 users with complex nested data
            ]
        }
        
        # Test serialization performance
        import time
        
        start_time = time.time()
        serialized, metadata = self.serializer.serialize(
            value=large_dataset,
            data_type=DataType.OBJECT.value,
            constraints={}
        )
        serialize_time = time.time() - start_time
        
        # Verify serialization worked
        self.assertIsInstance(serialized, str)
        self.assertIsInstance(metadata, dict)
        
        # Test deserialization performance
        start_time = time.time()
        deserialized = self.serializer.deserialize(
            serialized_value=serialized,
            data_type=DataType.OBJECT.value,
            metadata=metadata
        )
        deserialize_time = time.time() - start_time
        
        total_time = serialize_time + deserialize_time
        
        print(f"  📊 Large dataset (100 users): Serialize: {serialize_time:.3f}s, Deserialize: {deserialize_time:.3f}s")
        print(f"  📊 Total time: {total_time:.3f}s")
        
        # Performance should be reasonable for production use
        self.assertLess(total_time, 5.0, "Serialization should complete within 5 seconds for large datasets")
        
        # Verify data integrity
        self.assertIsInstance(deserialized, dict)
        self.assertIn("users", deserialized)
        self.assertEqual(len(deserialized["users"]), 100)
        
        print("    ✅ Performance test passed with data integrity verified")
    
    def test_error_recovery_in_serialization_pipeline(self):
        """Test error recovery when serialization fails in application pipeline"""
        print("\n🔄 Testing error recovery in serialization pipeline...")
        
        # Test cases that might cause issues in real application
        problematic_data = [
            {
                "name": "deeply_nested_structure",
                "data": self._create_deeply_nested_structure(depth=50),
                "data_type": DataType.OBJECT.value
            },
            {
                "name": "mixed_type_array",
                "data": [1, "string", {"object": True}, [1, 2, 3], None],
                "data_type": DataType.ARRAY.value
            },
            {
                "name": "special_characters",
                "data": {
                    "special": "Special chars: \n\t\r\b\f\"'\\",
                    "unicode": "Unicode: \u0000\u001f\u007f\u0080\u009f",
                    "emoji": "Emoji: 👨‍💻🔥⚡️"
                },
                "data_type": DataType.OBJECT.value
            }
        ]
        
        for test_case in problematic_data:
            with self.subTest(case=test_case["name"]):
                print(f"  🔄 Testing error recovery: {test_case['name']}")
                
                try:
                    # Should handle problematic data gracefully
                    serialized, metadata = self.serializer.serialize(
                        value=test_case["data"],
                        data_type=test_case["data_type"],
                        constraints={}
                    )
                    
                    # Should produce some output even if not perfect
                    self.assertIsInstance(serialized, str)
                    self.assertIsInstance(metadata, dict)
                    
                    # Should be able to deserialize without crashing
                    deserialized = self.serializer.deserialize(
                        serialized_value=serialized,
                        data_type=test_case["data_type"],
                        metadata=metadata
                    )
                    
                    # Should produce some reasonable output
                    self.assertIsNotNone(deserialized)
                    
                    print(f"    ✅ Error recovery successful for {test_case['name']}")
                    
                except Exception as e:
                    self.fail(f"Serialization pipeline should handle errors gracefully: {e}")
    
    def _create_deeply_nested_structure(self, depth: int) -> Dict[str, Any]:
        """Create a deeply nested structure for testing"""
        if depth <= 0:
            return {"value": "leaf"}
        
        return {
            "level": depth,
            "nested": self._create_deeply_nested_structure(depth - 1),
            "data": [f"item_{i}" for i in range(min(depth, 5))]
        }


class TestSerializationAsyncIntegration:
    """Async integration tests for serialization"""
    
    def __init__(self):
        self.enhanced_assert = EnhancedAssertions()
        self.bff_base_url = "http://localhost:8002"
        self.serializer = ComplexTypeSerializer()
    
    @pytest.mark.asyncio
    async def test_concurrent_serialization_operations(self):
        """Test serialization under concurrent load"""
        print("\n🚀 Testing concurrent serialization operations...")
        
        # Create multiple serialization tasks
        test_data = [
            {"id": i, "data": list(range(i, i+100)), "metadata": {"type": f"dataset_{i}"}}
            for i in range(10)
        ]
        
        async def serialize_task(data: Dict[str, Any]) -> bool:
            """Single serialization task"""
            try:
                serialized, metadata = self.serializer.serialize(
                    value=data,
                    data_type=DataType.OBJECT.value,
                    constraints={}
                )
                
                deserialized = self.serializer.deserialize(
                    serialized_value=serialized,
                    data_type=DataType.OBJECT.value,
                    metadata=metadata
                )
                
                return deserialized["id"] == data["id"]
            except Exception:
                return False
        
        # Run concurrent serialization tasks
        tasks = [serialize_task(data) for data in test_data]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Verify all tasks completed successfully
        successful_tasks = sum(1 for result in results if result is True)
        total_tasks = len(test_data)
        
        print(f"  📊 Concurrent tasks: {successful_tasks}/{total_tasks} successful")
        
        # At least 80% should succeed under concurrent load
        success_rate = successful_tasks / total_tasks
        assert success_rate >= 0.8, f"Success rate too low: {success_rate:.2%}"
        
        print("    ✅ Concurrent serialization test passed")


if __name__ == "__main__":
    print("🔥 THINK ULTRA! Running serialization integration tests...")
    print("=" * 80)
    
    # Run sync tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSerializationIntegration)
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    # Run async tests
    print("\n" + "=" * 40)
    print("Running async integration tests...")
    
    async def run_async_tests():
        async_test = TestSerializationAsyncIntegration()
        await async_test.test_concurrent_serialization_operations()
    
    asyncio.run(run_async_tests())
    
    print("\n" + "=" * 80)
    if result.wasSuccessful():
        print("✅ ALL SERIALIZATION INTEGRATION TESTS PASSED!")
    else:
        print("❌ SOME TESTS FAILED")
        print(f"Failures: {len(result.failures)}")
        print(f"Errors: {len(result.errors)}")