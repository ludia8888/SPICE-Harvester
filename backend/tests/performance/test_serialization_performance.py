"""
🔥 THINK ULTRA! Serialization Performance Tests
Performance benchmarks and stress tests for complex type serialization
"""

import gc
import json
import psutil
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

from shared.models.common import DataType
from shared.serializers.complex_type_serializer import ComplexTypeSerializer


class TestSerializationPerformance(unittest.TestCase):
    """Performance tests for complex type serialization"""
    
    def setUp(self):
        """Set up performance test fixtures"""
        self.serializer = ComplexTypeSerializer()
        
        # Performance test data sets
        self.perf_data = {
            "small": self._generate_test_data(size="small"),
            "medium": self._generate_test_data(size="medium"),
            "large": self._generate_test_data(size="large"),
            "xlarge": self._generate_test_data(size="xlarge")
        }
        
        # Performance thresholds (in seconds)
        self.thresholds = {
            "small": {"serialize": 0.01, "deserialize": 0.01, "total": 0.02},
            "medium": {"serialize": 0.1, "deserialize": 0.1, "total": 0.2},
            "large": {"serialize": 1.0, "deserialize": 1.0, "total": 2.0},
            "xlarge": {"serialize": 5.0, "deserialize": 5.0, "total": 10.0}
        }
    
    def _generate_test_data(self, size: str) -> Dict[str, Any]:
        """Generate test data of specified size"""
        size_configs = {
            "small": {"arrays": 10, "objects": 10, "nesting": 2},
            "medium": {"arrays": 100, "objects": 100, "nesting": 3},
            "large": {"arrays": 1000, "objects": 1000, "nesting": 4},
            "xlarge": {"arrays": 5000, "objects": 2000, "nesting": 5}
        }
        
        config = size_configs[size]
        
        return {
            DataType.ARRAY.value: self._generate_array_data(config["arrays"], config["nesting"]),
            DataType.OBJECT.value: self._generate_object_data(config["objects"], config["nesting"]),
            DataType.MONEY.value: {"amount": 123456.78, "currency": "USD"},
            DataType.COORDINATE.value: {"latitude": 37.5665, "longitude": 126.9780},
            DataType.PHONE.value: "+1-555-123-4567",
            DataType.EMAIL.value: "performance.test@example.com",
            DataType.ADDRESS.value: {
                "street": "123 Performance Test St",
                "city": "Seoul",
                "country": "Korea"
            }
        }
    
    def _generate_array_data(self, size: int, nesting: int) -> List[Any]:
        """Generate array test data"""
        if nesting <= 1:
            return [f"item_{i}" for i in range(size)]
        else:
            return [
                {
                    "id": i,
                    "name": f"item_{i}",
                    "nested_array": self._generate_array_data(min(size//10, 10), nesting-1),
                    "nested_object": self._generate_object_data(min(size//10, 10), nesting-1)
                }
                for i in range(size)
            ]
    
    def _generate_object_data(self, size: int, nesting: int) -> Dict[str, Any]:
        """Generate object test data"""
        base_data = {f"key_{i}": f"value_{i}" for i in range(size)}
        
        if nesting > 1:
            base_data.update({
                "nested_objects": {
                    f"nested_{i}": self._generate_object_data(min(size//10, 5), nesting-1)
                    for i in range(min(nesting, 3))
                },
                "nested_arrays": [
                    self._generate_array_data(min(size//10, 10), nesting-1)
                    for _ in range(min(nesting, 2))
                ]
            })
        
        return base_data
    
    def test_serialization_performance_by_size(self):
        """Test serialization performance across different data sizes"""
        print("\n⚡ Testing serialization performance by data size...")
        
        results = {}
        
        for size_name, data_set in self.perf_data.items():
            print(f"\n  📊 Testing {size_name} dataset:")
            
            size_results = {}
            
            for data_type, test_data in data_set.items():
                with self.subTest(size=size_name, data_type=data_type):
                    # Measure serialization
                    start_time = time.perf_counter()
                    serialized, metadata = self.serializer.serialize(
                        value=test_data,
                        data_type=data_type,
                        constraints={}
                    )
                    serialize_time = time.perf_counter() - start_time
                    
                    # Measure deserialization
                    start_time = time.perf_counter()
                    deserialized = self.serializer.deserialize(
                        serialized_value=serialized,
                        data_type=data_type,
                        metadata=metadata
                    )
                    deserialize_time = time.perf_counter() - start_time
                    
                    total_time = serialize_time + deserialize_time
                    
                    # Store results
                    size_results[data_type] = {
                        "serialize": serialize_time,
                        "deserialize": deserialize_time,
                        "total": total_time,
                        "data_size": len(str(test_data))
                    }
                    
                    print(f"    {data_type:15s}: {serialize_time:.4f}s | {deserialize_time:.4f}s | {total_time:.4f}s | {len(str(test_data)):8d} chars")
                    
                    # Check against thresholds
                    threshold = self.thresholds[size_name]
                    if data_type in [DataType.ARRAY.value, DataType.OBJECT.value]:
                        # Only check thresholds for complex types
                        self.assertLess(
                            serialize_time, 
                            threshold["serialize"],
                            f"Serialization too slow for {size_name} {data_type}: {serialize_time:.3f}s > {threshold['serialize']}s"
                        )
                        self.assertLess(
                            deserialize_time,
                            threshold["deserialize"], 
                            f"Deserialization too slow for {size_name} {data_type}: {deserialize_time:.3f}s > {threshold['deserialize']}s"
                        )
            
            results[size_name] = size_results
            
            # Print size summary
            avg_total = sum(r["total"] for r in size_results.values()) / len(size_results)
            print(f"    Average total time: {avg_total:.4f}s")
        
        print(f"\n  ✅ Performance test completed for all sizes")
        
        # Store results for analysis
        self._store_performance_results(results)
    
    def test_memory_usage_during_serialization(self):
        """Test memory usage patterns during serialization"""
        print("\n💾 Testing memory usage during serialization...")
        
        # Test with large dataset
        large_data = self.perf_data["large"][DataType.OBJECT.value]
        
        # Measure initial memory
        gc.collect()  # Clean up before measurement
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        print(f"  📊 Initial memory usage: {initial_memory:.2f} MB")
        
        # Measure memory during serialization
        memory_samples = []
        
        def memory_monitor():
            """Monitor memory usage"""
            for _ in range(10):
                memory_samples.append(psutil.Process().memory_info().rss / 1024 / 1024)
                time.sleep(0.1)
        
        # Start memory monitoring in background
        from threading import Thread
        monitor_thread = Thread(target=memory_monitor)
        monitor_thread.start()
        
        # Perform serialization
        serialized, metadata = self.serializer.serialize(
            value=large_data,
            data_type=DataType.OBJECT.value,
            constraints={}
        )
        
        # Perform deserialization
        deserialized = self.serializer.deserialize(
            serialized_value=serialized,
            data_type=DataType.OBJECT.value,
            metadata=metadata
        )
        
        # Wait for memory monitoring to complete
        monitor_thread.join()
        
        # Measure final memory
        gc.collect()
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        max_memory = max(memory_samples)
        avg_memory = sum(memory_samples) / len(memory_samples)
        
        print(f"  📊 Peak memory usage: {max_memory:.2f} MB")
        print(f"  📊 Average memory usage: {avg_memory:.2f} MB")
        print(f"  📊 Final memory usage: {final_memory:.2f} MB")
        print(f"  📊 Memory increase: {final_memory - initial_memory:.2f} MB")
        
        # Memory usage should be reasonable
        memory_increase = max_memory - initial_memory
        self.assertLess(
            memory_increase, 500,  # Should not use more than 500MB extra
            f"Memory usage too high: {memory_increase:.2f} MB"
        )
        
        print("  ✅ Memory usage within acceptable limits")
    
    def test_concurrent_serialization_performance(self):
        """Test serialization performance under concurrent load"""
        print("\n🚀 Testing concurrent serialization performance...")
        
        # Test data
        test_data = self.perf_data["medium"][DataType.OBJECT.value]
        
        def serialize_task(task_id: int) -> Tuple[int, float, bool]:
            """Single serialization task"""
            start_time = time.perf_counter()
            
            try:
                serialized, metadata = self.serializer.serialize(
                    value=test_data,
                    data_type=DataType.OBJECT.value,
                    constraints={}
                )
                
                deserialized = self.serializer.deserialize(
                    serialized_value=serialized,
                    data_type=DataType.OBJECT.value,
                    metadata=metadata
                )
                
                duration = time.perf_counter() - start_time
                return task_id, duration, True
                
            except Exception as e:
                duration = time.perf_counter() - start_time
                return task_id, duration, False
        
        # Test with different concurrency levels
        concurrency_levels = [1, 2, 4, 8, 16]
        
        for concurrency in concurrency_levels:
            print(f"\n  🔀 Testing with {concurrency} concurrent tasks:")
            
            start_time = time.perf_counter()
            
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                # Submit tasks
                futures = [executor.submit(serialize_task, i) for i in range(concurrency)]
                
                # Collect results
                results = []
                for future in as_completed(futures):
                    task_id, duration, success = future.result()
                    results.append((task_id, duration, success))
            
            total_time = time.perf_counter() - start_time
            
            # Analyze results
            successful_tasks = sum(1 for _, _, success in results if success)
            avg_task_time = sum(duration for _, duration, _ in results) / len(results)
            
            print(f"    📊 Total time: {total_time:.4f}s")
            print(f"    📊 Successful tasks: {successful_tasks}/{concurrency}")
            print(f"    📊 Average task time: {avg_task_time:.4f}s")
            print(f"    📊 Throughput: {concurrency/total_time:.2f} tasks/second")
            
            # All tasks should succeed
            self.assertEqual(successful_tasks, concurrency, "All concurrent tasks should succeed")
            
            # Performance should scale reasonably
            if concurrency == 1:
                baseline_time = avg_task_time
            else:
                # Task time should not degrade too much with concurrency
                overhead = avg_task_time / baseline_time
                self.assertLess(
                    overhead, 3.0,  # Allow up to 3x overhead
                    f"Concurrency overhead too high: {overhead:.2f}x"
                )
        
        print("  ✅ Concurrent performance test completed")
    
    def test_serialization_throughput_limits(self):
        """Test maximum throughput limits for serialization"""
        print("\n🏃 Testing serialization throughput limits...")
        
        # Test different data types for throughput
        throughput_tests = [
            {
                "name": "simple_objects",
                "data": {"name": "test", "value": 123},
                "data_type": DataType.OBJECT.value,
                "iterations": 1000,
                "min_throughput": 500  # ops/second
            },
            {
                "name": "simple_arrays",
                "data": list(range(100)),
                "data_type": DataType.ARRAY.value,
                "iterations": 1000,
                "min_throughput": 500
            },
            {
                "name": "complex_nested",
                "data": self.perf_data["medium"][DataType.OBJECT.value],
                "data_type": DataType.OBJECT.value,
                "iterations": 100,
                "min_throughput": 50
            }
        ]
        
        for test in throughput_tests:
            with self.subTest(test=test["name"]):
                print(f"\n  🏃 Testing {test['name']} throughput:")
                
                # Perform iterations
                start_time = time.perf_counter()
                
                successful_ops = 0
                for i in range(test["iterations"]):
                    try:
                        serialized, metadata = self.serializer.serialize(
                            value=test["data"],
                            data_type=test["data_type"],
                            constraints={}
                        )
                        
                        deserialized = self.serializer.deserialize(
                            serialized_value=serialized,
                            data_type=test["data_type"],
                            metadata=metadata
                        )
                        
                        successful_ops += 1
                        
                    except Exception as e:
                        print(f"    ⚠️  Iteration {i} failed: {e}")
                
                total_time = time.perf_counter() - start_time
                throughput = successful_ops / total_time
                
                print(f"    📊 Successful operations: {successful_ops}/{test['iterations']}")
                print(f"    📊 Total time: {total_time:.3f}s")
                print(f"    📊 Throughput: {throughput:.2f} ops/second")
                
                # Verify throughput meets minimum requirements
                self.assertGreaterEqual(
                    throughput,
                    test["min_throughput"],
                    f"Throughput too low for {test['name']}: {throughput:.2f} < {test['min_throughput']}"
                )
                
                # Verify success rate
                success_rate = successful_ops / test["iterations"]
                self.assertGreaterEqual(
                    success_rate, 0.95,
                    f"Success rate too low for {test['name']}: {success_rate:.2%}"
                )
        
        print("  ✅ Throughput limit tests completed")
    
    def _store_performance_results(self, results: Dict[str, Any]):
        """Store performance results for analysis"""
        try:
            import os
            results_dir = os.path.join(os.path.dirname(__file__), "..", "results")
            os.makedirs(results_dir, exist_ok=True)
            
            timestamp = int(time.time())
            results_file = os.path.join(results_dir, f"serialization_performance_{timestamp}.json")
            
            with open(results_file, 'w') as f:
                json.dump({
                    "timestamp": timestamp,
                    "results": results,
                    "system_info": {
                        "cpu_count": psutil.cpu_count(),
                        "memory_gb": psutil.virtual_memory().total / (1024**3),
                        "python_version": __import__("sys").version
                    }
                }, f, indent=2)
            
            print(f"  📄 Performance results saved to: {results_file}")
            
        except Exception as e:
            print(f"  ⚠️  Could not save performance results: {e}")


if __name__ == "__main__":
    print("🔥 THINK ULTRA! Running serialization performance tests...")
    print("=" * 80)
    
    # Set up test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSerializationPerformance)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    print("\n" + "=" * 80)
    if result.wasSuccessful():
        print("✅ ALL PERFORMANCE TESTS PASSED!")
        print("📊 Serialization performance meets requirements")
    else:
        print("❌ SOME PERFORMANCE TESTS FAILED")
        print(f"Failures: {len(result.failures)}")
        print(f"Errors: {len(result.errors)}")
        print("⚠️  Performance may not meet production requirements")