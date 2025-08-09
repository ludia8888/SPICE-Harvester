#!/usr/bin/env python3
"""
Quick Performance Test Script
Simplified version for testing basic performance metrics within 60 seconds
"""

import asyncio
import httpx
import json
import time
import statistics
from datetime import datetime
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tests.test_config import TestConfig

class QuickPerformanceTest:
    def __init__(self):
        self.test_db_name = f"quick_perf_test_{int(time.time())}"
        self.oms_url = TestConfig.get_oms_base_url()
        self.bff_url = TestConfig.get_bff_base_url()
        
    async def setup(self):
        """Setup test environment"""
        print(f"🚀 Setting up quick performance test with database: {self.test_db_name}")
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{self.oms_url}/api/v1/database/create",
                json={"name": self.test_db_name, "description": "Quick performance test"}
            )
            if response.status_code not in [200, 201, 409]:
                raise Exception(f"Failed to create test DB: {response.status_code}")
        print("✅ Test environment ready")

    async def cleanup(self):
        """Cleanup test environment"""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                await client.delete(f"{self.oms_url}/api/v1/database/{self.test_db_name}")
            print("✅ Test environment cleaned up")
        except Exception as e:
            print(f"⚠️ Cleanup warning: {e}")

    async def test_basic_performance(self):
        """Test basic performance metrics"""
        print("\n📊 Basic Performance Test")
        
        response_times = []
        success_count = 0
        error_count = 0
        
        # Test with 20 requests to get basic metrics
        num_requests = 20
        
        async with httpx.AsyncClient(timeout=30) as client:
            for i in range(num_requests):
                ontology_data = {
                    "id": f"QuickTest{i}",
                    "label": f"Quick Test {i}",
                    "properties": [{"name": "test_prop", "type": "string"}]
                }
                
                start_time = time.time()
                try:
                    response = await client.post(
                        f"{self.oms_url}/api/v1/ontology/{self.test_db_name}/create",
                        json=ontology_data
                    )
                    end_time = time.time()
                    
                    response_time = end_time - start_time
                    response_times.append(response_time)
                    
                    if response.status_code in [200, 409]:  # Success or duplicate
                        success_count += 1
                    else:
                        error_count += 1
                        
                except Exception as e:
                    end_time = time.time()
                    response_times.append(end_time - start_time)
                    error_count += 1
                
                if (i + 1) % 5 == 0:
                    print(f"  Progress: {i + 1}/{num_requests}")
        
        # Calculate metrics
        if response_times:
            avg_time = statistics.mean(response_times)
            min_time = min(response_times)
            max_time = max(response_times)
            median_time = statistics.median(response_times)
            
            print(f"\n📈 Performance Results:")
            print(f"  Total Requests: {num_requests}")
            print(f"  Successful: {success_count}")
            print(f"  Errors: {error_count}")
            print(f"  Success Rate: {success_count/num_requests:.2%}")
            print(f"  Average Response Time: {avg_time:.3f}s")
            print(f"  Min Response Time: {min_time:.3f}s")
            print(f"  Max Response Time: {max_time:.3f}s")
            print(f"  Median Response Time: {median_time:.3f}s")
            
            # Performance thresholds
            if avg_time > 2.0:
                print("⚠️ WARNING: Average response time > 2s")
            if error_count > 0:
                print(f"⚠️ WARNING: {error_count} errors detected")
            if success_count == num_requests:
                print("✅ All requests successful")

    async def test_concurrent_load(self):
        """Test concurrent load with smaller numbers"""
        print("\n👥 Concurrent Load Test")
        
        concurrent_levels = [5, 10, 15]
        
        for concurrent_users in concurrent_levels:
            print(f"  Testing {concurrent_users} concurrent requests...")
            
            tasks = []
            start_time = time.time()
            
            async with httpx.AsyncClient(timeout=30) as client:
                for user_id in range(concurrent_users):
                    ontology_data = {
                        "id": f"ConcurrentQuick{concurrent_users}_{user_id}",
                        "label": f"Concurrent Quick Test {user_id}",
                        "properties": [{"name": "user_id", "type": "string"}]
                    }
                    
                    task = client.post(
                        f"{self.oms_url}/api/v1/ontology/{self.test_db_name}/create",
                        json=ontology_data
                    )
                    tasks.append(task)
                
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                end_time = time.time()
                
                total_time = end_time - start_time
                successful = sum(1 for r in responses if hasattr(r, 'status_code') and r.status_code in [200, 409])
                errors = len(responses) - successful
                throughput = len(responses) / total_time
                
                print(f"    Successful: {successful}/{len(responses)}")
                print(f"    Errors: {errors}")
                print(f"    Throughput: {throughput:.2f} req/s")
                print(f"    Total Time: {total_time:.2f}s")
                
                await asyncio.sleep(1)  # Brief pause between tests

    async def run_quick_tests(self):
        """Run all quick tests"""
        start_time = time.time()
        
        try:
            await self.setup()
            
            await self.test_basic_performance()
            await self.test_concurrent_load()
            
            total_time = time.time() - start_time
            print(f"\n🏁 Quick Performance Test Complete")
            print(f"Total Test Time: {total_time:.2f} seconds")
            
        finally:
            await self.cleanup()

if __name__ == "__main__":
    test = QuickPerformanceTest()
    asyncio.run(test.run_quick_tests())