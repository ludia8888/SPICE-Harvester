#!/usr/bin/env python3
"""
Test script for API Rate Limiting
Tests the rate limiting implementation based on Context7 recommendations
"""

import asyncio
import time
import httpx
from typing import List, Dict, Any


async def test_rate_limit(
    url: str,
    num_requests: int = 70,
    delay: float = 0.1
) -> Dict[str, Any]:
    """
    Test rate limiting on an endpoint
    
    Args:
        url: Endpoint to test
        num_requests: Number of requests to send
        delay: Delay between requests in seconds
        
    Returns:
        Test results with statistics
    """
    results = {
        "total_requests": num_requests,
        "successful": 0,
        "rate_limited": 0,
        "errors": 0,
        "rate_limit_headers": [],
        "timings": []
    }
    
    async with httpx.AsyncClient() as client:
        for i in range(num_requests):
            start_time = time.time()
            try:
                response = await client.get(url)
                elapsed = time.time() - start_time
                results["timings"].append(elapsed)
                
                if response.status_code == 429:
                    results["rate_limited"] += 1
                    print(f"Request {i+1}: Rate limited (429)")
                    
                    # Extract rate limit info
                    headers = {
                        "limit": response.headers.get("X-RateLimit-Limit"),
                        "remaining": response.headers.get("X-RateLimit-Remaining"),
                        "reset": response.headers.get("X-RateLimit-Reset"),
                        "retry_after": response.headers.get("Retry-After")
                    }
                    results["rate_limit_headers"].append(headers)
                    
                elif response.status_code < 400:
                    results["successful"] += 1
                    print(f"Request {i+1}: Success ({response.status_code})")
                    
                    # Check for rate limit headers even on success
                    if "X-RateLimit-Limit" in response.headers:
                        headers = {
                            "limit": response.headers.get("X-RateLimit-Limit"),
                            "remaining": response.headers.get("X-RateLimit-Remaining"),
                            "reset": response.headers.get("X-RateLimit-Reset")
                        }
                        print(f"  Rate limit info: {headers['remaining']}/{headers['limit']} remaining")
                else:
                    results["errors"] += 1
                    print(f"Request {i+1}: Error ({response.status_code})")
                    
            except Exception as e:
                results["errors"] += 1
                print(f"Request {i+1}: Exception - {str(e)}")
                
            await asyncio.sleep(delay)
    
    return results


async def test_multiple_endpoints():
    """Test rate limiting on multiple endpoints"""
    
    base_url = "http://localhost:8002"  # BFF service
    endpoints = [
        "/health",  # Should have high limit
        "/database/test_db/ontologies",  # Should have relaxed limit
        "/database/test_db/ontology/Person",  # Should have standard limit
    ]
    
    print("=" * 60)
    print("API Rate Limiting Test")
    print("Based on Context7 Recommendations")
    print("=" * 60)
    
    for endpoint in endpoints:
        url = f"{base_url}{endpoint}"
        print(f"\nTesting: {endpoint}")
        print("-" * 40)
        
        results = await test_rate_limit(url, num_requests=70, delay=0.05)
        
        print("\nResults:")
        print(f"  Total requests: {results['total_requests']}")
        print(f"  Successful: {results['successful']}")
        print(f"  Rate limited: {results['rate_limited']}")
        print(f"  Errors: {results['errors']}")
        
        if results["timings"]:
            avg_time = sum(results["timings"]) / len(results["timings"])
            print(f"  Average response time: {avg_time:.3f}s")
        
        if results["rate_limit_headers"]:
            last_header = results["rate_limit_headers"][-1]
            print(f"  Last rate limit info: {last_header}")
        
        print()


async def test_burst_behavior():
    """Test burst request behavior"""
    print("\n" + "=" * 60)
    print("Burst Behavior Test")
    print("=" * 60)
    
    url = "http://localhost:8002/health"
    
    print("Sending 20 requests with no delay (burst)...")
    results = await test_rate_limit(url, num_requests=20, delay=0)
    
    print(f"\nBurst results:")
    print(f"  Successful: {results['successful']}")
    print(f"  Rate limited: {results['rate_limited']}")
    
    print("\nWaiting 60 seconds for rate limit reset...")
    await asyncio.sleep(60)
    
    print("Sending 20 more requests...")
    results = await test_rate_limit(url, num_requests=20, delay=0)
    
    print(f"\nAfter reset results:")
    print(f"  Successful: {results['successful']}")
    print(f"  Rate limited: {results['rate_limited']}")


async def test_different_strategies():
    """Test different rate limiting strategies"""
    print("\n" + "=" * 60)
    print("Rate Limiting Strategy Test")
    print("=" * 60)
    
    # Test with different headers to simulate different strategies
    base_url = "http://localhost:8002"
    endpoint = "/database/test_db/ontologies"
    
    headers_sets = [
        {"X-Forwarded-For": "192.168.1.1"},  # IP strategy
        {"X-User-ID": "user123"},  # User strategy
        {"X-API-Key": "test_key_123"},  # API key strategy
    ]
    
    for headers in headers_sets:
        print(f"\nTesting with headers: {headers}")
        
        async with httpx.AsyncClient(headers=headers) as client:
            for i in range(5):
                response = await client.get(f"{base_url}{endpoint}")
                print(f"  Request {i+1}: {response.status_code}")
                await asyncio.sleep(0.1)


async def main():
    """Run all rate limiting tests"""
    
    print("🔥 RATE LIMITING TEST SUITE")
    print("Testing API rate limiting implementation")
    print("Based on Context7 recommendations\n")
    
    # Check if services are running
    print("Checking if services are running...")
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get("http://localhost:8002/health")
            if response.status_code == 200:
                print("✅ BFF service is running")
            else:
                print("⚠️ BFF service returned:", response.status_code)
    except Exception as e:
        print(f"❌ BFF service is not accessible: {e}")
        print("Please ensure the BFF service is running on port 8002")
        return
    
    # Run tests
    await test_multiple_endpoints()
    
    # Optional: Run burst test (takes longer)
    # await test_burst_behavior()
    
    # Test different strategies
    await test_different_strategies()
    
    print("\n✅ Rate limiting tests completed!")
    print("Review the results to ensure rate limiting is working as expected.")


if __name__ == "__main__":
    asyncio.run(main())