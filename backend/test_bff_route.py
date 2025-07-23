#!/usr/bin/env python3
"""
Test if BFF route is accessible
"""

import asyncio
import httpx

async def test_route():
    """Test if the BFF route is reachable"""
    
    async with httpx.AsyncClient() as client:
        # Test health endpoint first
        resp = await client.get("http://localhost:8002/health")
        print(f"Health check: {resp.status_code}")
        
        # Test if the ontology endpoint exists with OPTIONS
        resp = await client.options("http://localhost:8002/api/v1/database/test/ontology")
        print(f"OPTIONS /api/v1/database/test/ontology: {resp.status_code}")
        print(f"Allowed methods: {resp.headers.get('allow', 'No Allow header')}")
        
        # Test with invalid data to see if route is hit
        resp = await client.post(
            "http://localhost:8002/api/v1/database/test/ontology",
            json={}  # Empty data should trigger validation error
        )
        print(f"\nPOST with empty data: {resp.status_code}")
        print(f"Response: {resp.text}")


if __name__ == "__main__":
    asyncio.run(test_route())