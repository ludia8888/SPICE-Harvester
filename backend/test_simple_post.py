#!/usr/bin/env python3
"""
Simple test to check if POST reaches BFF
"""

import asyncio
import httpx

async def test_simple():
    """Test if POST request is logged"""
    
    async with httpx.AsyncClient() as client:
        # Test database creation POST
        print("Testing POST /api/v1/databases")
        resp = await client.post(
            "http://localhost:8002/api/v1/databases",
            json={"name": "test_simple", "description": "Simple test"}
        )
        print(f"Database creation: {resp.status_code}")
        
        # Test ontology creation POST
        print("\nTesting POST /api/v1/database/test_simple/ontology")
        resp = await client.post(
            "http://localhost:8002/api/v1/database/test_simple/ontology",
            json={
                "label": "SimpleTest",
                "properties": []
            }
        )
        print(f"Ontology creation: {resp.status_code}")
        print(f"Response: {resp.text[:200]}")
        
        # Cleanup
        await client.delete("http://localhost:8002/api/v1/databases/test_simple")
        print("\nCleanup complete")


if __name__ == "__main__":
    asyncio.run(test_simple())