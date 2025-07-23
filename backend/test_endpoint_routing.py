#!/usr/bin/env python3
"""
Test which endpoint is actually being called
"""

import asyncio
import httpx
import json

async def test_endpoints():
    """Test both endpoint patterns"""
    
    db_name = "test_endpoint_debug"
    
    async with httpx.AsyncClient() as client:
        # Create database first
        resp = await client.post(
            "http://localhost:8002/api/v1/databases",
            json={"name": db_name, "description": "Endpoint test"}
        )
        print(f"Database creation: {resp.status_code}")
        
        # Test data with link property
        test_class = {
            "id": "TestClass",
            "label": "Test Class",
            "properties": [
                {
                    "name": "link_prop",
                    "type": "link",
                    "target": "SomeClass",  # Using target
                    "label": "Link Property"
                }
            ]
        }
        
        # Try endpoint 1: /api/v1/database/{db_name}/ontology (router version)
        print("\n--- Testing router endpoint ---")
        resp = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json=test_class
        )
        print(f"Router endpoint status: {resp.status_code}")
        if resp.status_code != 200:
            print(f"Router endpoint error: {resp.text}")
            
        # Try endpoint 2: /database/{db_name}/ontology (direct main.py version)
        print("\n--- Testing direct endpoint ---")
        resp = await client.post(
            f"http://localhost:8002/database/{db_name}/ontology",
            json=test_class
        )
        print(f"Direct endpoint status: {resp.status_code}")
        if resp.status_code != 200:
            print(f"Direct endpoint error: {resp.text}")
            
        # Cleanup
        await client.delete(f"http://localhost:8002/api/v1/databases/{db_name}")
        print("\nCleanup complete")


if __name__ == "__main__":
    asyncio.run(test_endpoints())