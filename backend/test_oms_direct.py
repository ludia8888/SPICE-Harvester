#!/usr/bin/env python3
"""
Test OMS directly to isolate the issue
"""

import asyncio
import httpx
import json

async def test_oms_direct():
    """Test OMS create_ontology directly"""
    
    db_name = "test_oms_direct"
    
    async with httpx.AsyncClient() as client:
        # Create database through BFF first
        resp = await client.post(
            "http://localhost:8002/api/v1/databases",
            json={"name": db_name, "description": "Direct OMS test"}
        )
        print(f"Database creation: {resp.status_code}")
        
        # Test data with linkTarget (OMS expects this)
        test_class = {
            "id": "DirectTest",
            "label": "Direct Test",
            "properties": [
                {
                    "name": "link_prop",
                    "type": "link",
                    "linkTarget": "SomeClass",  # OMS expects linkTarget
                    "label": "Link Property"
                }
            ]
        }
        
        # Call OMS directly
        print("\n--- Testing OMS directly ---")
        resp = await client.post(
            f"http://localhost:8000/api/v1/ontology/{db_name}/create",
            json=test_class
        )
        print(f"OMS direct status: {resp.status_code}")
        if resp.status_code != 200:
            print(f"OMS error: {resp.text}")
        else:
            print(f"OMS success: {resp.json()}")
            
        # Cleanup
        await client.delete(f"http://localhost:8002/api/v1/databases/{db_name}")
        print("\nCleanup complete")


if __name__ == "__main__":
    asyncio.run(test_oms_direct())