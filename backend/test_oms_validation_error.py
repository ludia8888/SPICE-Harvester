#!/usr/bin/env python3
"""
Test OMS validation error to see exact error message
"""

import asyncio
import httpx
import json

async def test_validation():
    """Test OMS validation to see the real error"""
    
    db_name = "test_validation"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Create database
        resp = await client.post(
            "http://localhost:8002/api/v1/databases",
            json={"name": db_name, "description": "Validation test"}
        )
        print(f"Database creation: {resp.status_code}")
        
        # Test with properties missing labels - this is what was causing the error
        test_class = {
            "id": "TestClass",
            "label": "Test Class",
            "properties": [
                {"name": "prop1", "type": "string", "required": True},  # Missing label!
                {"name": "prop2", "type": "integer"}  # Missing label!
            ]
        }
        
        print("\n--- Testing without property labels (OMS direct) ---")
        resp = await client.post(
            f"http://localhost:8000/api/v1/ontology/{db_name}/create",
            json=test_class
        )
        print(f"Status: {resp.status_code}")
        print(f"Response: {resp.text}")
        
        # Now test with correct format
        test_class_correct = {
            "id": "TestClass",
            "label": "Test Class",
            "properties": [
                {"name": "prop1", "type": "string", "required": True, "label": "Property 1"},
                {"name": "prop2", "type": "integer", "label": "Property 2"}
            ]
        }
        
        print("\n--- Testing with property labels (OMS direct) ---")
        resp = await client.post(
            f"http://localhost:8000/api/v1/ontology/{db_name}/create",
            json=test_class_correct
        )
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            print(f"Success!")
        else:
            print(f"Error: {resp.text}")
        
        # Cleanup
        await client.delete(f"http://localhost:8002/api/v1/databases/{db_name}")
        print("\nCleanup complete")


if __name__ == "__main__":
    asyncio.run(test_validation())