#!/usr/bin/env python3
"""
Test OMS directly to see the actual error
"""

import asyncio
import httpx
import json

async def test_oms_error():
    """Test OMS directly to see what error it returns"""
    
    db_name = "test_direct_error"
    
    async with httpx.AsyncClient() as client:
        # Create database through BFF first
        resp = await client.post(
            "http://localhost:8002/api/v1/databases",
            json={"name": db_name, "description": "Direct error test"}
        )
        print(f"Database creation: {resp.status_code}")
        
        # First, create Employee class through OMS
        employee_class = {
            "id": "Employee",
            "label": "Employee",
            "properties": [
                {"name": "name", "type": "string", "required": True}
            ]
        }
        
        resp = await client.post(
            f"http://localhost:8000/api/v1/ontology/{db_name}/create",
            json=employee_class
        )
        print(f"Employee creation: {resp.status_code}")
        
        # Now test the problematic Team class with link properties
        # This is what BFF would send to OMS (without transformation)
        team_class_wrong = {
            "id": "Team",
            "label": "Team",
            "properties": [
                {"name": "name", "type": "string", "required": True},
                {
                    "name": "leader",
                    "type": "link",
                    "target": "Employee",  # BFF sends this
                    "required": True
                },
                {
                    "name": "members",
                    "type": "array",
                    "items": {"type": "link", "target": "Employee"}  # BFF sends this
                }
            ]
        }
        
        print("\n--- Testing with 'target' (what BFF sends) ---")
        resp = await client.post(
            f"http://localhost:8000/api/v1/ontology/{db_name}/create",
            json=team_class_wrong
        )
        print(f"Status: {resp.status_code}")
        if resp.status_code != 200:
            print(f"Error: {resp.text}")
        
        # Now test with correct format
        team_class_correct = {
            "id": "Team",
            "label": "Team", 
            "properties": [
                {"name": "name", "type": "string", "required": True},
                {
                    "name": "leader",
                    "type": "link",
                    "linkTarget": "Employee",  # OMS expects this
                    "required": True
                },
                {
                    "name": "members",
                    "type": "array",
                    "items": {"type": "link", "linkTarget": "Employee"}  # OMS expects this
                }
            ]
        }
        
        print("\n--- Testing with 'linkTarget' (what OMS expects) ---")
        resp = await client.post(
            f"http://localhost:8000/api/v1/ontology/{db_name}/create",
            json=team_class_correct
        )
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            print(f"Success: {resp.json()}")
        else:
            print(f"Error: {resp.text}")
        
        # Cleanup
        await client.delete(f"http://localhost:8002/api/v1/databases/{db_name}")
        print("\nCleanup complete")


if __name__ == "__main__":
    asyncio.run(test_oms_error())