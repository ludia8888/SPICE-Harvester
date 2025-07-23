#!/usr/bin/env python3
"""
Test what BFF actually sends to OMS by intercepting the request
"""

import asyncio
import httpx
import json

async def test_bff_request():
    """Test BFF transformation by looking at actual HTTP calls"""
    
    db_name = "test_bff_request"
    
    async with httpx.AsyncClient() as client:
        # Create database through BFF
        resp = await client.post(
            "http://localhost:8002/api/v1/databases",
            json={"name": db_name, "description": "BFF request test"}
        )
        print(f"Database creation: {resp.status_code}")
        
        # Create Employee first
        employee_class = {
            "id": "Employee",
            "label": "Employee",
            "properties": [
                {"name": "name", "type": "string", "required": True, "label": "Name"}
            ]
        }
        
        resp = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json=employee_class
        )
        print(f"Employee creation via BFF: {resp.status_code}")
        
        # Now create Team with link properties through BFF
        team_class = {
            "id": "Team",
            "label": "Team",
            "properties": [
                {"name": "name", "type": "string", "required": True, "label": "Team Name"},
                {
                    "name": "leader",
                    "type": "link",
                    "target": "Employee",  # BFF should transform this to linkTarget
                    "required": True,
                    "label": "Team Leader"
                }
            ]
        }
        
        print("\n--- Sending to BFF ---")
        print(json.dumps(team_class, indent=2))
        
        resp = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json=team_class
        )
        print(f"\nBFF Response Status: {resp.status_code}")
        if resp.status_code != 200:
            print(f"BFF Error: {resp.text}")
        else:
            print(f"BFF Success: {resp.json()}")
        
        # Cleanup
        await client.delete(f"http://localhost:8002/api/v1/databases/{db_name}")
        print("\nCleanup complete")


if __name__ == "__main__":
    asyncio.run(test_bff_request())