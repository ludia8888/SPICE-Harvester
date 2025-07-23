#!/usr/bin/env python3
"""
Test OMS directly with link property
"""

import asyncio
import httpx
import json

async def test_oms_link():
    """Test OMS with link property after BFF transformation"""
    
    db_name = "test_oms_link"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Create database through BFF
        resp = await client.post(
            "http://localhost:8002/api/v1/databases",
            json={"name": db_name, "description": "OMS link test"}
        )
        print(f"Database creation: {resp.status_code}")
        
        # Create Employee class first
        employee_class = {
            "id": "Employee",
            "label": "Employee",
            "properties": [
                {"name": "name", "type": "string", "required": True, "label": "Name"}
            ]
        }
        
        resp = await client.post(
            f"http://localhost:8000/api/v1/ontology/{db_name}/create",
            json=employee_class
        )
        print(f"Employee creation via OMS: {resp.status_code}")
        
        # Test Team with linkTarget (what BFF should send after transformation)
        team_with_linktarget = {
            "id": "Team",
            "label": "Team",
            "properties": [
                {"name": "name", "type": "string", "required": True, "label": "Team Name"},
                {
                    "name": "leader",
                    "type": "link",
                    "linkTarget": "Employee",  # This is what BFF should transform to
                    "required": True,
                    "label": "Team Leader"
                }
            ]
        }
        
        print("\n--- Testing OMS directly with linkTarget ---")
        print(json.dumps(team_with_linktarget, indent=2))
        
        resp = await client.post(
            f"http://localhost:8000/api/v1/ontology/{db_name}/create",
            json=team_with_linktarget
        )
        print(f"\nOMS Response Status: {resp.status_code}")
        if resp.status_code == 200:
            print(f"Success: Team created")
            # Now check what was created
            resp = await client.get(f"http://localhost:8000/api/v1/ontology/{db_name}/Team")
            print(f"\nGET Team status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                print(f"Team data: {json.dumps(data, indent=2)}")
        else:
            print(f"Error: {resp.text}")
        
        # Cleanup
        await client.delete(f"http://localhost:8002/api/v1/databases/{db_name}")
        print("\nCleanup complete")


if __name__ == "__main__":
    asyncio.run(test_oms_link())