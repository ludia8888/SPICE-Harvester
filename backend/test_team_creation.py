#!/usr/bin/env python3
"""
🔥 ULTRA DEBUG! Team creation test
"""

import asyncio
import httpx
import json
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


async def test_team_creation():
    """Test Team creation with detailed debugging"""
    
    test_db = "test_team_debug"
    bff_url = "http://localhost:8002"
    
    async with httpx.AsyncClient() as client:
        # Create database
        logger.info("Creating test database...")
        resp = await client.post(
            f"{bff_url}/api/v1/databases",
            json={"name": test_db, "description": "Team creation test"}
        )
        logger.info(f"Database creation status: {resp.status_code}")
        
        # Create Employee class first
        logger.info("Creating Employee class...")
        employee_class = {
            "id": "Employee",
            "label": "Employee",
            "properties": [
                {"name": "name", "type": "string", "required": True, "label": "Name"}
            ]
        }
        resp = await client.post(
            f"{bff_url}/api/v1/database/{test_db}/ontology",
            json=employee_class
        )
        logger.info(f"Employee creation status: {resp.status_code}")
        
        # Create Team class with link properties
        logger.info("Creating Team class...")
        team_class = {
            "id": "Team",
            "label": "Team",
            "properties": [
                {"name": "name", "type": "string", "required": True, "label": "Team Name"},
                {
                    "name": "leader",
                    "type": "link",
                    "target": "Employee",  # Using 'target' for BFF transformation
                    "required": True,
                    "label": "Team Leader"
                },
                {
                    "name": "members",
                    "type": "array",
                    "items": {"type": "link", "target": "Employee"},  # Using 'target' for BFF transformation
                    "label": "Members"
                }
            ]
        }
        
        logger.debug(f"Sending Team class data: {json.dumps(team_class, indent=2)}")
        
        resp = await client.post(
            f"{bff_url}/api/v1/database/{test_db}/ontology",
            json=team_class
        )
        
        logger.info(f"Team creation status: {resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"Team creation error: {resp.text}")
        else:
            logger.info(f"Team creation response: {resp.json()}")
            
        # Cleanup
        logger.info("Cleaning up...")
        resp = await client.delete(f"{bff_url}/api/v1/databases/{test_db}")
        logger.info(f"Cleanup status: {resp.status_code}")


if __name__ == "__main__":
    asyncio.run(test_team_creation())