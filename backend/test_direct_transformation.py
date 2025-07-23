#!/usr/bin/env python3
"""
🔥 ULTRA DEBUG! Direct test of transformation logic
"""

import asyncio
import httpx
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_direct():
    """Test direct API call"""
    
    async with httpx.AsyncClient() as client:
        # First, let's see if the services are actually running
        resp = await client.get("http://localhost:8002/health")
        logger.info(f"BFF Health: {resp.status_code} - {resp.json()}")
        
        resp = await client.get("http://localhost:8000/health")
        logger.info(f"OMS Health: {resp.status_code} - {resp.json()}")
        
        # Create test database
        db_name = "test_transform_debug"
        resp = await client.post(
            "http://localhost:8002/api/v1/databases",
            json={"name": db_name, "description": "Transformation test"}
        )
        logger.info(f"Database creation: {resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"Database creation failed: {resp.text}")
            return
            
        # Simple property test first
        simple_class = {
            "id": "SimpleTest",
            "label": "Simple Test",
            "properties": [
                {"name": "name", "type": "string", "required": True, "label": "Name"}
            ]
        }
        
        resp = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json=simple_class
        )
        logger.info(f"Simple class creation: {resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"Simple class creation failed: {resp.text}")
        
        # Now test with link property
        link_class = {
            "id": "LinkTest",
            "label": "Link Test",
            "properties": [
                {"name": "name", "type": "string", "required": True, "label": "Name"},
                {
                    "name": "simple_ref",
                    "type": "link",
                    "target": "SimpleTest",  # Using target (not linkTarget)
                    "required": False,
                    "label": "Simple Reference"
                }
            ]
        }
        
        resp = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json=link_class
        )
        logger.info(f"Link class creation: {resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"Link class creation failed: {resp.text}")
        else:
            logger.info("Link class created successfully!")
            
        # Cleanup
        resp = await client.delete(f"http://localhost:8002/api/v1/databases/{db_name}")
        logger.info(f"Cleanup: {resp.status_code}")


if __name__ == "__main__":
    asyncio.run(test_direct())