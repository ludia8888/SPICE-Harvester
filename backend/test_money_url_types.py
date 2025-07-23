#!/usr/bin/env python3
"""
Test MONEY and URL types
"""

import asyncio
import httpx
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_types():
    """Test MONEY and URL types"""
    
    async with httpx.AsyncClient() as client:
        # Create test database
        db_name = "testtypesdb"
        
        logger.info(f"Creating database: {db_name}")
        payload = {"name": db_name, "description": "Test MONEY and URL types"}
        logger.info(f"Database creation payload: {json.dumps(payload, indent=2)}")
        resp = await client.post(
            "http://localhost:8002/api/v1/databases",
            json=payload
        )
        if resp.status_code != 200:
            logger.error(f"Failed to create database: {resp.text}")
            return
            
        # Test 1: Simple class without MONEY or URL
        logger.info("Test 1: Simple class")
        simple_class = {
            "label": "SimpleClass",
            "properties": [
                {
                    "name": "name",
                    "type": "STRING",
                    "label": "Name",
                    "required": True
                }
            ]
        }
        
        resp = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json=simple_class
        )
        logger.info(f"Simple class result: {resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"Simple class error: {resp.text}")
        
        # Test 2: Class with URL type
        logger.info("Test 2: Class with URL type")
        url_class = {
            "label": "URLClass",
            "properties": [
                {
                    "name": "website",
                    "type": "URL",
                    "label": "Website",
                    "required": False
                }
            ]
        }
        
        resp = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json=url_class
        )
        logger.info(f"URL class result: {resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"URL class error: {resp.text}")
            
        # Test 3: Class with MONEY type
        logger.info("Test 3: Class with MONEY type")
        money_class = {
            "label": "MoneyClass",
            "properties": [
                {
                    "name": "budget",
                    "type": "MONEY",
                    "label": "Budget",
                    "required": False,
                    "constraints": {"currency": "KRW"}
                }
            ]
        }
        
        resp = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json=money_class
        )
        logger.info(f"MONEY class result: {resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"MONEY class error: {resp.text}")
            
        # Cleanup
        await client.delete(f"http://localhost:8002/api/v1/databases/{db_name}")
        logger.info("Cleanup complete")

if __name__ == "__main__":
    asyncio.run(test_types())