#!/usr/bin/env python3
"""
Test enum constraint with Korean values
"""

import asyncio
import httpx
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_enum():
    """Test enum constraint"""
    
    async with httpx.AsyncClient() as client:
        # Create test database
        db_name = "testenumdb"
        
        logger.info(f"Creating database: {db_name}")
        resp = await client.post(
            "http://localhost:8002/api/v1/databases",
            json={"name": db_name, "description": "Test enum constraints"}
        )
        if resp.status_code != 200:
            logger.error(f"Failed to create database: {resp.text}")
            return
            
        # Test 1: Simple class with English enum
        logger.info("Test 1: English enum")
        english_enum_class = {
            "label": "StatusTest",
            "properties": [
                {
                    "name": "status",
                    "type": "STRING",
                    "label": "Status",
                    "required": True,
                    "constraints": {
                        "enum": ["pending", "active", "inactive"]
                    }
                }
            ]
        }
        
        resp = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json=english_enum_class
        )
        logger.info(f"English enum result: {resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"English enum error: {resp.text}")
        
        # Test 2: Class with Korean enum
        logger.info("Test 2: Korean enum")
        korean_enum_class = {
            "label": "PositionTest",
            "properties": [
                {
                    "name": "position",
                    "type": "STRING",
                    "label": "직급",
                    "required": True,
                    "constraints": {
                        "enum": ["사원", "대리", "과장", "차장", "부장", "이사", "상무", "전무", "사장"]
                    }
                }
            ]
        }
        
        resp = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json=korean_enum_class
        )
        logger.info(f"Korean enum result: {resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"Korean enum error: {resp.text}")
            
        # Test 3: Just ARRAY<STRING> without enum
        logger.info("Test 3: Just ARRAY<STRING>")
        array_class = {
            "label": "ArrayTest",
            "properties": [
                {
                    "name": "skills",
                    "type": "ARRAY<STRING>",
                    "label": "Skills",
                    "required": False
                }
            ]
        }
        
        resp = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json=array_class
        )
        logger.info(f"Array only result: {resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"Array only error: {resp.text}")
        
        # Test 4: Complex class like Employee
        logger.info("Test 4: Complex Employee class")
        employee_class = {
            "label": "Employee",
            "properties": [
                {
                    "name": "name",
                    "type": "STRING",
                    "label": "이름",
                    "required": True
                },
                {
                    "name": "position",
                    "type": "STRING",
                    "label": "직급",
                    "required": True,
                    "constraints": {
                        "enum": ["사원", "대리", "과장", "차장", "부장", "이사", "상무", "전무", "사장"]
                    }
                },
                {
                    "name": "skills",
                    "type": "ARRAY<STRING>",
                    "label": "보유 기술",
                    "required": False
                }
            ]
        }
        
        resp = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json=employee_class
        )
        logger.info(f"Complex Employee result: {resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"Complex Employee error: {resp.text}")
            
        # Cleanup
        await client.delete(f"http://localhost:8002/api/v1/databases/{db_name}")
        logger.info("Cleanup complete")

if __name__ == "__main__":
    asyncio.run(test_enum())