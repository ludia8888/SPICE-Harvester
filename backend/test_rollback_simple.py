#!/usr/bin/env python3
"""
Simple test to verify the rollback endpoint construction
"""
import asyncio
import httpx
import json
import logging

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_rollback_endpoint():
    """Test different rollback endpoint approaches"""
    
    base_url = "http://localhost:6363"
    db_name = "test_db"
    account = "admin"
    target_commit = "abc123"
    
    # Auth
    auth = httpx.BasicAuth("admin", "admin123")
    
    async with httpx.AsyncClient(auth=auth) as client:
        # Test 1: Original endpoint (that's failing)
        logger.info("=== Test 1: Original endpoint ===")
        endpoint1 = f"{base_url}/api/db/{account}/{db_name}/_reset"
        data1 = {
            "target": target_commit,
            "label": f"Rollback to {target_commit}"
        }
        logger.info(f"POST {endpoint1}")
        logger.info(f"Data: {json.dumps(data1, indent=2)}")
        
        try:
            response = await client.post(endpoint1, json=data1)
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response: {response.text}")
        except Exception as e:
            logger.error(f"Request failed: {e}")
        
        # Test 2: Alternative endpoint with commit_descriptor
        logger.info("\n=== Test 2: Alternative with commit_descriptor ===")
        endpoint2 = f"{base_url}/api/db/{account}/{db_name}/_reset"
        data2 = {
            "commit_descriptor": target_commit
        }
        logger.info(f"POST {endpoint2}")
        logger.info(f"Data: {json.dumps(data2, indent=2)}")
        
        try:
            response = await client.post(endpoint2, json=data2)
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response: {response.text}")
        except Exception as e:
            logger.error(f"Request failed: {e}")
        
        # Test 3: Branch update approach
        logger.info("\n=== Test 3: Branch update approach ===")
        branch = "main"
        endpoint3 = f"{base_url}/api/branch/{account}/{db_name}/local/branch/{branch}"
        data3 = {
            "head": target_commit,
            "origin": f"{account}/{db_name}/local/commit/{target_commit}"
        }
        logger.info(f"PUT {endpoint3}")
        logger.info(f"Data: {json.dumps(data3, indent=2)}")
        
        try:
            response = await client.put(endpoint3, json=data3)
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response: {response.text}")
        except Exception as e:
            logger.error(f"Request failed: {e}")
        
        # Test 4: Check if there's a reset endpoint at different path
        logger.info("\n=== Test 4: Alternative reset path ===")
        endpoint4 = f"{base_url}/api/reset/{account}/{db_name}"
        data4 = {
            "commit": target_commit
        }
        logger.info(f"POST {endpoint4}")
        logger.info(f"Data: {json.dumps(data4, indent=2)}")
        
        try:
            response = await client.post(endpoint4, json=data4)
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response: {response.text}")
        except Exception as e:
            logger.error(f"Request failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_rollback_endpoint())