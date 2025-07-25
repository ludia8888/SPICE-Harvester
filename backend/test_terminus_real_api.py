#!/usr/bin/env python3
"""
🔥 ULTRA: Test REAL TerminusDB API endpoints
NO MOCKS, NO FAKES - Only real API calls
"""

import asyncio
import logging
import sys
import os
import json
import httpx

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

TERMINUS_URL = "http://localhost:6363"
AUTH = ("admin", "admin123")

async def test_real_api():
    """Test actual TerminusDB API endpoints directly"""
    
    async with httpx.AsyncClient() as client:
        # Test 1: Get API info
        logger.info("\n🔍 Test 1: API Info")
        resp = await client.get(f"{TERMINUS_URL}/api", auth=AUTH)
        logger.info(f"API Info status: {resp.status_code}")
        
        # Test 2: Check for diff endpoint
        logger.info("\n🔍 Test 2: Testing diff endpoints")
        
        # Try different diff endpoints
        diff_endpoints = [
            "/api/diff",
            "/api/diff/admin/test_db",
            "/api/patch",
            "/api/compare",
            "/api/changes"
        ]
        
        for endpoint in diff_endpoints:
            try:
                # Try GET
                resp = await client.get(f"{TERMINUS_URL}{endpoint}", auth=AUTH)
                logger.info(f"GET {endpoint}: {resp.status_code}")
                if resp.status_code == 200:
                    logger.info(f"✅ Found working endpoint: {endpoint}")
                
                # Try POST
                resp = await client.post(f"{TERMINUS_URL}{endpoint}", auth=AUTH, json={})
                logger.info(f"POST {endpoint}: {resp.status_code}")
                if resp.status_code == 200:
                    logger.info(f"✅ Found working endpoint: {endpoint}")
                    
            except Exception as e:
                logger.error(f"{endpoint}: {e}")
        
        # Test 3: Check for merge endpoint
        logger.info("\n🔍 Test 3: Testing merge endpoints")
        
        merge_endpoints = [
            "/api/merge",
            "/api/branch/merge",
            "/api/rebase",
            "/api/pull"
        ]
        
        for endpoint in merge_endpoints:
            try:
                resp = await client.post(f"{TERMINUS_URL}{endpoint}", auth=AUTH, json={})
                logger.info(f"POST {endpoint}: {resp.status_code}")
                if resp.status_code != 404:
                    logger.info(f"✅ Endpoint exists: {endpoint}")
            except Exception as e:
                logger.error(f"{endpoint}: {e}")
        
        # Test 4: Check actual working database operations
        logger.info("\n🔍 Test 4: Real database operations")
        
        # Create test database
        test_db = "api_test_db"
        resp = await client.post(
            f"{TERMINUS_URL}/api/db/admin/{test_db}",
            auth=AUTH,
            json={"label": test_db, "comment": "API test"}
        )
        
        if resp.status_code == 200:
            logger.info(f"✅ Created database: {test_db}")
            
            # Try real diff between commits
            logger.info("\n🔍 Testing real diff")
            
            # Add schema
            schema1 = {
                "@type": "Class",
                "@id": "TestClass",
                "name": {"@type": "Optional", "@class": "xsd:string"}
            }
            
            resp = await client.post(
                f"{TERMINUS_URL}/api/document/admin/{test_db}",
                auth=AUTH,
                json=[schema1],
                params={"graph_type": "schema", "author": "test", "message": "Add TestClass"}
            )
            commit1 = resp.headers.get('terminusdb-data-version', '')
            logger.info(f"Commit 1: {commit1}")
            
            # Try actual diff API
            diff_params = {
                "before": "main",
                "after": commit1,
                "from": "main", 
                "to": commit1
            }
            
            for param_set in [{"before": "main", "after": commit1}, {"from": "main", "to": commit1}]:
                resp = await client.get(
                    f"{TERMINUS_URL}/api/diff",
                    auth=AUTH,
                    params=param_set
                )
                logger.info(f"Diff with {param_set}: {resp.status_code}")
                if resp.status_code == 200:
                    logger.info(f"✅ REAL DIFF WORKS: {resp.text[:200]}")
            
            # Cleanup
            await client.delete(f"{TERMINUS_URL}/api/db/admin/{test_db}", auth=AUTH)

async def main():
    """Main runner"""
    await test_real_api()
    
    print("\n" + "="*80)
    print("🔥 REAL API TEST COMPLETE")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())