#!/usr/bin/env python3
"""
🔥 ULTRA: Test REAL diff and merge API with correct parameters
"""

import asyncio
import logging
import sys
import os
import httpx
import json

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

TERMINUS_URL = "http://localhost:6363"
AUTH = ("admin", "admin123")

async def test_real_diff_merge():
    """Test real diff and merge with proper parameters"""
    
    async with httpx.AsyncClient() as client:
        # Create test database
        test_db = "real_api_test"
        
        # Delete if exists
        await client.delete(f"{TERMINUS_URL}/api/db/admin/{test_db}", auth=AUTH)
        
        # Create new
        resp = await client.post(
            f"{TERMINUS_URL}/api/db/admin/{test_db}",
            auth=AUTH,
            json={"label": test_db, "comment": "Real API test"}
        )
        
        if resp.status_code == 200:
            logger.info(f"✅ Created database: {test_db}")
            
            # Add initial schema to main branch
            schema1 = {
                "@type": "Class",
                "@id": "Product",
                "name": {"@type": "Optional", "@class": "xsd:string"}
            }
            
            resp = await client.post(
                f"{TERMINUS_URL}/api/document/admin/{test_db}",
                auth=AUTH,
                json=[schema1],
                params={"graph_type": "schema", "author": "test", "message": "Add Product"}
            )
            main_commit = resp.headers.get('terminusdb-data-version', '')
            logger.info(f"Main commit: {main_commit}")
            
            # Create a branch
            resp = await client.post(
                f"{TERMINUS_URL}/api/db/admin/{test_db}/local/branch/feature",
                auth=AUTH,
                json={"origin": "main"}
            )
            logger.info(f"Branch creation: {resp.status_code}")
            
            # Add schema to feature branch
            schema2 = {
                "@type": "Class", 
                "@id": "Customer",
                "email": {"@type": "Optional", "@class": "xsd:string"}
            }
            
            # Checkout feature branch
            resp = await client.post(
                f"{TERMINUS_URL}/api/document/admin/{test_db}/local/branch/feature",
                auth=AUTH,
                json=[schema2],
                params={"graph_type": "schema", "author": "test", "message": "Add Customer"}
            )
            feature_commit = resp.headers.get('terminusdb-data-version', '')
            logger.info(f"Feature commit: {feature_commit}")
            
            # Test 1: Real DIFF with different parameter combinations
            logger.info("\n🔍 Testing REAL DIFF API")
            
            diff_tests = [
                # Test different parameter names and formats
                {"from": f"admin/{test_db}/local/branch/main", "to": f"admin/{test_db}/local/branch/feature"},
                {"from": "main", "to": "feature"},
                {"before": "main", "after": "feature"},
                {"source": f"admin/{test_db}/local/branch/main", "target": f"admin/{test_db}/local/branch/feature"},
                {"from_branch": "main", "to_branch": "feature"},
                # Try with database context
                {"database": f"admin/{test_db}", "from": "main", "to": "feature"},
            ]
            
            for params in diff_tests:
                try:
                    logger.info(f"\nTrying diff params: {params}")
                    resp = await client.post(
                        f"{TERMINUS_URL}/api/diff",
                        auth=AUTH,
                        json=params
                    )
                    logger.info(f"Response: {resp.status_code}")
                    if resp.status_code == 200:
                        logger.info(f"✅ SUCCESS! Response: {resp.text[:500]}")
                        break
                    else:
                        logger.info(f"Response: {resp.text[:200]}")
                except Exception as e:
                    logger.error(f"Error: {e}")
            
            # Test 2: Real MERGE with different parameter combinations
            logger.info("\n🔍 Testing REAL MERGE API")
            
            merge_tests = [
                {"from": "feature", "into": "main"},
                {"source": "feature", "target": "main"},
                {"from_branch": "feature", "to_branch": "main"},
                {"branch": "feature", "into": "main"},
                # With database context
                {"database": f"admin/{test_db}", "from": "feature", "into": "main"},
                {"db": f"admin/{test_db}", "source": "feature", "target": "main"},
            ]
            
            for params in merge_tests:
                try:
                    logger.info(f"\nTrying merge params: {params}")
                    resp = await client.post(
                        f"{TERMINUS_URL}/api/merge",
                        auth=AUTH,
                        json=params
                    )
                    logger.info(f"Response: {resp.status_code}")
                    if resp.status_code == 200:
                        logger.info(f"✅ SUCCESS! Response: {resp.text[:500]}")
                        break
                    else:
                        logger.info(f"Response: {resp.text[:200]}")
                except Exception as e:
                    logger.error(f"Error: {e}")
            
            # Cleanup
            await client.delete(f"{TERMINUS_URL}/api/db/admin/{test_db}", auth=AUTH)

async def main():
    """Main runner"""
    await test_real_diff_merge()
    
    print("\n" + "="*80)
    print("🔥 REAL DIFF/MERGE API TEST COMPLETE")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())