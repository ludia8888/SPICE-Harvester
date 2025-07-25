#!/usr/bin/env python3
"""
🔥 ULTRA: Discover actual TerminusDB API structure
"""

import asyncio
import logging
import sys
import os
import json

# PYTHONPATH 설정
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oms.services.async_terminus import AsyncTerminusService

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

async def test_api_discovery():
    """Discover TerminusDB API endpoints"""
    terminus = AsyncTerminusService()
    test_db = "api_discovery_test"
    
    try:
        # Create test database
        await terminus.create_database(test_db, "API Discovery Test")
        logger.info(f"✅ Created test database: {test_db}")
        
        # Add schema to main
        product_schema = {
            "@type": "Class",
            "@id": "Product",
            "name": {"@class": "xsd:string", "@type": "Optional"}
        }
        
        await terminus._make_request(
            "POST",
            f"/api/document/{terminus.connection_info.account}/{test_db}",
            [product_schema],
            params={"graph_type": "schema", "author": "test", "message": "Add Product"}
        )
        logger.info("✅ Added Product schema")
        
        # Create branch
        await terminus.create_branch(test_db, "feature", "main")
        logger.info("✅ Created feature branch")
        
        # Add different schema to feature branch
        await terminus.checkout(test_db, "feature")
        
        customer_schema = {
            "@type": "Class",
            "@id": "Customer",
            "email": {"@class": "xsd:string", "@type": "Optional"}
        }
        
        await terminus._make_request(
            "POST",
            f"/api/document/{terminus.connection_info.account}/{test_db}",
            [customer_schema],
            params={"graph_type": "schema", "author": "test", "message": "Add Customer"}
        )
        logger.info("✅ Added Customer schema to feature branch")
        
        # Test 1: Test REBASE (which exists)
        logger.info("\n🔍 Test 1: Testing REBASE API")
        try:
            # Try to rebase feature onto main
            rebase_result = await terminus.rebase(test_db, onto="main", branch="feature")
            logger.info(f"✅ REBASE SUCCESS: {rebase_result}")
        except Exception as e:
            logger.error(f"❌ REBASE FAILED: {e}")
        
        # Test 2: Test branch-specific endpoints
        logger.info("\n🔍 Test 2: Testing branch operations")
        
        # Check if there's a branch merge endpoint
        branch_endpoints = [
            f"/api/branch/{terminus.connection_info.account}/{test_db}/merge",
            f"/api/db/{terminus.connection_info.account}/{test_db}/merge",
            f"/api/db/{terminus.connection_info.account}/{test_db}/local/merge",
            f"/api/db/{terminus.connection_info.account}/{test_db}/local/_merge",
        ]
        
        for endpoint in branch_endpoints:
            try:
                result = await terminus._make_request(
                    "POST",
                    endpoint,
                    {"from": "feature", "into": "main"}
                )
                logger.info(f"✅ Found merge endpoint: {endpoint}")
                logger.info(f"   Result: {result}")
                break
            except Exception as e:
                if "404" not in str(e):
                    logger.info(f"⚠️  {endpoint}: {str(e)[:100]}")
        
        # Test 3: Test PATCH endpoint (for getting diffs)
        logger.info("\n🔍 Test 3: Testing PATCH/DIFF endpoints")
        
        # Try to get a patch between branches
        patch_endpoints = [
            (f"/api/patch/{terminus.connection_info.account}/{test_db}", {"from": "main", "to": "feature"}),
            (f"/api/db/{terminus.connection_info.account}/{test_db}/patch", {"from": "main", "to": "feature"}),
            (f"/api/db/{terminus.connection_info.account}/{test_db}/local/patch", {"from": "main", "to": "feature"}),
        ]
        
        for endpoint, params in patch_endpoints:
            try:
                # Try both GET and POST
                for method in ["GET", "POST"]:
                    try:
                        if method == "GET":
                            result = await terminus._make_request(method, endpoint, params=params)
                        else:
                            result = await terminus._make_request(method, endpoint, data=params)
                        logger.info(f"✅ {method} {endpoint}: SUCCESS")
                        logger.info(f"   Result: {json.dumps(result, indent=2) if isinstance(result, dict) else str(result)[:200]}")
                        break
                    except Exception as e:
                        if "404" not in str(e) and "405" not in str(e):
                            logger.info(f"   {method}: {str(e)[:100]}")
            except:
                pass
        
        # Test 4: Check commit/log structure
        logger.info("\n🔍 Test 4: Checking commit structure")
        
        commits = await terminus.get_commit_history(test_db)
        logger.info(f"Found {len(commits)} commits")
        
        if commits:
            logger.info("Commit structure:")
            logger.info(json.dumps(commits[0], indent=2))
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
        
    finally:
        # Cleanup
        try:
            await terminus.delete_database(test_db)
            logger.info(f"🧹 Cleaned up: {test_db}")
        except:
            pass

async def main():
    """Main runner"""
    success = await test_api_discovery()
    
    print("\n" + "="*80)
    print("🔥 API DISCOVERY RESULT")
    print("="*80)
    print(f"Result: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())