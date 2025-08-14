#!/usr/bin/env python3
"""
🔥 Test TerminusDB v11.x actual API structure
"""

import asyncio
import logging
import sys
import os
import json
import time

# PYTHONPATH 설정
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oms.services.async_terminus import AsyncTerminusService

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

async def test_terminus_v11_api():
    """Test actual TerminusDB v11.x API endpoints"""
    terminus = AsyncTerminusService()
    test_db = f"api_test_{int(time.time())}"
    
    try:
        # Create test database
        await terminus.create_database(test_db, "API Test Database")
        logger.info(f"✅ Created test database: {test_db}")
        
        # Test 1: Check database info
        logger.info("\n🔍 Test 1: Database info")
        db_info = await terminus._make_request("GET", f"/api/db/{terminus.connection_info.account}/{test_db}")
        logger.info(f"Database info: {json.dumps(db_info, indent=2)}")
        
        # Test 2: Try different branch endpoints
        logger.info("\n🔍 Test 2: Branch endpoints")
        branch_endpoints = [
            f"/api/branch/{terminus.connection_info.account}/{test_db}",
            f"/api/db/{terminus.connection_info.account}/{test_db}/branch",
            f"/api/db/{terminus.connection_info.account}/{test_db}/_branch",
            f"/api/branches/{terminus.connection_info.account}/{test_db}",
        ]
        
        for endpoint in branch_endpoints:
            try:
                result = await terminus._make_request("GET", endpoint)
                logger.info(f"✅ {endpoint}: {result}")
                break
            except Exception as e:
                logger.error(f"❌ {endpoint}: {str(e)[:100]}")
        
        # Test 3: Create a branch and see what happens
        logger.info("\n🔍 Test 3: Create branch and check structure")
        
        # Create branch using known working method
        branch_created = await terminus.create_branch(test_db, "test-branch", "main")
        logger.info(f"Branch created: {branch_created}")
        
        # Check if branch is a separate database
        branch_as_db_paths = [
            f"{test_db}/local/branch/test-branch",
            f"{test_db}_branch_test-branch",
            f"{test_db}/branch/test-branch",
            f"{test_db}_test-branch",
        ]
        
        for path in branch_as_db_paths:
            try:
                branch_info = await terminus._make_request("GET", f"/api/db/{terminus.connection_info.account}/{path}")
                logger.info(f"✅ Branch as DB at {path}: {branch_info}")
                break
            except Exception as e:
                logger.error(f"❌ Branch not at {path}")
        
        # Test 4: Try to get commits/log
        logger.info("\n🔍 Test 4: Commit/log endpoints")
        log_endpoints = [
            f"/api/log/{terminus.connection_info.account}/{test_db}",
            f"/api/commits/{terminus.connection_info.account}/{test_db}",
            f"/api/db/{terminus.connection_info.account}/{test_db}/commits",
            f"/api/db/{terminus.connection_info.account}/{test_db}/log",
        ]
        
        for endpoint in log_endpoints:
            try:
                result = await terminus._make_request("GET", endpoint)
                logger.info(f"✅ {endpoint}: Found {len(result) if isinstance(result, list) else 1} entries")
                break
            except Exception as e:
                logger.error(f"❌ {endpoint}: {str(e)[:100]}")
        
        # Test 5: Check diff endpoint methods
        logger.info("\n🔍 Test 5: Diff endpoint methods")
        
        # Try POST method for diff
        try:
            diff_data = {
                "from": f"{terminus.connection_info.account}/{test_db}/local/branch/main",
                "to": f"{terminus.connection_info.account}/{test_db}/local/branch/test-branch"
            }
            diff_result = await terminus._make_request("POST", "/api/diff", data=diff_data)
            logger.info(f"✅ POST /api/diff: {diff_result}")
        except Exception as e:
            logger.error(f"❌ POST /api/diff: {e}")
        
        # Try PATCH endpoint
        try:
            patch_endpoint = f"/api/patch/{terminus.connection_info.account}/{test_db}"
            patch_params = {"from": "main", "to": "test-branch"}
            patch_result = await terminus._make_request("GET", patch_endpoint, params=patch_params)
            logger.info(f"✅ GET {patch_endpoint}: {patch_result}")
        except Exception as e:
            logger.error(f"❌ GET {patch_endpoint}: {e}")
        
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
    success = await test_terminus_v11_api()
    
    print("\n" + "="*80)
    print("🔥 TERMINUS v11.x API TEST RESULT")
    print("="*80)
    print(f"Result: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print("="*80)
    
    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)