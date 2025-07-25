#!/usr/bin/env python3
"""
🔥 ULTRA: Test PATCH endpoints for getting diffs
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
    format="%(message)s"
)
logger = logging.getLogger(__name__)

async def test_patch_endpoints():
    """Test PATCH endpoints for diffs"""
    terminus = AsyncTerminusService()
    test_db = "patch_test"
    
    try:
        # Create test database
        await terminus.create_database(test_db, "PATCH endpoint test")
        logger.info(f"✅ Created test database: {test_db}")
        
        # Add schema
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
        
        # Get some commits to diff
        commits = await terminus.get_commit_history(test_db)
        logger.info(f"Found {len(commits)} commits")
        
        if len(commits) >= 2:
            commit1 = commits[1]["id"]  # older
            commit2 = commits[0]["id"]  # newer
            
            logger.info(f"\n🔍 Testing PATCH endpoints between commits:")
            logger.info(f"  From: {commit1[:8]}")
            logger.info(f"  To: {commit2[:8]}")
            
            # Test various PATCH endpoint formats
            patch_endpoints = [
                # Standard patch endpoints
                (f"/api/patch/{terminus.connection_info.account}/{test_db}", 
                 {"from": commit1, "to": commit2}),
                (f"/api/db/{terminus.connection_info.account}/{test_db}/patch", 
                 {"from": commit1, "to": commit2}),
                (f"/api/db/{terminus.connection_info.account}/{test_db}/local/patch", 
                 {"from": commit1, "to": commit2}),
                (f"/api/db/{terminus.connection_info.account}/{test_db}/local/_patch", 
                 {"from": commit1, "to": commit2}),
                
                # Alternative parameter names
                (f"/api/db/{terminus.connection_info.account}/{test_db}/local/patch", 
                 {"before": commit1, "after": commit2}),
                (f"/api/db/{terminus.connection_info.account}/{test_db}/local/patch", 
                 {"base": commit1, "compare": commit2}),
            ]
            
            logger.info("\n📊 Testing PATCH endpoints:")
            logger.info("=" * 60)
            
            for endpoint, params in patch_endpoints:
                logger.info(f"\n🔗 {endpoint}")
                logger.info(f"   Params: {params}")
                
                # Try both GET and POST
                for method in ["GET", "POST"]:
                    try:
                        if method == "GET":
                            result = await terminus._make_request(method, endpoint, params=params)
                        else:
                            result = await terminus._make_request(method, endpoint, data=params)
                        
                        logger.info(f"   ✅ {method}: SUCCESS")
                        if isinstance(result, dict):
                            logger.info(f"   Response keys: {list(result.keys())}")
                        elif isinstance(result, list):
                            logger.info(f"   Response: list with {len(result)} items")
                        else:
                            logger.info(f"   Response type: {type(result)}")
                        
                        # Found working endpoint!
                        return True
                        
                    except Exception as e:
                        error_msg = str(e)
                        if "404" in error_msg:
                            logger.info(f"   ❌ {method}: 404 Not Found")
                        elif "405" in error_msg:
                            logger.info(f"   ❌ {method}: 405 Method Not Allowed")
                        elif "400" in error_msg:
                            logger.info(f"   ⚠️  {method}: 400 Bad Request")
                        else:
                            logger.info(f"   ❌ {method}: {error_msg[:60]}")
        
        logger.info("\n" + "=" * 60)
        logger.info("❌ No working PATCH endpoint found")
        return False
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
        
    finally:
        # Cleanup
        try:
            await terminus.delete_database(test_db)
            logger.info(f"\n🧹 Cleaned up: {test_db}")
        except:
            pass

async def main():
    """Main runner"""
    success = await test_patch_endpoints()
    
    print("\n" + "="*80)
    print("🔥 PATCH ENDPOINT TEST RESULT")
    print("="*80)
    print(f"Result: {'✅ FOUND WORKING ENDPOINT' if success else '❌ NO PATCH ENDPOINT FOUND'}")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())