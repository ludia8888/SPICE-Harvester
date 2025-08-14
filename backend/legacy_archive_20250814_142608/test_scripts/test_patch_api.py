#!/usr/bin/env python3
"""
🔥 ULTRA: Test PATCH API parameters
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

async def test_patch_api():
    """Test PATCH API with different parameters"""
    terminus = AsyncTerminusService()
    test_db = "patch_test"
    
    try:
        # Create test database
        await terminus.create_database(test_db, "Patch Test")
        logger.info(f"✅ Created test database: {test_db}")
        
        # Add schema
        await terminus._make_request(
            "POST",
            f"/api/document/{terminus.connection_info.account}/{test_db}",
            [{"@type": "Class", "@id": "Product", "name": {"@class": "xsd:string", "@type": "Optional"}}],
            params={"graph_type": "schema", "author": "test", "message": "Add Product"}
        )
        
        # Create branch
        await terminus.create_branch(test_db, "feature", "main")
        
        # Add different schema to branch
        await terminus._make_request(
            "POST",
            f"/api/document/{terminus.connection_info.account}/{test_db}/local/branch/feature",
            [{"@type": "Class", "@id": "Customer", "email": {"@class": "xsd:string", "@type": "Optional"}}],
            params={"graph_type": "schema", "author": "test", "message": "Add Customer"}
        )
        
        logger.info("\n🔍 Testing PATCH API parameters")
        
        # Test different parameter combinations
        patch_tests = [
            # Basic branch references
            {"before": "main", "after": "feature"},
            {"before": f"{test_db}/local/branch/main", "after": f"{test_db}/local/branch/feature"},
            {"before": f"admin/{test_db}/local/branch/main", "after": f"admin/{test_db}/local/branch/feature"},
            
            # With full paths
            {"before": f"{terminus.connection_info.account}/{test_db}/local/branch/main", 
             "after": f"{terminus.connection_info.account}/{test_db}/local/branch/feature"},
            
            # Try "from/to" instead of "before/after"
            {"from": "main", "to": "feature"},
            {"from": f"{test_db}/local/branch/main", "to": f"{test_db}/local/branch/feature"},
            
            # With explicit database context
            {"db": f"{terminus.connection_info.account}/{test_db}", "before": "main", "after": "feature"},
        ]
        
        for params in patch_tests:
            try:
                logger.info(f"\nTrying: {params}")
                result = await terminus._make_request(
                    "POST",
                    f"/api/patch/{terminus.connection_info.account}/{test_db}",
                    data=params
                )
                logger.info(f"✅ SUCCESS! Result: {json.dumps(result, indent=2)}")
                return True
            except Exception as e:
                error_msg = str(e)
                if "500" in error_msg and "not sufficiently instantiated" in error_msg:
                    logger.info(f"❌ Arguments not instantiated")
                elif "400" in error_msg:
                    logger.info(f"❌ Bad request: {error_msg[:100]}")
                else:
                    logger.info(f"❌ Error: {error_msg[:100]}")
        
        # If none worked, try the real diff endpoint
        logger.info("\n🔍 Testing /api/diff endpoint")
        
        diff_tests = [
            {"from": f"{terminus.connection_info.account}/{test_db}/local/branch/main",
             "to": f"{terminus.connection_info.account}/{test_db}/local/branch/feature"},
            {"before": f"{terminus.connection_info.account}/{test_db}/local/branch/main",
             "after": f"{terminus.connection_info.account}/{test_db}/local/branch/feature"},
        ]
        
        for params in diff_tests:
            try:
                logger.info(f"\nTrying diff: {params}")
                result = await terminus._make_request("POST", "/api/diff", data=params)
                logger.info(f"✅ DIFF SUCCESS! Result: {json.dumps(result, indent=2)}")
                return True
            except Exception as e:
                logger.info(f"❌ Diff error: {str(e)[:100]}")
        
        return False
        
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
    success = await test_patch_api()
    
    print("\n" + "="*80)
    print("🔥 PATCH API TEST RESULT")
    print("="*80)
    print(f"Result: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())