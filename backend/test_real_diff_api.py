#!/usr/bin/env python3
"""
🔥 ULTRA: Test REAL diff functionality with actual branch differences
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

async def test_real_diff():
    """Test REAL diff with actual different branches"""
    terminus = AsyncTerminusService()
    test_db = "real_diff_test"
    
    try:
        # Create test database
        await terminus.create_database(test_db, "Real Diff Test")
        logger.info(f"✅ Created test database: {test_db}")
        
        # Add initial schema to main
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
        logger.info("✅ Added Product schema to main")
        
        # Create feature branch
        await terminus.create_branch(test_db, "feature", "main")
        logger.info("✅ Created feature branch")
        
        # Switch to feature branch and add different schema
        # Note: TerminusDB v11.x might share data between branches
        # but let's test what actually happens
        
        customer_schema = {
            "@type": "Class",
            "@id": "Customer",
            "email": {"@class": "xsd:string", "@type": "Optional"}
        }
        
        await terminus._make_request(
            "POST",
            f"/api/document/{terminus.connection_info.account}/{test_db}/local/branch/feature",
            [customer_schema],
            params={"graph_type": "schema", "author": "test", "message": "Add Customer"}
        )
        logger.info("✅ Added Customer schema to feature branch")
        
        # Now test REAL diff
        logger.info("\n" + "="*60)
        logger.info("🔥 TESTING REAL DIFF API")
        logger.info("="*60)
        
        # Test 1: Diff from main to feature
        logger.info("\n📊 Test 1: Diff main -> feature")
        diff1 = await terminus.diff(test_db, "main", "feature")
        logger.info(f"Result: {json.dumps(diff1, indent=2)}")
        
        # Test 2: Diff from feature to main  
        logger.info("\n📊 Test 2: Diff feature -> main")
        diff2 = await terminus.diff(test_db, "feature", "main")
        logger.info(f"Result: {json.dumps(diff2, indent=2)}")
        
        # Test 3: Check if we get the dummy response
        logger.info("\n🚨 CRITICAL CHECK: Is this REAL or FAKE?")
        for diff_result in [diff1, diff2]:
            if diff_result and len(diff_result) > 0:
                first_item = diff_result[0]
                if (first_item.get("type") == "info" and 
                    "does not support true branch-based diff" in first_item.get("message", "")):
                    logger.error("❌ FAKE DIFF DETECTED! This is the dummy fallback response!")
                    logger.error("   The diff is NOT using real API!")
                    return False
                else:
                    logger.info("✅ This appears to be real diff data")
        
        # Test 4: Direct API call to verify
        logger.info("\n📊 Test 4: Direct diff API call")
        try:
            direct_result = await terminus._make_request(
                "POST",
                f"/api/db/{terminus.connection_info.account}/{test_db}/local/_diff",
                {"from": "main", "to": "feature"}
            )
            logger.info(f"Direct API result: {json.dumps(direct_result, indent=2)}")
        except Exception as e:
            logger.error(f"Direct API failed: {e}")
        
        return True
        
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
    success = await test_real_diff()
    
    print("\n" + "="*80)
    print("🔥 REAL DIFF TEST RESULT")
    print("="*80)
    print(f"Result: {'✅ USING REAL API' if success else '❌ STILL USING FAKE/DUMMY DATA'}")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())