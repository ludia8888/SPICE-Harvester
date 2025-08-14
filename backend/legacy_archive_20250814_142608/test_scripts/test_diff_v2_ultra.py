#!/usr/bin/env python3
"""
🔥 ULTRA: Test diff v2 implementation
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
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

async def test_diff_v2():
    """Test diff v2 implementation"""
    terminus = AsyncTerminusService()
    test_db = f"diff_v2_test_{int(time.time())}"
    
    try:
        # Create test database
        await terminus.create_database(test_db, "Diff v2 Test Database")
        logger.info(f"✅ Created test database: {test_db}")
        
        # Add initial schema to main
        product_schema = {
            "@type": "Class",
            "@id": "Product",
            "@key": {"@type": "Random"},
            "name": {"@class": "xsd:string", "@type": "Optional"}
        }
        
        await terminus._make_request(
            "POST",
            f"/api/document/{terminus.connection_info.account}/{test_db}",
            [product_schema],
            params={"graph_type": "schema", "author": "test", "message": "Initial Product"}
        )
        logger.info("✅ Added Product schema to main branch")
        
        # Create feature branch (no slashes in name)
        await terminus.create_branch(test_db, "feature-test", "main")
        logger.info("✅ Created feature-test branch")
        
        # Add Customer schema to feature branch
        customer_schema = {
            "@type": "Class",
            "@id": "Customer",
            "@key": {"@type": "Random"},
            "email": {"@class": "xsd:string", "@type": "Optional"}
        }
        
        # Get the feature branch path
        feature_db_path = f"{test_db}/local/branch/feature-test"
        
        await terminus._make_request(
            "POST",
            f"/api/document/{terminus.connection_info.account}/{feature_db_path}",
            [customer_schema],
            params={"graph_type": "schema", "author": "test", "message": "Add Customer"}
        )
        logger.info("✅ Added Customer schema to feature branch")
        
        # Test 1: Direct schema comparison
        logger.info("\n🔍 Test 1: Direct schema comparison")
        
        # Get main schemas
        main_result = await terminus._make_request(
            "GET",
            f"/api/document/{terminus.connection_info.account}/{test_db}",
            params={"graph_type": "schema"}
        )
        main_schemas = terminus._parse_schema_response(main_result)
        logger.info(f"Main schemas: {[s.get('@id') for s in main_schemas]}")
        
        # Get feature schemas
        feature_result = await terminus._make_request(
            "GET",
            f"/api/document/{terminus.connection_info.account}/{feature_db_path}",
            params={"graph_type": "schema"}
        )
        feature_schemas = terminus._parse_schema_response(feature_result)
        logger.info(f"Feature schemas: {[s.get('@id') for s in feature_schemas]}")
        
        # Test 2: Use diff method
        logger.info("\n🔍 Test 2: Using diff method")
        diff_result = await terminus.diff(test_db, "main", "feature-test")
        logger.info(f"Diff result: {len(diff_result)} changes")
        
        if diff_result:
            for i, change in enumerate(diff_result):
                logger.info(f"Change {i}: type={change['type']}, path={change['path']}")
                if change['type'] == 'added':
                    logger.info(f"  Added: {change.get('new_value', {}).get('@id')}")
                elif change['type'] == 'deleted':
                    logger.info(f"  Deleted: {change.get('old_value', {}).get('@id')}")
                elif change['type'] == 'modified':
                    logger.info(f"  Modified: {change.get('old_value', {}).get('@id')} -> {change.get('new_value', {}).get('@id')}")
        
        # Test 3: Reverse diff
        logger.info("\n🔍 Test 3: Reverse diff")
        reverse_diff = await terminus.diff(test_db, "feature-test", "main")
        logger.info(f"Reverse diff: {len(reverse_diff)} changes")
        
        return len(diff_result) > 0
        
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
    success = await test_diff_v2()
    
    print("\n" + "="*80)
    print("🔥 DIFF V2 TEST RESULT")
    print("="*80)
    print(f"Result: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print("="*80)
    
    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)