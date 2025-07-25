#!/usr/bin/env python3
"""
🔥 Test actual TerminusDB v11.x branch behavior
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

async def test_branch_behavior():
    """Test how branches actually work in TerminusDB v11.x"""
    terminus = AsyncTerminusService()
    test_db = f"branch_test_{int(time.time())}"
    
    try:
        # Create test database
        await terminus.create_database(test_db, "Branch Behavior Test")
        logger.info(f"✅ Created test database: {test_db}")
        
        # Add initial schema
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
        logger.info("✅ Added Product schema")
        
        # Create a branch
        branch_name = "test-branch"
        await terminus.create_branch(test_db, branch_name, "main")
        logger.info(f"✅ Created branch: {branch_name}")
        
        # Test 1: Check if branch exists as a database
        logger.info("\n🔍 Test 1: Check branch as database")
        branch_paths = [
            f"{test_db}/local/branch/{branch_name}",
            f"{test_db}_branch_{branch_name}",
            f"{test_db}/branch/{branch_name}",
            f"{test_db}/{branch_name}",
        ]
        
        branch_db_found = None
        for path in branch_paths:
            try:
                result = await terminus._make_request(
                    "GET", 
                    f"/api/db/{terminus.connection_info.account}/{path}"
                )
                logger.info(f"✅ Branch found at: {path}")
                logger.info(f"   Result: {result}")
                branch_db_found = path
                break
            except Exception as e:
                logger.info(f"❌ Not at: {path}")
        
        # Test 2: Try to add schema directly to branch database (if found)
        if branch_db_found:
            logger.info(f"\n🔍 Test 2: Add schema to branch database")
            try:
                customer_schema = {
                    "@type": "Class",
                    "@id": "Customer",
                    "@key": {"@type": "Random"},
                    "email": {"@class": "xsd:string", "@type": "Optional"}
                }
                
                await terminus._make_request(
                    "POST",
                    f"/api/document/{terminus.connection_info.account}/{branch_db_found}",
                    [customer_schema],
                    params={"graph_type": "schema", "author": "test", "message": "Add Customer to branch"}
                )
                logger.info("✅ Successfully added Customer to branch database")
            except Exception as e:
                logger.error(f"❌ Failed to add to branch database: {e}")
        
        # Test 3: Check if TerminusDB tracks current branch
        logger.info(f"\n🔍 Test 3: Branch tracking")
        
        # Try different endpoints to get current branch
        branch_endpoints = [
            f"/api/db/{terminus.connection_info.account}/{test_db}/local/branch",
            f"/api/db/{terminus.connection_info.account}/{test_db}/_meta",
            f"/api/db/{terminus.connection_info.account}/{test_db}/head",
        ]
        
        for endpoint in branch_endpoints:
            try:
                result = await terminus._make_request("GET", endpoint)
                logger.info(f"✅ {endpoint}: {result}")
            except Exception as e:
                logger.info(f"❌ {endpoint}: Failed")
        
        # Test 4: List all databases to see branch structure
        logger.info(f"\n🔍 Test 4: List all databases")
        try:
            dbs = await terminus._make_request("GET", f"/api/db/{terminus.connection_info.account}")
            if isinstance(dbs, list):
                for db in dbs:
                    if test_db in str(db):
                        logger.info(f"Found related DB: {db}")
        except Exception as e:
            logger.error(f"Failed to list databases: {e}")
        
        # Test 5: Compare schemas between "branches"
        logger.info(f"\n🔍 Test 5: Schema comparison")
        
        # Get main schema
        main_schema = await terminus._make_request(
            "GET",
            f"/api/document/{terminus.connection_info.account}/{test_db}",
            params={"graph_type": "schema"}
        )
        logger.info(f"Main schema type: {type(main_schema)}")
        
        # If branch database was found, get its schema
        if branch_db_found:
            try:
                branch_schema = await terminus._make_request(
                    "GET",
                    f"/api/document/{terminus.connection_info.account}/{branch_db_found}",
                    params={"graph_type": "schema"}
                )
                logger.info(f"Branch schema type: {type(branch_schema)}")
                
                # Compare
                if str(main_schema) == str(branch_schema):
                    logger.info("⚠️ Main and branch schemas are identical")
                else:
                    logger.info("✅ Main and branch schemas differ")
            except Exception as e:
                logger.error(f"Failed to get branch schema: {e}")
        
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
    success = await test_branch_behavior()
    
    print("\n" + "="*80)
    print("🔥 TERMINUS BRANCH BEHAVIOR TEST")
    print("="*80)
    print(f"Result: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print("="*80)

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)