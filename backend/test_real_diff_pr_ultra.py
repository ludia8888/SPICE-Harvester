#!/usr/bin/env python3
"""
🔥 ULTRA: Test REAL diff and PR functionality with enhanced implementation
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

async def test_real_diff_and_pr():
    """Test REAL diff and PR with actual branch differences"""
    terminus = AsyncTerminusService()
    test_db = f"ultra_diff_pr_test_{int(asyncio.get_event_loop().time())}"
    
    try:
        # Create test database
        await terminus.create_database(test_db, "Ultra Diff/PR Test")
        logger.info(f"✅ Created test database: {test_db}")
        
        # Add initial schema to main branch
        product_schema = {
            "@type": "Class",
            "@id": "Product",
            "name": {"@class": "xsd:string", "@type": "Optional"},
            "price": {"@class": "xsd:decimal", "@type": "Optional"}
        }
        
        await terminus._make_request(
            "POST",
            f"/api/document/{terminus.connection_info.account}/{test_db}",
            [product_schema],
            params={"graph_type": "schema", "author": "test", "message": "Initial Product schema"}
        )
        logger.info("✅ Added Product schema to main")
        
        # Create feature branch
        await terminus.create_branch(test_db, "feature/add-customer", "main")
        logger.info("✅ Created feature branch: feature/add-customer")
        
        # Add Customer schema to main (simulating parallel development)
        order_schema = {
            "@type": "Class",
            "@id": "Order",
            "order_date": {"@class": "xsd:dateTime", "@type": "Optional"},
            "total": {"@class": "xsd:decimal", "@type": "Optional"}
        }
        
        await terminus._make_request(
            "POST",
            f"/api/document/{terminus.connection_info.account}/{test_db}",
            [order_schema],
            params={"graph_type": "schema", "author": "dev1", "message": "Add Order class"}
        )
        logger.info("✅ Added Order schema to main")
        
        # Modify Product in main
        updated_product = {
            "@type": "Class",
            "@id": "Product",
            "name": {"@class": "xsd:string", "@type": "Optional"},
            "price": {"@class": "xsd:decimal", "@type": "Optional"},
            "description": {"@class": "xsd:string", "@type": "Optional"},  # Added field
            "category": {"@class": "xsd:string", "@type": "Optional"}      # Added field
        }
        
        await terminus._make_request(
            "PUT",
            f"/api/document/{terminus.connection_info.account}/{test_db}",
            [updated_product],
            params={"graph_type": "schema", "author": "dev1", "message": "Add description and category to Product"}
        )
        logger.info("✅ Modified Product in main branch")
        
        # Now test enhanced diff
        logger.info("\n" + "="*60)
        logger.info("🔥 TESTING ENHANCED DIFF")
        logger.info("="*60)
        
        # Test 1: Diff between main and feature branch
        diff_result = await terminus.diff(test_db, "feature/add-customer", "main")
        
        logger.info(f"\n📊 Diff Results: {len(diff_result)} changes found")
        for i, change in enumerate(diff_result):
            logger.info(f"\n🔍 Change #{i+1}:")
            logger.info(f"  Type: {change.get('type')}")
            logger.info(f"  Path: {change.get('path')}")
            logger.info(f"  Description: {change.get('description', 'N/A')}")
            
            if change.get('property_changes'):
                logger.info(f"  Property changes:")
                for prop_change in change['property_changes']:
                    logger.info(f"    - {prop_change['property']}: {prop_change['change']}")
        
        # Now test Pull Request functionality
        logger.info("\n" + "="*60)
        logger.info("🔥 TESTING PULL REQUEST")
        logger.info("="*60)
        
        # Create a PR
        pr = await terminus.create_pull_request(
            test_db,
            source_branch="main",
            target_branch="feature/add-customer",
            title="Merge main updates into feature branch",
            description="This PR brings the latest changes from main including Order class and Product updates",
            author="dev1"
        )
        
        logger.info(f"\n📋 Pull Request Created:")
        logger.info(f"  ID: {pr['id']}")
        logger.info(f"  Title: {pr['title']}")
        logger.info(f"  Status: {pr['status']}")
        logger.info(f"  Can merge: {pr['can_merge']}")
        logger.info(f"  Stats: {json.dumps(pr['stats'], indent=2)}")
        
        if pr['conflicts']:
            logger.info(f"\n⚠️  Conflicts detected:")
            for conflict in pr['conflicts']:
                logger.info(f"  - {conflict['description']}")
        
        # Test PR diff retrieval
        pr_diff = await terminus.get_pull_request_diff(test_db, pr['id'])
        logger.info(f"\n📊 PR Diff: {len(pr_diff)} changes")
        
        # Test merging the PR
        if pr['can_merge']:
            logger.info("\n🔀 Attempting to merge PR...")
            merge_result = await terminus.merge_pull_request(
                test_db,
                pr['id'],
                merge_message="Merged via automated test",
                author="test-system"
            )
            
            if merge_result.get('merged'):
                logger.info("✅ PR merged successfully!")
                logger.info(f"  Merge commit: {merge_result.get('commit_id', 'N/A')}")
            else:
                logger.info("❌ PR merge failed")
                logger.info(f"  Error: {merge_result.get('error', 'Unknown')}")
        else:
            logger.info("\n⚠️  PR has conflicts and cannot be merged automatically")
        
        # Final verification
        logger.info("\n" + "="*60)
        logger.info("🎯 VERIFICATION")
        logger.info("="*60)
        
        # Check if we're getting real data, not dummy responses
        is_real = True
        for change in diff_result:
            if "does not support true branch-based diff" in str(change):
                is_real = False
                break
        
        logger.info(f"Using REAL diff implementation: {'✅ YES' if is_real else '❌ NO'}")
        logger.info(f"Diff quality: {'✅ Detailed with property changes' if any(c.get('property_changes') for c in diff_result) else '⚠️  Basic schema level only'}")
        
        return is_real and len(diff_result) > 0
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
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
    success = await test_real_diff_and_pr()
    
    print("\n" + "="*80)
    print("🔥 ULTRA DIFF/PR TEST RESULT")
    print("="*80)
    print(f"Result: {'✅ REAL WORKING DIFF & PR' if success else '❌ STILL ISSUES WITH IMPLEMENTATION'}")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())