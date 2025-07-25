#!/usr/bin/env python3
"""
🔥 ULTRA: Test rollback functionality
"""

import asyncio
import logging
import sys
import os
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

async def test_rollback():
    """Test rollback functionality"""
    terminus = AsyncTerminusService()
    test_db = f"rollback_test_{int(time.time())}"
    
    try:
        # Create test database
        await terminus.create_database(test_db, "Rollback Test Database")
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
            params={"graph_type": "schema", "author": "test", "message": "Initial Product schema"}
        )
        logger.info("✅ Added Product schema")
        
        # Add second schema
        customer_schema = {
            "@type": "Class",
            "@id": "Customer",
            "@key": {"@type": "Random"},
            "email": {"@class": "xsd:string", "@type": "Optional"}
        }
        
        await terminus._make_request(
            "POST",
            f"/api/document/{terminus.connection_info.account}/{test_db}",
            [customer_schema],
            params={"graph_type": "schema", "author": "test", "message": "Add Customer schema"}
        )
        logger.info("✅ Added Customer schema")
        
        # Get commit history
        history = await terminus.get_commit_history(test_db)
        logger.info(f"📝 Commit history: {len(history)} commits")
        
        for i, commit in enumerate(history[:5]):
            logger.info(f"  Commit {i}: {commit.get('message', 'No message')} ({commit.get('identifier', '')[:8]})")
        
        # Test rollback
        if len(history) >= 2:
            target_commit = history[1]  # Second latest commit
            commit_id = target_commit.get("identifier", "")
            
            logger.info(f"\n🔄 Testing rollback to commit: {commit_id[:8]} - {target_commit.get('message')}")
            
            # Check if revert exists
            if hasattr(terminus, 'revert'):
                logger.info("✅ Found revert method")
                try:
                    result = await terminus.revert(test_db, commit_id)
                    logger.info(f"✅ Revert succeeded: {result}")
                    return True
                except Exception as e:
                    logger.error(f"❌ Revert failed: {e}")
            else:
                logger.info("⚠️ No revert method, trying rollback")
            
            # Try rollback
            try:
                result = await terminus.rollback(test_db, commit_id)
                logger.info(f"✅ Rollback succeeded: {result}")
                
                # List branches to see rollback branch
                branches = await terminus.list_branches(test_db)
                logger.info(f"📌 Current branches: {branches}")
                
                return True
            except Exception as e:
                logger.error(f"❌ Rollback failed: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return False
        else:
            logger.error("❌ Not enough commits to test rollback")
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
    success = await test_rollback()
    
    print("\n" + "="*80)
    print("🔥 ROLLBACK TEST RESULT")
    print("="*80)
    print(f"Result: {'✅ SUCCESS' if success else '❌ FAILED'}")
    print("="*80)
    
    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)