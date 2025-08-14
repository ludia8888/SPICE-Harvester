#!/usr/bin/env python3
"""
🔥 ULTRA: Simple diff test
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

async def test_simple_diff():
    """Simple diff test"""
    terminus = AsyncTerminusService()
    test_db = f"diff_test_{int(asyncio.get_event_loop().time())}"
    
    try:
        # Create test database
        await terminus.create_database(test_db, "Simple Diff Test")
        logger.info(f"✅ Created database: {test_db}")
        
        # Test diff on same branch (should return empty)
        diff1 = await terminus.diff(test_db, "main", "main")
        logger.info(f"\n🔍 Diff main->main: {json.dumps(diff1, indent=2)}")
        
        # Test diff between main and non-existent branch
        diff2 = await terminus.diff(test_db, "main", "feature")
        logger.info(f"\n🔍 Diff main->feature: {json.dumps(diff2, indent=2)}")
        
        # Check if we're getting fake data
        for diff in [diff1, diff2]:
            if diff and any("does not support true branch-based diff" in str(item) for item in diff):
                logger.error("\n❌ FAKE DIFF DETECTED!")
                return False
        
        logger.info("\n✅ NO FAKE DIFF DETECTED!")
        return True
        
    finally:
        try:
            await terminus.delete_database(test_db)
            logger.info(f"🧹 Cleaned up: {test_db}")
        except:
            pass

async def main():
    result = await test_simple_diff()
    print("\n" + "="*60)
    print(f"RESULT: {'REAL DIFF (no fake data)' if result else 'FAKE DIFF STILL EXISTS'}")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(main())