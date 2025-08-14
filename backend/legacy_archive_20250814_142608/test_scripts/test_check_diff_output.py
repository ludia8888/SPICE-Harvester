#!/usr/bin/env python3
"""
🔥 ULTRA: Check what diff actually returns
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

async def check_diff():
    """Check what diff returns"""
    terminus = AsyncTerminusService()
    test_db = "check_diff_test"
    
    try:
        # Create test database
        await terminus.create_database(test_db, "Check Diff Test")
        
        # Create branch
        await terminus.create_branch(test_db, "feature", "main")
        
        # Test diff
        diff_result = await terminus.diff(test_db, "main", "feature")
        
        logger.info("🔥 DIFF RESULT:")
        logger.info(json.dumps(diff_result, indent=2))
        
        # Check if it's the dummy response
        if diff_result and len(diff_result) > 0:
            first = diff_result[0]
            if first.get("type") == "info" and "does not support true branch-based diff" in first.get("message", ""):
                logger.error("\n❌ THIS IS THE FAKE DUMMY RESPONSE!")
                logger.error("Found at lines 2120-2127 in async_terminus.py")
                return False
        
        return True
        
    finally:
        try:
            await terminus.delete_database(test_db)
        except:
            pass

async def main():
    result = await check_diff()
    print("\n" + "="*60)
    print("RESULT:", "FAKE DIFF" if not result else "MAYBE REAL")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(main())