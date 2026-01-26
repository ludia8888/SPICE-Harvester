#!/usr/bin/env python3
"""
Debug OMS Database Creation
실제 OMS 서비스의 데이터베이스 생성 플로우 테스트
"""

import asyncio
import logging

from shared.config.settings import get_settings

# Set up logging to see debug messages
logging.basicConfig(level=logging.DEBUG)

async def debug_oms_database_creation():
    print("🔍 OMS Database Creation Debug")
    print("=" * 50)
    
    # Test OMS DatabaseService directly
    print("🏢 Testing OMS DatabaseService:")
    try:
        from shared.models.config import ConnectionConfig
        from oms.services.terminus.database import DatabaseService
        cfg = get_settings().database
        
        # Create connection config
        connection_info = ConnectionConfig(
            server_url=cfg.terminus_url,
            account=cfg.terminus_account,
            user=cfg.terminus_user,
            key=cfg.terminus_password,
        )
        
        print(f"   Using connection: {connection_info.server_url}")
        print(f"   Account: {connection_info.account}")
        print(f"   User: {connection_info.user}")
        
        # Create service
        db_service = DatabaseService(connection_info)
        
        # Connect
        await db_service.connect()
        print("   ✅ OMS DatabaseService connected")
        
        # Test database existence check first
        test_db_name = "oms_debug_test"
        print(f"\n   🔍 Testing database existence: {test_db_name}")
        exists = await db_service.database_exists(test_db_name)
        print(f"   📊 Database exists: {exists}")
        
        if exists:
            print(f"   🗑️  Deleting existing database...")
            try:
                await db_service.delete_database(test_db_name)
                print(f"   ✅ Database deleted")
            except Exception as e:
                print(f"   ⚠️  Delete failed: {e}")
        
        # Test database creation
        print(f"\n   📦 Creating database: {test_db_name}")
        try:
            result = await db_service.create_database(
                test_db_name, 
                "OMS debug test database"
            )
            print("   ✅ Database creation SUCCESSFUL!")
            print(f"   📊 Result: {result}")
            
            # Verify it exists
            exists_after = await db_service.database_exists(test_db_name)
            print(f"   ✅ Verification: Database exists = {exists_after}")
            
            # Clean up
            print(f"\n   🧹 Cleaning up test database...")
            await db_service.delete_database(test_db_name)
            print(f"   ✅ Cleanup successful")
            
        except Exception as e:
            print(f"   ❌ Database creation FAILED: {e}")
            import traceback
            print(f"   📋 Traceback: {traceback.format_exc()}")
        
        await db_service.disconnect()
        
    except Exception as e:
        print(f"   💥 OMS DatabaseService error: {e}")
        import traceback
        print(f"   📋 Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(debug_oms_database_creation())
