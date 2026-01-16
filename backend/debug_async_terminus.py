#!/usr/bin/env python3
"""
Debug AsyncTerminusService
실제 HTTP API에서 사용되는 AsyncTerminusService 테스트
"""

import asyncio
import logging

from shared.config.settings import get_settings

# Set up logging to see debug messages
logging.basicConfig(level=logging.DEBUG)

async def debug_async_terminus_service():
    print("🔍 AsyncTerminusService Debug")
    print("=" * 50)
    
    # Test AsyncTerminusService (used by HTTP API)
    print("🌐 Testing AsyncTerminusService (HTTP API layer):")
    try:
        from oms.services.async_terminus import AsyncTerminusService
        from shared.models.config import ConnectionConfig
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
        async_service = AsyncTerminusService(connection_info)
        
        # Connect
        await async_service.connect()
        print("   ✅ AsyncTerminusService connected")
        
        # Test database existence check first
        test_db_name = "async_debug_test"
        print(f"\n   🔍 Testing database existence: {test_db_name}")
        exists = await async_service.database_exists(test_db_name)
        print(f"   📊 Database exists: {exists}")
        
        if exists:
            print(f"   🗑️  Deleting existing database...")
            try:
                deleted = await async_service.delete_database(test_db_name)
                print(f"   ✅ Database deleted: {deleted}")
            except Exception as e:
                print(f"   ⚠️  Delete failed: {e}")
        
        # Test database creation
        print(f"\n   📦 Creating database via AsyncTerminusService: {test_db_name}")
        try:
            result = await async_service.create_database(
                test_db_name, 
                "Async debug test database"
            )
            print(f"   📊 Create result: {result} (type: {type(result)})")
            
            if result:
                print("   ✅ Database creation SUCCESSFUL!")
                
                # Verify it exists
                exists_after = await async_service.database_exists(test_db_name)
                print(f"   ✅ Verification: Database exists = {exists_after}")
                
                # Clean up
                print(f"\n   🧹 Cleaning up test database...")
                deleted = await async_service.delete_database(test_db_name)
                print(f"   ✅ Cleanup successful: {deleted}")
            else:
                print("   ❌ Database creation returned False!")
                
        except Exception as e:
            print(f"   ❌ Database creation FAILED: {e}")
            import traceback
            print(f"   📋 Traceback: {traceback.format_exc()}")
        
        await async_service.disconnect()
        
    except Exception as e:
        print(f"   💥 AsyncTerminusService error: {e}")
        import traceback
        print(f"   📋 Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(debug_async_terminus_service())
