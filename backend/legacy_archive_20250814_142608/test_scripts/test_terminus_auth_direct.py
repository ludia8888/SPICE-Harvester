#!/usr/bin/env python3
"""
Direct TerminusDB Authentication Test

This test directly checks the TerminusDB authentication and database creation
to isolate the exact authentication issue.
"""

import asyncio
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def test_terminus_auth():
    """Test TerminusDB authentication directly"""
    print("🔍 DIRECT TERMINUSDB AUTHENTICATION TEST")
    print("=" * 50)
    
    # Import after loading .env
    from oms.services.terminus.base import BaseTerminusService
    from oms.services.terminus.database import DatabaseService
    
    print("1. Testing BaseTerminusService connection...")
    
    # Create service instance
    terminus_service = BaseTerminusService()
    print(f"   Connection info: {terminus_service.connection_info}")
    
    try:
        # Test connection
        await terminus_service.connect()
        print("   ✅ Connected to TerminusDB successfully!")
        
        # Test info endpoint
        info = await terminus_service._make_request("GET", "/api/info")
        print(f"   📋 TerminusDB Info: {info}")
        
        # Test database creation endpoint directly
        print("\n2. Testing database creation via BaseTerminusService...")
        
        db_name = "direct_auth_test"
        create_payload = {
            "label": "Direct Auth Test Database",
            "comment": "Testing direct authentication",
            "prefixes": {
                "@base": f"terminusdb:///data/",
                "@schema": f"terminusdb:///schema#",
                "@type": "@context"
            }
        }
        
        try:
            # Direct API call to TerminusDB
            result = await terminus_service._make_request(
                "POST", 
                f"/api/db/admin/{db_name}",
                data=create_payload
            )
            print(f"   🎉 Database creation result: {result}")
            
            # Clean up - delete the test database
            delete_result = await terminus_service._make_request(
                "DELETE",
                f"/api/db/admin/{db_name}"
            )
            print(f"   🧹 Database deletion result: {delete_result}")
            
        except Exception as e:
            print(f"   ❌ Database creation failed: {e}")
        
        await terminus_service.disconnect()
        
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
    
    print("\n3. Testing DatabaseService (OMS service layer)...")
    
    try:
        # Test via OMS DatabaseService
        db_service = DatabaseService()
        
        result = await db_service.create_database("oms_service_test", "OMS service test database")
        print(f"   📊 OMS DatabaseService result: {result}")
        
        # Clean up
        cleanup_result = await db_service.delete_database("oms_service_test")
        print(f"   🧹 OMS DatabaseService cleanup: {cleanup_result}")
        
    except Exception as e:
        print(f"   ❌ OMS DatabaseService failed: {e}")
    
    print("\n✨ Direct authentication test completed!")

if __name__ == "__main__":
    asyncio.run(test_terminus_auth())