#!/usr/bin/env python3
"""
Debug TerminusDB Authentication
실제로 OMS가 어떤 인증 정보를 사용하는지 확인
"""

import os
import asyncio
import base64
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def debug_terminus_auth():
    print("🔍 TerminusDB Authentication Debug")
    print("=" * 50)
    
    # Show environment variables
    print("📋 Environment Variables:")
    print(f"   TERMINUS_SERVER_URL: {os.getenv('TERMINUS_SERVER_URL')}")
    print(f"   TERMINUS_USER: {os.getenv('TERMINUS_USER')}")
    print(f"   TERMINUS_ACCOUNT: {os.getenv('TERMINUS_ACCOUNT')}")
    print(f"   TERMINUS_KEY: {os.getenv('TERMINUS_KEY')}")
    print("")
    
    # Test OMS connection config
    print("🔧 OMS Connection Configuration:")
    from shared.models.config import ConnectionConfig
    
    connection_info = ConnectionConfig(
        server_url=os.getenv("TERMINUS_SERVER_URL", "http://localhost:6364"),
        account=os.getenv("TERMINUS_ACCOUNT", "admin"),
        user=os.getenv("TERMINUS_USER", "admin"),
        key=os.getenv("TERMINUS_KEY", "admin"),
        ssl_verify=os.getenv("TERMINUS_SSL_VERIFY", "true").lower() == "true",
    )
    
    print(f"   server_url: {connection_info.server_url}")
    print(f"   account: {connection_info.account}")
    print(f"   user: {connection_info.user}")
    print(f"   key: {connection_info.key}")
    print("")
    
    # Test authentication format
    print("🔑 Authentication Format:")
    credentials = f"{connection_info.user}:{connection_info.key}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    auth_header = f"Basic {encoded_credentials}"
    
    print(f"   Credentials: {credentials}")
    print(f"   Base64 Encoded: {encoded_credentials}")
    print(f"   Auth Header: {auth_header}")
    print("")
    
    # Test direct TerminusDB connection
    print("🌐 Testing Direct TerminusDB Connection:")
    import httpx
    
    try:
        async with httpx.AsyncClient() as client:
            # Test info endpoint
            response = await client.get(
                f"{connection_info.server_url}/api/info",
                headers={"Authorization": auth_header}
            )
            
            print(f"   GET /api/info: {response.status_code}")
            if response.status_code == 200:
                print("   ✅ Direct authentication SUCCESSFUL")
                info_data = response.json()
                print(f"   📊 Version: {info_data.get('api:info', {}).get('terminusdb', {}).get('version')}")
            else:
                print("   ❌ Direct authentication FAILED")
                print(f"   📋 Response: {response.text}")
            
            print("")
            
            # Test database creation endpoint
            print("🗄️  Testing Database Creation Endpoint:")
            create_payload = {
                "label": "Debug Test Database",
                "comment": "Testing authentication",
                "prefixes": {
                    "@base": "terminusdb:///data/",
                    "@schema": "terminusdb:///schema#",
                    "@type": "@context"
                }
            }
            
            test_db_name = "debug_auth_test"
            create_response = await client.post(
                f"{connection_info.server_url}/api/db/admin/{test_db_name}",
                headers={"Authorization": auth_header, "Content-Type": "application/json"},
                json=create_payload
            )
            
            print(f"   POST /api/db/admin/{test_db_name}: {create_response.status_code}")
            if create_response.status_code in [200, 201]:
                print("   ✅ Database creation SUCCESSFUL")
                
                # Clean up - delete the test database
                delete_response = await client.delete(
                    f"{connection_info.server_url}/api/db/admin/{test_db_name}",
                    headers={"Authorization": auth_header}
                )
                print(f"   DELETE /api/db/admin/{test_db_name}: {delete_response.status_code}")
                
            else:
                print("   ❌ Database creation FAILED")
                print(f"   📋 Response: {create_response.text}")
    
    except Exception as e:
        print(f"   💥 Connection error: {e}")
    
    print("")
    
    # Test OMS service connection
    print("🏢 Testing OMS Service TerminusDB Connection:")
    try:
        from oms.services.terminus.base import BaseTerminusService
        
        terminus_service = BaseTerminusService(connection_info)
        
        # Test connection
        await terminus_service.connect()
        print("   ✅ OMS BaseTerminusService connection successful")
        
        # Test info request
        info_result = await terminus_service._make_request("GET", "/api/info")
        print("   ✅ OMS service API request successful")
        print(f"   📊 Version: {info_result.get('api:info', {}).get('terminusdb', {}).get('version')}")
        
        await terminus_service.disconnect()
        
    except Exception as e:
        print(f"   ❌ OMS service connection failed: {e}")
        import traceback
        print(f"   📋 Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(debug_terminus_auth())