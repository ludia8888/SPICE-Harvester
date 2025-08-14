#!/usr/bin/env python3
"""
Test script to debug Ontology Worker TerminusDB connection issues
"""

import asyncio
import os
import sys
import base64
import httpx

# Add backend to Python path
sys.path.insert(0, '/Users/isihyeon/Desktop/SPICE HARVESTER/backend')

from shared.config.service_config import ServiceConfig
from shared.models.config import ConnectionConfig
from oms.services.async_terminus import AsyncTerminusService


async def test_direct_httpx_connection():
    """Test direct HTTPx connection to TerminusDB"""
    print("🔍 Testing direct HTTPx connection to TerminusDB...")
    
    terminus_url = ServiceConfig.get_terminus_url()
    print(f"   TerminusDB URL: {terminus_url}")
    
    try:
        # Test exact same client configuration as BaseTerminusService
        timeout = httpx.Timeout(
            connect=10.0,
            read=30.0,
            write=30.0,
            pool=5.0
        )
        
        transport = httpx.AsyncHTTPTransport(
            retries=3,
            verify=True  # ssl_verify default
        )
        
        async with httpx.AsyncClient(
            base_url=terminus_url,
            timeout=timeout,
            transport=transport,
            follow_redirects=True,
            headers={
                "User-Agent": "SPICE-HARVESTER/1.0",
                "Accept": "application/json",
            }
        ) as client:
            
            # Test basic connection
            print("   Testing basic connection to /api/info...")
            response = await client.get("/api/info")
            print(f"   ✅ Basic connection successful: {response.status_code}")
            
            # Test with admin authentication
            print("   Testing with admin authentication...")
            credentials = "admin:admin"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            auth_token = f"Basic {encoded_credentials}"
            
            headers = {
                "Authorization": auth_token,
                "Content-Type": "application/json",
            }
            
            response = await client.get("/api/info", headers=headers)
            print(f"   ✅ Authenticated connection successful: {response.status_code}")
            
            return True
            
    except Exception as e:
        print(f"   ❌ Direct HTTPx connection failed: {e}")
        return False


async def test_async_terminus_service():
    """Test AsyncTerminusService connection"""
    print("\n🔍 Testing AsyncTerminusService connection...")
    
    try:
        # Create connection config exactly like Ontology Worker
        connection_info = ConnectionConfig(
            server_url=ServiceConfig.get_terminus_url(),
            user=os.getenv("TERMINUS_USER", "admin"),
            account=os.getenv("TERMINUS_ACCOUNT", "admin"),
            key=os.getenv("TERMINUS_KEY", "admin"),
        )
        
        print(f"   Connection config:")
        print(f"   - server_url: {connection_info.server_url}")
        print(f"   - user: {connection_info.user}")
        print(f"   - account: {connection_info.account}")
        print(f"   - key: {connection_info.key}")
        
        # Test connection
        terminus_service = AsyncTerminusService(connection_info)
        await terminus_service.connect()
        
        print("   ✅ AsyncTerminusService connection successful")
        
        # Test database creation (the operation that's failing)
        print("   Testing database creation...")
        try:
            test_db_name = "ontology_worker_test_db"
            await terminus_service.create_database(test_db_name, "Test database")
            print(f"   ✅ Database creation successful: {test_db_name}")
            
            # Clean up
            await terminus_service.delete_database(test_db_name)
            print(f"   ✅ Database cleanup successful")
            
        except Exception as db_e:
            print(f"   ❌ Database operation failed: {db_e}")
        
        await terminus_service.disconnect()
        return True
        
    except Exception as e:
        print(f"   ❌ AsyncTerminusService connection failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_network_connectivity():
    """Test basic network connectivity"""
    print("\n🔍 Testing network connectivity...")
    
    import subprocess
    
    # Test ping
    try:
        result = subprocess.run(['ping', '-c', '1', 'localhost'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            print("   ✅ Localhost ping successful")
        else:
            print("   ❌ Localhost ping failed")
    except Exception as e:
        print(f"   ❌ Ping test failed: {e}")
    
    # Test port connectivity
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', 6364))
        sock.close()
        
        if result == 0:
            print("   ✅ Port 6364 connectivity successful")
        else:
            print("   ❌ Port 6364 connectivity failed")
    except Exception as e:
        print(f"   ❌ Port connectivity test failed: {e}")


async def main():
    """Run all connection tests"""
    print("🔥 ONTOLOGY WORKER CONNECTION DIAGNOSIS")
    print("=" * 50)
    
    # Test 1: Network connectivity
    await test_network_connectivity()
    
    # Test 2: Direct HTTPx connection
    httpx_success = await test_direct_httpx_connection()
    
    # Test 3: AsyncTerminusService connection
    service_success = await test_async_terminus_service()
    
    print("\n📋 Summary:")
    print(f"   - HTTPx connection: {'✅ Success' if httpx_success else '❌ Failed'}")
    print(f"   - AsyncTerminusService: {'✅ Success' if service_success else '❌ Failed'}")
    
    if httpx_success and service_success:
        print("\n🎉 All connection tests passed!")
        print("   The Ontology Worker should be able to connect to TerminusDB.")
        print("   The issue might be related to environment variables or runtime context.")
    else:
        print("\n⚠️  Connection issues detected.")
        print("   Check TerminusDB container status and network configuration.")


if __name__ == "__main__":
    asyncio.run(main())