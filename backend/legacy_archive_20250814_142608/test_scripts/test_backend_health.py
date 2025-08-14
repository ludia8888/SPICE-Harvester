"""
Quick health check for all backend services
"""
import asyncio
import httpx
from datetime import datetime

async def check_all_services():
    """Check health of all backend services"""
    
    services = {
        "BFF": "http://localhost:8002",
        "OMS": "http://localhost:8000", 
        "Funnel": "http://localhost:8003",
        "TerminusDB": "http://localhost:6363/api/info"
    }
    
    results = {}
    
    async with httpx.AsyncClient() as client:
        print(f"\n🔍 Checking backend services at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        for service, base_url in services.items():
            try:
                if service == "TerminusDB":
                    # TerminusDB requires auth
                    response = await client.get(
                        base_url,
                        auth=("admin", "admin123"),
                        timeout=5.0
                    )
                else:
                    # Other services health endpoint
                    response = await client.get(
                        f"{base_url}/health",
                        timeout=5.0
                    )
                
                if response.status_code == 200:
                    results[service] = {
                        "status": "✅ Running",
                        "response_time": f"{response.elapsed.total_seconds():.2f}s"
                    }
                else:
                    results[service] = {
                        "status": f"⚠️ Error {response.status_code}",
                        "response_time": f"{response.elapsed.total_seconds():.2f}s"
                    }
                    
            except Exception as e:
                results[service] = {
                    "status": f"❌ Failed: {str(e)}",
                    "response_time": "N/A"
                }
        
        # Display results
        print("Service Health Status:")
        print("-" * 50)
        for service, result in results.items():
            print(f"{service:12} | {result['status']:20} | Response: {result['response_time']}")
        
        # Test basic functionality
        print("\n🧪 Testing basic functionality...")
        
        # Test 1: List databases via BFF
        try:
            resp = await client.get("http://localhost:8002/api/v1/databases")
            if resp.status_code == 200:
                data = resp.json()
                db_count = len(data.get("data", {}).get("databases", []))
                print(f"✅ BFF Database List: {db_count} databases found")
            else:
                print(f"❌ BFF Database List: Failed with status {resp.status_code}")
        except Exception as e:
            print(f"❌ BFF Database List: {str(e)}")
        
        # Test 2: Create and delete test database
        test_db = f"test_health_{int(datetime.now().timestamp())}"
        try:
            # Create
            resp = await client.post(
                "http://localhost:8002/api/v1/databases",
                json={"name": test_db}
            )
            if resp.status_code in [200, 201]:
                print(f"✅ Database Creation: Created '{test_db}'")
                
                # Delete
                resp = await client.delete(f"http://localhost:8002/api/v1/databases/{test_db}")
                if resp.status_code == 200:
                    print(f"✅ Database Deletion: Deleted '{test_db}'")
                else:
                    print(f"⚠️ Database Deletion: Failed to delete '{test_db}'")
            else:
                print(f"❌ Database Creation: Failed with status {resp.status_code}")
        except Exception as e:
            print(f"❌ Database Operations: {str(e)}")
        
        # Overall status
        all_running = all("✅" in r["status"] for r in results.values())
        
        print("\n" + "="*50)
        if all_running:
            print("✅ All backend services are running perfectly!")
        else:
            print("⚠️ Some services have issues. Check the status above.")
        print("="*50)
        
        return all_running

if __name__ == "__main__":
    asyncio.run(check_all_services())