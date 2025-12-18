#!/usr/bin/env python3
"""
Quick test to verify Event Sourcing is 100% working after field name fix
"""

import asyncio
import aiohttp
from datetime import datetime

async def test_event_sourcing_complete():
    """Test the complete Event Sourcing flow with the fixed field name"""
    
    print("=" * 80)
    print("🚀 TESTING EVENT SOURCING 100% COMPLETION")
    print("=" * 80)
    
    # Test database and class names
    test_db = f"test_es_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    test_class = "TestProduct"
    
    async with aiohttp.ClientSession() as session:
        # 1. Create database
        print(f"\n1️⃣ Creating database: {test_db}")
        async with session.post(
            "http://localhost:8000/api/v1/database/create",
            json={"name": test_db, "description": "Test Event Sourcing 100%"}
        ) as resp:
            result = await resp.json()
            print(f"   Database creation response: {resp.status}")
            if resp.status == 202:
                print(f"   ✅ Event Sourcing mode active - command accepted")
            
        # Wait for database to be created asynchronously
        if resp.status == 202:
            print(f"\n⏳ Waiting for database to be created...")
            for i in range(10):  # Try for 10 seconds
                await asyncio.sleep(1)
                async with session.get(
                    f"http://localhost:8000/api/v1/database/exists/{test_db}"
                ) as check_resp:
                    check_result = await check_resp.json()
                    if check_result.get('data', {}).get('exists'):
                        print(f"   ✅ Database created after {i+1} seconds")
                        break
            else:
                print(f"   ⚠️ Database not created after 10 seconds")
            
        # 2. Create ontology class  
        print(f"\n2️⃣ Creating ontology class: {test_class}")
        ontology_data = {
            "id": test_class,
            "label": "Test Product",
            "description": "Product for testing Event Sourcing",
            "properties": [
                {
                    "name": "name",
                    "label": "Product Name",
                    "type": "string",
                    "required": True
                },
                {
                    "name": "price",
                    "label": "Product Price",
                    "type": "number",
                    "required": True
                }
            ]
        }
        
        async with session.post(
            f"http://localhost:8000/api/v1/ontology/{test_db}/create",
            json=ontology_data
        ) as resp:
            result = await resp.json()
            print(f"   Ontology creation response: {resp.status}")
            if resp.status == 200:
                print(f"   ✅ Ontology created successfully")
            elif resp.status != 201:
                print(f"   Error: {result}")
            
        # 3. Wait a moment for async processing
        print(f"\n3️⃣ Waiting 7 seconds for Event Sourcing to process...")
        await asyncio.sleep(7)
        
        # 4. Verify in TerminusDB
        print(f"\n4️⃣ Verifying in TerminusDB...")
        
        # Check if database exists
        async with session.get(
            f"http://localhost:8000/api/v1/database/exists/{test_db}"
        ) as resp:
            result = await resp.json()
            if result.get('data', {}).get('exists'):
                print(f"   ✅ Database exists in TerminusDB")
            else:
                print(f"   ⚠️  Database not yet in TerminusDB")
                
        # Check if ontology class exists
        async with session.get(
            f"http://localhost:8000/api/v1/ontology/{test_db}/class/{test_class}"
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                print(f"   ✅ Ontology class exists in TerminusDB")
                print(f"      Class data: {result.get('data', {}).get('id')}")
            else:
                print(f"   ⚠️  Ontology class not yet in TerminusDB")
                
        # 5. Cleanup
        print(f"\n5️⃣ Cleaning up test database...")
        async with session.delete(
            f"http://localhost:8000/api/v1/database/{test_db}"
        ) as resp:
            print(f"   Cleanup response: {resp.status}")
            
    print("\n" + "=" * 80)
    print("✅ EVENT SOURCING TEST COMPLETE!")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(test_event_sourcing_complete())
