#!/usr/bin/env python3
"""
Fixed test to verify Event Sourcing is 100% working
- No timing issues
- Verify ontology immediately after creation
- Cleanup only at the end
"""

import asyncio
import aiohttp
import asyncpg
import json
from datetime import datetime

async def test_event_sourcing_fixed():
    """Test Event Sourcing with proper timing"""
    
    print("=" * 80)
    print("🚀 TESTING EVENT SOURCING - FIXED VERSION")
    print("=" * 80)
    
    # Test database and class names
    test_db = f"test_es_fixed_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    test_class = "FixedTestProduct"
    
    async with aiohttp.ClientSession() as session:
        try:
            # 1. Create database
            print(f"\n1️⃣ Creating database: {test_db}")
            async with session.post(
                "http://localhost:8000/api/v1/database/create",
                json={"name": test_db, "description": "Test Event Sourcing Fixed"}
            ) as resp:
                result = await resp.json()
                print(f"   Database creation response: {resp.status}")
                if resp.status == 202:
                    print(f"   ✅ Event Sourcing mode active")
                
            # 2. Wait for database to be created
            print(f"\n2️⃣ Waiting for database creation...")
            for i in range(10):
                await asyncio.sleep(1)
                async with session.get(
                    f"http://localhost:8000/api/v1/database/exists/{test_db}"
                ) as check_resp:
                    check_result = await check_resp.json()
                    if check_result.get('data', {}).get('exists'):
                        print(f"   ✅ Database exists after {i+1} seconds")
                        break
            else:
                print(f"   ❌ Database not created after 10 seconds")
                return
                
            # 3. Create ontology class
            print(f"\n3️⃣ Creating ontology class: {test_class}")
            ontology_data = {
                "id": test_class,
                "label": "Fixed Test Product",
                "description": "Product for testing Event Sourcing - Fixed",
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
                if resp.status == 202:
                    print(f"   ✅ Event Sourcing mode - command accepted")
                    
            # 4. Wait for ontology creation and verify immediately
            print(f"\n4️⃣ Waiting for ontology creation...")
            ontology_created = False
            for i in range(15):  # Try for 15 seconds
                await asyncio.sleep(1)
                
                # Check if ontology exists
                async with session.get(
                    f"http://localhost:8000/api/v1/ontology/{test_db}/class/{test_class}"
                ) as check_resp:
                    if check_resp.status == 200:
                        result = await check_resp.json()
                        print(f"   ✅ Ontology created and verified after {i+1} seconds!")
                        print(f"      Class ID: {result.get('data', {}).get('id')}")
                        ontology_created = True
                        break
                        
            if not ontology_created:
                print(f"   ❌ Ontology not created after 15 seconds")
                
                # Check worker logs for debugging
                print(f"\n🔍 Debugging - checking if commands were processed...")
                
                # Check outbox
                conn = await asyncpg.connect(
                    host='localhost',
                    port=5433,
                    user='spiceadmin',
                    password='spicepass123',
                    database='spicedb'
                )
                
                try:
                    # Check if CREATE_ONTOLOGY_CLASS was processed
                    onto_commands = await conn.fetch("""
                        SELECT processed_at, retry_count
                        FROM spice_outbox.outbox
                        WHERE payload->>'command_type' = 'CREATE_ONTOLOGY_CLASS'
                        AND payload->'payload'->>'db_name' = $1
                        AND payload->'payload'->>'class_id' = $2
                        ORDER BY created_at DESC
                        LIMIT 1
                    """, test_db, test_class)
                    
                    if onto_commands:
                        cmd = onto_commands[0]
                        if cmd['processed_at']:
                            print(f"      ✅ Command was processed successfully")
                            print(f"      🔍 But ontology not queryable - possible TerminusDB issue?")
                        else:
                            print(f"      ⚠️ Command not yet processed")
                    else:
                        print(f"      ❌ No CREATE_ONTOLOGY_CLASS command found!")
                        
                finally:
                    await conn.close()
            else:
                print(f"\n🎉 SUCCESS: Event Sourcing system working perfectly!")
                print(f"   - Database created ✅")  
                print(f"   - Ontology created ✅")
                print(f"   - Ontology queryable ✅")
                
        finally:
            # 5. Cleanup - only at the very end
            print(f"\n5️⃣ Cleaning up...")
            async with session.delete(
                f"http://localhost:8000/api/v1/database/{test_db}"
            ) as resp:
                print(f"   Cleanup response: {resp.status}")
                
    print("\n" + "=" * 80)
    print("🎯 FIXED EVENT SOURCING TEST COMPLETE!")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(test_event_sourcing_fixed())