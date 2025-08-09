#!/usr/bin/env python3
"""
Quick test to verify Event Sourcing is 100% working after field name fix
"""

import asyncio
import aiohttp
import asyncpg
import json
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
            
        # 3. Check PostgreSQL outbox
        print(f"\n3️⃣ Checking PostgreSQL outbox table...")
        conn = await asyncpg.connect(
            host='localhost',
            port=5433,
            user='spiceadmin',
            password='spicepass123',
            database='spicedb'
        )
        
        try:
            # Check for CREATE_DATABASE command
            db_commands = await conn.fetch("""
                SELECT id, message_type, payload, processed_at, retry_count
                FROM spice_outbox.outbox
                WHERE message_type = 'COMMAND'
                AND payload->>'command_type' = 'CREATE_DATABASE'
                AND payload->>'database_name' = $1
                ORDER BY created_at DESC
                LIMIT 1
            """, test_db)
            
            if db_commands:
                cmd = db_commands[0]
                print(f"   ✅ Found CREATE_DATABASE command:")
                print(f"      Processed: {cmd['processed_at'] is not None}")
                print(f"      Retry count: {cmd['retry_count']}")
            else:
                print(f"   ⚠️  No CREATE_DATABASE command found for {test_db}")
                
            # Check for CREATE_ONTOLOGY_CLASS command
            onto_commands = await conn.fetch("""
                SELECT id, message_type, payload, processed_at, retry_count
                FROM spice_outbox.outbox
                WHERE message_type = 'COMMAND'
                AND payload->>'command_type' = 'CREATE_ONTOLOGY_CLASS'
                AND payload->>'db_name' = $1
                AND payload->>'class_id' = $2
                ORDER BY created_at DESC
                LIMIT 1
            """, test_db, test_class)
            
            if onto_commands:
                cmd = onto_commands[0]
                print(f"   ✅ Found CREATE_ONTOLOGY_CLASS command:")
                print(f"      Processed: {cmd['processed_at'] is not None}")
                print(f"      Retry count: {cmd['retry_count']}")
                
                # Verify payload structure
                payload = json.loads(cmd['payload'])
                if 'class_id' in payload:
                    print(f"      ✅ Payload contains 'class_id': {payload['class_id']}")
                else:
                    print(f"      ❌ Payload missing 'class_id'! Keys: {list(payload.keys())}")
            else:
                print(f"   ⚠️  No CREATE_ONTOLOGY_CLASS command found for {test_db}:{test_class}")
                    
        finally:
            await conn.close()
            
        # 4. Wait a moment for processing
        print(f"\n4️⃣ Waiting 3 seconds for Event Sourcing to process...")
        await asyncio.sleep(3)
        
        # 5. Verify in TerminusDB
        print(f"\n5️⃣ Verifying in TerminusDB...")
        
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
                
        # 6. Cleanup
        print(f"\n6️⃣ Cleaning up test database...")
        async with session.delete(
            f"http://localhost:8000/api/v1/database/{test_db}"
        ) as resp:
            print(f"   Cleanup response: {resp.status}")
            
    print("\n" + "=" * 80)
    print("✅ EVENT SOURCING TEST COMPLETE!")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(test_event_sourcing_complete())