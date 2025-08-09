#!/usr/bin/env python3
"""
Direct TerminusDB API test to bypass BFF/OMS query layer
Check if ontology documents actually exist in TerminusDB
"""

import asyncio
import aiohttp
import json
from datetime import datetime

async def test_direct_terminusdb():
    """Test by directly querying TerminusDB API"""
    
    print("=" * 80)
    print("🚀 TESTING DIRECT TERMINUSDB API ACCESS")
    print("=" * 80)
    
    # Test database name
    test_db = f"test_direct_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    test_class = "DirectTestProduct"
    
    async with aiohttp.ClientSession() as session:
        try:
            # 1. Create database via BFF
            print(f"\n1️⃣ Creating database via BFF: {test_db}")
            async with session.post(
                "http://localhost:8000/api/v1/database/create",
                json={"name": test_db, "description": "Direct TerminusDB Test"}
            ) as resp:
                print(f"   BFF response: {resp.status}")
                
            # 2. Wait for database creation
            print(f"\n2️⃣ Waiting for database...")
            for i in range(10):
                await asyncio.sleep(1)
                async with session.get(
                    f"http://localhost:8000/api/v1/database/exists/{test_db}"
                ) as check_resp:
                    if check_resp.status == 200:
                        result = await check_resp.json()
                        if result.get('data', {}).get('exists'):
                            print(f"   ✅ Database exists after {i+1} seconds")
                            break
            
            # 3. Create ontology via BFF  
            print(f"\n3️⃣ Creating ontology via BFF: {test_class}")
            ontology_data = {
                "id": test_class,
                "label": "Direct Test Product",
                "description": "Product for direct TerminusDB testing",
                "properties": [
                    {
                        "name": "name",
                        "label": "Product Name",
                        "type": "string", 
                        "required": True
                    }
                ]
            }
            
            async with session.post(
                f"http://localhost:8000/api/v1/ontology/{test_db}/create",
                json=ontology_data
            ) as resp:
                print(f"   BFF ontology creation: {resp.status}")
                
            # 4. Wait for processing
            print(f"\n4️⃣ Waiting 10 seconds for Event Sourcing...")
            await asyncio.sleep(10)
            
            # 5. Direct TerminusDB API queries
            print(f"\n5️⃣ Querying TerminusDB directly...")
            
            # Check if database exists in TerminusDB
            async with session.get(
                f"http://localhost:6363/api/db/admin/{test_db}"
            ) as resp:
                if resp.status == 200:
                    print(f"   ✅ Database exists in TerminusDB")
                else:
                    print(f"   ❌ Database not found in TerminusDB: {resp.status}")
            
            # Check schema documents directly
            async with session.get(
                f"http://localhost:6363/api/document/admin/{test_db}?graph_type=schema"
            ) as resp:
                print(f"   Schema documents query: {resp.status}")
                if resp.status == 200:
                    schema_docs = await resp.json()
                    print(f"   📄 Found {len(schema_docs) if isinstance(schema_docs, list) else 1} schema documents")
                    
                    if isinstance(schema_docs, list):
                        for i, doc in enumerate(schema_docs):
                            doc_id = doc.get('@id', 'Unknown')
                            doc_type = doc.get('@type', 'Unknown')
                            print(f"      Document {i+1}: ID='{doc_id}', Type='{doc_type}'")
                    else:
                        doc_id = schema_docs.get('@id', 'Unknown')
                        doc_type = schema_docs.get('@type', 'Unknown')
                        print(f"      Single document: ID='{doc_id}', Type='{doc_type}'")
                else:
                    error_text = await resp.text()
                    print(f"   ❌ Schema documents not found: {error_text[:200]}")
            
            # Check specific ontology document
            async with session.get(
                f"http://localhost:6363/api/document/admin/{test_db}?graph_type=schema&id={test_class}"
            ) as resp:
                print(f"   Specific ontology query: {resp.status}")
                if resp.status == 200:
                    ontology_doc = await resp.json()
                    print(f"   ✅ Ontology document found!")
                    print(f"      Document: {json.dumps(ontology_doc, indent=2)[:300]}...")
                else:
                    error_text = await resp.text()
                    print(f"   ❌ Ontology document not found: {error_text[:200]}")
            
            # 6. Compare with BFF query
            print(f"\n6️⃣ Comparing with BFF query...")
            async with session.get(
                f"http://localhost:8000/api/v1/ontology/{test_db}/class/{test_class}"
            ) as resp:
                print(f"   BFF ontology query: {resp.status}")
                if resp.status == 200:
                    bff_result = await resp.json()
                    print(f"   ✅ BFF found ontology: {bff_result.get('data', {}).get('id')}")
                else:
                    error_text = await resp.text()
                    print(f"   ❌ BFF didn't find ontology: {error_text[:200]}")
                    
        finally:
            # Cleanup
            print(f"\n7️⃣ Cleaning up...")
            async with session.delete(
                f"http://localhost:8000/api/v1/database/{test_db}"
            ) as resp:
                print(f"   Cleanup: {resp.status}")
                
    print("\n" + "=" * 80)
    print("🎯 DIRECT TERMINUSDB TEST COMPLETE!")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(test_direct_terminusdb())