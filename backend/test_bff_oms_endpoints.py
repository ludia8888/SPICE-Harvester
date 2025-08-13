#!/usr/bin/env python3
"""
Test BFF to OMS endpoint communication after fixing paths
"""

import asyncio
import aiohttp
import json

async def test_bff_oms_communication():
    """Test the corrected BFF -> OMS communication paths"""
    
    print("🔍 Testing BFF -> OMS Communication with Fixed Endpoints")
    print("=" * 60)
    
    async with aiohttp.ClientSession() as session:
        # 1. Test database list through BFF
        print("\n1️⃣ Testing database list (BFF -> OMS)...")
        
        try:
            async with session.get("http://localhost:8002/api/v1/databases") as resp:
                if resp.status == 200:
                    result = await resp.json()
                    db_count = len(result.get("data", {}).get("databases", []))
                    print(f"   ✅ BFF successfully called OMS: {db_count} databases found")
                else:
                    error = await resp.text()
                    print(f"   ❌ BFF -> OMS failed: {resp.status}")
                    print(f"      {error[:200]}")
        except Exception as e:
            print(f"   ❌ Connection error: {e}")
        
        # 2. Test ontology creation through BFF
        print("\n2️⃣ Testing ontology creation (BFF -> OMS)...")
        
        test_db = "bff_oms_test_db"
        
        # First create database
        async with session.post(
            "http://localhost:8002/api/v1/databases",
            json={"name": test_db, "description": "Test BFF-OMS communication"}
        ) as resp:
            if resp.status in [200, 202]:
                print(f"   ✅ Database creation initiated: {test_db} (status: {resp.status})")
            else:
                print(f"   ⚠️ Database creation: {resp.status}")
        
        # Wait longer for Event Sourcing to complete
        print("   ⏳ Waiting for Event Sourcing propagation...")
        await asyncio.sleep(8)
        
        # Verify database exists
        async with session.get("http://localhost:8002/api/v1/databases") as resp:
            if resp.status == 200:
                result = await resp.json()
                databases = result.get("data", {}).get("databases", [])
                db_names = [db.get("name") for db in databases]
                if test_db in db_names:
                    print(f"   ✅ Database verified: {test_db} exists")
                else:
                    print(f"   ⚠️ Database {test_db} not found in list: {db_names[:5]}...")
            else:
                print(f"   ⚠️ Could not verify database existence: {resp.status}")
        
        # Try to create ontology through BFF
        ontology_data = {
            "id": "TestClass",
            "label": "Test Class",
            "description": "Testing BFF to OMS communication",
            "properties": [
                {"name": "test_field", "type": "string", "label": "Test Field"},
                {"name": "es_doc_id", "type": "string", "label": "ES Doc ID"},
                {"name": "s3_uri", "type": "string", "label": "S3 URI"}
            ]
        }
        
        try:
            async with session.post(
                f"http://localhost:8002/api/v1/database/{test_db}/ontology",  # Fixed: removed /create
                json=ontology_data
            ) as resp:
                print(f"   BFF ontology creation response: {resp.status}")
                
                if resp.status == 200:
                    result = await resp.json()
                    print(f"   ✅ BFF successfully created ontology via OMS")
                    print(f"      Ontology ID: {result.get('id', 'unknown')}")
                    print(f"      Label: {result.get('label', 'unknown')}")
                else:
                    error = await resp.text()
                    print(f"   ❌ BFF -> OMS ontology creation failed: {resp.status}")
                    print(f"      Error: {error[:300]}")
        except Exception as e:
            print(f"   ❌ Exception during ontology creation: {e}")
        
        # Wait for Event Sourcing to process the creation
        await asyncio.sleep(3)
        
        # 3. Test ontology list through BFF
        print("\n3️⃣ Testing ontology list (BFF -> OMS)...")
        
        try:
            async with session.get(
                f"http://localhost:8002/api/v1/database/{test_db}/ontology/list"  # Fixed: added /list
            ) as resp:
                print(f"   BFF ontology list response: {resp.status}")
                
                if resp.status == 200:
                    result = await resp.json()
                    ontologies = result.get("data", [])
                    print(f"   ✅ BFF successfully listed ontologies: {len(ontologies)} found")
                    
                    for ont in ontologies[:3]:
                        print(f"      - {ont.get('id', 'unknown')}")
                else:
                    error = await resp.text()
                    print(f"   ❌ BFF -> OMS list failed: {resp.status}")
                    print(f"      Error: {error[:200]}")
        except Exception as e:
            print(f"   ❌ Exception during ontology list: {e}")
        
        # 4. Test Graph Federation endpoint
        print("\n4️⃣ Testing Graph Federation (BFF)...")
        
        try:
            simple_query = {
                "class_name": "TestClass",
                "limit": 10
            }
            
            async with session.post(
                f"http://localhost:8002/api/v1/graph-query/{test_db}/simple",
                json=simple_query
            ) as resp:
                print(f"   Graph Federation response: {resp.status}")
                
                if resp.status == 200:
                    result = await resp.json()
                    nodes = result.get("data", {}).get("nodes", [])
                    print(f"   ✅ Graph Federation working: {len(nodes)} nodes")
                else:
                    error = await resp.text()
                    print(f"   ⚠️ Graph Federation: {resp.status}")
                    print(f"      {error[:200]}")
        except Exception as e:
            print(f"   ❌ Graph Federation error: {e}")
    
    print("\n📋 SUMMARY:")
    print("   Fixed BFF endpoints:")
    print("   - POST /api/v1/database/{db_name}/ontology (create)")
    print("   - GET /api/v1/database/{db_name}/ontology/list (list)")
    print("\n   Key differences from OMS:")
    print("   - BFF: No /create suffix for ontology creation")
    print("   - BFF: Requires /list suffix for ontology listing")
    print("\n🎯 BFF endpoint structure now correctly tested!")

if __name__ == "__main__":
    asyncio.run(test_bff_oms_communication())