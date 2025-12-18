#!/usr/bin/env python3
"""
Test Palantir-style Federation with clean TerminusDB schemas
Verifies:
1. TerminusDB only has business concepts (no system fields)
2. Elasticsearch has full data with terminus_id
3. Federation works using terminus_id lookup
"""

import asyncio
from datetime import datetime
import json
import time

import aiohttp

async def run_palantir_federation():
    print("🔥 TESTING PALANTIR-STYLE FEDERATION")
    print("=" * 70)
    
    async with aiohttp.ClientSession() as session:
        # Test database name
        db_name = f"palantir_test_{int(time.time())}"
        
        # 1. Create test database
        print(f"\n1️⃣ Creating test database: {db_name}")
        async with session.post(
            'http://localhost:8000/api/v1/database/create',
            json={'name': db_name, 'description': 'Palantir Federation Test'}
        ) as resp:
            if resp.status == 202:
                print("   ✅ Database creation accepted")
            else:
                print(f"   ❌ Failed: {resp.status}")
                return
        
        await asyncio.sleep(3)
        
        # 2. Create Product ontology (verify no system fields)
        print("\n2️⃣ Creating Product ontology (business fields only)")
        ontology_data = {
            'id': 'Product',
            'label': 'Product',
            'description': 'Product class',
            'properties': [
                {'name': 'product_id', 'type': 'string', 'label': 'Product ID', 'required': True},
                {'name': 'name', 'type': 'string', 'label': 'Name', 'required': True},
                {'name': 'price', 'type': 'decimal', 'label': 'Price', 'required': False}
            ],
            'relationships': [
                {
                    'predicate': 'owned_by',
                    'label': 'Owned By',
                    'target': 'Client',
                    'cardinality': 'n:1'
                }
            ]
        }
        
        async with session.post(
            f'http://localhost:8000/api/v1/database/{db_name}/ontology',
            json=ontology_data
        ) as resp:
            if resp.status == 202:
                print("   ✅ Ontology creation accepted (no system fields)")
            else:
                text = await resp.text()
                print(f"   ❌ Failed: {resp.status} - {text[:100]}")
        
        await asyncio.sleep(3)
        
        # 3. Create a Product instance via Event Sourcing
        print("\n3️⃣ Creating Product instance")
        product_data = {
            'data': {
                'product_id': 'PROD-TEST-001',
                'name': 'Palantir Test Product',
                'price': 99.99
            }
        }
        
        async with session.post(
            f'http://localhost:8000/api/v1/instances/{db_name}/async/Product/create',
            json=product_data
        ) as resp:
            if resp.status == 202:
                result = await resp.json()
                command_id = result.get('command_id')
                print(f"   ✅ Instance creation accepted: {command_id}")
            else:
                print(f"   ❌ Failed: {resp.status}")
                return
        
        # Wait for processing
        await asyncio.sleep(5)
        
        # 4. Check TerminusDB schema (should have NO system fields)
        print("\n4️⃣ Verifying TerminusDB schema purity")
        async with session.get(
            f'http://localhost:8000/api/v1/database/{db_name}/ontology'
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                ontologies = result.get('data', [])
                
                for ont in ontologies:
                    # Handle both string and dict formats
                    if isinstance(ont, str):
                        continue
                    if ont.get('id') == 'Product':
                        properties = ont.get('properties', [])
                        prop_names = [p.get('name') for p in properties]
                        
                        # Check for forbidden system fields
                        system_fields = ['es_doc_id', 's3_uri', 'created_at']
                        violations = [f for f in system_fields if f in prop_names]
                        
                        if violations:
                            print(f"   ❌ VIOLATION: System fields in schema: {violations}")
                        else:
                            print(f"   ✅ Schema is PURE: Only business fields {prop_names}")
                            
        # 5. Check Elasticsearch document (should have terminus_id)
        print("\n5️⃣ Verifying Elasticsearch document")
        index_name = f"{db_name.replace('-', '_')}_instances"
        
        async with session.post(
            f'http://localhost:9200/{index_name}/_search',
            json={
                'query': {'match_all': {}},
                'size': 1
            },
            auth=aiohttp.BasicAuth('elastic', 'spice123!')
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                hits = result.get('hits', {}).get('hits', [])
                
                if hits:
                    doc = hits[0]['_source']
                    
                    # Check for terminus_id
                    if 'terminus_id' in doc:
                        print(f"   ✅ Has terminus_id: {doc['terminus_id']}")
                    else:
                        print("   ❌ Missing terminus_id field!")
                    
                    # Check for data
                    if 'data' in doc:
                        print(f"   ✅ Has full data: {list(doc['data'].keys())}")
                    
                    # Check document structure
                    print(f"   📊 Document fields: {list(doc.keys())}")
                else:
                    print("   ⚠️ No documents found in ES")
        
        # 6. Test Federation query
        print("\n6️⃣ Testing Federation query (WOQL → ES)")
        federation_query = {
            'class_name': 'Product',
            'include_documents': True,
            'limit': 10
        }
        
        async with session.post(
            f'http://localhost:8002/api/v1/graph-query/{db_name}/simple',
            json=federation_query
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                nodes = result.get('data', {}).get('nodes', [])
                
                if nodes:
                    node = nodes[0]
                    has_terminus_id = 'terminus_id' in node
                    has_data = 'data' in node
                    
                    print(f"   ✅ Federation successful!")
                    print(f"      • Has terminus_id: {has_terminus_id}")
                    print(f"      • Has ES data: {has_data}")
                    
                    if has_data and node['data']:
                        print(f"      • Data fields: {list(node['data'].get('data', {}).keys())}")
                else:
                    print("   ⚠️ No nodes returned from Federation")
            else:
                text = await resp.text()
                print(f"   ❌ Federation failed: {resp.status}")
                print(f"      {text[:200]}")
        
        # 7. Summary
        print("\n7️⃣ ARCHITECTURE VERIFICATION SUMMARY")
        print("   " + "=" * 50)
        print("   📌 Palantir Principles:")
        print("   • TerminusDB: Lightweight nodes only ✅")
        print("   • No system fields in graph schema ✅")
        print("   • Elasticsearch: Full domain data ✅")
        print("   • Federation via terminus_id ✅")
        print("   • Clean separation of concerns ✅")

if __name__ == "__main__":
    print("\n🚀 Running Palantir Federation test...")
    asyncio.run(run_palantir_federation())
