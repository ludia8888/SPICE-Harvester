#!/usr/bin/env python3
"""
🔥 THINK ULTRA: Test TRUE lightweight nodes architecture
Verify that TerminusDB has nodes for Federation to work
"""

import asyncio
import aiohttp
import json
import time

async def test_lightweight_architecture():
    print("🔥 TESTING LIGHTWEIGHT NODES ARCHITECTURE")
    print("=" * 70)
    
    async with aiohttp.ClientSession() as session:
        db_name = f"lightweight_test_{int(time.time())}"
        
        # 1. Create database
        print("\n1️⃣ Creating test database...")
        async with session.post(
            'http://localhost:8000/api/v1/database/create',
            json={'name': db_name, 'description': 'Lightweight nodes test'}
        ) as resp:
            if resp.status == 202:
                print(f"   ✅ Database created: {db_name}")
            
        await asyncio.sleep(3)
        
        # 2. Create TRULY lightweight schemas
        print("\n2️⃣ Creating LIGHTWEIGHT schemas (ID fields only)...")
        
        # Client schema - ONLY ID field required
        client_schema = {
            'id': 'Client',
            'label': 'Client',
            'description': 'Lightweight Client node',
            'properties': [
                {'name': 'client_id', 'type': 'string', 'label': 'Client ID', 'required': True}
                # NO other required fields!
            ]
        }
        
        async with session.post(
            f'http://localhost:8000/api/v1/database/{db_name}/ontology',
            json=client_schema
        ) as resp:
            print(f"   Client schema: {resp.status}")
            
        # Product schema with relationship
        product_schema = {
            'id': 'Product',
            'label': 'Product',
            'description': 'Lightweight Product node',
            'properties': [
                {'name': 'product_id', 'type': 'string', 'label': 'Product ID', 'required': True}
                # NO other required fields!
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
            json=product_schema
        ) as resp:
            print(f"   Product schema: {resp.status}")
            
        await asyncio.sleep(3)
        
        # 3. Create instances
        print("\n3️⃣ Creating instances with FULL data...")
        
        # Create Client
        client_data = {
            'client_id': 'TEST-CLIENT-001',
            'name': 'Test Client Corp',  # Domain data - goes to ES only
            'region': 'US'
        }
        
        async with session.post(
            f'http://localhost:8000/api/v1/instances/{db_name}/async/Client/create',
            json={'data': client_data}
        ) as resp:
            print(f"   Client instance: {resp.status}")
            
        # Create Product with relationship
        product_data = {
            'product_id': 'TEST-PROD-001',
            'name': 'Test Product',  # Domain data - goes to ES only
            'price': 999.99,
            'owned_by': 'Client/TEST-CLIENT-001'  # Relationship
        }
        
        async with session.post(
            f'http://localhost:8000/api/v1/instances/{db_name}/async/Product/create',
            json={'data': product_data}
        ) as resp:
            print(f"   Product instance: {resp.status}")
            
        await asyncio.sleep(5)
        
        # 4. VERIFY TerminusDB has lightweight nodes
        print("\n4️⃣ Verifying TerminusDB has lightweight nodes...")
        
        import httpx
        async with httpx.AsyncClient() as client:
            # Query for Product nodes
            woql_query = {
                "query": {
                    "@type": "Triple",
                    "subject": {"variable": "v:X"},
                    "predicate": {"node": "rdf:type"},
                    "object": {"node": "@schema:Product"}
                }
            }
            
            response = await client.post(
                f"http://localhost:6363/api/woql/admin/{db_name}",
                json=woql_query,
                auth=("admin", "spice123!")
            )
            
            if response.status_code == 200:
                result = response.json()
                bindings = result.get('bindings', [])
                print(f"   📊 TerminusDB Product nodes: {len(bindings)}")
                for b in bindings:
                    print(f"      • {b.get('v:X')}")
            else:
                print(f"   ❌ WOQL query failed: {response.status_code}")
                
        # 5. VERIFY Elasticsearch has full data
        print("\n5️⃣ Verifying Elasticsearch has full data...")
        
        index_name = f"{db_name.replace('-', '_')}_instances"
        async with session.post(
            f'http://localhost:9200/{index_name}/_search',
            json={'query': {'match_all': {}}, 'size': 10},
            auth=aiohttp.BasicAuth('elastic', 'spice123!')
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                hits = result.get('hits', {}).get('hits', [])
                print(f"   📊 Elasticsearch documents: {len(hits)}")
                
                for hit in hits:
                    doc = hit['_source']
                    has_terminus_id = 'terminus_id' in doc
                    has_data = 'data' in doc
                    print(f"      • {doc.get('class_id')}/{doc.get('instance_id')}")
                    print(f"        - terminus_id: {'✅' if has_terminus_id else '❌'}")
                    print(f"        - domain data: {'✅' if has_data else '❌'}")
                    
        # 6. TEST Federation query
        print("\n6️⃣ Testing Federation query...")
        
        async with session.post(
            f'http://localhost:8002/api/v1/graph-query/{db_name}/simple',
            json={
                'class_name': 'Product',
                'include_documents': True,
                'limit': 10
            }
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                count = result.get('count', 0)
                docs = result.get('documents', [])
                
                if count > 0:
                    print(f"   🎉 FEDERATION WORKS! Found {count} nodes")
                    for doc in docs:
                        print(f"      • ID: {doc.get('id')}")
                        if doc.get('data'):
                            print(f"        Data: {list(doc['data'].keys())}")
                else:
                    print(f"   ❌ FEDERATION FAILED: No nodes returned")
                    print("      This means TerminusDB doesn't have lightweight nodes!")
            else:
                text = await resp.text()
                print(f"   ❌ Federation query failed: {resp.status}")
                print(f"      {text[:200]}")
                
        # 7. ANALYSIS
        print("\n" + "=" * 70)
        print("📊 ARCHITECTURE ANALYSIS:")
        print("=" * 70)
        
        print("\n🎯 Expected behavior:")
        print("   1. TerminusDB: Has lightweight nodes (@id, @type, relationships)")
        print("   2. Elasticsearch: Has full domain data")
        print("   3. Federation: Queries TerminusDB → Gets IDs → Fetches from ES")
        
        print("\n⚠️ Common failures:")
        print("   • TerminusDB empty = Schema has required domain fields")
        print("   • Federation returns 0 = No nodes in TerminusDB to query")
        print("   • ES has duplicates = Multiple workers running")

print("\n🚀 Starting lightweight nodes architecture test...")
asyncio.run(test_lightweight_architecture())