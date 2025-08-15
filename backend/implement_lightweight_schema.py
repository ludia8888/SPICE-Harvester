#!/usr/bin/env python3
"""
🔥 THINK ULTRA: Implement TRUE lightweight schemas for TerminusDB
Following the recommended approach - ONLY business keys required
"""

import asyncio
import aiohttp
import json
import time

async def implement_lightweight_architecture():
    print("🔥 IMPLEMENTING TRUE LIGHTWEIGHT ARCHITECTURE")
    print("=" * 70)
    
    async with aiohttp.ClientSession() as session:
        db_name = f"lightweight_production_{int(time.time())}"
        
        # ========================================================================
        # PHASE 1: Create Database
        # ========================================================================
        print("\n📌 PHASE 1: Database Creation")
        print("-" * 60)
        
        async with session.post(
            'http://localhost:8000/api/v1/database/create',
            json={'name': db_name, 'description': 'True lightweight architecture'}
        ) as resp:
            if resp.status == 202:
                result = await resp.json()
                print(f"✅ Database created: {db_name}")
                print(f"   Command ID: {result.get('data', {}).get('command_id')}")
            else:
                print(f"❌ Database creation failed: {resp.status}")
                return
        
        await asyncio.sleep(5)
        
        # ========================================================================
        # PHASE 2: Create TRULY Lightweight Schemas
        # ========================================================================
        print("\n📌 PHASE 2: Creating TRULY Lightweight Schemas")
        print("-" * 60)
        
        # Client schema - ONLY business key required
        client_schema = {
            'id': 'Client',
            'label': 'Client',
            'description': 'Lightweight Client node for graph traversal',
            'properties': [
                {
                    'name': 'client_id',
                    'type': 'string',
                    'label': 'Client ID',
                    'required': True  # ONLY business key required
                }
                # NO other fields required - domain data goes to ES only
            ]
        }
        
        print("\n1️⃣ Creating Client schema (client_id only)...")
        async with session.post(
            f'http://localhost:8000/api/v1/database/{db_name}/ontology',
            json=client_schema
        ) as resp:
            if resp.status == 202:
                print("   ✅ Client schema created (lightweight)")
            else:
                text = await resp.text()
                print(f"   ❌ Failed: {resp.status} - {text[:100]}")
        
        # Product schema with relationship
        product_schema = {
            'id': 'Product',
            'label': 'Product',
            'description': 'Lightweight Product node for graph traversal',
            'properties': [
                {
                    'name': 'product_id',
                    'type': 'string',
                    'label': 'Product ID',
                    'required': True  # ONLY business key required
                }
                # NO domain fields (name, price, etc.) required
            ],
            'relationships': [
                {
                    'predicate': 'owned_by',
                    'label': 'Owned By',
                    'target': 'Client',
                    'cardinality': 'n:1',
                    'description': 'Product ownership relationship'
                }
            ]
        }
        
        print("\n2️⃣ Creating Product schema (product_id only + relationship)...")
        async with session.post(
            f'http://localhost:8000/api/v1/database/{db_name}/ontology',
            json=product_schema
        ) as resp:
            if resp.status == 202:
                print("   ✅ Product schema created (lightweight)")
            else:
                text = await resp.text()
                print(f"   ❌ Failed: {resp.status} - {text[:100]}")
        
        # Order schema with multiple relationships
        order_schema = {
            'id': 'Order',
            'label': 'Order',
            'description': 'Lightweight Order node for graph traversal',
            'properties': [
                {
                    'name': 'order_id',
                    'type': 'string',
                    'label': 'Order ID',
                    'required': True  # ONLY business key required
                }
            ],
            'relationships': [
                {
                    'predicate': 'ordered_by',
                    'label': 'Ordered By',
                    'target': 'Client',
                    'cardinality': 'n:1'
                },
                {
                    'predicate': 'contains',
                    'label': 'Contains',
                    'target': 'Product',
                    'cardinality': 'n:n'
                }
            ]
        }
        
        print("\n3️⃣ Creating Order schema (order_id only + relationships)...")
        async with session.post(
            f'http://localhost:8000/api/v1/database/{db_name}/ontology',
            json=order_schema
        ) as resp:
            if resp.status == 202:
                print("   ✅ Order schema created (lightweight)")
            else:
                text = await resp.text()
                print(f"   ❌ Failed: {resp.status} - {text[:100]}")
        
        await asyncio.sleep(5)
        
        # ========================================================================
        # PHASE 3: Create Instances with Full Data
        # ========================================================================
        print("\n📌 PHASE 3: Creating Instances")
        print("-" * 60)
        
        # Create Clients
        print("\n1️⃣ Creating Client instances...")
        clients = [
            {'client_id': 'CL-ULTRA-001', 'name': 'Ultra Corp', 'region': 'US'},
            {'client_id': 'CL-ULTRA-002', 'name': 'Mega Industries', 'region': 'EU'}
        ]
        
        for client in clients:
            async with session.post(
                f'http://localhost:8000/api/v1/instances/{db_name}/async/Client/create',
                json={'data': client}
            ) as resp:
                if resp.status == 202:
                    print(f"   ✅ Client {client['client_id']} created")
                else:
                    print(f"   ❌ Client {client['client_id']} failed: {resp.status}")
        
        await asyncio.sleep(3)
        
        # Create Products with relationships
        print("\n2️⃣ Creating Product instances with relationships...")
        products = [
            {
                'product_id': 'PRD-ULTRA-001',
                'name': 'Ultra Widget',
                'price': 99.99,
                'category': 'Hardware',
                'owned_by': 'Client/CL-ULTRA-001'
            },
            {
                'product_id': 'PRD-ULTRA-002',
                'name': 'Mega Service',
                'price': 199.99,
                'category': 'Software',
                'owned_by': 'Client/CL-ULTRA-002'
            },
            {
                'product_id': 'PRD-ULTRA-003',
                'name': 'Super Solution',
                'price': 299.99,
                'category': 'Software',
                'owned_by': 'Client/CL-ULTRA-001'
            }
        ]
        
        for product in products:
            async with session.post(
                f'http://localhost:8000/api/v1/instances/{db_name}/async/Product/create',
                json={'data': product}
            ) as resp:
                if resp.status == 202:
                    print(f"   ✅ Product {product['product_id']} created")
                    print(f"      → owned_by: {product['owned_by']}")
                else:
                    print(f"   ❌ Product {product['product_id']} failed: {resp.status}")
        
        await asyncio.sleep(5)
        
        # ========================================================================
        # PHASE 4: Verify Lightweight Nodes in TerminusDB
        # ========================================================================
        print("\n📌 PHASE 4: Verifying TerminusDB Lightweight Nodes")
        print("-" * 60)
        
        import httpx
        async with httpx.AsyncClient() as client:
            # Check Client nodes
            print("\n1️⃣ Checking Client nodes in TerminusDB...")
            response = await client.post(
                f"http://localhost:6363/api/woql/admin/{db_name}",
                json={
                    "query": {
                        "@type": "Triple",
                        "subject": {"variable": "v:X"},
                        "predicate": {"node": "rdf:type"},
                        "object": {"node": "@schema:Client"}
                    }
                },
                auth=("admin", "spice123!")
            )
            
            if response.status_code == 200:
                result = response.json()
                bindings = result.get('bindings', [])
                print(f"   ✅ Found {len(bindings)} Client nodes in TerminusDB")
                for b in bindings:
                    print(f"      • {b.get('v:X')}")
            else:
                print(f"   ❌ Failed to query Clients: {response.status_code}")
            
            # Check Product nodes
            print("\n2️⃣ Checking Product nodes in TerminusDB...")
            response = await client.post(
                f"http://localhost:6363/api/woql/admin/{db_name}",
                json={
                    "query": {
                        "@type": "Triple",
                        "subject": {"variable": "v:X"},
                        "predicate": {"node": "rdf:type"},
                        "object": {"node": "@schema:Product"}
                    }
                },
                auth=("admin", "spice123!")
            )
            
            if response.status_code == 200:
                result = response.json()
                bindings = result.get('bindings', [])
                print(f"   ✅ Found {len(bindings)} Product nodes in TerminusDB")
                for b in bindings:
                    print(f"      • {b.get('v:X')}")
            else:
                print(f"   ❌ Failed to query Products: {response.status_code}")
        
        # ========================================================================
        # PHASE 5: Test Federation
        # ========================================================================
        print("\n📌 PHASE 5: Testing Federation (TerminusDB → ES)")
        print("-" * 60)
        
        # Simple Federation query
        print("\n1️⃣ Simple Federation query for Products...")
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
                    print(f"   🎉 FEDERATION WORKS! Found {count} products")
                    for doc in docs[:3]:
                        print(f"      • ID: {doc.get('id')}")
                        if doc.get('data'):
                            data = doc['data'].get('data', {})
                            print(f"        Name: {data.get('name')}")
                            print(f"        Price: {data.get('price')}")
                else:
                    print(f"   ❌ Federation returned 0 nodes")
                    print("      TerminusDB might not have lightweight nodes")
            else:
                text = await resp.text()
                print(f"   ❌ Federation failed: {resp.status}")
                print(f"      {text[:200]}")
        
        # Multi-hop query
        print("\n2️⃣ Multi-hop query: Products → owned_by → Client...")
        async with session.post(
            f'http://localhost:8002/api/v1/graph-query/{db_name}/multi-hop',
            json={
                'start_class': 'Product',
                'hops': [['owned_by', 'Client']],
                'filters': {'category': 'Software'},
                'include_documents': True
            }
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                data = result.get('data', {})
                nodes = data.get('nodes', [])
                edges = data.get('edges', [])
                
                if nodes:
                    print(f"   🎉 MULTI-HOP WORKS!")
                    print(f"      Nodes: {len(nodes)}")
                    print(f"      Edges: {len(edges)}")
                    
                    products = [n for n in nodes if n.get('type') == 'Product']
                    clients = [n for n in nodes if n.get('type') == 'Client']
                    print(f"      • Products: {len(products)}")
                    print(f"      • Clients: {len(clients)}")
                else:
                    print(f"   ❌ Multi-hop returned no nodes")
            else:
                text = await resp.text()
                print(f"   ❌ Multi-hop failed: {resp.status}")
                print(f"      {text[:200]}")
        
        # ========================================================================
        # PHASE 6: Verify Data Consistency
        # ========================================================================
        print("\n📌 PHASE 6: Data Consistency Check")
        print("-" * 60)
        
        # Check Elasticsearch
        index_name = f"{db_name.replace('-', '_')}_instances"
        async with session.post(
            f'http://localhost:9200/{index_name}/_search',
            json={
                'query': {'match_all': {}},
                'aggs': {
                    'by_class': {
                        'terms': {'field': 'class_id'}
                    }
                },
                'size': 0
            },
            auth=aiohttp.BasicAuth('elastic', 'spice123!')
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                buckets = result.get('aggregations', {}).get('by_class', {}).get('buckets', [])
                
                print("\n📊 Elasticsearch document counts:")
                for bucket in buckets:
                    print(f"   • {bucket['key']}: {bucket['doc_count']} documents")
                
                # Check for duplicates
                total = result.get('hits', {}).get('total', {}).get('value', 0)
                expected = len(clients) + len(products)
                
                if total == expected:
                    print(f"\n   ✅ No duplicates! Expected {expected}, Got {total}")
                else:
                    print(f"\n   ⚠️ Possible duplicates! Expected {expected}, Got {total}")
        
        # ========================================================================
        # FINAL SUMMARY
        # ========================================================================
        print("\n" + "=" * 70)
        print("🎯 LIGHTWEIGHT ARCHITECTURE IMPLEMENTATION SUMMARY")
        print("=" * 70)
        
        print("\n✅ What we implemented:")
        print("   1. TerminusDB schemas with ONLY business keys required")
        print("   2. Domain fields (name, price) stored in ES only")
        print("   3. Relationships defined in graph for traversal")
        print("   4. Federation: TerminusDB (graph) → ES (data)")
        
        print("\n📊 Key principles followed:")
        print("   • NO required domain fields in TerminusDB")
        print("   • NO system fields in schemas")
        print("   • NO dummy/placeholder values")
        print("   • Business key as the ONLY required field")
        
        print("\n🔥 This is TRUE Palantir/Foundry architecture!")
        print(f"\n📌 Database name for testing: {db_name}")

print("\n🚀 Starting TRUE lightweight architecture implementation...")
asyncio.run(implement_lightweight_architecture())