#!/usr/bin/env python3
"""
🔥 THINK ULTRA: REAL USER FLOW TEST - NO MOCKING, REAL PRODUCTION

Tests the ACTUAL end-to-end flow as a real user would experience:
1. Create database
2. Create ontologies  
3. Insert real data
4. Verify Palantir architecture works
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime

async def main():
    print("🔥 THINK ULTRA: REAL USER FLOW TEST")
    print("="*70)
    
    async with aiohttp.ClientSession() as session:
        # Generate unique database name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        db_name = f"test_palantir_{timestamp}"
        
        # ============================================
        # STEP 1: CREATE NEW DATABASE
        # ============================================
        print(f"\n1️⃣ Creating database: {db_name}")
        
        async with session.post(
            "http://localhost:8000/api/v1/database/create",
            json={"name": db_name, "description": "Palantir architecture test"}
        ) as resp:
            if resp.status in [200, 201, 202]:
                result = await resp.json()
                if resp.status == 202:
                    command_id = result.get('data', {}).get('command_id')
                    print(f"   ✅ Database creation accepted (Event Sourcing)")
                    print(f"   📝 Command ID: {command_id}")
                else:
                    print(f"   ✅ Database created directly (status: {resp.status})")
            else:
                error = await resp.text()
                print(f"   ❌ Failed ({resp.status}): {error[:200]}")
                return
        
        # Wait for database to be created
        print("   ⏳ Waiting for database creation...")
        await asyncio.sleep(15)
        
        # Verify database exists
        async with session.get(f"http://localhost:8000/api/v1/database/exists/{db_name}") as resp:
            if resp.status == 200:
                result = await resp.json()
                exists = result.get('data', {}).get('exists', False)
                if exists:
                    print(f"   ✅ Database {db_name} verified!")
                else:
                    print(f"   ❌ Database not found, waiting more...")
                    await asyncio.sleep(10)
            else:
                print(f"   ❌ Could not verify database")
                return
        
        # ============================================
        # STEP 2: CREATE PRODUCT ONTOLOGY
        # ============================================
        print("\n2️⃣ Creating Product ontology...")
        
        product_schema = {
            "id": "Product",
            "label": "Product",
            "description": "Product entity for testing",
            "properties": [
                {"name": "product_id", "type": "xsd:string", "label": "Product ID", "required": True},
                {"name": "name", "type": "xsd:string", "label": "Product Name", "required": True},
                {"name": "price", "type": "xsd:decimal", "label": "Price"},
                {"name": "category", "type": "xsd:string", "label": "Category"}
            ]
        }
        
        async with session.post(
            f"http://localhost:8000/api/v1/database/{db_name}/ontology",
            json=product_schema
        ) as resp:
            if resp.status in [200, 202]:
                print(f"   ✅ Product ontology created (status: {resp.status})")
            else:
                error = await resp.text()
                print(f"   ❌ Failed: {error[:200]}")
        
        await asyncio.sleep(5)
        
        # ============================================
        # STEP 3: INSERT TEST DATA
        # ============================================
        print("\n3️⃣ Inserting test products...")
        
        test_products = [
            {"product_id": "P001", "name": "iPhone 15", "price": 999.99, "category": "Electronics"},
            {"product_id": "P002", "name": "MacBook Pro", "price": 2499.99, "category": "Computers"},
            {"product_id": "P003", "name": "AirPods Pro", "price": 249.99, "category": "Audio"}
        ]
        
        for product in test_products:
            try:
                async with session.post(
                    f"http://localhost:8000/api/v1/instances/{db_name}/async/Product/create",
                    json={"data": product},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 202:
                        result = await resp.json()
                        cmd_id = result.get('command_id', 'unknown')[:8]
                        print(f"   ✅ {product['product_id']}: Command {cmd_id}...")
                    else:
                        error = await resp.text()
                        print(f"   ❌ {product['product_id']}: {error[:100]}")
            except Exception as e:
                print(f"   ❌ {product['product_id']}: Connection error - {str(e)[:50]}")
            
            # Add delay between requests
            await asyncio.sleep(1)
        
        print("   ⏳ Waiting for Event Sourcing to process...")
        await asyncio.sleep(10)
        
        # ============================================
        # STEP 4: VERIFY PALANTIR ARCHITECTURE
        # ============================================
        print("\n4️⃣ VERIFYING PALANTIR ARCHITECTURE...")
        
        # Check TerminusDB
        print("\n   📊 Checking TerminusDB (should have lightweight nodes)...")
        async with session.post(
            f"http://localhost:8000/api/v1/query/{db_name}",
            json={"query": "SELECT * FROM Product"}
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                data = result.get('data', [])
                print(f"      Found {len(data)} products in TerminusDB")
                if data:
                    sample = data[0]
                    fields = list(sample.keys())
                    print(f"      Fields: {fields}")
                    
                    # Check if it's lightweight
                    if '@id' in fields and 'es_doc_id' in fields:
                        print("      ✅ CORRECT: Lightweight nodes with ES references")
                    elif 'price' in fields or 'name' in fields:
                        print("      ⚠️ WARNING: Full data in TerminusDB (should be lightweight)")
            else:
                print(f"      ❌ Query failed")
        
        # Check Elasticsearch
        print("\n   📊 Checking Elasticsearch (should have full documents)...")
        es_index = f"{db_name}_instances"
        async with session.post(
            f"http://localhost:9200/{es_index}/_search",
            json={"query": {"match_all": {}}, "size": 10}
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                hits = result.get('hits', {}).get('hits', [])
                total = result.get('hits', {}).get('total', {}).get('value', 0)
                print(f"      Found {total} documents in Elasticsearch")
                
                if hits:
                    sample = hits[0].get('_source', {})
                    fields = list(sample.keys())
                    print(f"      Fields: {fields}")
                    
                    if 'price' in fields and 'name' in fields:
                        print("      ✅ CORRECT: Full documents with all data")
                        # Show sample data
                        for hit in hits[:3]:
                            doc = hit.get('_source', {})
                            print(f"      - {doc.get('product_id')}: {doc.get('name')} (${doc.get('price')})")
            else:
                print(f"      ❌ ES query failed: {resp.status}")
        
        # ============================================
        # STEP 5: TEST GRAPH FEDERATION
        # ============================================
        print("\n5️⃣ Testing Graph Federation...")
        
        async with session.post(
            f"http://localhost:8002/api/v1/graph-query/{db_name}/simple",
            json={"class_name": "Product", "limit": 10}
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                nodes = result.get('data', {}).get('nodes', [])
                print(f"   ✅ Graph Federation returned {len(nodes)} nodes")
                
                if nodes:
                    sample = nodes[0]
                    if sample.get('id') and sample.get('data'):
                        print("   ✅ CORRECT: Federation combines TerminusDB + ES")
                        # Show combined data
                        for node in nodes[:3]:
                            node_id = node.get('id', 'unknown')
                            node_data = node.get('data', {})
                            print(f"   - Node {node_id}: {node_data.get('name')} (${node_data.get('price')})")
            else:
                error = await resp.text()
                print(f"   ❌ Graph query failed: {error[:200]}")
        
        # ============================================
        # FINAL VERDICT
        # ============================================
        print("\n" + "="*70)
        print("🎯 PALANTIR ARCHITECTURE TEST COMPLETE!")
        print("="*70)
        print(f"Database: {db_name}")
        print("Check the results above to verify:")
        print("1. TerminusDB has lightweight nodes (IDs + relationships)")
        print("2. Elasticsearch has full documents (all data)")
        print("3. Graph Federation combines both")
        print("\n🔥 THINK ULTRA: This is REAL production flow, no mocking!")


if __name__ == "__main__":
    asyncio.run(main())