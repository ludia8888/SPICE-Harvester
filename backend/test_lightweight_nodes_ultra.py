#!/usr/bin/env python3
"""
🔥 THINK ULTRA: Test TRUE lightweight nodes architecture
Verify that TerminusDB has nodes for Federation to work
"""

import asyncio
import json

import aiohttp
import time
import pytest
from shared.config.settings import get_settings


_SETTINGS = get_settings()
OMS_URL = _SETTINGS.services.oms_base_url.rstrip("/")
BFF_URL = _SETTINGS.services.bff_base_url.rstrip("/")
ELASTICSEARCH_URL = _SETTINGS.database.elasticsearch_url.rstrip("/")


async def _post_json_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    payload: dict,
    *,
    timeout_seconds: float = 60.0,
    retry_sleep: float = 2.0,
) -> tuple[int, dict | None, str]:
    deadline = time.monotonic() + timeout_seconds
    last_error = None
    while time.monotonic() < deadline:
        try:
            async with session.post(url, json=payload) as resp:
                text = await resp.text()
                data = None
                if text:
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError:
                        data = None
                return resp.status, data, text
        except aiohttp.ClientError as exc:
            last_error = str(exc)
            await asyncio.sleep(retry_sleep)
    raise AssertionError(f"POST {url} failed after waiting: {last_error}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lightweight_architecture():
    print("🔥 TESTING LIGHTWEIGHT NODES ARCHITECTURE")
    print("=" * 70)
    
    settings = get_settings()
    admin_token = str(settings.clients.oms_client_token or settings.clients.bff_admin_token or "test-token").strip()
    headers = {"X-Admin-Token": admin_token}
    async with aiohttp.ClientSession(headers=headers) as session:
        db_name = f"lightweight_test_{int(time.time())}"
        
        # 1. Create database
        print("\n1️⃣ Creating test database...")
        async with session.post(
            f"{OMS_URL}/api/v1/database/create",
            json={'name': db_name, 'description': 'Lightweight nodes test'}
        ) as resp:
            if resp.status in (200, 201, 202):
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
            f"{OMS_URL}/api/v1/database/{db_name}/ontology",
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
            f"{OMS_URL}/api/v1/database/{db_name}/ontology",
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
            f"{OMS_URL}/api/v1/instances/{db_name}/async/Client/create",
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
            f"{OMS_URL}/api/v1/instances/{db_name}/async/Product/create",
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
            
            terminus_url = settings.database.terminus_url.rstrip("/")
            terminus_user = settings.database.terminus_user
            terminus_key = settings.database.terminus_password
            terminus_account = settings.database.terminus_account
            response = await client.post(
                f"{terminus_url}/api/woql/{terminus_account}/{db_name}",
                json=woql_query,
                auth=(terminus_user, terminus_key),
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
        status, result, text = await _post_json_with_retry(
            session,
            f"{ELASTICSEARCH_URL}/{index_name}/_search",
            {'query': {'match_all': {}}, 'size': 10},
        )
        if status in (200, 201, 202):
            result = result or {}
            hits = result.get('hits', {}).get('hits', [])
            print(f"   📊 Elasticsearch documents: {len(hits)}")

            for hit in hits:
                doc = hit['_source']
                has_terminus_id = 'terminus_id' in doc
                has_data = 'data' in doc
                print(f"      • {doc.get('class_id')}/{doc.get('instance_id')}")
                print(f"        - terminus_id: {'✅' if has_terminus_id else '❌'}")
                print(f"        - domain data: {'✅' if has_data else '❌'}")
        else:
            print(f"   ❌ Elasticsearch query failed: {status}")
            print(f"      {text[:200]}")
                    
        # 6. TEST Federation query
        print("\n6️⃣ Testing Federation query...")
        
        status, result, text = await _post_json_with_retry(
            session,
            f"{BFF_URL}/api/v1/graph-query/{db_name}/simple",
            {
                'class_name': 'Product',
                'include_documents': True,
                'limit': 10
            },
        )
        if status in (200, 201, 202):
            result = result or {}
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
            print(f"   ❌ Federation query failed: {status}")
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

if __name__ == "__main__":
    print("\n🚀 Starting lightweight nodes architecture test...")
    asyncio.run(test_lightweight_architecture())
