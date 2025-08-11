#!/usr/bin/env python3
"""
Test WOQL with 'query' wrapper parameter
THINK ULTRA³ - Finding the exact API format
"""

import asyncio
import httpx
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_woql_with_wrapper():
    """Test WOQL with query wrapper"""
    
    auth = ('admin', 'admin')
    db_name = 'graph_federation_test'
    base_url = 'http://localhost:6363'
    
    logger.info("🔥 THINK ULTRA³: Testing WOQL with query wrapper")
    logger.info("=" * 60)
    
    async with httpx.AsyncClient() as client:
        # Test 1: Triple wrapped in query parameter
        logger.info("\n1️⃣ Test: Triple wrapped in 'query' parameter")
        request1 = {
            "query": {
                "@type": "Triple",
                "subject": {"variable": "v:X"},
                "predicate": {"node": "rdf:type"},
                "object": {"node": "Client"}
            }
        }
        
        resp1 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=request1,
            auth=auth
        )
        logger.info(f"Response: {resp1.status_code}")
        if resp1.status_code == 200:
            result = resp1.json()
            logger.info(f"✅ SUCCESS! Bindings: {result.get('bindings', [])}")
        else:
            logger.error(f"❌ Error: {resp1.text[:500]}")
        
        # Test 2: Select wrapped in query parameter
        logger.info("\n2️⃣ Test: Select wrapped in 'query' parameter")
        request2 = {
            "query": {
                "@type": "Select",
                "variables": ["v:X"],
                "query": {
                    "@type": "Triple",
                    "subject": {"variable": "v:X"},
                    "predicate": {"node": "rdf:type"},
                    "object": {"node": "Product"}
                }
            }
        }
        
        resp2 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=request2,
            auth=auth
        )
        logger.info(f"Response: {resp2.status_code}")
        if resp2.status_code == 200:
            result = resp2.json()
            logger.info(f"Result: {json.dumps(result, indent=2)[:600]}")
        else:
            logger.error(f"❌ Error: {resp2.text[:500]}")
        
        # Test 3: Try with instance graph specified
        logger.info("\n3️⃣ Test: Query instance graph explicitly")
        request3 = {
            "query": {
                "@type": "From",
                "graph": "instance",  # or "instance/main" or just "instance"
                "query": {
                    "@type": "Triple",
                    "subject": {"variable": "v:X"},
                    "predicate": {"node": "rdf:type"},
                    "object": {"node": "Client"}
                }
            }
        }
        
        resp3 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=request3,
            auth=auth
        )
        logger.info(f"Response: {resp3.status_code}")
        if resp3.status_code == 200:
            result = resp3.json()
            logger.info(f"Bindings: {result.get('bindings', [])}")
        else:
            logger.error(f"❌ Error: {resp3.text[:500]}")
        
        # Test 4: Try different class references
        logger.info("\n4️⃣ Test: Different ways to reference classes")
        
        # Try with @id prefix
        request4a = {
            "query": {
                "@type": "Triple",
                "subject": {"variable": "v:X"},
                "predicate": {"node": "rdf:type"},
                "object": {"node": "@schema:Client"}
            }
        }
        
        resp4a = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=request4a,
            auth=auth
        )
        logger.info(f"With @schema:Client - Response: {resp4a.status_code}")
        if resp4a.status_code == 200:
            result = resp4a.json()
            logger.info(f"Bindings count: {len(result.get('bindings', []))}")
        
        # Try with full URI
        request4b = {
            "query": {
                "@type": "Triple",
                "subject": {"variable": "v:X"},
                "predicate": {"node": "rdf:type"},
                "object": {"node": f"terminusdb://admin/{db_name}/schema#Client"}
            }
        }
        
        resp4b = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=request4b,
            auth=auth
        )
        logger.info(f"With full URI - Response: {resp4b.status_code}")
        if resp4b.status_code == 200:
            result = resp4b.json()
            logger.info(f"Bindings count: {len(result.get('bindings', []))}")
        
        # Test 5: Check what's actually in the database
        logger.info("\n5️⃣ Verifying data exists via Document API")
        doc_resp = await client.get(
            f"{base_url}/api/document/admin/{db_name}",
            params={"graph_type": "instance"},
            auth=auth
        )
        if doc_resp.status_code == 200:
            docs = []
            for line in doc_resp.text.strip().split('\n'):
                if line:
                    docs.append(json.loads(line))
            logger.info(f"Found {len(docs)} documents via Document API")
            for doc in docs[:2]:
                logger.info(f"  Doc: @type={doc.get('@type')}, @id={doc.get('@id')}")


async def main():
    """Main execution"""
    await test_woql_with_wrapper()
    logger.info("\n" + "=" * 60)
    logger.info("Deep analysis of WOQL query format complete!")


if __name__ == "__main__":
    asyncio.run(main())