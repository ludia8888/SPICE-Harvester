#!/usr/bin/env python3
"""
Test CORRECT WOQL Syntax based on official schema
THINK ULTRA³ - Finding the REAL working WOQL format
"""

import asyncio
import httpx
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_correct_woql():
    """Test the CORRECT WOQL format from official schema"""
    
    auth = ('admin', 'admin')
    db_name = 'graph_federation_test'
    base_url = 'http://localhost:6363'
    
    logger.info("🔥 THINK ULTRA³: Testing CORRECT WOQL Syntax")
    logger.info("=" * 60)
    
    async with httpx.AsyncClient() as client:
        # First verify database exists
        db_resp = await client.get(f"{base_url}/api/db/admin/{db_name}", auth=auth)
        if db_resp.status_code != 200:
            logger.error(f"Database {db_name} not found")
            return
        
        logger.info(f"✅ Database {db_name} exists")
        
        # Test 1: Simple Select with Triple (based on official schema)
        logger.info("\n1️⃣ Test: Simple Select with correct format")
        woql1 = {
            "@type": "Select",
            "variables": ["v:X"],
            "query": {
                "@type": "Triple",
                "subject": {"variable": "v:X"},
                "predicate": {"node": "rdf:type"},
                "object": {"node": "Client"}
            }
        }
        
        resp1 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=woql1,
            auth=auth
        )
        logger.info(f"Response: {resp1.status_code}")
        if resp1.status_code == 200:
            result = resp1.json()
            logger.info(f"✅ SUCCESS! Result: {json.dumps(result, indent=2)[:500]}")
        else:
            logger.error(f"❌ Error: {resp1.text[:500]}")
        
        # Test 2: Using From to specify instance graph
        logger.info("\n2️⃣ Test: Using From to specify instance graph")
        woql2 = {
            "@type": "From",
            "graph": f"instance/{db_name}",
            "query": {
                "@type": "Select",
                "variables": ["v:X"],
                "query": {
                    "@type": "Triple",
                    "subject": {"variable": "v:X"},
                    "predicate": {"node": "rdf:type"},
                    "object": {"node": "Client"}
                }
            }
        }
        
        resp2 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=woql2,
            auth=auth
        )
        logger.info(f"Response: {resp2.status_code}")
        if resp2.status_code == 200:
            result = resp2.json()
            logger.info(f"✅ SUCCESS! Result: {json.dumps(result, indent=2)[:500]}")
        else:
            logger.error(f"❌ Error: {resp2.text[:500]}")
        
        # Test 3: Using And for multiple conditions
        logger.info("\n3️⃣ Test: Using And for multiple conditions")
        woql3 = {
            "@type": "And",
            "and": [
                {
                    "@type": "Triple",
                    "subject": {"variable": "v:X"},
                    "predicate": {"node": "rdf:type"},
                    "object": {"node": "Product"}
                },
                {
                    "@type": "Triple",
                    "subject": {"variable": "v:X"},
                    "predicate": {"node": "product_id"},
                    "object": {"data": "PROD001"}
                }
            ]
        }
        
        resp3 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=woql3,
            auth=auth
        )
        logger.info(f"Response: {resp3.status_code}")
        if resp3.status_code == 200:
            result = resp3.json()
            logger.info(f"✅ SUCCESS! Result: {json.dumps(result, indent=2)[:500]}")
        else:
            logger.error(f"❌ Error: {resp3.text[:500]}")
        
        # Test 4: Direct Triple without Select
        logger.info("\n4️⃣ Test: Direct Triple query")
        woql4 = {
            "@type": "Triple",
            "subject": {"variable": "v:X"},
            "predicate": {"node": "rdf:type"},
            "object": {"node": "Client"}
        }
        
        resp4 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=woql4,
            auth=auth
        )
        logger.info(f"Response: {resp4.status_code}")
        if resp4.status_code == 200:
            result = resp4.json()
            logger.info(f"✅ SUCCESS! Result: {json.dumps(result, indent=2)[:500]}")
        else:
            logger.error(f"❌ Error: {resp4.text[:500]}")
        
        # Test 5: Query with graph specified in Triple
        logger.info("\n5️⃣ Test: Triple with graph parameter")
        woql5 = {
            "@type": "Triple",
            "subject": {"variable": "v:X"},
            "predicate": {"node": "rdf:type"},
            "object": {"node": "Client"},
            "graph": f"instance/{db_name}"
        }
        
        resp5 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=woql5,
            auth=auth
        )
        logger.info(f"Response: {resp5.status_code}")
        if resp5.status_code == 200:
            result = resp5.json()
            logger.info(f"✅ SUCCESS! Result: {json.dumps(result, indent=2)[:500]}")
        else:
            logger.error(f"❌ Error: {resp5.text[:500]}")


async def main():
    """Main execution"""
    await test_correct_woql()
    logger.info("\n" + "=" * 60)
    logger.info("Testing complete - checking which format works!")


if __name__ == "__main__":
    asyncio.run(main())