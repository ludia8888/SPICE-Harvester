#!/usr/bin/env python3
"""
Debug WOQL Syntax Issues
Find the correct WOQL format for TerminusDB queries
"""

import asyncio
import httpx
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_woql_queries():
    """Test different WOQL query formats"""
    auth = ('admin', 'admin')
    db_name = 'graph_federation_test'
    base_url = 'http://localhost:6363'
    
    logger.info("🔍 Testing WOQL query formats...")
    
    async with httpx.AsyncClient() as client:
        # First check database exists
        db_resp = await client.get(f"{base_url}/api/db/admin/{db_name}", auth=auth)
        if db_resp.status_code != 200:
            logger.error(f"Database {db_name} not found")
            return
        
        # Test 1: Basic triple pattern with proper namespace
        logger.info("\n1️⃣ Test: Basic triple with full URI")
        woql1 = {
            "@type": "woql:Select",
            "woql:variable_list": [
                {"@type": "woql:Variable", "woql:variable_name": "X"}
            ],
            "woql:query": {
                "@type": "woql:Triple",
                "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "X"},
                "woql:predicate": {"@type": "woql:Node", "woql:node": "rdf:type"},
                "woql:object": {"@type": "woql:Node", "woql:node": f"terminusdb://admin/{db_name}/schema#Client"}
            }
        }
        
        resp1 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json={"query": woql1},
            auth=auth
        )
        logger.info(f"Response: {resp1.status_code}")
        if resp1.status_code == 200:
            result = resp1.json()
            logger.info(f"✅ Success! Bindings: {len(result.get('bindings', []))}")
        else:
            logger.error(f"❌ Error: {resp1.text[:300]}")
        
        # Test 2: Using quad to specify graph
        logger.info("\n2️⃣ Test: Quad with instance graph specification")
        woql2 = {
            "@type": "woql:Select",
            "woql:variable_list": [
                {"@type": "woql:Variable", "woql:variable_name": "X"}
            ],
            "woql:query": {
                "@type": "woql:Quad",
                "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "X"},
                "woql:predicate": {"@type": "woql:Node", "woql:node": "rdf:type"},
                "woql:object": {"@type": "woql:Node", "woql:node": "Client"},
                "woql:graph": {"@type": "woql:Node", "woql:node": f"terminusdb://admin/{db_name}/data/"}
            }
        }
        
        resp2 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json={"query": woql2},
            auth=auth
        )
        logger.info(f"Response: {resp2.status_code}")
        if resp2.status_code == 200:
            result = resp2.json()
            logger.info(f"✅ Success! Bindings: {result.get('bindings', [])[:1]}")
        else:
            logger.error(f"❌ Error: {resp2.text[:300]}")
        
        # Test 3: Using lib.woql style (Python client style)
        logger.info("\n3️⃣ Test: Lib style query")
        woql3 = {
            "@type": "woql:And",
            "woql:query_list": [
                {
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "X"},
                    "woql:predicate": "rdf:type",
                    "woql:object": "Client"
                }
            ]
        }
        
        resp3 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json={"query": woql3},
            auth=auth
        )
        logger.info(f"Response: {resp3.status_code}")
        if resp3.status_code == 200:
            result = resp3.json()
            logger.info(f"✅ Success! Result: {json.dumps(result, indent=2)[:200]}")
        else:
            logger.error(f"❌ Error: {resp3.text[:300]}")
        
        # Test 4: Using from and into for graph specification
        logger.info("\n4️⃣ Test: Using 'from' for graph specification")
        woql4 = {
            "@type": "woql:From",
            "woql:graph": f"terminusdb://admin/{db_name}/data/",
            "woql:query": {
                "@type": "woql:Triple",
                "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "X"},
                "woql:predicate": "rdf:type",
                "woql:object": "Client"
            }
        }
        
        resp4 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json={"query": woql4},
            auth=auth
        )
        logger.info(f"Response: {resp4.status_code}")
        if resp4.status_code == 200:
            result = resp4.json()
            logger.info(f"✅ Success! Found instances: {result}")
        else:
            logger.error(f"❌ Error: {resp4.text[:300]}")


async def test_working_format():
    """Test the format that should work based on TerminusDB docs"""
    auth = ('admin', 'admin')
    db_name = 'graph_federation_test'
    base_url = 'http://localhost:6363'
    
    logger.info("\n5️⃣ Testing documented format...")
    
    async with httpx.AsyncClient() as client:
        # This format is from TerminusDB documentation
        woql = {
            "type": "woql:Select",
            "variables": ["v:Client"],
            "query": {
                "type": "woql:Triple",
                "subject": "v:Client",
                "predicate": "type",
                "object": "@schema:Client"
            }
        }
        
        # Try with explicit commit
        resp = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json={"query": woql, "commit": {"author": "test", "message": "test query"}},
            auth=auth
        )
        logger.info(f"Response: {resp.status_code}")
        if resp.status_code == 200:
            logger.info(f"✅ Success: {resp.json()}")
        else:
            logger.error(f"❌ Error: {resp.text}")


async def main():
    """Run all tests"""
    logger.info("🔥 WOQL Syntax Debugging")
    logger.info("=" * 60)
    
    await test_woql_queries()
    await test_working_format()
    
    logger.info("\n" + "=" * 60)
    logger.info("📊 Testing complete")


if __name__ == "__main__":
    asyncio.run(main())