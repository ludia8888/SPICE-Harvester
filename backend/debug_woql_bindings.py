#!/usr/bin/env python3
"""
Debug WOQL bindings issue
THINK ULTRA³ - Find the root cause of Unknown bindings
"""

import asyncio
import httpx
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def debug_bindings():
    """Debug why bindings show Unknown"""
    
    auth = ('admin', 'admin')
    db_name = 'graph_federation_test'
    base_url = 'http://localhost:6363'
    
    logger.info("🔍 DEBUGGING WOQL BINDINGS ISSUE")
    logger.info("=" * 60)
    
    async with httpx.AsyncClient() as client:
        # Test exact same query that shows Unknown
        logger.info("\n1️⃣ Testing Multi-hop query with variable inspection")
        query = {
            "query": {
                "@type": "Select",
                "variables": ["v:Product", "v:Client", "v:Owner"],
                "query": {
                    "@type": "And",
                    "and": [
                        {
                            "@type": "Triple",
                            "subject": {"variable": "v:Product"},
                            "predicate": {"node": "rdf:type"},
                            "object": {"node": "@schema:Product"}
                        },
                        {
                            "@type": "Triple",
                            "subject": {"variable": "v:Product"},
                            "predicate": {"node": "@schema:owned_by"},
                            "object": {"variable": "v:Owner"}
                        },
                        {
                            "@type": "Triple",
                            "subject": {"variable": "v:Owner"},
                            "predicate": {"node": "rdf:type"},
                            "object": {"node": "@schema:Client"}
                        }
                    ]
                }
            }
        }
        
        resp = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=query,
            auth=auth
        )
        
        if resp.status_code == 200:
            result = resp.json()
            logger.info(f"Response status: SUCCESS")
            logger.info(f"Variable names: {result.get('api:variable_names', [])}")
            
            bindings = result.get('bindings', [])
            logger.info(f"Number of bindings: {len(bindings)}")
            
            for i, binding in enumerate(bindings):
                logger.info(f"\nBinding {i+1}:")
                logger.info(f"  Raw binding: {json.dumps(binding, indent=2)}")
                
                # Check each expected variable
                for var in ['Product', 'Client', 'Owner', 'v:Product', 'v:Client', 'v:Owner']:
                    if var in binding:
                        logger.info(f"  ✅ Found {var}: {binding[var]}")
                    else:
                        logger.info(f"  ❌ Missing {var}")
        
        # Test simpler query to understand variable naming
        logger.info("\n2️⃣ Testing simple query to understand variable format")
        simple_query = {
            "query": {
                "@type": "Select",
                "variables": ["v:X", "v:Type"],
                "query": {
                    "@type": "And",
                    "and": [
                        {
                            "@type": "Triple",
                            "subject": {"variable": "v:X"},
                            "predicate": {"node": "rdf:type"},
                            "object": {"variable": "v:Type"}
                        },
                        {
                            "@type": "Triple",
                            "subject": {"variable": "v:X"},
                            "predicate": {"node": "rdf:type"},
                            "object": {"node": "@schema:Product"}
                        }
                    ]
                }
            }
        }
        
        resp2 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=simple_query,
            auth=auth
        )
        
        if resp2.status_code == 200:
            result2 = resp2.json()
            logger.info(f"\nSimple query variable names: {result2.get('api:variable_names', [])}")
            
            bindings2 = result2.get('bindings', [])
            for binding in bindings2[:2]:
                logger.info(f"Binding keys: {list(binding.keys())}")
                logger.info(f"Binding values: {binding}")
        
        # Test without Select wrapper to see difference
        logger.info("\n3️⃣ Testing without Select wrapper")
        no_select_query = {
            "query": {
                "@type": "And",
                "and": [
                    {
                        "@type": "Triple",
                        "subject": {"variable": "v:Product"},
                        "predicate": {"node": "rdf:type"},
                        "object": {"node": "@schema:Product"}
                    },
                    {
                        "@type": "Triple",
                        "subject": {"variable": "v:Product"},
                        "predicate": {"node": "@schema:owned_by"},
                        "object": {"variable": "v:Owner"}
                    }
                ]
            }
        }
        
        resp3 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=no_select_query,
            auth=auth
        )
        
        if resp3.status_code == 200:
            result3 = resp3.json()
            logger.info(f"Without Select - variable names: {result3.get('api:variable_names', [])}")
            bindings3 = result3.get('bindings', [])
            if bindings3:
                logger.info(f"First binding: {bindings3[0]}")


async def main():
    """Main execution"""
    await debug_bindings()
    logger.info("\n" + "=" * 60)
    logger.info("Root cause analysis complete!")


if __name__ == "__main__":
    asyncio.run(main())