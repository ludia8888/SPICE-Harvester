#!/usr/bin/env python3
"""
Test FIXED WOQL variable mapping
THINK ULTRA³ - Correct variable naming for bindings
"""

import asyncio
import httpx
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_fixed_mapping():
    """Test with fixed variable mapping"""
    
    auth = ('admin', 'admin')
    db_name = 'graph_federation_test'
    base_url = 'http://localhost:6363'
    
    logger.info("🔧 TESTING FIXED VARIABLE MAPPING")
    logger.info("=" * 60)
    
    async with httpx.AsyncClient() as client:
        # Test 1: Use consistent variable naming
        logger.info("\n1️⃣ Multi-hop with consistent variable names")
        
        # Variables: v:Product, v:RelatedClient (instead of v:Client0)
        query1 = {
            "query": {
                "@type": "Select",
                "variables": ["v:Product", "v:RelatedClient"],
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
                            "predicate": {"node": "@schema:product_id"},
                            "object": {"data": {"@type": "xsd:string", "@value": "PROD001"}}
                        },
                        {
                            "@type": "Triple",
                            "subject": {"variable": "v:Product"},
                            "predicate": {"node": "@schema:owned_by"},
                            "object": {"variable": "v:RelatedClient"}
                        },
                        {
                            "@type": "Triple",
                            "subject": {"variable": "v:RelatedClient"},
                            "predicate": {"node": "rdf:type"},
                            "object": {"node": "@schema:Client"}
                        }
                    ]
                }
            }
        }
        
        resp1 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=query1,
            auth=auth
        )
        
        if resp1.status_code == 200:
            result = resp1.json()
            logger.info(f"Variable names: {result.get('api:variable_names', [])}")
            
            bindings = result.get('bindings', [])
            logger.info(f"Number of bindings: {len(bindings)}")
            
            for binding in bindings:
                logger.info(f"\nBinding:")
                for key, value in binding.items():
                    logger.info(f"  {key}: {value}")
                
                # Extract data correctly
                product_id = binding.get('v:Product')
                client_id = binding.get('v:RelatedClient')
                logger.info(f"  ✅ Product: {product_id}")
                logger.info(f"  ✅ Related Client: {client_id}")
        
        # Test 2: Multiple hops - Product -> Client -> Order
        logger.info("\n2️⃣ Testing chained multi-hop")
        
        query2 = {
            "query": {
                "@type": "Select",
                "variables": ["v:Order", "v:OrderClient"],
                "query": {
                    "@type": "And",
                    "and": [
                        {
                            "@type": "Triple",
                            "subject": {"variable": "v:Order"},
                            "predicate": {"node": "rdf:type"},
                            "object": {"node": "@schema:Order"}
                        },
                        {
                            "@type": "Triple",
                            "subject": {"variable": "v:Order"},
                            "predicate": {"node": "@schema:ordered_by"},
                            "object": {"variable": "v:OrderClient"}
                        },
                        {
                            "@type": "Triple",
                            "subject": {"variable": "v:OrderClient"},
                            "predicate": {"node": "rdf:type"},
                            "object": {"node": "@schema:Client"}
                        }
                    ]
                }
            }
        }
        
        resp2 = await client.post(
            f"{base_url}/api/woql/admin/{db_name}",
            json=query2,
            auth=auth
        )
        
        if resp2.status_code == 200:
            result2 = resp2.json()
            bindings2 = result2.get('bindings', [])
            
            logger.info(f"Order -> Client bindings: {len(bindings2)}")
            for binding in bindings2:
                order_id = binding.get('v:Order')
                client_id = binding.get('v:OrderClient')
                logger.info(f"  Order {order_id} -> Client {client_id}")


async def main():
    """Main execution"""
    await test_fixed_mapping()
    logger.info("\n" + "=" * 60)
    logger.info("Fixed variable mapping test complete!")


if __name__ == "__main__":
    asyncio.run(main())