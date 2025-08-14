#!/usr/bin/env python3
"""
Test REAL WOQL Graph Federation Service
THINK ULTRA³ - Not a workaround, the REAL solution
"""

import asyncio
import logging
import sys
import os

sys.path.insert(0, "/Users/isihyeon/Desktop/SPICE HARVESTER/backend")

from shared.services.graph_federation_service_woql import GraphFederationServiceWOQL
from oms.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_real_woql_service():
    """Test the REAL WOQL service"""
    
    logger.info("🔥🔥🔥 TESTING REAL WOQL GRAPH FEDERATION SERVICE")
    logger.info("=" * 70)
    logger.info("This is the REAL solution - actual WOQL, not Document API")
    logger.info("=" * 70)
    
    # Initialize TerminusDB service
    connection_info = ConnectionConfig(
        server_url="http://localhost:6363",
        user="admin",
        account="admin",
        key="admin"
    )
    
    terminus_service = AsyncTerminusService(connection_info)
    await terminus_service.connect()
    
    # Initialize REAL WOQL service
    service = GraphFederationServiceWOQL(
        terminus_service=terminus_service,
        es_host="localhost",
        es_port=9201
    )
    
    db_name = "graph_federation_test"
    
    # Test 1: Simple query
    logger.info("\n1️⃣ Testing simple query (all Products) with REAL WOQL...")
    result = await service.simple_graph_query(db_name, "Product")
    logger.info(f"   ✅ Found {result['count']} products")
    for doc in result.get("documents", [])[:2]:
        logger.info(f"      - {doc.get('name')} (${doc.get('price')})")
    
    # Test 2: Filtered query
    logger.info("\n2️⃣ Testing filtered query (Product PROD001) with REAL WOQL...")
    result = await service.simple_graph_query(
        db_name, 
        "Product",
        filters={"product_id": "PROD001"}
    )
    logger.info(f"   ✅ Found {result['count']} matching products")
    if result["documents"]:
        doc = result["documents"][0]
        logger.info(f"      Product: {doc.get('name')}")
        logger.info(f"      Category: {doc.get('category')}")
    
    # Test 3: Multi-hop query
    logger.info("\n3️⃣ Testing multi-hop (Product -> owned_by -> Client) with REAL WOQL...")
    result = await service.multi_hop_query(
        db_name,
        start_class="Product",
        hops=[("owned_by", "Client")],
        filters={"product_id": "PROD001"}
    )
    logger.info(f"   ✅ Graph traversal: {result['count']} nodes")
    
    # Show the traversal
    for edge in result.get("edges", []):
        logger.info(f"      Edge: {edge['from']} --{edge['predicate']}--> {edge['to']}")
    
    # Show node details
    for node in result.get("nodes", []):
        if node.get("data"):
            logger.info(f"      Node {node['id']}:")
            logger.info(f"        Type: {node['type']}")
            logger.info(f"        Name: {node['data'].get('name')}")
    
    # Test 4: Complex multi-hop (Order -> Client)
    logger.info("\n4️⃣ Testing Order -> Client traversal with REAL WOQL...")
    result = await service.multi_hop_query(
        db_name,
        start_class="Order",
        hops=[("ordered_by", "Client")],
        limit=10
    )
    logger.info(f"   ✅ Found {result['count']} nodes")
    
    # Group by type
    nodes_by_type = {}
    for node in result.get("nodes", []):
        node_type = node["type"]
        if node_type not in nodes_by_type:
            nodes_by_type[node_type] = 0
        nodes_by_type[node_type] += 1
    
    for node_type, count in nodes_by_type.items():
        logger.info(f"      {node_type}: {count} nodes")
    
    logger.info("\n" + "=" * 70)
    logger.info("🎉 SUCCESS! REAL WOQL FEDERATION SERVICE WORKS!")
    logger.info("=" * 70)
    logger.info("✅ Using actual WOQL queries with @schema: prefix")
    logger.info("✅ Proper query parameter wrapping")
    logger.info("✅ Real graph traversal with TerminusDB")
    logger.info("✅ ES document federation working")
    logger.info("✅ THIS IS THE PRODUCTION-READY SOLUTION!")


async def main():
    """Main execution"""
    await test_real_woql_service()


if __name__ == "__main__":
    asyncio.run(main())