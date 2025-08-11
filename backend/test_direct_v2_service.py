#!/usr/bin/env python3
"""
Test GraphFederationServiceV2 directly
"""

import asyncio
import logging
import sys

sys.path.insert(0, "/Users/isihyeon/Desktop/SPICE HARVESTER/backend")

from shared.services.graph_federation_service_v2 import GraphFederationServiceV2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_service():
    """Test the service directly"""
    
    logger.info("🔥 Testing GraphFederationServiceV2 Directly")
    logger.info("=" * 60)
    
    # Initialize service
    service = GraphFederationServiceV2(
        terminus_url="http://localhost:6363",
        terminus_user="admin",
        terminus_pass="admin",
        es_host="localhost",
        es_port=9201
    )
    
    db_name = "graph_federation_test"
    
    # Test 1: Simple query
    logger.info("\n1️⃣ Testing simple query for Products...")
    result = await service.simple_graph_query(db_name, "Product")
    logger.info(f"   Result: {result['count']} products found")
    for doc in result.get("documents", [])[:2]:
        logger.info(f"   - {doc.get('name')} (${doc.get('price')})")
    
    # Test 2: Filtered query
    logger.info("\n2️⃣ Testing filtered query (Product PROD001)...")
    result = await service.simple_graph_query(
        db_name,
        "Product",
        filters={"product_id": "PROD001"}
    )
    logger.info(f"   Result: {result['count']} products found")
    if result["documents"]:
        doc = result["documents"][0]
        logger.info(f"   Product: {doc.get('name')}")
        logger.info(f"   Category: {doc.get('category')}")
    
    # Test 3: Multi-hop query
    logger.info("\n3️⃣ Testing multi-hop query (Product -> Client)...")
    result = await service.multi_hop_query(
        db_name,
        start_class="Product",
        hops=[("owned_by", "Client")],
        filters={"product_id": "PROD001"}
    )
    logger.info(f"   Result: {result['count']} nodes")
    
    # Show edges
    for edge in result.get("edges", []):
        logger.info(f"   Edge: {edge['from']} -> {edge['to']} ({edge['predicate']})")
    
    # Show nodes
    for node in result.get("nodes", []):
        logger.info(f"   Node: {node['id']} ({node['type']})")
        if node.get("data"):
            logger.info(f"     Name: {node['data'].get('name')}")
    
    logger.info("\n✅ Direct service test complete!")


asyncio.run(test_service())