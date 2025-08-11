#!/usr/bin/env python3
"""
Test Graph Query Endpoint
THINK ULTRA³ - Verifying multi-hop graph queries through BFF

This script tests the new /graph-query endpoint in BFF
that combines TerminusDB graph traversal with ES document retrieval.
"""

import asyncio
import aiohttp
import json
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_graph_endpoint():
    """Test the graph query endpoint"""
    
    # BFF endpoint
    bff_url = "http://localhost:8002"
    
    # Test database (from previous tests)
    db_name = "fixed_test_20250811_144928"
    
    async with aiohttp.ClientSession() as session:
        
        # 1. Test health endpoint
        logger.info("1️⃣ Testing graph service health...")
        async with session.get(f"{bff_url}/api/v1/graph-query/health") as resp:
            if resp.status == 200:
                health = await resp.json()
                logger.info(f"   ✅ Health: {health.get('status')}")
                logger.info(f"   Services: {health.get('services')}")
            else:
                logger.error(f"   ❌ Health check failed: {resp.status}")
        
        # 2. Test simple graph query
        logger.info("\n2️⃣ Testing simple graph query (all Products)...")
        simple_query = {
            "class_name": "Product",
            "limit": 10
        }
        
        async with session.post(
            f"{bff_url}/api/v1/graph-query/{db_name}/simple",
            json=simple_query
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                logger.info(f"   ✅ Found {result.get('count')} products")
                
                # Show first product if available
                docs = result.get("documents", [])
                if docs:
                    first_doc = docs[0]
                    logger.info(f"   First product:")
                    logger.info(f"     - ID: {first_doc.get('product_id')}")
                    logger.info(f"     - Name: {first_doc.get('name')}")
                    logger.info(f"     - Owner: {first_doc.get('owned_by')}")
            else:
                error = await resp.text()
                logger.error(f"   ❌ Simple query failed: {error}")
        
        # 3. Test multi-hop query (Product -> owned_by -> Client)
        logger.info("\n3️⃣ Testing multi-hop query (Product -> Client)...")
        multi_hop_query = {
            "start_class": "Product",
            "hops": [
                {"predicate": "owned_by", "target_class": "Client"}
            ],
            "filters": {"product_id": "PROD-001"},
            "limit": 10
        }
        
        async with session.post(
            f"{bff_url}/api/v1/graph-query/{db_name}",
            json=multi_hop_query
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                logger.info(f"   ✅ Graph traversal complete:")
                logger.info(f"     - Nodes: {result.get('count')}")
                logger.info(f"     - Edges: {len(result.get('edges', []))}")
                
                # Show nodes
                for node in result.get("nodes", []):
                    logger.info(f"     Node: {node['type']} - {node['id']}")
                    if node.get("data"):
                        logger.info(f"       Data: {json.dumps(node['data'], indent=2)[:200]}...")
                
                # Show edges
                for edge in result.get("edges", []):
                    logger.info(f"     Edge: {edge['from_node']} --{edge['predicate']}--> {edge['to_node']}")
            else:
                error = await resp.text()
                logger.error(f"   ❌ Multi-hop query failed: {error}")
        
        # 4. Test path finding
        logger.info("\n4️⃣ Testing path finding (SKU to Client)...")
        async with session.get(
            f"{bff_url}/api/v1/graph-query/{db_name}/paths",
            params={
                "source_class": "SKU",
                "target_class": "Client",
                "max_depth": 3
            }
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                logger.info(f"   ✅ Found {result.get('count')} paths from SKU to Client")
                
                for i, path in enumerate(result.get("paths", []), 1):
                    path_str = " -> ".join([
                        f"{hop['from']} --{hop['predicate']}--> {hop['to']}"
                        for hop in path
                    ])
                    logger.info(f"     Path {i}: {path_str}")
            else:
                error = await resp.text()
                logger.error(f"   ❌ Path finding failed: {error}")
        
        # 5. Test complex multi-hop (SKU -> Product -> Client)
        logger.info("\n5️⃣ Testing 2-hop query (SKU -> Product -> Client)...")
        complex_query = {
            "start_class": "SKU",
            "hops": [
                {"predicate": "belongs_to", "target_class": "Product"},
                {"predicate": "owned_by", "target_class": "Client"}
            ],
            "limit": 5
        }
        
        async with session.post(
            f"{bff_url}/api/v1/graph-query/{db_name}",
            json=complex_query
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                logger.info(f"   ✅ 2-hop traversal complete:")
                logger.info(f"     - Total nodes: {result.get('count')}")
                
                # Group nodes by type
                nodes_by_type = {}
                for node in result.get("nodes", []):
                    node_type = node["type"]
                    if node_type not in nodes_by_type:
                        nodes_by_type[node_type] = []
                    nodes_by_type[node_type].append(node["id"])
                
                for node_type, node_ids in nodes_by_type.items():
                    logger.info(f"     {node_type}: {len(node_ids)} nodes")
                    for node_id in node_ids[:3]:  # Show first 3
                        logger.info(f"       - {node_id}")
            else:
                error = await resp.text()
                logger.error(f"   ❌ 2-hop query failed: {error}")


async def create_test_data_if_needed():
    """Create test data if it doesn't exist"""
    
    logger.info("\n📊 Ensuring test data exists...")
    
    # Check if we have data in Elasticsearch
    es_url = "http://localhost:9201"
    db_name = "fixed_test_20250811_144928"
    index_name = f"instances_{db_name.replace('-', '_')}"
    
    async with aiohttp.ClientSession() as session:
        # Check if index exists
        async with session.head(f"{es_url}/{index_name}") as resp:
            if resp.status == 404:
                logger.info("   Index doesn't exist, creating test data...")
                
                # Create index with mapping
                mapping = {
                    "mappings": {
                        "properties": {
                            "@type": {"type": "keyword"},
                            "@id": {"type": "keyword"},
                            "instance_id": {"type": "keyword"},
                            "class_id": {"type": "keyword"},
                            "client_id": {"type": "keyword"},
                            "product_id": {"type": "keyword"},
                            "sku_id": {"type": "keyword"},
                            "name": {"type": "text"},
                            "owned_by": {"type": "keyword"},
                            "belongs_to": {"type": "keyword"}
                        }
                    }
                }
                
                async with session.put(f"{es_url}/{index_name}", json=mapping) as create_resp:
                    if create_resp.status in [200, 201]:
                        logger.info(f"   ✅ Created index: {index_name}")
                
                # Create test documents
                test_data = [
                    # Clients
                    {
                        "@type": "Client",
                        "@id": "CL-001",
                        "instance_id": "CL-001",
                        "class_id": "Client",
                        "client_id": "CL-001",
                        "name": "Acme Corporation"
                    },
                    {
                        "@type": "Client",
                        "@id": "CL-002",
                        "instance_id": "CL-002",
                        "class_id": "Client",
                        "client_id": "CL-002",
                        "name": "TechCorp Industries"
                    },
                    # Products
                    {
                        "@type": "Product",
                        "@id": "PROD-001",
                        "instance_id": "PROD-001",
                        "class_id": "Product",
                        "product_id": "PROD-001",
                        "name": "Premium Widget",
                        "owned_by": "CL-001"
                    },
                    {
                        "@type": "Product",
                        "@id": "PROD-002",
                        "instance_id": "PROD-002",
                        "class_id": "Product",
                        "product_id": "PROD-002",
                        "name": "Super Gadget",
                        "owned_by": "CL-002"
                    },
                    # SKUs
                    {
                        "@type": "SKU",
                        "@id": "SKU-001",
                        "instance_id": "SKU-001",
                        "class_id": "SKU",
                        "sku_id": "SKU-001",
                        "quantity": 100,
                        "belongs_to": "PROD-001"
                    },
                    {
                        "@type": "SKU",
                        "@id": "SKU-002",
                        "instance_id": "SKU-002",
                        "class_id": "SKU",
                        "sku_id": "SKU-002",
                        "quantity": 50,
                        "belongs_to": "PROD-001"
                    },
                    {
                        "@type": "SKU",
                        "@id": "SKU-003",
                        "instance_id": "SKU-003",
                        "class_id": "SKU",
                        "sku_id": "SKU-003",
                        "quantity": 75,
                        "belongs_to": "PROD-002"
                    }
                ]
                
                # Insert test documents
                for doc in test_data:
                    doc_id = doc["@id"]
                    async with session.post(
                        f"{es_url}/{index_name}/_doc/{doc_id}",
                        json=doc
                    ) as insert_resp:
                        if insert_resp.status in [200, 201]:
                            logger.info(f"   ✅ Created: {doc['@type']} - {doc_id}")
            else:
                logger.info("   ✅ Index already exists, test data should be present")


async def main():
    """Main execution"""
    logger.info("🔥 THINK ULTRA³: Testing Graph Query Endpoint")
    logger.info("=" * 60)
    
    # Ensure test data exists
    await create_test_data_if_needed()
    
    # Wait a moment for ES to index
    await asyncio.sleep(2)
    
    # Run tests
    await test_graph_endpoint()
    
    logger.info("\n✅ Graph query endpoint testing complete!")
    logger.info("Next steps:")
    logger.info("1. Create lightweight graph nodes in TerminusDB for existing ES data")
    logger.info("2. Run A/B testing between direct ES queries and graph federation")
    logger.info("3. Implement monitoring for graph query performance")


if __name__ == "__main__":
    asyncio.run(main())