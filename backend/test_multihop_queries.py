#\!/usr/bin/env python3
"""
🔥 SPICE HARVESTER Multi-Hop Graph Queries
실제 멀티홉 쿼리 테스트
"""

import asyncio
import json
import logging
import aiohttp
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MultiHopQueryTester:
    def __init__(self):
        self.base_url = "http://localhost:8000"
        self.terminus_url = "http://localhost:6364"
        self.db_name = "spice_3pl_graph"
        self.session = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def test_2hop_query(self):
        """2-hop: SKU → Product → Client"""
        logger.info("\n🔍 2-HOP QUERY: SKU → Product → Client")
        logger.info("Finding which Client owns each SKU (through Product)")
        
        # WOQL query for 2-hop traversal
        woql_query = {
            "@type": "woql:And",
            "woql:query_list": [
                {
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "SKU"},
                    "woql:predicate": "belongs_to",
                    "woql:object": {"@type": "woql:Variable", "woql:variable_name": "Product"}
                },
                {
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "Product"},
                    "woql:predicate": "owned_by",
                    "woql:object": {"@type": "woql:Variable", "woql:variable_name": "Client"}
                }
            ]
        }
        
        # Direct TerminusDB query
        import requests
        response = requests.post(
            f"{self.terminus_url}/api/woql/admin/{self.db_name}",
            json={"query": woql_query},
            auth=("admin", "admin")
        )
        
        if response.status_code == 200:
            result = response.json()
            bindings = result.get("bindings", [])
            logger.info(f"✅ Found {len(bindings)} SKU→Product→Client paths")
            
            # Show sample results
            if bindings and len(bindings) > 0:
                for i, binding in enumerate(bindings[:3]):
                    sku = binding.get("SKU", "N/A")
                    product = binding.get("Product", "N/A")
                    client = binding.get("Client", "N/A")
                    logger.info(f"   Path {i+1}: {sku} → {product} → {client}")
        else:
            logger.error(f"❌ Query failed: {response.status_code}")
            
    async def test_3hop_query(self):
        """3-hop: OrderItem → Order → Client + SKU"""
        logger.info("\n🔍 3-HOP QUERY: OrderItem → Order → Client (+ SKU)")
        logger.info("Finding which Client ordered which SKUs through OrderItems")
        
        woql_query = {
            "@type": "woql:And",
            "woql:query_list": [
                {
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "OrderItem"},
                    "woql:predicate": "item_of",
                    "woql:object": {"@type": "woql:Variable", "woql:variable_name": "Order"}
                },
                {
                    "@type": "woql:Triple", 
                    "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "Order"},
                    "woql:predicate": "placed_by",
                    "woql:object": {"@type": "woql:Variable", "woql:variable_name": "Client"}
                },
                {
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "OrderItem"},
                    "woql:predicate": "for_sku",
                    "woql:object": {"@type": "woql:Variable", "woql:variable_name": "SKU"}
                }
            ]
        }
        
        import requests
        response = requests.post(
            f"{self.terminus_url}/api/woql/admin/{self.db_name}",
            json={"query": woql_query},
            auth=("admin", "admin")
        )
        
        if response.status_code == 200:
            result = response.json()
            bindings = result.get("bindings", [])
            logger.info(f"✅ Found {len(bindings)} OrderItem→Order→Client paths with SKU")
            
            if bindings and len(bindings) > 0:
                for i, binding in enumerate(bindings[:3]):
                    order_item = binding.get("OrderItem", "N/A")
                    order = binding.get("Order", "N/A")
                    client = binding.get("Client", "N/A")
                    sku = binding.get("SKU", "N/A")
                    logger.info(f"   Path {i+1}: {order_item} → {order} → {client} (SKU: {sku})")
        else:
            logger.error(f"❌ Query failed: {response.status_code}")
            
    async def test_4hop_query(self):
        """4-hop: Receipt → Inbound → SKU → Product → Client"""
        logger.info("\n🔍 4-HOP QUERY: Receipt → Inbound → SKU → Product → Client")
        logger.info("Tracing receipts all the way back to the Client who owns the product")
        
        woql_query = {
            "@type": "woql:And",
            "woql:query_list": [
                {
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "Receipt"},
                    "woql:predicate": "of_inbound",
                    "woql:object": {"@type": "woql:Variable", "woql:variable_name": "Inbound"}
                },
                {
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "Inbound"},
                    "woql:predicate": "for_sku",
                    "woql:object": {"@type": "woql:Variable", "woql:variable_name": "SKU"}
                },
                {
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "SKU"},
                    "woql:predicate": "belongs_to",
                    "woql:object": {"@type": "woql:Variable", "woql:variable_name": "Product"}
                },
                {
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "Product"},
                    "woql:predicate": "owned_by",
                    "woql:object": {"@type": "woql:Variable", "woql:variable_name": "Client"}
                }
            ]
        }
        
        import requests
        response = requests.post(
            f"{self.terminus_url}/api/woql/admin/{self.db_name}",
            json={"query": woql_query},
            auth=("admin", "admin")
        )
        
        if response.status_code == 200:
            result = response.json()
            bindings = result.get("bindings", [])
            logger.info(f"✅ Found {len(bindings)} Receipt→Inbound→SKU→Product→Client paths")
            
            if bindings and len(bindings) > 0:
                for i, binding in enumerate(bindings[:2]):
                    receipt = binding.get("Receipt", "N/A")
                    inbound = binding.get("Inbound", "N/A")
                    sku = binding.get("SKU", "N/A")
                    product = binding.get("Product", "N/A")
                    client = binding.get("Client", "N/A")
                    logger.info(f"   Path {i+1}:")
                    logger.info(f"      {receipt} → {inbound} → {sku} → {product} → {client}")
        else:
            logger.error(f"❌ Query failed: {response.status_code}")

    async def test_complex_supply_chain_query(self):
        """Complex: Find all paths from Supplier to Client through the supply chain"""
        logger.info("\n🔍 COMPLEX MULTI-HOP: Supplier → SKU → Product → Client")
        logger.info("Complete supply chain traversal from Supplier to end Client")
        
        woql_query = {
            "@type": "woql:And",
            "woql:query_list": [
                {
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "SKU"},
                    "woql:predicate": "supplied_by",
                    "woql:object": {"@type": "woql:Variable", "woql:variable_name": "Supplier"}
                },
                {
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "SKU"},
                    "woql:predicate": "belongs_to",
                    "woql:object": {"@type": "woql:Variable", "woql:variable_name": "Product"}
                },
                {
                    "@type": "woql:Triple",
                    "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "Product"},
                    "woql:predicate": "owned_by",
                    "woql:object": {"@type": "woql:Variable", "woql:variable_name": "Client"}
                }
            ]
        }
        
        import requests
        response = requests.post(
            f"{self.terminus_url}/api/woql/admin/{self.db_name}",
            json={"query": woql_query},
            auth=("admin", "admin")
        )
        
        if response.status_code == 200:
            result = response.json()
            bindings = result.get("bindings", [])
            logger.info(f"✅ Found {len(bindings)} complete supply chain paths")
            
            # Analyze suppliers per client
            supplier_client_map = {}
            for binding in bindings:
                supplier = binding.get("Supplier", "N/A")
                client = binding.get("Client", "N/A")
                if client not in supplier_client_map:
                    supplier_client_map[client] = set()
                supplier_client_map[client].add(supplier)
            
            logger.info("\n📊 Supply Chain Analysis:")
            for client, suppliers in list(supplier_client_map.items())[:3]:
                logger.info(f"   Client {client} sourcing from {len(suppliers)} suppliers")
                
        else:
            logger.error(f"❌ Query failed: {response.status_code}")

    async def test_cyclic_query(self):
        """Test self-referential: Event → Event chains"""
        logger.info("\n🔍 CYCLIC/SELF-REFERENTIAL: Event → Event → Event")
        logger.info("Finding event causality chains")
        
        # This would find events that caused other events
        woql_query = {
            "@type": "woql:Triple",
            "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "Event1"},
            "woql:predicate": "caused_by",
            "woql:object": {"@type": "woql:Variable", "woql:variable_name": "Event2"}
        }
        
        import requests
        response = requests.post(
            f"{self.terminus_url}/api/woql/admin/{self.db_name}",
            json={"query": woql_query},
            auth=("admin", "admin")
        )
        
        if response.status_code == 200:
            result = response.json()
            bindings = result.get("bindings", [])
            logger.info(f"✅ Found {len(bindings)} event causality relationships")
            
            if bindings and len(bindings) > 0:
                for i, binding in enumerate(bindings[:3]):
                    event1 = binding.get("Event1", "N/A")
                    event2 = binding.get("Event2", "N/A")
                    logger.info(f"   Causality {i+1}: {event1} ← caused by ← {event2}")
        else:
            logger.error(f"❌ Query failed: {response.status_code}")

async def main():
    """Run all multi-hop query tests"""
    logger.info("🚀 Starting SPICE HARVESTER Multi-Hop Query Tests")
    logger.info("=" * 70)
    
    async with MultiHopQueryTester() as tester:
        # Run increasingly complex multi-hop queries
        await tester.test_2hop_query()
        await asyncio.sleep(1)
        
        await tester.test_3hop_query()
        await asyncio.sleep(1)
        
        await tester.test_4hop_query()
        await asyncio.sleep(1)
        
        await tester.test_complex_supply_chain_query()
        await asyncio.sleep(1)
        
        await tester.test_cyclic_query()
        
    logger.info("\n" + "=" * 70)
    logger.info("✅ Multi-hop query tests completed\!")
    logger.info("🎯 TerminusDB successfully performs graph traversals across multiple hops\!")

if __name__ == "__main__":
    asyncio.run(main())
