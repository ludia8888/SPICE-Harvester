#!/usr/bin/env python3
"""
A/B Test: Graph Federation vs Direct Elasticsearch
THINK ULTRA³ - Performance and Correctness Comparison

This script compares the performance and results of:
1. Direct Elasticsearch queries
2. Graph federation queries (TerminusDB + ES)
"""

import asyncio
import aiohttp
import json
import time
import logging
from typing import Dict, Any, List
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ABTestRunner:
    """Runs A/B tests between ES and Graph Federation"""
    
    def __init__(self):
        self.es_url = "http://localhost:9201"
        self.bff_url = "http://localhost:8002"
        self.db_name = "ab_test_db"
        self.index_name = f"instances_{self.db_name.replace('-', '_')}"
        self.results = {
            "elasticsearch": {
                "queries": [],
                "total_time": 0,
                "avg_time": 0,
                "results_count": 0
            },
            "graph_federation": {
                "queries": [],
                "total_time": 0,
                "avg_time": 0,
                "results_count": 0
            }
        }
    
    async def setup_test_data(self):
        """Create test data in Elasticsearch"""
        logger.info("📊 Setting up test data...")
        
        async with aiohttp.ClientSession() as session:
            # Create index
            mapping = {
                "mappings": {
                    "properties": {
                        "@type": {"type": "keyword"},
                        "@id": {"type": "keyword"},
                        "instance_id": {"type": "keyword"},
                        "class_id": {"type": "keyword"},
                        "client_id": {"type": "keyword"},
                        "product_id": {"type": "keyword"},
                        "order_id": {"type": "keyword"},
                        "name": {"type": "text"},
                        "owned_by": {"type": "keyword"},
                        "ordered_by": {"type": "keyword"},
                        "products": {"type": "keyword"},
                        "created_at": {"type": "date"},
                        "amount": {"type": "float"}
                    }
                }
            }
            
            # Delete existing index if exists
            await session.delete(f"{self.es_url}/{self.index_name}")
            
            # Create new index
            async with session.put(f"{self.es_url}/{self.index_name}", json=mapping) as resp:
                if resp.status in [200, 201]:
                    logger.info("✅ Created index")
            
            # Create test documents
            test_data = []
            
            # Create 10 clients
            for i in range(1, 11):
                client = {
                    "@type": "Client",
                    "@id": f"CL-{i:03d}",
                    "instance_id": f"CL-{i:03d}",
                    "class_id": "Client",
                    "client_id": f"CL-{i:03d}",
                    "name": f"Client {i}",
                    "created_at": datetime.now().isoformat()
                }
                test_data.append(client)
            
            # Create 50 products owned by different clients
            for i in range(1, 51):
                owner_id = f"CL-{((i-1) % 10) + 1:03d}"
                product = {
                    "@type": "Product",
                    "@id": f"PROD-{i:03d}",
                    "instance_id": f"PROD-{i:03d}",
                    "class_id": "Product",
                    "product_id": f"PROD-{i:03d}",
                    "name": f"Product {i}",
                    "owned_by": owner_id,
                    "created_at": datetime.now().isoformat()
                }
                test_data.append(product)
            
            # Create 100 orders
            for i in range(1, 101):
                client_id = f"CL-{((i-1) % 10) + 1:03d}"
                # Each order has 1-3 products
                num_products = (i % 3) + 1
                product_ids = [f"PROD-{((i + j) % 50) + 1:03d}" for j in range(num_products)]
                
                order = {
                    "@type": "Order",
                    "@id": f"ORD-{i:03d}",
                    "instance_id": f"ORD-{i:03d}",
                    "class_id": "Order",
                    "order_id": f"ORD-{i:03d}",
                    "ordered_by": client_id,
                    "products": product_ids,
                    "amount": 100.0 * num_products,
                    "created_at": datetime.now().isoformat()
                }
                test_data.append(order)
            
            # Bulk insert documents
            for doc in test_data:
                doc_id = doc["@id"]
                async with session.post(
                    f"{self.es_url}/{self.index_name}/_doc/{doc_id}",
                    json=doc
                ) as resp:
                    if resp.status not in [200, 201]:
                        logger.error(f"Failed to create {doc_id}")
            
            logger.info(f"✅ Created {len(test_data)} test documents")
            
            # Refresh index for immediate search
            await session.post(f"{self.es_url}/{self.index_name}/_refresh")
    
    async def test_direct_es_query(self, query_name: str, es_query: Dict[str, Any]) -> Dict[str, Any]:
        """Run a direct Elasticsearch query"""
        start_time = time.time()
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.es_url}/{self.index_name}/_search",
                json=es_query
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    elapsed = time.time() - start_time
                    
                    hits = result.get("hits", {}).get("hits", [])
                    
                    return {
                        "name": query_name,
                        "time": elapsed,
                        "count": len(hits),
                        "success": True,
                        "sample": hits[:3] if hits else []
                    }
                else:
                    return {
                        "name": query_name,
                        "time": 0,
                        "count": 0,
                        "success": False,
                        "error": await resp.text()
                    }
    
    async def test_graph_query(self, query_name: str, graph_query: Dict[str, Any]) -> Dict[str, Any]:
        """Run a graph federation query through BFF"""
        start_time = time.time()
        
        async with aiohttp.ClientSession() as session:
            # First check if endpoint exists
            endpoint = f"{self.bff_url}/api/v1/graph-query/{self.db_name}"
            
            # Try simple query first
            if "class_name" in graph_query:
                endpoint = f"{self.bff_url}/api/v1/graph-query/{self.db_name}/simple"
            
            async with session.post(endpoint, json=graph_query) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    elapsed = time.time() - start_time
                    
                    # Extract count based on response format
                    if "nodes" in result:
                        count = result.get("count", len(result.get("nodes", [])))
                    elif "documents" in result:
                        count = len(result.get("documents", []))
                    else:
                        count = result.get("count", 0)
                    
                    return {
                        "name": query_name,
                        "time": elapsed,
                        "count": count,
                        "success": True,
                        "sample": result
                    }
                else:
                    return {
                        "name": query_name,
                        "time": 0,
                        "count": 0,
                        "success": False,
                        "error": await resp.text()
                    }
    
    async def run_comparison_tests(self):
        """Run comparison tests"""
        logger.info("\n🔬 Running A/B Tests...")
        
        test_cases = [
            {
                "name": "Simple: All Clients",
                "es_query": {
                    "query": {"term": {"class_id": "Client"}},
                    "size": 100
                },
                "graph_query": {
                    "class_name": "Client",
                    "limit": 100
                }
            },
            {
                "name": "Filtered: Products owned by CL-001",
                "es_query": {
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"class_id": "Product"}},
                                {"term": {"owned_by": "CL-001"}}
                            ]
                        }
                    },
                    "size": 100
                },
                "graph_query": {
                    "class_name": "Product",
                    "filters": {"owned_by": "CL-001"},
                    "limit": 100
                }
            },
            {
                "name": "Join: Orders with client info",
                "es_query": {
                    "query": {"term": {"class_id": "Order"}},
                    "size": 10,
                    "_source": ["order_id", "ordered_by", "amount"]
                },
                "graph_query": {
                    "start_class": "Order",
                    "hops": [{"predicate": "ordered_by", "target_class": "Client"}],
                    "limit": 10
                }
            }
        ]
        
        for test_case in test_cases:
            logger.info(f"\n📌 Test: {test_case['name']}")
            
            # Run ES query
            es_result = await self.test_direct_es_query(
                test_case["name"],
                test_case["es_query"]
            )
            
            if es_result["success"]:
                logger.info(f"   ES: {es_result['count']} results in {es_result['time']:.3f}s")
                self.results["elasticsearch"]["queries"].append(es_result)
                self.results["elasticsearch"]["total_time"] += es_result["time"]
                self.results["elasticsearch"]["results_count"] += es_result["count"]
            else:
                logger.error(f"   ES failed: {es_result.get('error', 'Unknown error')[:100]}")
            
            # Run Graph query (if BFF is running)
            try:
                graph_result = await self.test_graph_query(
                    test_case["name"],
                    test_case["graph_query"]
                )
                
                if graph_result["success"]:
                    logger.info(f"   Graph: {graph_result['count']} results in {graph_result['time']:.3f}s")
                    self.results["graph_federation"]["queries"].append(graph_result)
                    self.results["graph_federation"]["total_time"] += graph_result["time"]
                    self.results["graph_federation"]["results_count"] += graph_result["count"]
                else:
                    logger.warning(f"   Graph failed: {graph_result.get('error', 'Unknown error')[:100]}")
            except Exception as e:
                logger.warning(f"   Graph query skipped (BFF may not be running): {e}")
    
    async def run_performance_test(self):
        """Run performance tests with larger datasets"""
        logger.info("\n⚡ Running Performance Tests...")
        
        # Test with aggregations
        agg_query_es = {
            "size": 0,
            "aggs": {
                "by_client": {
                    "terms": {
                        "field": "ordered_by",
                        "size": 10
                    },
                    "aggs": {
                        "total_amount": {
                            "sum": {"field": "amount"}
                        }
                    }
                }
            }
        }
        
        start_time = time.time()
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.es_url}/{self.index_name}/_search",
                json=agg_query_es
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    es_time = time.time() - start_time
                    buckets = result.get("aggregations", {}).get("by_client", {}).get("buckets", [])
                    logger.info(f"   ES Aggregation: {len(buckets)} groups in {es_time:.3f}s")
    
    def generate_report(self):
        """Generate comparison report"""
        logger.info("\n" + "=" * 60)
        logger.info("📊 A/B TEST REPORT")
        logger.info("=" * 60)
        
        # Calculate averages
        for system in ["elasticsearch", "graph_federation"]:
            queries = self.results[system]["queries"]
            if queries:
                self.results[system]["avg_time"] = self.results[system]["total_time"] / len(queries)
        
        # Comparison table
        logger.info("\n📈 Performance Comparison:")
        logger.info(f"{'Metric':<30} {'Elasticsearch':<20} {'Graph Federation':<20}")
        logger.info("-" * 70)
        
        es_queries = len(self.results["elasticsearch"]["queries"])
        graph_queries = len(self.results["graph_federation"]["queries"])
        logger.info(f"{'Queries Executed':<30} {es_queries:<20} {graph_queries:<20}")
        
        es_avg = self.results["elasticsearch"]["avg_time"]
        graph_avg = self.results["graph_federation"]["avg_time"]
        logger.info(f"{'Avg Query Time (s)':<30} {es_avg:<20.3f} {graph_avg if graph_avg else 'N/A':<20}")
        
        es_total = self.results["elasticsearch"]["total_time"]
        graph_total = self.results["graph_federation"]["total_time"]
        logger.info(f"{'Total Time (s)':<30} {es_total:<20.3f} {graph_total:<20.3f}")
        
        es_results = self.results["elasticsearch"]["results_count"]
        graph_results = self.results["graph_federation"]["results_count"]
        logger.info(f"{'Total Results':<30} {es_results:<20} {graph_results:<20}")
        
        # Performance analysis
        if es_avg and graph_avg:
            if es_avg < graph_avg:
                ratio = graph_avg / es_avg
                logger.info(f"\n⚡ Elasticsearch is {ratio:.2f}x faster")
            else:
                ratio = es_avg / graph_avg
                logger.info(f"\n⚡ Graph Federation is {ratio:.2f}x faster")
        
        # Recommendations
        logger.info("\n💡 Recommendations:")
        if graph_queries == 0:
            logger.info("   ⚠️ Graph federation not tested (BFF may not be running)")
            logger.info("   → Start BFF service to enable graph queries")
        elif es_avg and graph_avg:
            if graph_avg > es_avg * 2:
                logger.info("   → Use direct ES for simple queries")
                logger.info("   → Use graph federation only for complex relationships")
            elif graph_avg < es_avg:
                logger.info("   → Graph federation is performing well")
                logger.info("   → Consider using it for all queries")
            else:
                logger.info("   → Performance is comparable")
                logger.info("   → Choose based on query complexity")


async def main():
    """Main execution"""
    logger.info("🔥 THINK ULTRA³: A/B Test - Graph vs Elasticsearch")
    logger.info("=" * 60)
    
    runner = ABTestRunner()
    
    # Setup test data
    await runner.setup_test_data()
    
    # Wait for indexing
    await asyncio.sleep(2)
    
    # Run comparison tests
    await runner.run_comparison_tests()
    
    # Run performance tests
    await runner.run_performance_test()
    
    # Generate report
    runner.generate_report()
    
    logger.info("\n✅ A/B testing complete!")


if __name__ == "__main__":
    asyncio.run(main())