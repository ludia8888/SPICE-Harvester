#!/usr/bin/env python3
"""
Complete Graph Federation Test Script
Tests all aspects of the Graph Federation implementation
"""

import asyncio
import aiohttp
import json
import sys
from datetime import datetime
from typing import Dict, List, Any

# Color codes for output
RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'
BOLD = '\033[1m'

class GraphFederationTester:
    def __init__(self):
        self.oms_url = "http://localhost:8000"
        self.bff_url = "http://localhost:8002"
        self.terminus_url = "http://localhost:6363"
        self.es_url = "http://localhost:9200"
        self.test_db = f"graph_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.results = []
        
    def print_header(self, text: str):
        print(f"\n{BOLD}{BLUE}{'='*60}{RESET}")
        print(f"{BOLD}{BLUE}{text.center(60)}{RESET}")
        print(f"{BOLD}{BLUE}{'='*60}{RESET}\n")
        
    def print_test(self, name: str, passed: bool, details: str = ""):
        status = f"{GREEN}✅ PASSED{RESET}" if passed else f"{RED}❌ FAILED{RESET}"
        print(f"  {name}: {status}")
        if details:
            print(f"    {YELLOW}→ {details}{RESET}")
        self.results.append({"test": name, "passed": passed, "details": details})
        
    async def test_services_health(self, session: aiohttp.ClientSession) -> bool:
        """Test if all required services are running"""
        self.print_header("1. SERVICE HEALTH CHECK")
        all_healthy = True
        
        # Test TerminusDB
        try:
            async with session.get(f"{self.terminus_url}/api/info") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    version = data.get('terminusdb', {}).get('version', 'unknown')
                    self.print_test("TerminusDB", True, f"Version {version}")
                else:
                    self.print_test("TerminusDB", False, f"Status {resp.status}")
                    all_healthy = False
        except Exception as e:
            self.print_test("TerminusDB", False, str(e))
            all_healthy = False
            
        # Test Elasticsearch (with authentication)
        try:
            auth = aiohttp.BasicAuth('elastic', 'spice123!')
            async with session.get(self.es_url, auth=auth) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    version = data.get('version', {}).get('number', 'unknown')
                    self.print_test("Elasticsearch", True, f"Version {version}")
                else:
                    self.print_test("Elasticsearch", False, f"Status {resp.status}")
                    all_healthy = False
        except Exception as e:
            self.print_test("Elasticsearch", False, str(e))
            all_healthy = False
            
        # Test OMS
        try:
            async with session.get(f"{self.oms_url}/health") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    status = data.get('data', {}).get('status', 'unknown')
                    if status == 'healthy':
                        self.print_test("OMS Service", True, "Healthy")
                    else:
                        self.print_test("OMS Service", False, f"Status: {status}")
                        all_healthy = False
                else:
                    self.print_test("OMS Service", False, f"Status {resp.status}")
                    all_healthy = False
        except Exception as e:
            self.print_test("OMS Service", False, str(e))
            all_healthy = False
            
        # Test BFF
        try:
            async with session.get(f"{self.bff_url}/health") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    status = data.get('data', {}).get('status', 'healthy')
                    self.print_test("BFF Service", True, status.capitalize())
                else:
                    self.print_test("BFF Service", False, f"Status {resp.status}")
                    all_healthy = False
        except Exception as e:
            self.print_test("BFF Service", False, str(e))
            all_healthy = False
            
        return all_healthy
        
    async def create_test_database(self, session: aiohttp.ClientSession) -> bool:
        """Create a test database with sample ontology"""
        self.print_header("2. DATABASE & ONTOLOGY SETUP")
        
        # Create database
        try:
            async with session.post(
                f"{self.oms_url}/api/v1/database/create",
                json={"name": self.test_db, "description": "Graph Federation Test DB"}
            ) as resp:
                if resp.status in [200, 201, 202]:
                    self.print_test("Create Database", True, self.test_db)
                    await asyncio.sleep(5)  # Wait for Event Sourcing to process
                else:
                    text = await resp.text()
                    self.print_test("Create Database", False, f"Status {resp.status}: {text[:100]}")
                    return False
        except Exception as e:
            self.print_test("Create Database", False, str(e))
            return False
            
        # Create Client ontology FIRST (Product depends on it)
        client_ontology = {
            "id": "Client",
            "label": "Client",
            "description": "Client class for testing",
            "properties": [
                {"name": "client_id", "type": "string", "label": "Client ID", "required": True},
                {"name": "name", "type": "string", "label": "Name", "required": True},
                {"name": "country", "type": "string", "label": "Country"}
            ]
        }
        
        try:
            async with session.post(
                f"{self.oms_url}/api/v1/ontology/{self.test_db}/create",
                json=client_ontology
            ) as resp:
                if resp.status in [200, 201, 202]:
                    self.print_test("Create Client Ontology", True, "Success" if resp.status != 202 else "Accepted (Event Sourcing)")
                    if resp.status == 202:
                        await asyncio.sleep(2)  # Wait for Event Sourcing
                else:
                    text = await resp.text()
                    self.print_test("Create Client Ontology", False, f"Status {resp.status}")
                    return False
        except Exception as e:
            self.print_test("Create Client Ontology", False, str(e))
            return False
            
        # Create Product ontology (depends on Client)
        product_ontology = {
            "id": "Product",
            "label": "Product",
            "description": "Product class for testing",
            "properties": [
                {"name": "product_id", "type": "string", "label": "Product ID", "required": True},
                {"name": "name", "type": "string", "label": "Name", "required": True},
                {"name": "category", "type": "string", "label": "Category"},
                {"name": "price", "type": "decimal", "label": "Price"}
            ],
            "relationships": [
                {
                    "predicate": "owned_by",
                    "label": "Owned By",
                    "target": "Client",
                    "cardinality": "n:1"
                }
            ]
        }
        
        try:
            async with session.post(
                f"{self.oms_url}/api/v1/ontology/{self.test_db}/create",
                json=product_ontology
            ) as resp:
                if resp.status in [200, 201, 202]:
                    self.print_test("Create Product Ontology", True, "With relationships" if resp.status != 202 else "Accepted (Event Sourcing)")
                    # Wait for Event Sourcing processing if 202
                    if resp.status == 202:
                        await asyncio.sleep(2)
                    return True
                else:
                    text = await resp.text()
                    self.print_test("Create Product Ontology", False, f"Status {resp.status}")
                    return False
        except Exception as e:
            self.print_test("Create Product Ontology", False, str(e))
            return False
            
    async def create_test_instances(self, session: aiohttp.ClientSession) -> bool:
        """Create test instances via Event Sourcing"""
        self.print_header("3. CREATE TEST INSTANCES")
        
        # Create Client instance
        client_data = {
            "client_id": "CL-TEST-001",
            "name": "Test Client Corp",
            "country": "USA"
        }
        
        try:
            async with session.post(
                f"{self.oms_url}/api/v1/instances/{self.test_db}/async/Client/create",
                json={"data": client_data}
            ) as resp:
                if resp.status == 202:
                    result = await resp.json()
                    command_id = result.get('command_id', 'unknown')
                    self.print_test("Create Client Instance", True, f"Command: {command_id[:8]}...")
                else:
                    text = await resp.text()
                    self.print_test("Create Client Instance", False, f"Status {resp.status}")
                    return False
        except Exception as e:
            self.print_test("Create Client Instance", False, str(e))
            return False
            
        # Create Product instances
        products = [
            {
                "product_id": "PROD-TEST-001",
                "name": "Test Product 1",
                "category": "Electronics",
                "price": 99.99,
                "owned_by": "CL-TEST-001"
            },
            {
                "product_id": "PROD-TEST-002",
                "name": "Test Product 2",
                "category": "Electronics",
                "price": 149.99,
                "owned_by": "CL-TEST-001"
            }
        ]
        
        for product in products:
            try:
                async with session.post(
                    f"{self.oms_url}/api/v1/instances/{self.test_db}/async/Product/create",
                    json={"data": product}
                ) as resp:
                    if resp.status == 202:
                        result = await resp.json()
                        self.print_test(f"Create {product['product_id']}", True, "Accepted")
                    else:
                        text = await resp.text()
                        self.print_test(f"Create {product['product_id']}", False, f"Status {resp.status}")
            except Exception as e:
                self.print_test(f"Create {product['product_id']}", False, str(e))
                
        # Wait for Event Sourcing to process
        print(f"\n  {YELLOW}Waiting for Event Sourcing processing (5 seconds)...{RESET}")
        await asyncio.sleep(5)
        
        return True
        
    async def test_graph_federation_queries(self, session: aiohttp.ClientSession) -> bool:
        """Test Graph Federation query endpoints"""
        self.print_header("4. GRAPH FEDERATION QUERIES")
        
        # Test 1: Simple class query
        try:
            query = {
                "class_name": "Product",
                "limit": 10
            }
            
            async with session.post(
                f"{self.bff_url}/api/v1/graph-query/{self.test_db}/simple",
                json=query
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    nodes = result.get('data', {}).get('nodes', [])
                    self.print_test("Simple Query (Product)", True, f"Found {len(nodes)} nodes")
                    
                    # Check if nodes have ES data
                    if nodes:
                        has_data = any(node.get('data') for node in nodes)
                        self.print_test("ES Data Fetching", has_data, 
                                      "Nodes have ES data" if has_data else "No ES data")
                else:
                    text = await resp.text()
                    self.print_test("Simple Query", False, f"Status {resp.status}: {text[:100]}")
        except Exception as e:
            self.print_test("Simple Query", False, str(e))
            
        # Test 2: Multi-hop query
        try:
            multi_hop_query = {
                "start_class": "Product",
                "hops": [
                    {"predicate": "owned_by", "target_class": "Client"}
                ],
                "filters": {"category": "Electronics"},
                "limit": 10
            }
            
            async with session.post(
                f"{self.bff_url}/api/v1/graph-query/{self.test_db}",
                json=multi_hop_query
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    nodes = result.get('data', {}).get('nodes', [])
                    edges = result.get('data', {}).get('edges', [])
                    self.print_test("Multi-hop Query", True, 
                                  f"{len(nodes)} nodes, {len(edges)} edges")
                else:
                    text = await resp.text()
                    self.print_test("Multi-hop Query", False, f"Status {resp.status}")
        except Exception as e:
            self.print_test("Multi-hop Query", False, str(e))
            
        # Test 3: Path finding
        try:
            async with session.get(
                f"{self.bff_url}/api/v1/graph-query/{self.test_db}/paths",
                params={
                    "source_class": "Product",
                    "target_class": "Client",
                    "max_depth": 3
                }
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    paths = result.get('data', {}).get('paths', [])
                    self.print_test("Path Finding", True, f"Found {len(paths)} paths")
                else:
                    text = await resp.text()
                    self.print_test("Path Finding", False, f"Status {resp.status}")
        except Exception as e:
            self.print_test("Path Finding", False, str(e))
            
        return True
        
    async def test_schema_suggestion(self, session: aiohttp.ClientSession) -> bool:
        """Test Schema Suggestion endpoints"""
        self.print_header("5. SCHEMA SUGGESTION (ML)")
        
        # Test data for schema suggestion
        test_data = {
            "data": [
                ["John Doe", 25, "john@example.com", "2024-01-15"],
                ["Jane Smith", 30, "jane@example.com", "2024-02-20"],
                ["Bob Johnson", 28, "bob@example.com", "2024-03-10"]
            ],
            "column_names": ["name", "age", "email", "date_joined"]
        }
        
        try:
            async with session.post(
                f"{self.bff_url}/api/v1/ml/suggest-schema",
                json=test_data
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    schema = result.get('data', {}).get('suggested_schema', {})
                    properties = schema.get('properties', [])
                    self.print_test("Schema Suggestion", True, 
                                  f"Generated {len(properties)} properties")
                    
                    # Check property types
                    for prop in properties[:2]:
                        name = prop.get('name', 'unknown')
                        dtype = prop.get('type', 'unknown')
                        print(f"      • {name}: {dtype}")
                else:
                    text = await resp.text()
                    self.print_test("Schema Suggestion", False, f"Status {resp.status}")
        except Exception as e:
            self.print_test("Schema Suggestion", False, str(e))
            
        return True
        
    async def verify_integration(self, session: aiohttp.ClientSession) -> bool:
        """Verify TerminusDB and Elasticsearch integration"""
        self.print_header("6. INTEGRATION VERIFICATION")
        
        # Check TerminusDB has lightweight nodes
        try:
            # Use WOQL to query TerminusDB directly
            woql_query = {
                "query": f"SELECT * FROM Product LIMIT 5"
            }
            
            async with session.post(
                f"{self.oms_url}/api/v1/query/{self.test_db}",
                json=woql_query
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    data = result.get('data', [])
                    self.print_test("TerminusDB Nodes", len(data) > 0, 
                                  f"Found {len(data)} nodes in graph")
                else:
                    text = await resp.text()
                    self.print_test("TerminusDB Nodes", False, f"Query failed: {resp.status}")
        except Exception as e:
            self.print_test("TerminusDB Nodes", False, str(e))
            
        # Check Elasticsearch has document data
        try:
            es_index = f"{self.test_db}_instances"
            async with session.get(f"{self.es_url}/{es_index}/_count") as resp:
                if resp.status == 200:
                    result = await resp.json()
                    count = result.get('count', 0)
                    self.print_test("Elasticsearch Documents", count > 0, 
                                  f"Found {count} documents")
                else:
                    self.print_test("Elasticsearch Documents", False, 
                                  f"Index not found or empty")
        except Exception as e:
            self.print_test("Elasticsearch Documents", False, str(e))
            
        return True
        
    async def cleanup(self, session: aiohttp.ClientSession):
        """Clean up test database"""
        self.print_header("7. CLEANUP")
        
        try:
            async with session.delete(
                f"{self.oms_url}/api/v1/database/{self.test_db}"
            ) as resp:
                if resp.status in [200, 202, 204]:
                    self.print_test("Delete Test Database", True, self.test_db)
                else:
                    self.print_test("Delete Test Database", False, f"Status {resp.status}")
        except Exception as e:
            self.print_test("Delete Test Database", False, str(e))
            
    def print_summary(self):
        """Print test summary"""
        self.print_header("TEST SUMMARY")
        
        total = len(self.results)
        passed = sum(1 for r in self.results if r['passed'])
        failed = total - passed
        
        print(f"  Total Tests: {total}")
        print(f"  {GREEN}Passed: {passed}{RESET}")
        print(f"  {RED}Failed: {failed}{RESET}")
        print(f"  Success Rate: {passed/total*100:.1f}%")
        
        if failed > 0:
            print(f"\n  {RED}Failed Tests:{RESET}")
            for result in self.results:
                if not result['passed']:
                    print(f"    • {result['test']}")
                    if result['details']:
                        print(f"      {result['details']}")
                        
    async def run_tests(self):
        """Run all tests"""
        print(f"{BOLD}{GREEN}🚀 GRAPH FEDERATION COMPLETE TEST SUITE{RESET}")
        print(f"{YELLOW}Testing implementation described in GRAPH_FEDERATION_SUMMARY.md{RESET}")
        
        # Set longer timeout for Event Sourcing operations
        timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Run tests in sequence
            if not await self.test_services_health(session):
                print(f"\n{RED}⚠️  Some services are not healthy. Tests may fail.{RESET}")
                
            if await self.create_test_database(session):
                await self.create_test_instances(session)
                await self.test_graph_federation_queries(session)
                await self.test_schema_suggestion(session)
                await self.verify_integration(session)
                await self.cleanup(session)
            
            self.print_summary()

if __name__ == "__main__":
    tester = GraphFederationTester()
    asyncio.run(tester.run_tests())