#!/usr/bin/env python3
"""
🔥 THINK ULTRA! BFF-OMS Integration Test
Complete end-to-end test of the BFF (Backend for Frontend) and OMS (Ontology Management Service) integration
"""

import asyncio
import httpx
import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, Any, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BFFOMSIntegrationTest:
    """Test suite for BFF-OMS integration"""
    
    def __init__(self):
        self.bff_url = os.getenv("BFF_URL", "http://localhost:8002")
        self.oms_url = os.getenv("OMS_URL", "http://localhost:8000") 
        self.terminus_url = os.getenv("TERMINUS_SERVER_URL", "http://localhost:6363")
        self.test_db = f"test_bff_oms_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.errors = []
        self.successes = []
    
    def add_error(self, test: str, error: str):
        self.errors.append({"test": test, "error": error})
        logger.error(f"❌ {test}: {error}")
    
    def add_success(self, test: str, message: str):
        self.successes.append({"test": test, "message": message})
        logger.info(f"✅ {test}: {message}")
    
    async def test_health_check(self):
        """Test 1: Health check endpoints"""
        logger.info("\n🔬 Test 1: Health Check Endpoints")
        
        async with httpx.AsyncClient() as client:
            # BFF health check
            try:
                resp = await client.get(f"{self.bff_url}/health")
                if resp.status_code == 200:
                    self.add_success("BFF Health", f"BFF is healthy: {resp.json()}")
                else:
                    self.add_error("BFF Health", f"Status {resp.status_code}")
            except Exception as e:
                self.add_error("BFF Health", str(e))
            
            # OMS health check
            try:
                resp = await client.get(f"{self.oms_url}/health")
                if resp.status_code == 200:
                    self.add_success("OMS Health", f"OMS is healthy: {resp.json()}")
                else:
                    self.add_error("OMS Health", f"Status {resp.status_code}")
            except Exception as e:
                self.add_error("OMS Health", str(e))
    
    async def test_database_operations(self):
        """Test 2: Database CRUD operations through BFF"""
        logger.info("\n🔬 Test 2: Database Operations (BFF -> OMS -> TerminusDB)")
        
        async with httpx.AsyncClient() as client:
            # Create database
            try:
                resp = await client.post(
                    f"{self.bff_url}/api/v1/databases",
                    json={
                        "name": self.test_db,
                        "description": "Integration test database"
                    }
                )
                if resp.status_code in [200, 201]:
                    self.add_success("Create Database", f"Created {self.test_db}")
                else:
                    self.add_error("Create Database", f"Status {resp.status_code}: {resp.text}")
                    return  # Can't continue without database
            except Exception as e:
                self.add_error("Create Database", str(e))
                return
            
            # List databases
            try:
                resp = await client.get(f"{self.bff_url}/api/v1/databases")
                if resp.status_code == 200:
                    response_data = resp.json()
                    dbs = response_data.get("data", {}).get("databases", [])
                    if any(db["name"] == self.test_db for db in dbs):
                        self.add_success("List Databases", f"Found {self.test_db} in list")
                    else:
                        self.add_error("List Databases", "Created database not in list")
                else:
                    self.add_error("List Databases", f"Status {resp.status_code}")
            except Exception as e:
                self.add_error("List Databases", str(e))
    
    async def test_ontology_operations(self):
        """Test 3: Ontology CRUD operations"""
        logger.info("\n🔬 Test 3: Ontology Operations")
        
        async with httpx.AsyncClient() as client:
            # Create Person class
            person_class = {
                "id": "Person",
                "label": "Person",
                "description": "A human being",
                "properties": [
                    {
                        "name": "name",
                        "type": "string",
                        "required": True,
                        "label": "Name"
                    },
                    {
                        "name": "age",
                        "type": "integer",
                        "required": False,
                        "label": "Age",
                        "constraints": {"min": 0, "max": 150}
                    }
                ]
            }
            
            try:
                resp = await client.post(
                    f"{self.bff_url}/api/v1/database/{self.test_db}/ontology",
                    json=person_class
                )
                if resp.status_code in [200, 201]:
                    self.add_success("Create Ontology", "Created Person class")
                else:
                    self.add_error("Create Ontology", f"Status {resp.status_code}: {resp.text}")
            except Exception as e:
                self.add_error("Create Ontology", str(e))
            
            # Get ontology
            try:
                resp = await client.get(f"{self.bff_url}/api/v1/database/{self.test_db}/ontology/Person")
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("id") == "Person":
                        self.add_success("Get Ontology", "Retrieved Person class")
                    else:
                        self.add_error("Get Ontology", "Invalid response data")
                else:
                    self.add_error("Get Ontology", f"Status {resp.status_code}")
            except Exception as e:
                self.add_error("Get Ontology", str(e))
    
    async def test_advanced_relationships(self):
        """Test 4: Advanced relationship management"""
        logger.info("\n🔬 Test 4: Advanced Relationship Management")
        
        async with httpx.AsyncClient() as client:
            # Create Company class
            company_class = {
                "id": "Company",
                "label": "Company",
                "properties": [
                    {"name": "name", "type": "string", "required": True, "label": "Company Name"}
                ]
            }
            
            try:
                resp = await client.post(
                    f"{self.bff_url}/api/v1/database/{self.test_db}/ontology",
                    json=company_class
                )
                if resp.status_code not in [200, 201]:
                    self.add_error("Create Company", f"Status {resp.status_code}")
                    return
            except Exception as e:
                self.add_error("Create Company", str(e))
                return
            
            # Create Employee inheriting from Person with relationships
            employee_class = {
                "id": "Employee",
                "label": "Employee",
                "parent_class": "Person",
                "properties": [
                    {"name": "employee_id", "type": "string", "required": True, "label": "Employee ID"}
                ],
                "relationships": [
                    {
                        "predicate": "works_for",
                        "target": "Company",
                        "label": "Works For",
                        "cardinality": "n:1"
                    }
                ]
            }
            
            try:
                resp = await client.post(
                    f"{self.bff_url}/api/v1/database/{self.test_db}/ontology",
                    json=employee_class
                )
                if resp.status_code in [200, 201]:
                    self.add_success("Create Employee with Relationships", "Created Employee class")
                else:
                    self.add_error("Create Employee", f"Status {resp.status_code}: {resp.text}")
            except Exception as e:
                self.add_error("Create Employee", str(e))
            
            # Verify inheritance
            try:
                resp = await client.get(f"{self.bff_url}/api/v1/database/{self.test_db}/ontology/Employee")
                if resp.status_code == 200:
                    data = resp.json()
                    props = {p["name"] for p in data.get("properties", [])}
                    rels = {r["predicate"] for r in data.get("relationships", [])}
                    
                    if "name" in props and "age" in props:  # Inherited from Person
                        self.add_success("Inheritance", "Properties correctly inherited")
                    else:
                        self.add_error("Inheritance", f"Missing inherited properties: {props}")
                    
                    if "works_for" in rels:
                        self.add_success("Relationships", "Relationships correctly defined")
                    else:
                        self.add_error("Relationships", f"Missing relationships: {rels}")
                else:
                    self.add_error("Verify Employee", f"Status {resp.status_code}")
            except Exception as e:
                self.add_error("Verify Employee", str(e))
    
    async def test_property_conversion(self):
        """Test 5: Property to relationship conversion"""
        logger.info("\n🔬 Test 5: Property to Relationship Conversion")
        
        async with httpx.AsyncClient() as client:
            # Create class with link properties
            # 🔥 ULTRA FIX! Use 'target' instead of 'linkTarget' - BFF will transform it
            team_class = {
                "id": "Team",
                "label": "Team",
                "properties": [
                    {"name": "name", "type": "string", "required": True, "label": "Team Name"},
                    {
                        "name": "leader",
                        "type": "link",
                        "target": "Employee",  # Changed from linkTarget to target
                        "required": True,
                        "label": "Team Leader"
                    },
                    {
                        "name": "members",
                        "type": "array",
                        "items": {"type": "link", "target": "Employee"},  # Changed from linkTarget to target
                        "label": "Members"
                    }
                ]
            }
            
            try:
                resp = await client.post(
                    f"{self.bff_url}/api/v1/database/{self.test_db}/ontology",
                    json=team_class
                )
                if resp.status_code in [200, 201]:
                    # 🔥 ULTRA VERIFY! Check if Team was actually created
                    data = resp.json()
                    if data.get("id") == "Team":
                        self.add_success("Create Team", "Created Team class with link properties")
                    else:
                        self.add_error("Create Team", f"Unexpected response: {data}")
                else:
                    self.add_error("Create Team", f"Status {resp.status_code}: {resp.text}")
            except Exception as e:
                self.add_error("Create Team", str(e))
            
            # Verify conversion
            try:
                resp = await client.get(f"{self.bff_url}/api/v1/database/{self.test_db}/ontology/Team")
                if resp.status_code == 200:
                    data = resp.json()
                    logger.info(f"🔥 ULTRA DEBUG! Team GET response: {json.dumps(data, ensure_ascii=False, indent=2)}")
                    rels = {r["predicate"] for r in data.get("relationships", [])}
                    props = {p["name"] for p in data.get("properties", [])}
                    logger.info(f"🔍 Found relationships: {rels}")
                    logger.info(f"🔍 Found properties: {props}")
                    
                    expected_rels = {"leader", "members"}
                    missing_rels = expected_rels - rels
                    
                    if not missing_rels:
                        self.add_success("Link Conversion", "Link properties converted to relationships")
                    else:
                        self.add_error("Link Conversion", f"Expected relationships not found: {missing_rels}")
                        logger.error(f"🔥 Missing relationships: {missing_rels}")
                        logger.error(f"🔥 Available relationships: {rels}")
                else:
                    self.add_error("Verify Team", f"Status {resp.status_code}")
            except Exception as e:
                self.add_error("Verify Team", str(e))
    
    async def test_cleanup(self):
        """Test cleanup: Delete test database"""
        logger.info("\n🧹 Cleanup: Deleting test database")
        
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.delete(f"{self.bff_url}/api/v1/databases/{self.test_db}")
                if resp.status_code in [200, 204]:
                    self.add_success("Cleanup", f"Deleted {self.test_db}")
                else:
                    self.add_error("Cleanup", f"Status {resp.status_code}")
            except Exception as e:
                self.add_error("Cleanup", str(e))
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate test report"""
        total = len(self.successes) + len(self.errors)
        return {
            "total_tests": total,
            "passed": len(self.successes),
            "failed": len(self.errors),
            "success_rate": f"{(len(self.successes) / total * 100):.1f}%" if total > 0 else "0%",
            "successes": self.successes,
            "errors": self.errors
        }
    
    async def run_all_tests(self):
        """Run all integration tests"""
        logger.info("🚀 Starting BFF-OMS Integration Tests")
        logger.info("=" * 80)
        logger.info(f"BFF URL: {self.bff_url}")
        logger.info(f"OMS URL: {self.oms_url}")
        logger.info(f"TerminusDB URL: {self.terminus_url}")
        logger.info(f"Test Database: {self.test_db}")
        logger.info("=" * 80)
        
        # Run tests in sequence
        await self.test_health_check()
        await self.test_database_operations()
        await self.test_ontology_operations()
        await self.test_advanced_relationships()
        await self.test_property_conversion()
        await self.test_cleanup()
        
        # Generate report
        report = self.generate_report()
        
        logger.info("\n" + "=" * 80)
        logger.info("📊 TEST REPORT")
        logger.info("=" * 80)
        logger.info(f"Total Tests: {report['total_tests']}")
        logger.info(f"Passed: {report['passed']}")
        logger.info(f"Failed: {report['failed']}")
        logger.info(f"Success Rate: {report['success_rate']}")
        
        if report['errors']:
            logger.error("\n❌ FAILED TESTS:")
            for error in report['errors']:
                logger.error(f"  - {error['test']}: {error['error']}")
        
        if report['failed'] > 0:
            logger.error(f"\n🚨 {report['failed']} tests failed! Immediate action required!")
        else:
            logger.info("\n🎉 All tests passed successfully!")
        
        return report


async def main():
    """Main test runner"""
    test_suite = BFFOMSIntegrationTest()
    report = await test_suite.run_all_tests()
    
    # Exit with appropriate code
    if report['failed'] > 0:
        exit(1)
    else:
        exit(0)


if __name__ == "__main__":
    asyncio.run(main())