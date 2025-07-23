#!/usr/bin/env python3
"""🔥 THINK ULTRA! Comprehensive test for advanced relationship management"""

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Any
from oms.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RelationshipTestSuite:
    """Test suite for advanced relationship features"""
    
    def __init__(self, terminus: AsyncTerminusService, db_name: str):
        self.terminus = terminus
        self.db_name = db_name
        self.errors = []
        self.successes = []
    
    def add_error(self, test_name: str, error: str):
        """Track errors immediately"""
        self.errors.append({"test": test_name, "error": error})
        logger.error(f"❌ {test_name}: {error}")
    
    def add_success(self, test_name: str, message: str):
        """Track successes"""
        self.successes.append({"test": test_name, "message": message})
        logger.info(f"✅ {test_name}: {message}")
    
    async def test_basic_relationships(self):
        """Test 1: Basic relationship cardinalities"""
        logger.info("\n🔬 Test 1: Basic Relationship Cardinalities")
        
        try:
            # Create base classes first
            person = {
                "id": "Person",
                "label": "Person",
                "properties": [
                    {"name": "name", "type": "string", "required": True, "label": "Name"},
                    {"name": "age", "type": "integer", "required": False, "label": "Age"}
                ]
            }
            
            company = {
                "id": "Company",
                "label": "Company",
                "properties": [
                    {"name": "name", "type": "string", "required": True, "label": "Company Name"},
                    {"name": "founded", "type": "date", "required": False, "label": "Founded Date"}
                ]
            }
            
            await self.terminus.create_ontology_class(self.db_name, person)
            await self.terminus.create_ontology_class(self.db_name, company)
            
            # Test 1:1 relationship
            marriage = {
                "id": "Marriage",
                "label": "Marriage",
                "properties": [
                    {"name": "date", "type": "date", "required": True, "label": "Marriage Date"}
                ],
                "relationships": [
                    {
                        "predicate": "spouse1",
                        "target": "Person",
                        "label": "First Spouse",
                        "cardinality": "1:1"
                    },
                    {
                        "predicate": "spouse2",
                        "target": "Person",
                        "label": "Second Spouse",
                        "cardinality": "1:1"
                    }
                ]
            }
            
            result = await self.terminus.create_ontology_class(self.db_name, marriage)
            if result:
                self.add_success("1:1 Relationship", "Marriage class with 1:1 relationships created")
            else:
                self.add_error("1:1 Relationship", "Failed to create Marriage class")
            
            # Test 1:n relationship
            department = {
                "id": "Department",
                "label": "Department",
                "properties": [
                    {"name": "name", "type": "string", "required": True, "label": "Department Name"}
                ],
                "relationships": [
                    {
                        "predicate": "employees",
                        "target": "Person",
                        "label": "Employees",
                        "cardinality": "1:n"
                    }
                ]
            }
            
            result = await self.terminus.create_ontology_class(self.db_name, department)
            if result:
                self.add_success("1:n Relationship", "Department with 1:n relationship created")
            else:
                self.add_error("1:n Relationship", "Failed to create Department class")
            
            # Test n:n relationship
            project = {
                "id": "Project",
                "label": "Project",
                "properties": [
                    {"name": "name", "type": "string", "required": True, "label": "Project Name"},
                    {"name": "budget", "type": "decimal", "required": False, "label": "Budget"}
                ],
                "relationships": [
                    {
                        "predicate": "participants",
                        "target": "Person",
                        "label": "Participants",
                        "cardinality": "n:n"
                    }
                ]
            }
            
            result = await self.terminus.create_ontology_class(self.db_name, project)
            if result:
                self.add_success("n:n Relationship", "Project with n:n relationship created")
            else:
                self.add_error("n:n Relationship", "Failed to create Project class")
                
        except Exception as e:
            self.add_error("Basic Relationships", str(e))
    
    async def test_property_to_relationship_conversion(self):
        """Test 2: Property to relationship auto-conversion"""
        logger.info("\n🔬 Test 2: Property to Relationship Conversion")
        
        try:
            # Test type="link" conversion
            employee = {
                "id": "Employee",
                "label": "Employee",
                "parent_class": "Person",
                "properties": [
                    {"name": "employee_id", "type": "string", "required": True, "label": "Employee ID"},
                    # This should be converted to relationship
                    {"name": "manager", "type": "link", "linkTarget": "Employee", "required": False, "label": "Manager"},
                    {"name": "company", "type": "link", "linkTarget": "Company", "required": True, "label": "Company"}
                ]
            }
            
            result = await self.terminus.create_ontology_class(self.db_name, employee)
            if result:
                # Verify conversion
                retrieved = await self.terminus.get_ontology(self.db_name, "Employee")
                props = {p["name"] for p in retrieved.get("properties", [])}
                rels = {r["predicate"] for r in retrieved.get("relationships", [])}
                
                if "manager" in rels and "company" in rels:
                    self.add_success("Link Type Conversion", "Link properties correctly converted to relationships")
                else:
                    self.add_error("Link Type Conversion", f"Expected relationships not found. Props: {props}, Rels: {rels}")
            else:
                self.add_error("Link Type Conversion", "Failed to create Employee class")
            
            # Test array of links
            team = {
                "id": "Team",
                "label": "Team",
                "properties": [
                    {"name": "name", "type": "string", "required": True, "label": "Team Name"},
                    # Array of links should become 1:n relationship
                    {
                        "name": "members",
                        "type": "array",
                        "items": {"type": "link", "linkTarget": "Employee"},
                        "required": False,
                        "label": "Team Members"
                    }
                ]
            }
            
            result = await self.terminus.create_ontology_class(self.db_name, team)
            if result:
                retrieved = await self.terminus.get_ontology(self.db_name, "Team")
                rels = retrieved.get("relationships", [])
                members_rel = next((r for r in rels if r["predicate"] == "members"), None)
                
                if members_rel and members_rel.get("cardinality") == "1:n":
                    self.add_success("Array Link Conversion", "Array of links correctly converted to 1:n relationship")
                else:
                    self.add_error("Array Link Conversion", f"Array link conversion failed: {members_rel}")
            else:
                self.add_error("Array Link Conversion", "Failed to create Team class")
                
        except Exception as e:
            self.add_error("Property Conversion", str(e))
    
    async def test_inverse_relationships(self):
        """Test 3: Inverse relationships"""
        logger.info("\n🔬 Test 3: Inverse Relationships")
        
        try:
            # Create class with inverse relationship
            parent_child = {
                "id": "ParentChild",
                "label": "Parent-Child Relationship",
                "relationships": [
                    {
                        "predicate": "parent",
                        "target": "Person",
                        "label": "Parent",
                        "cardinality": "n:1",
                        "inverse_predicate": "children",
                        "inverse_label": "Children"
                    },
                    {
                        "predicate": "child",
                        "target": "Person",
                        "label": "Child",
                        "cardinality": "n:1",
                        "inverse_predicate": "parents",
                        "inverse_label": "Parents"
                    }
                ]
            }
            
            result = await self.terminus.create_ontology_class(self.db_name, parent_child)
            if result:
                self.add_success("Inverse Relationships", "ParentChild with inverse relationships created")
            else:
                self.add_error("Inverse Relationships", "Failed to create ParentChild class")
                
        except Exception as e:
            self.add_error("Inverse Relationships", str(e))
    
    async def test_self_referencing(self):
        """Test 4: Self-referencing relationships"""
        logger.info("\n🔬 Test 4: Self-Referencing Relationships")
        
        try:
            # Update Person to have self-referencing relationships
            person_update = {
                "relationships": [
                    {
                        "predicate": "friends",
                        "target": "Person",
                        "label": "Friends",
                        "cardinality": "n:n"
                    },
                    {
                        "predicate": "mentor",
                        "target": "Person",
                        "label": "Mentor",
                        "cardinality": "n:1"
                    }
                ]
            }
            
            result = await self.terminus.update_ontology(self.db_name, "Person", person_update)
            if result:
                self.add_success("Self-Referencing", "Person class updated with self-referencing relationships")
            else:
                self.add_error("Self-Referencing", "Failed to update Person with self-references")
                
        except Exception as e:
            self.add_error("Self-Referencing", str(e))
    
    async def test_circular_dependencies(self):
        """Test 5: Circular dependencies"""
        logger.info("\n🔬 Test 5: Circular Dependencies")
        
        try:
            # Create two classes that reference each other
            author = {
                "id": "Author",
                "label": "Author",
                "properties": [
                    {"name": "name", "type": "string", "required": True, "label": "Author Name"}
                ]
            }
            
            book = {
                "id": "Book",
                "label": "Book",
                "properties": [
                    {"name": "title", "type": "string", "required": True, "label": "Book Title"},
                    {"name": "isbn", "type": "string", "required": False, "label": "ISBN"}
                ]
            }
            
            # Create base classes first
            await self.terminus.create_ontology_class(self.db_name, author)
            await self.terminus.create_ontology_class(self.db_name, book)
            
            # Update with circular references
            author_update = {
                "relationships": [
                    {
                        "predicate": "books",
                        "target": "Book",
                        "label": "Books Written",
                        "cardinality": "1:n"
                    }
                ]
            }
            
            book_update = {
                "relationships": [
                    {
                        "predicate": "author",
                        "target": "Author",
                        "label": "Written By",
                        "cardinality": "n:1"
                    },
                    {
                        "predicate": "co_authors",
                        "target": "Author",
                        "label": "Co-Authors",
                        "cardinality": "n:n"
                    }
                ]
            }
            
            result1 = await self.terminus.update_ontology(self.db_name, "Author", author_update)
            result2 = await self.terminus.update_ontology(self.db_name, "Book", book_update)
            
            if result1 and result2:
                self.add_success("Circular Dependencies", "Author-Book circular dependencies created successfully")
            else:
                self.add_error("Circular Dependencies", "Failed to create circular dependencies")
                
        except Exception as e:
            self.add_error("Circular Dependencies", str(e))
    
    async def test_complex_inheritance_with_relationships(self):
        """Test 6: Complex inheritance with relationships"""
        logger.info("\n🔬 Test 6: Complex Inheritance with Relationships")
        
        try:
            # Create a complex inheritance hierarchy
            vehicle = {
                "id": "Vehicle",
                "label": "Vehicle",
                "abstract": True,
                "properties": [
                    {"name": "make", "type": "string", "required": True, "label": "Make"},
                    {"name": "model", "type": "string", "required": True, "label": "Model"}
                ],
                "relationships": [
                    {
                        "predicate": "owner",
                        "target": "Person",
                        "label": "Owner",
                        "cardinality": "n:1"
                    }
                ]
            }
            
            car = {
                "id": "Car",
                "label": "Car",
                "parent_class": "Vehicle",
                "properties": [
                    {"name": "num_doors", "type": "integer", "required": True, "label": "Number of Doors"}
                ],
                "relationships": [
                    {
                        "predicate": "garage",
                        "target": "Company",  # Assuming garage is a company
                        "label": "Garage",
                        "cardinality": "n:1"
                    }
                ]
            }
            
            motorcycle = {
                "id": "Motorcycle",
                "label": "Motorcycle",
                "parent_class": "Vehicle",
                "properties": [
                    {"name": "engine_cc", "type": "integer", "required": True, "label": "Engine CC"}
                ]
            }
            
            # Create hierarchy
            await self.terminus.create_ontology_class(self.db_name, vehicle)
            await self.terminus.create_ontology_class(self.db_name, car)
            await self.terminus.create_ontology_class(self.db_name, motorcycle)
            
            # Verify inheritance
            car_data = await self.terminus.get_ontology(self.db_name, "Car")
            car_rels = {r["predicate"] for r in car_data.get("relationships", [])}
            
            if "owner" in car_rels and "garage" in car_rels:
                self.add_success("Inheritance with Relationships", "Car correctly inherits owner relationship from Vehicle")
            else:
                self.add_error("Inheritance with Relationships", f"Car relationships: {car_rels}, expected 'owner' and 'garage'")
                
        except Exception as e:
            self.add_error("Complex Inheritance", str(e))
    
    async def test_optional_relationships(self):
        """Test 7: Optional vs Required relationships"""
        logger.info("\n🔬 Test 7: Optional vs Required Relationships")
        
        try:
            # Create class with mixed optional/required relationships
            order = {
                "id": "Order",
                "label": "Order",
                "properties": [
                    {"name": "order_number", "type": "string", "required": True, "label": "Order Number"},
                    {"name": "total", "type": "decimal", "required": True, "label": "Total Amount"}
                ],
                "relationships": [
                    {
                        "predicate": "customer",
                        "target": "Person",
                        "label": "Customer",
                        "cardinality": "n:1"  # Required by default
                    },
                    {
                        "predicate": "delivery_person",
                        "target": "Person",
                        "label": "Delivery Person",
                        "cardinality": "n:1"  # Should be optional
                    }
                ]
            }
            
            result = await self.terminus.create_ontology_class(self.db_name, order)
            if result:
                self.add_success("Optional Relationships", "Order class with mixed relationships created")
            else:
                self.add_error("Optional Relationships", "Failed to create Order class")
                
        except Exception as e:
            self.add_error("Optional Relationships", str(e))
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive test report"""
        return {
            "total_tests": len(self.successes) + len(self.errors),
            "passed": len(self.successes),
            "failed": len(self.errors),
            "success_rate": f"{(len(self.successes) / (len(self.successes) + len(self.errors)) * 100):.1f}%",
            "successes": self.successes,
            "errors": self.errors
        }


async def main():
    """Run comprehensive relationship tests"""
    
    connection_config = ConnectionConfig(
        server_url=os.getenv("TERMINUS_SERVER_URL", "http://localhost:6363"),
        user=os.getenv("TERMINUS_USER", "admin"),
        account=os.getenv("TERMINUS_ACCOUNT", "admin"),
        key=os.getenv("TERMINUS_KEY", "admin123"),
    )
    terminus = AsyncTerminusService(connection_config)
    test_db = f"test_relationships_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        await terminus.connect()
        await terminus.create_database(test_db, "Advanced Relationships Test")
        
        # Create test suite
        suite = RelationshipTestSuite(terminus, test_db)
        
        # Run all tests
        logger.info("🚀 Starting Advanced Relationship Tests...")
        logger.info("=" * 80)
        
        await suite.test_basic_relationships()
        await suite.test_property_to_relationship_conversion()
        await suite.test_inverse_relationships()
        await suite.test_self_referencing()
        await suite.test_circular_dependencies()
        await suite.test_complex_inheritance_with_relationships()
        await suite.test_optional_relationships()
        
        # Generate report
        report = suite.generate_report()
        
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
            
    except Exception as e:
        logger.error(f"❌ Test suite failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        try:
            await terminus.delete_database(test_db)
        except:
            pass
        await terminus.disconnect()


if __name__ == "__main__":
    asyncio.run(main())