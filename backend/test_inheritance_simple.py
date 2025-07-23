#!/usr/bin/env python3
"""🔥 THINK ULTRA! Simple inheritance test with TerminusDB v11.x API"""

import asyncio
import httpx
import json
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_terminusdb_inheritance():
    """Test if TerminusDB inherits properties and relationships from parent class"""
    
    # Connection details
    server_url = os.getenv("TERMINUS_SERVER_URL", "http://localhost:6363")
    auth = httpx.BasicAuth(
        username=os.getenv("TERMINUS_USER", "admin"),
        password=os.getenv("TERMINUS_KEY", "admin123")
    )
    account = os.getenv("TERMINUS_ACCOUNT", "admin")
    test_db = f"test_inherit_v11_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    async with httpx.AsyncClient(base_url=server_url, auth=auth) as client:
        try:
            # Create database
            logger.info(f"Creating database {test_db}")
            resp = await client.post(f"/api/db/{account}/{test_db}", json={
                "label": test_db,
                "comment": "Test inheritance"
            })
            assert resp.status_code == 200
            
            # Create Person class (target of relationships)
            logger.info("Creating Person class")
            resp = await client.post(
                f"/api/document/{account}/{test_db}",
                params={"graph_type": "schema", "author": "admin", "message": "Create Person"},
                json=[{
                    "@type": "Class",
                    "@id": "Person",
                    "name": "xsd:string"
                }]
            )
            assert resp.status_code == 200
            
            # Create abstract Vehicle class with properties and relationships
            logger.info("Creating abstract Vehicle class")
            resp = await client.post(
                f"/api/document/{account}/{test_db}",
                params={"graph_type": "schema", "author": "admin", "message": "Create Vehicle"},
                json=[{
                    "@type": "Class",
                    "@id": "Vehicle",
                    "@abstract": [],
                    "make": "xsd:string",
                    "model": "xsd:string",
                    "owner": {"@type": "Optional", "@class": "Person"}
                }]
            )
            assert resp.status_code == 200
            
            # Create Car class inheriting from Vehicle
            logger.info("Creating Car class inheriting from Vehicle")
            resp = await client.post(
                f"/api/document/{account}/{test_db}",
                params={"graph_type": "schema", "author": "admin", "message": "Create Car"},
                json=[{
                    "@type": "Class",
                    "@id": "Car",
                    "@inherits": "Vehicle",
                    "num_doors": "xsd:integer",
                    "garage": {"@type": "Optional", "@class": "Person"}
                }]
            )
            assert resp.status_code == 200
            
            # Get Car schema to check inheritance
            logger.info("Retrieving Car schema to check inheritance")
            resp = await client.get(
                f"/api/document/{account}/{test_db}",
                params={"graph_type": "schema"}
            )
            assert resp.status_code == 200
            
            schemas = resp.text.strip().split('\n')
            car_schema = None
            for line in schemas:
                if line:
                    doc = json.loads(line)
                    if doc.get("@id") == "Car":
                        car_schema = doc
                        break
            
            logger.info("=" * 80)
            logger.info("🔍 CAR SCHEMA:")
            logger.info(json.dumps(car_schema, indent=2))
            logger.info("=" * 80)
            
            # Analysis
            if car_schema:
                # Check direct properties
                has_num_doors = "num_doors" in car_schema
                has_garage = "garage" in car_schema
                
                # Check inherited properties
                has_make = "make" in car_schema
                has_model = "model" in car_schema
                has_owner = "owner" in car_schema
                
                logger.info("🔍 INHERITANCE ANALYSIS:")
                logger.info(f"  Direct properties:")
                logger.info(f"    - num_doors: {has_num_doors}")
                logger.info(f"    - garage: {has_garage}")
                logger.info(f"  Inherited from Vehicle:")
                logger.info(f"    - make: {has_make}")
                logger.info(f"    - model: {has_model}")
                logger.info(f"    - owner: {has_owner}")
                logger.info(f"  @inherits: {car_schema.get('@inherits', 'NOT FOUND')}")
                
                if not (has_make and has_model and has_owner):
                    logger.error("❌ INHERITANCE NOT WORKING: Properties/relationships from Vehicle are not present in Car!")
                    logger.info("\n🔥 ULTRA! TerminusDB v11.x does NOT automatically inherit properties/relationships!")
                    logger.info("We need to manually resolve inheritance in our code!")
                else:
                    logger.info("✅ INHERITANCE WORKING: All properties and relationships are inherited!")
            
            # Delete test database
            logger.info(f"Deleting test database {test_db}")
            await client.delete(f"/api/db/{account}/{test_db}")
            
        except Exception as e:
            logger.error(f"Test failed: {e}")
            import traceback
            logger.error(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(test_terminusdb_inheritance())