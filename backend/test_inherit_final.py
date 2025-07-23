#!/usr/bin/env python3
"""🔥 THINK ULTRA! Final inheritance test"""

import asyncio
import json
import logging
import os
from datetime import datetime
from oms.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_inheritance():
    """Test inheritance fix"""
    
    connection_config = ConnectionConfig(
        server_url=os.getenv("TERMINUS_SERVER_URL", "http://localhost:6363"),
        user=os.getenv("TERMINUS_USER", "admin"),
        account=os.getenv("TERMINUS_ACCOUNT", "admin"),
        key=os.getenv("TERMINUS_KEY", "admin123"),
    )
    terminus = AsyncTerminusService(connection_config)
    test_db = f"test_inherit_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        await terminus.connect()
        await terminus.create_database(test_db, "Final Inheritance Test")
        
        # Create base classes
        person = {
            "id": "Person",
            "label": "Person",
            "properties": [
                {"name": "name", "type": "string", "required": True, "label": "Name"}
            ]
        }
        company = {
            "id": "Company",
            "label": "Company",
            "properties": [
                {"name": "name", "type": "string", "required": True, "label": "Company Name"}
            ]
        }
        await terminus.create_ontology_class(test_db, person)
        await terminus.create_ontology_class(test_db, company)
        logger.info("✅ Created Person and Company classes")
        
        # Create abstract Vehicle with properties and relationship
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
        await terminus.create_ontology_class(test_db, vehicle)
        logger.info("✅ Created abstract Vehicle class")
        
        # Create Car inheriting from Vehicle
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
                    "target": "Company",
                    "label": "Garage",
                    "cardinality": "n:1"
                }
            ]
        }
        await terminus.create_ontology_class(test_db, car)
        logger.info("✅ Created Car class inheriting from Vehicle")
        
        # Retrieve and analyze
        car_data = await terminus.get_ontology(test_db, "Car")
        
        logger.info("=" * 80)
        logger.info("🎯 FINAL RESULTS:")
        logger.info("=" * 80)
        
        # Properties analysis
        props = car_data.get("properties", [])
        prop_names = {p["name"] for p in props}
        logger.info(f"\n📦 PROPERTIES ({len(props)} total):")
        for p in props:
            logger.info(f"  - {p['name']} ({p['type']})")
        
        # Relationships analysis
        rels = car_data.get("relationships", [])
        rel_predicates = {r["predicate"] for r in rels}
        logger.info(f"\n🔗 RELATIONSHIPS ({len(rels)} total):")
        for r in rels:
            logger.info(f"  - {r['predicate']} -> {r['target']} ({r.get('cardinality', 'unknown')})")
        
        # Inheritance check
        logger.info(f"\n🧬 INHERITANCE:")
        logger.info(f"  - Inherits from: {car_data.get('inherits', 'None')}")
        logger.info(f"  - Has inherited 'make' property: {'make' in prop_names}")
        logger.info(f"  - Has inherited 'model' property: {'model' in prop_names}")
        logger.info(f"  - Has inherited 'owner' relationship: {'owner' in rel_predicates}")
        logger.info(f"  - Has own 'garage' relationship: {'garage' in rel_predicates}")
        
        # Final verdict
        success = (
            'make' in prop_names and 
            'model' in prop_names and 
            'owner' in rel_predicates and 
            'garage' in rel_predicates
        )
        
        logger.info("\n" + "=" * 80)
        if success:
            logger.info("✅ SUCCESS: Inheritance is working correctly!")
            logger.info("   - Properties inherited: ✓")
            logger.info("   - Relationships inherited: ✓")
        else:
            logger.error("❌ FAILURE: Inheritance issues detected!")
            if 'make' not in prop_names or 'model' not in prop_names:
                logger.error("   - Properties NOT inherited properly")
            if 'owner' not in rel_predicates:
                logger.error("   - Relationships NOT inherited properly")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        try:
            await terminus.delete_database(test_db)
        except:
            pass
        await terminus.disconnect()


if __name__ == "__main__":
    asyncio.run(test_inheritance())