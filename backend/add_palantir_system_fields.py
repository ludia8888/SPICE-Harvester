#!/usr/bin/env python3
"""
🔥 THINK ULTRA: Add Palantir-style system fields to TerminusDB schemas

This script adds essential system fields to enable lightweight node storage
in TerminusDB while keeping full data in Elasticsearch.

Palantir Architecture:
- TerminusDB: Stores ONLY lightweight nodes (IDs + relationships + system metadata)
- Elasticsearch: Stores ALL domain data (full documents)
- S3: Stores large binary data (files, images, etc.)
"""

import asyncio
import logging
from typing import List, Dict, Any
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# System fields required for Palantir architecture
PALANTIR_SYSTEM_FIELDS = [
    {
        "name": "es_doc_id",
        "type": "xsd:string",
        "label": "Elasticsearch Document ID",
        "description": "Reference to the full document in Elasticsearch",
        "required": False
    },
    {
        "name": "s3_uri",
        "type": "xsd:string", 
        "label": "S3 Storage URI",
        "description": "Reference to large data stored in S3",
        "required": False
    },
    {
        "name": "instance_id",
        "type": "xsd:string",
        "label": "Instance Identifier",
        "description": "Unique identifier for this instance",
        "required": True
    },
    {
        "name": "created_at",
        "type": "xsd:dateTime",
        "label": "Creation Timestamp",
        "description": "When this instance was created",
        "required": False
    },
    {
        "name": "updated_at",
        "type": "xsd:dateTime",
        "label": "Update Timestamp",
        "description": "When this instance was last updated",
        "required": False
    },
    {
        "name": "graph_version",
        "type": "xsd:integer",
        "label": "Graph Version",
        "description": "Version number for optimistic locking",
        "required": False
    }
]


async def add_system_fields_to_ontology(
    db_name: str,
    class_id: str,
    terminus_service: Any
) -> Dict[str, Any]:
    """Add Palantir system fields to a specific ontology class"""
    
    try:
        # Get existing ontology
        logger.info(f"📖 Getting ontology {class_id} from {db_name}")
        ontologies = await terminus_service.get_ontology(db_name, class_id)
        
        if not ontologies:
            logger.error(f"❌ Ontology {class_id} not found")
            return {"status": "error", "message": f"Ontology {class_id} not found"}
        
        ontology = ontologies[0] if isinstance(ontologies, list) else ontologies
        
        # Convert to dict if needed
        if hasattr(ontology, "model_dump"):
            ontology_dict = ontology.model_dump()
        elif hasattr(ontology, "dict"):
            ontology_dict = ontology.dict()
        else:
            ontology_dict = ontology
        
        # Get existing properties
        existing_properties = ontology_dict.get('properties', [])
        existing_prop_names = {prop['name'] for prop in existing_properties if isinstance(prop, dict)}
        
        # Add system fields that don't exist yet
        added_fields = []
        for field in PALANTIR_SYSTEM_FIELDS:
            if field['name'] not in existing_prop_names:
                existing_properties.append(field)
                added_fields.append(field['name'])
                logger.info(f"   ➕ Adding system field: {field['name']}")
        
        if not added_fields:
            logger.info(f"   ✅ All system fields already exist in {class_id}")
            return {
                "status": "unchanged",
                "message": f"All system fields already exist in {class_id}",
                "class_id": class_id
            }
        
        # Update ontology with new properties
        ontology_dict['properties'] = existing_properties
        
        # Convert to OntologyBase model
        from shared.models.ontology import OntologyBase
        updated_ontology = OntologyBase(**ontology_dict)
        
        # Update in TerminusDB
        logger.info(f"   📤 Updating ontology {class_id} with {len(added_fields)} new system fields")
        result = await terminus_service.update_ontology(db_name, class_id, updated_ontology)
        
        logger.info(f"   ✅ Successfully added system fields to {class_id}: {', '.join(added_fields)}")
        
        return {
            "status": "success",
            "message": f"Added {len(added_fields)} system fields to {class_id}",
            "class_id": class_id,
            "added_fields": added_fields
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to add system fields to {class_id}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "message": str(e),
            "class_id": class_id
        }


async def add_system_fields_to_all_ontologies(db_name: str) -> Dict[str, Any]:
    """Add Palantir system fields to all ontologies in a database"""
    
    # Initialize TerminusDB connection
    from oms.services.async_terminus import AsyncTerminusService
    from shared.models.config import ConnectionConfig
    import os
    
    connection_info = ConnectionConfig(
        server_url=os.getenv("TERMINUS_SERVER_URL", "http://localhost:6363"),
        user="admin",
        account="admin",
        key="spice123!"
    )
    
    terminus_service = AsyncTerminusService(connection_info)
    await terminus_service.connect()
    
    try:
        # Get all ontologies
        logger.info(f"🔍 Getting all ontologies from {db_name}")
        ontologies = await terminus_service.list_ontology_classes(db_name)
        
        if not ontologies:
            logger.warning(f"⚠️ No ontologies found in {db_name}")
            return {
                "status": "warning",
                "message": f"No ontologies found in {db_name}",
                "database": db_name
            }
        
        logger.info(f"📊 Found {len(ontologies)} ontologies in {db_name}")
        
        # Process each ontology
        results = []
        success_count = 0
        unchanged_count = 0
        error_count = 0
        
        for ontology in ontologies:
            class_id = ontology.get('id') if isinstance(ontology, dict) else ontology.id
            logger.info(f"\n🔧 Processing ontology: {class_id}")
            
            result = await add_system_fields_to_ontology(db_name, class_id, terminus_service)
            results.append(result)
            
            if result['status'] == 'success':
                success_count += 1
            elif result['status'] == 'unchanged':
                unchanged_count += 1
            else:
                error_count += 1
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("📋 SUMMARY:")
        logger.info(f"   Total ontologies: {len(ontologies)}")
        logger.info(f"   ✅ Successfully updated: {success_count}")
        logger.info(f"   ➖ Already had fields: {unchanged_count}")
        logger.info(f"   ❌ Errors: {error_count}")
        
        return {
            "status": "completed",
            "database": db_name,
            "total_ontologies": len(ontologies),
            "updated": success_count,
            "unchanged": unchanged_count,
            "errors": error_count,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to process database {db_name}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "message": str(e),
            "database": db_name
        }
    finally:
        await terminus_service.disconnect()


async def verify_palantir_architecture(db_name: str = "spice_3pl_synthetic"):
    """Verify that the Palantir architecture is properly configured"""
    
    logger.info("\n" + "="*60)
    logger.info("🔥 THINK ULTRA: VERIFYING PALANTIR ARCHITECTURE")
    logger.info("="*60)
    
    # Add system fields to all ontologies
    result = await add_system_fields_to_all_ontologies(db_name)
    
    if result['status'] == 'completed':
        logger.info("\n✅ Palantir system fields added successfully!")
        logger.info("🎯 Next steps:")
        logger.info("   1. Instance Worker will now store lightweight nodes in TerminusDB")
        logger.info("   2. Full data will be stored in Elasticsearch")
        logger.info("   3. Large files will be stored in S3")
        logger.info("\n🚀 Ready for end-to-end Palantir architecture testing!")
    else:
        logger.error(f"\n❌ Failed to configure Palantir architecture: {result.get('message', 'Unknown error')}")
    
    return result


if __name__ == "__main__":
    # Test with spice_3pl_synthetic database
    asyncio.run(verify_palantir_architecture("spice_3pl_synthetic"))
