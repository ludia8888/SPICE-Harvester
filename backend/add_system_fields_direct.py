#!/usr/bin/env python3
"""
Directly add system fields to known classes via OMS API
"""

import asyncio
import aiohttp
import json

async def update_ontology_with_system_fields():
    """Update ontologies to include system fields"""
    
    print("🚀 Adding System Fields via OMS API")
    print("=" * 60)
    
    # Known classes that need system fields
    databases_and_classes = {
        "spice_3pl_synthetic": ["Product", "Client", "Order", "SKU", "Return", "Exception"],
        "integration_test_db": ["IntegrationProduct", "IntegrationClient"]
    }
    
    async with aiohttp.ClientSession() as session:
        for db_name, classes in databases_and_classes.items():
            print(f"\n📦 Processing database: {db_name}")
            
            for class_name in classes:
                print(f"\n   📝 Updating {class_name}...")
                
                # First, get the current ontology
                async with session.get(
                    f"http://localhost:8000/api/v1/database/{db_name}/ontology/{class_name}"
                ) as resp:
                    if resp.status != 200:
                        print(f"      ⚠️ Could not get {class_name}: {resp.status}")
                        continue
                    
                    result = await resp.json()
                    ontology = result.get("data")
                    
                    if not ontology:
                        print(f"      ⚠️ No ontology data for {class_name}")
                        continue
                    
                    # Add system properties if not already present
                    existing_props = {p.get("name") for p in ontology.get("properties", [])}
                    system_props = []
                    
                    if "es_doc_id" not in existing_props:
                        system_props.append({
                            "name": "es_doc_id",
                            "type": "string",
                            "label": "Elasticsearch Document ID",
                            "required": False,
                            "description": "Reference to ES document"
                        })
                    
                    if "s3_uri" not in existing_props:
                        system_props.append({
                            "name": "s3_uri",
                            "type": "string",
                            "label": "S3 Event Store URI",
                            "required": False,
                            "description": "Reference to S3 event"
                        })
                    
                    if "instance_id" not in existing_props:
                        system_props.append({
                            "name": "instance_id",
                            "type": "string",
                            "label": "Instance Identifier",
                            "required": False,
                            "description": "Unique instance ID"
                        })
                    
                    if "created_at" not in existing_props:
                        system_props.append({
                            "name": "created_at",
                            "type": "datetime",
                            "label": "Creation Timestamp",
                            "required": False,
                            "description": "When instance was created"
                        })
                    
                    if not system_props:
                        print(f"      ✅ {class_name} already has all system fields")
                        continue
                    
                    # Add system properties to ontology
                    ontology["properties"].extend(system_props)
                    
                    # Update via OMS API
                    update_data = {
                        "id": ontology.get("id"),
                        "label": ontology.get("label"),
                        "description": ontology.get("description"),
                        "properties": ontology.get("properties"),
                        "relationships": ontology.get("relationships", [])
                    }
                    
                    async with session.put(
                        f"http://localhost:8000/api/v1/database/{db_name}/ontology/{class_name}",
                        json=update_data
                    ) as update_resp:
                        if update_resp.status == 200:
                            print(f"      ✅ Added {len(system_props)} system fields to {class_name}")
                            for prop in system_props:
                                print(f"         - {prop['name']}")
                        else:
                            error = await update_resp.text()
                            print(f"      ❌ Failed to update {class_name}: {error[:100]}")
    
    print("\n🎉 System fields added successfully!")
    print("\nLightweight nodes can now be stored with:")
    print("  - es_doc_id: Reference to Elasticsearch")
    print("  - s3_uri: Reference to S3 event store")
    print("  - instance_id: Instance identifier")
    print("  - created_at: Creation timestamp")

if __name__ == "__main__":
    asyncio.run(update_ontology_with_system_fields())