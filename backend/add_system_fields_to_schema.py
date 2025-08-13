#!/usr/bin/env python3
"""
Add system fields to TerminusDB schema for lightweight nodes
These fields are needed for Palantir-style architecture
"""

import asyncio
import httpx
from typing import Dict, List, Any

async def add_system_fields_to_class(
    terminus_url: str,
    db_name: str, 
    class_name: str,
    auth: tuple
):
    """Add system fields to a single class"""
    
    # System fields needed for lightweight nodes
    system_properties = [
        {
            "@type": "Property",
            "@id": f"@schema:{class_name}_es_doc_id",
            "domain": f"@schema:{class_name}",
            "range": "xsd:string",
            "label": "Elasticsearch Document ID"
        },
        {
            "@type": "Property", 
            "@id": f"@schema:{class_name}_s3_uri",
            "domain": f"@schema:{class_name}",
            "range": "xsd:string",
            "label": "S3 Event Store URI"
        },
        {
            "@type": "Property",
            "@id": f"@schema:{class_name}_instance_id",
            "domain": f"@schema:{class_name}",
            "range": "xsd:string", 
            "label": "Instance Identifier"
        },
        {
            "@type": "Property",
            "@id": f"@schema:{class_name}_created_at",
            "domain": f"@schema:{class_name}",
            "range": "xsd:dateTime",
            "label": "Creation Timestamp"
        }
    ]
    
    print(f"\n📝 Adding system fields to {class_name}...")
    
    for prop in system_properties:
        # Create WOQL query to add property
        woql_query = {
            "@type": "AddTriple",
            "subject": prop["@id"],
            "predicate": "rdf:type",
            "object": "owl:DatatypeProperty"
        }
        
        # Execute query
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{terminus_url}/api/woql/{auth[0]}/{db_name}",
                    json={"query": woql_query},
                    auth=auth
                )
                
                if response.status_code == 200:
                    prop_name = prop["@id"].split("_")[-1]
                    print(f"   ✅ Added {prop_name}")
                else:
                    print(f"   ⚠️ Failed to add {prop['@id']}: {response.status_code}")
                    
            except Exception as e:
                print(f"   ❌ Error adding {prop['@id']}: {e}")

async def get_all_classes(terminus_url: str, db_name: str, auth: tuple) -> List[str]:
    """Get all classes in the database"""
    
    # Use different approach - query for schema classes
    woql_query = {
        "@type": "Select",
        "variables": ["v:Class"],
        "query": {
            "@type": "Triple",
            "subject": {"variable": "v:Class"},
            "predicate": {"node": "rdf:type"},
            "object": {"node": "sys:Class"}
        }
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{terminus_url}/api/woql/{auth[0]}/{db_name}",
            json={"query": woql_query},
            auth=auth
        )
        
        if response.status_code == 200:
            result = response.json()
            bindings = result.get("bindings", [])
            
            classes = []
            for binding in bindings:
                class_uri = binding.get("v:Class", "")
                if "@schema:" in class_uri:
                    class_name = class_uri.split(":")[-1]
                    # Skip system classes
                    if not class_name.startswith("sys:"):
                        classes.append(class_name)
            
            return classes
        else:
            print(f"Failed to get classes: {response.status_code}")
            return []

async def main():
    """Main function to add system fields to all classes"""
    
    # Configuration
    terminus_url = "http://localhost:6363"
    auth = ("admin", "spice123!")
    
    databases = [
        "spice_3pl_synthetic",
        "integration_test_db"
    ]
    
    print("🚀 Adding System Fields for Palantir Architecture")
    print("=" * 60)
    
    for db_name in databases:
        print(f"\n📦 Processing database: {db_name}")
        
        # Get all classes
        classes = await get_all_classes(terminus_url, db_name, auth)
        print(f"   Found {len(classes)} classes: {', '.join(classes[:5])}...")
        
        # Add system fields to each class
        for class_name in classes:
            await add_system_fields_to_class(terminus_url, db_name, class_name, auth)
        
        print(f"   ✅ Completed {db_name}")
    
    print("\n🎉 System fields added successfully!")
    print("\nNOTE: Now lightweight nodes can be stored in TerminusDB:")
    print("  - es_doc_id: Reference to Elasticsearch document")
    print("  - s3_uri: Reference to S3 event store")
    print("  - instance_id: Instance identifier")
    print("  - created_at: Creation timestamp")

if __name__ == "__main__":
    asyncio.run(main())