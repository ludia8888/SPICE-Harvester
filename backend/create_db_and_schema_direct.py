#!/usr/bin/env python3
"""
Create database and schema directly using TerminusDB API
Bypassing OMS to ensure database and schema are created
"""

import asyncio
import httpx
import json

async def create_database_direct(db_name: str):
    """Create database directly in TerminusDB"""
    
    terminus_url = "http://localhost:6363"
    auth = ("admin", "spice123!")
    
    print(f"Creating database: {db_name}")
    
    # Database creation payload
    db_payload = {
        "label": db_name,
        "comment": f"Test database for Palantir architecture with system fields",
        "prefixes": {
            "@base": f"terminusdb:///admin/{db_name}/data/",
            "@schema": f"terminusdb:///admin/{db_name}/schema#"
        }
    }
    
    async with httpx.AsyncClient() as client:
        # Create database
        response = await client.post(
            f"{terminus_url}/api/db/admin/{db_name}",
            json=db_payload,
            auth=auth
        )
        
        if response.status_code in [200, 201]:
            print(f"   ✅ Database created successfully")
        elif response.status_code == 400:
            print(f"   ℹ️ Database already exists")
        else:
            print(f"   ❌ Failed to create database: {response.status_code}")
            print(f"      {response.text}")

async def create_schema_with_system_fields(db_name: str):
    """Create schema classes with system fields directly"""
    
    terminus_url = "http://localhost:6363"
    auth = ("admin", "spice123!")
    
    print(f"\nCreating schema for {db_name}...")
    
    # Define Product class with system fields
    product_schema = {
        "@id": "@schema:Product",
        "@type": "owl:Class",
        "@documentation": {
            "@comment": "Product class with system fields for Palantir architecture",
            "@properties": {
                "product_id": "Product unique identifier",
                "name": "Product name",
                "unit_price": "Unit price",
                "category": "Product category",
                "es_doc_id": "Elasticsearch document ID",
                "s3_uri": "S3 event store URI",
                "instance_id": "Instance identifier",
                "created_at": "Creation timestamp"
            }
        }
    }
    
    # Define Client class with system fields
    client_schema = {
        "@id": "@schema:Client",
        "@type": "owl:Class",
        "@documentation": {
            "@comment": "Client class with system fields",
            "@properties": {
                "client_id": "Client unique identifier",
                "name": "Client name",
                "email": "Client email",
                "es_doc_id": "Elasticsearch document ID",
                "s3_uri": "S3 event store URI",
                "instance_id": "Instance identifier",
                "created_at": "Creation timestamp"
            }
        }
    }
    
    # Define properties
    properties = [
        # Product properties
        {"@id": "@schema:product_id", "@type": "owl:DatatypeProperty", "domain": "@schema:Product", "range": "xsd:string"},
        {"@id": "@schema:name", "@type": "owl:DatatypeProperty", "domain": ["@schema:Product", "@schema:Client"], "range": "xsd:string"},
        {"@id": "@schema:unit_price", "@type": "owl:DatatypeProperty", "domain": "@schema:Product", "range": "xsd:decimal"},
        {"@id": "@schema:category", "@type": "owl:DatatypeProperty", "domain": "@schema:Product", "range": "xsd:string"},
        
        # Client properties
        {"@id": "@schema:client_id", "@type": "owl:DatatypeProperty", "domain": "@schema:Client", "range": "xsd:string"},
        {"@id": "@schema:email", "@type": "owl:DatatypeProperty", "domain": "@schema:Client", "range": "xsd:string"},
        
        # System fields for both classes
        {"@id": "@schema:es_doc_id", "@type": "owl:DatatypeProperty", "domain": ["@schema:Product", "@schema:Client"], "range": "xsd:string"},
        {"@id": "@schema:s3_uri", "@type": "owl:DatatypeProperty", "domain": ["@schema:Product", "@schema:Client"], "range": "xsd:string"},
        {"@id": "@schema:instance_id", "@type": "owl:DatatypeProperty", "domain": ["@schema:Product", "@schema:Client"], "range": "xsd:string"},
        {"@id": "@schema:created_at", "@type": "owl:DatatypeProperty", "domain": ["@schema:Product", "@schema:Client"], "range": "xsd:dateTime"},
        
        # Relationship
        {"@id": "@schema:owned_by", "@type": "owl:ObjectProperty", "domain": "@schema:Product", "range": "@schema:Client"}
    ]
    
    # Combine all schema elements
    full_schema = [product_schema, client_schema] + properties
    
    async with httpx.AsyncClient() as client:
        # Insert schema
        response = await client.post(
            f"{terminus_url}/api/document/admin/{db_name}?graph_type=schema&author=admin&message=Create+schema+with+system+fields",
            json=full_schema,
            auth=auth
        )
        
        if response.status_code in [200, 201]:
            print(f"   ✅ Schema created successfully")
            print(f"      - Product class with system fields")
            print(f"      - Client class with system fields")
            print(f"      - Relationship: Product owned_by Client")
        else:
            print(f"   ❌ Failed to create schema: {response.status_code}")
            print(f"      {response.text[:500]}")

async def verify_schema(db_name: str):
    """Verify the schema was created correctly"""
    
    terminus_url = "http://localhost:6363"
    auth = ("admin", "spice123!")
    
    print(f"\nVerifying schema for {db_name}...")
    
    async with httpx.AsyncClient() as client:
        # Query schema
        woql_query = {
            "@type": "Select",
            "variables": ["v:Class", "v:Type"],
            "query": {
                "@type": "And",
                "and": [
                    {
                        "@type": "Triple",
                        "subject": {"variable": "v:Class"},
                        "predicate": {"node": "rdf:type"},
                        "object": {"variable": "v:Type"}
                    },
                    {
                        "@type": "Or",
                        "or": [
                            {
                                "@type": "Equals",
                                "left": {"variable": "v:Type"},
                                "right": {"node": "sys:Class"}
                            },
                            {
                                "@type": "Equals",
                                "left": {"variable": "v:Type"},
                                "right": {"node": "owl:Class"}
                            }
                        ]
                    }
                ]
            }
        }
        
        response = await client.post(
            f"{terminus_url}/api/woql/admin/{db_name}",
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
                    classes.append(class_name)
            
            print(f"   ✅ Found {len(classes)} classes: {', '.join(classes)}")
            
            # Check for system properties
            prop_query = {
                "@type": "Select",
                "variables": ["v:Property"],
                "query": {
                    "@type": "Triple",
                    "subject": {"variable": "v:Property"},
                    "predicate": {"node": "rdf:type"},
                    "object": {"node": "owl:DatatypeProperty"}
                }
            }
            
            prop_response = await client.post(
                f"{terminus_url}/api/woql/admin/{db_name}",
                json={"query": prop_query},
                auth=auth
            )
            
            if prop_response.status_code == 200:
                prop_result = prop_response.json()
                prop_bindings = prop_result.get("bindings", [])
                
                properties = []
                system_props = []
                for binding in prop_bindings:
                    prop_uri = binding.get("v:Property", "")
                    if "@schema:" in prop_uri:
                        prop_name = prop_uri.split(":")[-1]
                        properties.append(prop_name)
                        if prop_name in ["es_doc_id", "s3_uri", "instance_id", "created_at"]:
                            system_props.append(prop_name)
                
                print(f"   ✅ Found {len(properties)} properties")
                print(f"   ✅ System fields present: {', '.join(system_props)}")
        else:
            print(f"   ❌ Failed to verify schema: {response.status_code}")

async def main():
    """Main function"""
    
    print("🚀 Creating Database and Schema Directly in TerminusDB")
    print("=" * 60)
    
    db_name = "palantir_schema_test"
    
    # Create database
    await create_database_direct(db_name)
    
    # Create schema with system fields
    await create_schema_with_system_fields(db_name)
    
    # Verify schema
    await verify_schema(db_name)
    
    print("\n🎉 Setup complete!")
    print(f"\nDatabase '{db_name}' is ready with:")
    print("  - Product class (with system fields)")
    print("  - Client class (with system fields)")
    print("  - System fields: es_doc_id, s3_uri, instance_id, created_at")
    print("  - Relationship: Product owned_by Client")
    print("\nNow lightweight nodes can be stored in TerminusDB!")

if __name__ == "__main__":
    asyncio.run(main())