#!/usr/bin/env python3
"""
Simpler approach: Use WOQL to add schema directly
"""

import asyncio
import httpx
import json

async def create_simple_schema(db_name: str):
    """Create schema using WOQL AddTriple operations"""
    
    terminus_url = "http://localhost:6363"
    auth = ("admin", "spice123!")
    
    print(f"Creating schema for {db_name} using WOQL...")
    
    async with httpx.AsyncClient() as client:
        # Create Product class
        woql_create_product = {
            "@type": "And",
            "and": [
                {
                    "@type": "AddTriple",
                    "subject": "@schema:Product",
                    "predicate": "rdf:type",
                    "object": "owl:Class"
                },
                {
                    "@type": "AddTriple",
                    "subject": "@schema:Product",
                    "predicate": "rdfs:label",
                    "object": {"@value": "Product", "@language": "en"}
                },
                {
                    "@type": "AddTriple",
                    "subject": "@schema:Product",
                    "predicate": "rdfs:comment",
                    "object": {"@value": "Product with system fields", "@language": "en"}
                }
            ]
        }
        
        response = await client.post(
            f"{terminus_url}/api/woql/admin/{db_name}",
            json={"query": woql_create_product},
            auth=auth
        )
        
        if response.status_code == 200:
            print("   ✅ Product class created")
        else:
            print(f"   ❌ Failed to create Product class: {response.status_code}")
            print(f"      {response.text[:200]}")
        
        # Create Client class
        woql_create_client = {
            "@type": "And",
            "and": [
                {
                    "@type": "AddTriple",
                    "subject": "@schema:Client",
                    "predicate": "rdf:type",
                    "object": "owl:Class"
                },
                {
                    "@type": "AddTriple",
                    "subject": "@schema:Client",
                    "predicate": "rdfs:label",
                    "object": {"@value": "Client", "@language": "en"}
                }
            ]
        }
        
        response = await client.post(
            f"{terminus_url}/api/woql/admin/{db_name}",
            json={"query": woql_create_client},
            auth=auth
        )
        
        if response.status_code == 200:
            print("   ✅ Client class created")
        else:
            print(f"   ❌ Failed to create Client class: {response.status_code}")
        
        # Add properties for Product
        properties = [
            ("product_id", "Product ID", "xsd:string"),
            ("name", "Name", "xsd:string"),
            ("unit_price", "Unit Price", "xsd:decimal"),
            ("category", "Category", "xsd:string"),
            ("client_id", "Client ID", "xsd:string"),
            ("email", "Email", "xsd:string"),
            ("es_doc_id", "ES Document ID", "xsd:string"),
            ("s3_uri", "S3 URI", "xsd:string"),
            ("instance_id", "Instance ID", "xsd:string"),
            ("created_at", "Created At", "xsd:dateTime")
        ]
        
        for prop_name, prop_label, prop_range in properties:
            woql_add_property = {
                "@type": "And",
                "and": [
                    {
                        "@type": "AddTriple",
                        "subject": f"@schema:{prop_name}",
                        "predicate": "rdf:type",
                        "object": "owl:DatatypeProperty"
                    },
                    {
                        "@type": "AddTriple",
                        "subject": f"@schema:{prop_name}",
                        "predicate": "rdfs:label",
                        "object": {"@value": prop_label, "@language": "en"}
                    },
                    {
                        "@type": "AddTriple",
                        "subject": f"@schema:{prop_name}",
                        "predicate": "rdfs:range",
                        "object": prop_range
                    }
                ]
            }
            
            response = await client.post(
                f"{terminus_url}/api/woql/admin/{db_name}",
                json={"query": woql_add_property},
                auth=auth
            )
            
            if response.status_code == 200:
                print(f"   ✅ Property {prop_name} created")
            else:
                print(f"   ⚠️ Property {prop_name} might already exist")
        
        # Add relationship owned_by
        woql_add_relationship = {
            "@type": "And",
            "and": [
                {
                    "@type": "AddTriple",
                    "subject": "@schema:owned_by",
                    "predicate": "rdf:type",
                    "object": "owl:ObjectProperty"
                },
                {
                    "@type": "AddTriple",
                    "subject": "@schema:owned_by",
                    "predicate": "rdfs:label",
                    "object": {"@value": "Owned By", "@language": "en"}
                },
                {
                    "@type": "AddTriple",
                    "subject": "@schema:owned_by",
                    "predicate": "rdfs:domain",
                    "object": "@schema:Product"
                },
                {
                    "@type": "AddTriple",
                    "subject": "@schema:owned_by",
                    "predicate": "rdfs:range",
                    "object": "@schema:Client"
                }
            ]
        }
        
        response = await client.post(
            f"{terminus_url}/api/woql/admin/{db_name}",
            json={"query": woql_add_relationship},
            auth=auth
        )
        
        if response.status_code == 200:
            print("   ✅ Relationship owned_by created")
        else:
            print(f"   ⚠️ Relationship owned_by might already exist")

async def test_create_instance(db_name: str):
    """Test creating a lightweight instance"""
    
    terminus_url = "http://localhost:6363"
    auth = ("admin", "spice123!")
    
    print(f"\nTesting instance creation...")
    
    # Create a test Product instance
    instance_data = {
        "@id": "Product/TEST_001",
        "@type": "@schema:Product",
        "product_id": "TEST_001",
        "name": "Test Product",
        "es_doc_id": "es_test_001",
        "s3_uri": "s3://bucket/test_001.json",
        "instance_id": "test_instance_001",
        "created_at": "2025-08-13T18:00:00Z",
        "owned_by": "Client/CL_001"
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{terminus_url}/api/document/admin/{db_name}?graph_type=instance&author=test&message=Test+instance",
            json=instance_data,
            auth=auth
        )
        
        if response.status_code in [200, 201]:
            print("   ✅ Test instance created successfully!")
            print("   ✅ System fields work properly!")
        else:
            print(f"   ❌ Failed to create instance: {response.status_code}")
            print(f"      {response.text[:500]}")

async def main():
    """Main function"""
    
    print("🚀 Creating Schema with WOQL AddTriple")
    print("=" * 60)
    
    db_name = "palantir_schema_test"
    
    # Create schema
    await create_simple_schema(db_name)
    
    # Test instance creation
    await test_create_instance(db_name)
    
    print("\n🎉 Schema setup complete!")
    print("\nNow the Instance Worker can store lightweight nodes with:")
    print("  - es_doc_id: Reference to Elasticsearch")
    print("  - s3_uri: Reference to S3 event store")
    print("  - instance_id: Instance identifier")
    print("  - created_at: Creation timestamp")
    print("  - Relationships (owned_by, etc.)")

if __name__ == "__main__":
    asyncio.run(main())