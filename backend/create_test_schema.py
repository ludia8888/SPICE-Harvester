#!/usr/bin/env python3
"""
Create test database and schema with system fields
"""

import asyncio
import aiohttp
import json

async def create_test_environment():
    """Create test database with proper schema including system fields"""
    
    print("🚀 Creating Test Database with System Fields")
    print("=" * 60)
    
    async with aiohttp.ClientSession() as session:
        # 1. Create test database
        db_name = "palantir_test_db"
        print(f"\n1️⃣ Creating database: {db_name}")
        
        async with session.post(
            f"http://localhost:8000/api/v1/database/create",
            json={
                "name": db_name,
                "description": "Test database for Palantir architecture",
                "label": "Palantir Test DB"
            }
        ) as resp:
            if resp.status in [200, 202]:
                print(f"   ✅ Database created: {db_name}")
            else:
                error = await resp.text()
                print(f"   ⚠️ Database creation: {resp.status} - {error[:100]}")
        
        await asyncio.sleep(3)
        
        # 2. Create Product class with system fields
        print(f"\n2️⃣ Creating Product ontology with system fields...")
        
        product_ontology = {
            "id": "Product",
            "label": "Product",
            "description": "Product with system fields for Palantir architecture",
            "properties": [
                # Domain properties
                {"name": "product_id", "type": "string", "label": "Product ID", "required": True},
                {"name": "name", "type": "string", "label": "Name", "required": True},
                {"name": "unit_price", "type": "decimal", "label": "Unit Price", "required": False},
                {"name": "category", "type": "string", "label": "Category", "required": False},
                
                # System fields for lightweight nodes
                {"name": "es_doc_id", "type": "string", "label": "ES Document ID", "required": False},
                {"name": "s3_uri", "type": "string", "label": "S3 URI", "required": False},
                {"name": "instance_id", "type": "string", "label": "Instance ID", "required": False},
                {"name": "created_at", "type": "datetime", "label": "Created At", "required": False}
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
        
        async with session.post(
            f"http://localhost:8000/api/v1/database/{db_name}/ontology",
            json=product_ontology
        ) as resp:
            if resp.status == 200:
                print(f"   ✅ Product ontology created with system fields")
            else:
                error = await resp.text()
                print(f"   ❌ Failed: {resp.status} - {error[:200]}")
        
        # 3. Create Client class with system fields
        print(f"\n3️⃣ Creating Client ontology with system fields...")
        
        client_ontology = {
            "id": "Client",
            "label": "Client",
            "description": "Client with system fields",
            "properties": [
                # Domain properties
                {"name": "client_id", "type": "string", "label": "Client ID", "required": True},
                {"name": "name", "type": "string", "label": "Name", "required": True},
                {"name": "email", "type": "string", "label": "Email", "required": False},
                
                # System fields
                {"name": "es_doc_id", "type": "string", "label": "ES Document ID", "required": False},
                {"name": "s3_uri", "type": "string", "label": "S3 URI", "required": False},
                {"name": "instance_id", "type": "string", "label": "Instance ID", "required": False},
                {"name": "created_at", "type": "datetime", "label": "Created At", "required": False}
            ]
        }
        
        async with session.post(
            f"http://localhost:8000/api/v1/database/{db_name}/ontology",
            json=client_ontology
        ) as resp:
            if resp.status == 200:
                print(f"   ✅ Client ontology created with system fields")
            else:
                error = await resp.text()
                print(f"   ❌ Failed: {resp.status} - {error[:200]}")
        
        # 4. Verify ontologies
        print(f"\n4️⃣ Verifying ontologies...")
        
        async with session.get(
            f"http://localhost:8000/api/v1/database/{db_name}/ontology"
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                ontologies = result.get("data", [])
                print(f"   ✅ Found {len(ontologies)} ontologies")
                
                for ont in ontologies:
                    class_id = ont.get("id")
                    properties = ont.get("properties", [])
                    system_fields = [p for p in properties if p.get("name") in ["es_doc_id", "s3_uri", "instance_id", "created_at"]]
                    print(f"      - {class_id}: {len(properties)} props ({len(system_fields)} system fields)")
            else:
                print(f"   ❌ Failed to list ontologies: {resp.status}")
    
    print("\n🎉 Test environment created successfully!")
    print(f"\nDatabase: {db_name}")
    print("Classes: Product, Client")
    print("System fields: es_doc_id, s3_uri, instance_id, created_at")
    
    return db_name

if __name__ == "__main__":
    db_name = asyncio.run(create_test_environment())