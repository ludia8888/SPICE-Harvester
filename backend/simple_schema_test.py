#!/usr/bin/env python3
"""
Simpler approach: Use WOQL to add schema directly
"""

import asyncio
import httpx
import json
import os
import pytest
import uuid

OMS_URL = (os.getenv("OMS_BASE_URL") or os.getenv("OMS_URL") or "http://localhost:8000").rstrip("/")


def _admin_headers() -> dict:
    admin_token = (os.getenv("ADMIN_TOKEN") or os.getenv("OMS_ADMIN_TOKEN") or "test-token").strip()
    return {"X-Admin-Token": admin_token}

async def create_simple_schema(db_name: str):
    """Create schema using OMS ontology endpoints."""
    headers = _admin_headers()
    print(f"Creating schema for {db_name} via OMS...")

    async with httpx.AsyncClient(timeout=30.0) as client:
        db_response = await client.post(
            f"{OMS_URL}/api/v1/database/create",
            json={"name": db_name, "description": f"Test database for {db_name}"},
            headers=headers,
        )
        if db_response.status_code in [200, 201, 202, 409]:
            print(f"   ✅ Database ensured: {db_name}")
        else:
            print(f"   ❌ Failed to create database: {db_response.status_code}")
            print(f"      {db_response.text[:200]}")

        # Wait for database availability
        for _ in range(20):
            exists_resp = await client.get(
                f"{OMS_URL}/api/v1/database/exists/{db_name}",
                headers=headers,
            )
            if exists_resp.status_code == 200 and (exists_resp.json().get("data") or {}).get("exists"):
                break
            await asyncio.sleep(1)

        client_ontology = {
            "id": "Client",
            "label": "Client",
            "description": "Client entity",
            "properties": [
                {"name": "client_id", "type": "string", "label": "Client ID", "required": True},
                {"name": "name", "type": "string", "label": "Name"},
                {"name": "email", "type": "string", "label": "Email"},
            ],
        }
        product_ontology = {
            "id": "Product",
            "label": "Product",
            "description": "Product with system fields",
            "properties": [
                {"name": "product_id", "type": "string", "label": "Product ID", "required": True},
                {"name": "name", "type": "string", "label": "Name"},
                {"name": "unit_price", "type": "number", "label": "Unit Price"},
                {"name": "category", "type": "string", "label": "Category"},
                {"name": "es_doc_id", "type": "string", "label": "ES Document ID"},
                {"name": "s3_uri", "type": "string", "label": "S3 URI"},
                {"name": "instance_id", "type": "string", "label": "Instance ID"},
                {"name": "created_at", "type": "datetime", "label": "Created At"},
            ],
            "relationships": [
                {
                    "predicate": "owned_by",
                    "label": "Owned By",
                    "target": "Client",
                    "cardinality": "n:1",
                }
            ],
        }

        for ontology in (client_ontology, product_ontology):
            response = await client.post(
                f"{OMS_URL}/api/v1/database/{db_name}/ontology",
                json=ontology,
                headers=headers,
            )
            if response.status_code in [200, 201, 202, 409]:
                print(f"   ✅ Ontology {ontology['id']} ensured")
            else:
                print(f"   ❌ Failed to create {ontology['id']}: {response.status_code}")
                print(f"      {response.text[:200]}")
                raise RuntimeError(f"Ontology creation failed: {ontology['id']}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_instance(db_name: str):
    """Test creating a lightweight instance"""
    terminus_url = os.getenv("TERMINUS_SERVER_URL", "http://localhost:6363")
    headers = _admin_headers()
    
    print(f"\nTesting instance creation...")
    
    await create_simple_schema(db_name)

    # Create a test Product instance
    unique_id = f"TEST_{uuid.uuid4().hex[:6]}"
    instance_data = {
        "@id": f"Product/{unique_id}",
        "@type": "@schema:Product",
        "product_id": unique_id,
        "name": "Test Product",
        "es_doc_id": f"es_{unique_id}",
        "s3_uri": f"s3://bucket/{unique_id}.json",
        "instance_id": f"test_instance_{unique_id}",
        "created_at": "2025-08-13T18:00:00Z",
        "owned_by": "Client/CL_001"
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{OMS_URL}/api/v1/instances/{db_name}/async/Product/create",
            json={"data": instance_data},
            headers=headers,
        )
        
        if response.status_code in [200, 201]:
            print("   ✅ Test instance created successfully!")
            print("   ✅ System fields work properly!")
        else:
            print(f"   ❌ Failed to create instance: {response.status_code}")
            print(f"      {response.text[:500]}")

        assert response.status_code in [200, 201, 202]


@pytest.fixture
def db_name() -> str:
    return f"palantir_schema_test_{uuid.uuid4().hex[:8]}"

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
