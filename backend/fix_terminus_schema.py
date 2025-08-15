#!/usr/bin/env python3
"""
Fix TerminusDB schemas to be truly lightweight
Remove domain fields from being required
"""

import asyncio
import httpx
import json

async def fix_schemas():
    print("🔧 FIXING TERMINUS SCHEMAS FOR LIGHTWEIGHT NODES")
    print("=" * 60)
    
    # Test database
    db_name = "full_api_test_1755234156"
    
    # Fix Product schema
    product_schema = {
        "@id": "Product",
        "@type": "Class",
        "@documentation": {
            "@comment": "Product class - lightweight",
            "@properties": {
                "product_id": "Product identifier"
            }
        },
        "product_id": "xsd:string"
        # Remove 'name' and other domain fields from required
    }
    
    # Fix Client schema
    client_schema = {
        "@id": "Client", 
        "@type": "Class",
        "@documentation": {
            "@comment": "Client class - lightweight",
            "@properties": {
                "client_id": "Client identifier"
            }
        },
        "client_id": "xsd:string"
        # Remove 'name' and other domain fields from required
    }
    
    async with httpx.AsyncClient() as client:
        # Update schemas
        for schema in [product_schema, client_schema]:
            response = await client.post(
                f"http://localhost:6363/api/document/admin/{db_name}",
                json=schema,
                params={
                    "graph_type": "schema",
                    "author": "admin",
                    "message": f"Make {schema['@id']} truly lightweight"
                },
                auth=("admin", "spice123!")
            )
            
            if response.status_code == 200:
                print(f"✅ Fixed {schema['@id']} schema")
            else:
                print(f"❌ Failed to fix {schema['@id']}: {response.status_code}")
                print(response.text)

asyncio.run(fix_schemas())