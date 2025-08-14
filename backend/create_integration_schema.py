#!/usr/bin/env python3
"""
Create IntegrationProduct schema in TerminusDB
"""

import asyncio
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.models.config import ConnectionConfig
from shared.models.ontology import OntologyBase, Property
from oms.services.async_terminus import AsyncTerminusService

async def create_schema():
    # Connect to TerminusDB
    connection_info = ConnectionConfig(
        server_url="http://localhost:6363",
        user="admin",
        account="admin",
        key="spice123!"
    )
    
    service = AsyncTerminusService(connection_info)
    await service.connect()
    
    # Define IntegrationProduct schema
    ontology = OntologyBase(
        id="IntegrationProduct",
        label="Integration Product",
        description="Product schema for integration testing",
        properties=[
            Property(name="instance_id", type="string", label="Instance ID", required=True),
            Property(name="name", type="string", label="Product Name", required=True),
            Property(name="price", type="decimal", label="Price", required=True),
            Property(name="quantity", type="integer", label="Quantity", required=True),
            Property(name="description", type="string", label="Description", required=False),
            Property(name="created_at", type="datetime", label="Created At", required=True),
            Property(name="es_doc_id", type="string", label="Elasticsearch Document ID", required=False),
            Property(name="s3_uri", type="string", label="S3 Storage URI", required=False)
        ],
        relationships=[]
    )
    
    try:
        # Create the ontology
        result = await service.create_ontology("integration_test_db", ontology)
        print(f"✅ Schema created successfully: {result.id}")
        
        # Verify it was created
        ontologies = await service.list_ontologies("integration_test_db")
        for ont in ontologies:
            if ont.get('id') == 'IntegrationProduct':
                print(f"✅ Verified: IntegrationProduct schema exists")
                print(f"   Properties: {len(ont.get('properties', []))}")
                break
        
    except Exception as e:
        print(f"❌ Error creating schema: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await service.disconnect()

if __name__ == "__main__":
    asyncio.run(create_schema())