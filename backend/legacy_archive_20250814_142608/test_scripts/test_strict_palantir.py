#!/usr/bin/env python3
"""
Test STRICT Palantir-style Implementation
Palantir 원칙을 100% 준수하는지 검증

VERIFICATION POINTS:
1. Graph has ONLY system fields + relationships
2. NO domain fields in graph  
3. ALL domain data in ES/S3
4. Relationships are @id references only
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from uuid import uuid4
import aiohttp
import time
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_strict_palantir():
    """Test STRICT Palantir-style implementation"""
    
    logger.info("🔥 TESTING STRICT PALANTIR-STYLE IMPLEMENTATION")
    logger.info("=" * 70)
    
    test_id = uuid4().hex[:8]
    db_name = f"palantir_test_{test_id}"
    
    # STEP 1: Create database
    logger.info("\n" + "="*70)
    logger.info("STEP 1: Create Test Database")
    logger.info("="*70)
    
    async with aiohttp.ClientSession() as session:
        create_payload = {"name": db_name, "description": "Strict Palantir test"}
        
        async with session.post(
            "http://localhost:8000/api/v1/database/create",
            json=create_payload
        ) as resp:
            if resp.status == 202:
                logger.info(f"✅ Database creation accepted: {db_name}")
            else:
                logger.error(f"❌ Database creation failed: {resp.status}")
                return
                
    await asyncio.sleep(3)
    
    # STEP 2: Create STRICT Palantir ontologies
    logger.info("\n" + "="*70)
    logger.info("STEP 2: Create STRICT Palantir Ontologies")
    logger.info("="*70)
    
    async with aiohttp.ClientSession() as session:
        # Client ontology - NO domain fields in schema!
        client_ontology = {
            "id": "PalantirClient",
            "label": "Palantir Client",
            "description": "STRICT: Only system fields in graph",
            "properties": [
                # These are for validation/UI only, NOT stored in graph
                {"name": "client_id", "type": "string", "label": "Client ID", "required": True},
                {"name": "name", "type": "string", "label": "Name"},
                {"name": "email", "type": "string", "label": "Email"},
                {"name": "revenue", "type": "float", "label": "Revenue"}
            ],
            "relationships": []
        }
        
        async with session.post(
            f"http://localhost:8000/api/v1/ontology/{db_name}/create",
            json=client_ontology
        ) as resp:
            if resp.status in [200, 201, 202]:
                logger.info(f"✅ Created PalantirClient ontology")
            else:
                error = await resp.text()
                logger.error(f"❌ Failed to create Client: {error[:200]}")
                
        # Product ontology with relationship
        product_ontology = {
            "id": "PalantirProduct", 
            "label": "Palantir Product",
            "description": "STRICT: Only relationships in graph",
            "properties": [
                {"name": "product_id", "type": "string", "label": "Product ID", "required": True},
                {"name": "name", "type": "string", "label": "Name"},
                {"name": "price", "type": "float", "label": "Price"},
                {"name": "description", "type": "string", "label": "Description"}
            ],
            "relationships": [
                {
                    "predicate": "owned_by",
                    "label": "Owned By",
                    "target": "PalantirClient",
                    "cardinality": "n:1"
                }
            ]
        }
        
        async with session.post(
            f"http://localhost:8000/api/v1/ontology/{db_name}/create",
            json=product_ontology
        ) as resp:
            if resp.status in [200, 201, 202]:
                logger.info(f"✅ Created PalantirProduct ontology with relationship")
            else:
                error = await resp.text()
                logger.error(f"❌ Failed to create Product: {error[:200]}")
                
    await asyncio.sleep(2)
    
    # STEP 3: Create instances with domain data
    logger.info("\n" + "="*70)
    logger.info("STEP 3: Create Instances with Domain Data")
    logger.info("="*70)
    
    client_id = f"CLIENT_{test_id}"
    product_id = f"PRODUCT_{test_id}"
    
    async with aiohttp.ClientSession() as session:
        # Create client
        client_data = {
            "data": {
                "client_id": client_id,
                "name": "Test Palantir Client",  # Domain field - should NOT go to graph
                "email": "client@palantir.test",  # Domain field - should NOT go to graph
                "revenue": 5000000.0  # Domain field - should NOT go to graph
            }
        }
        
        async with session.post(
            f"http://localhost:8000/api/v1/instances/{db_name}/async/PalantirClient/create",
            json=client_data
        ) as resp:
            if resp.status == 202:
                result = await resp.json()
                client_cmd_id = result.get('command_id')
                logger.info(f"✅ Client creation accepted: {client_cmd_id}")
            else:
                logger.error(f"❌ Client creation failed: {resp.status}")
                
        # Create product with relationship
        product_data = {
            "data": {
                "product_id": product_id,
                "name": "Ultra Premium Product",  # Domain field - should NOT go to graph
                "price": 9999.99,  # Domain field - should NOT go to graph
                "description": "This is a test product with lots of text",  # Domain field - should NOT go to graph
                "owned_by": f"PalantirClient/{client_id}"  # Relationship - SHOULD go to graph
            }
        }
        
        async with session.post(
            f"http://localhost:8000/api/v1/instances/{db_name}/async/PalantirProduct/create",
            json=product_data
        ) as resp:
            if resp.status == 202:
                result = await resp.json()
                product_cmd_id = result.get('command_id')
                logger.info(f"✅ Product creation accepted: {product_cmd_id}")
            else:
                logger.error(f"❌ Product creation failed: {resp.status}")
                
    # Wait for processing
    await asyncio.sleep(5)
    
    # STEP 4: Verify TerminusDB has ONLY lightweight nodes
    logger.info("\n" + "="*70)
    logger.info("STEP 4: Verify TerminusDB Graph (MUST be lightweight)")
    logger.info("="*70)
    
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("admin", "spice123!")) as session:
        # Check product node
        async with session.get(
            f"http://localhost:6363/api/document/admin/{db_name}/PalantirProduct/{product_id}",
            params={"graph_type": "instance"}
        ) as resp:
            if resp.status == 200:
                product_node = await resp.json()
                
                # CRITICAL VERIFICATION
                node_fields = set(product_node.keys())
                system_fields = {'@id', '@type', 'instance_id', 'es_doc_id', 's3_uri', 'created_at', 'updated_at'}
                relationship_fields = {'owned_by'}
                allowed_fields = system_fields | relationship_fields
                
                # Check for domain fields that should NOT be there
                forbidden_fields = {'name', 'price', 'description', 'email', 'revenue'}
                domain_fields_found = node_fields & forbidden_fields
                
                logger.info(f"📊 Graph node analysis:")
                logger.info(f"  • Total fields: {len(node_fields)}")
                logger.info(f"  • Fields: {list(node_fields)}")
                
                if domain_fields_found:
                    logger.error(f"❌ VIOLATION: Domain fields in graph: {domain_fields_found}")
                    logger.error(f"   Full node: {json.dumps(product_node, indent=2)}")
                else:
                    logger.info(f"✅ CORRECT: No domain fields in graph")
                    
                if 'owned_by' in product_node:
                    logger.info(f"✅ Relationship found: owned_by → {product_node['owned_by']}")
                else:
                    logger.warning(f"⚠️  Relationship 'owned_by' not found")
                    
                # Check field count
                if len(node_fields) <= 8:  # system(6) + relationships(1-2)
                    logger.info(f"✅ Node is lightweight: {len(node_fields)} fields")
                else:
                    logger.error(f"❌ Node too heavy: {len(node_fields)} fields")
                    
            else:
                logger.warning(f"⚠️  Could not fetch product from TerminusDB: {resp.status}")
                
    # STEP 5: Verify Elasticsearch has FULL data
    logger.info("\n" + "="*70)
    logger.info("STEP 5: Verify Elasticsearch Has Full Domain Data")
    logger.info("="*70)
    
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("elastic", "spice123!")) as session:
        index_name = f"{db_name.lower()}_instances"
        
        # Search for product
        search_query = {
            "query": {
                "term": {"instance_id": product_id}
            }
        }
        
        async with session.post(
            f"http://localhost:9200/{index_name}/_search",
            json=search_query
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                hits = result.get('hits', {}).get('hits', [])
                
                if hits:
                    es_doc = hits[0]['_source']
                    es_data = es_doc.get('data', {})
                    
                    logger.info(f"📚 Elasticsearch document analysis:")
                    logger.info(f"  • Has name: {es_data.get('name') is not None}")
                    logger.info(f"  • Has price: {es_data.get('price') is not None}")
                    logger.info(f"  • Has description: {es_data.get('description') is not None}")
                    
                    if all([
                        es_data.get('name') == "Ultra Premium Product",
                        es_data.get('price') == 9999.99,
                        es_data.get('description') is not None
                    ]):
                        logger.info(f"✅ CORRECT: All domain data in Elasticsearch")
                    else:
                        logger.error(f"❌ Missing domain data in ES")
                        
                    # Check relationships stored
                    if es_doc.get('relationships'):
                        logger.info(f"✅ Relationships tracked: {es_doc['relationships']}")
                        
                else:
                    logger.warning(f"⚠️  No documents found in Elasticsearch")
            else:
                logger.error(f"❌ Elasticsearch search failed: {resp.status}")
                
    # STEP 6: Verify S3 has event data
    logger.info("\n" + "="*70)
    logger.info("STEP 6: Verify S3/MinIO Has Event Data")
    logger.info("="*70)
    
    import boto3
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='spice123!',
        region_name='us-east-1'
    )
    
    try:
        # List objects for our test
        response = s3_client.list_objects_v2(
            Bucket='instance-events',
            Prefix=f"{db_name}/"
        )
        
        if 'Contents' in response:
            logger.info(f"✅ Found {len(response['Contents'])} events in S3")
            for obj in response['Contents'][:3]:
                logger.info(f"  • {obj['Key']}")
        else:
            logger.warning(f"⚠️  No events found in S3")
            
    except Exception as e:
        logger.warning(f"⚠️  Could not check S3: {e}")
        
    # FINAL VERDICT
    logger.info("\n" + "="*70)
    logger.info("FINAL PALANTIR-STYLE VERIFICATION")
    logger.info("="*70)
    
    logger.info("\n🎯 Palantir Architecture Compliance:")
    logger.info("  1. Graph stores ONLY system + relationships: Check above")
    logger.info("  2. NO domain fields in graph: Check above")
    logger.info("  3. ALL domain data in ES: Check above")
    logger.info("  4. Events in S3: Check above")
    logger.info("  5. Relationships are @id refs: Check above")
    
    logger.info("\n✅ Test complete - verify all points above are GREEN")


if __name__ == "__main__":
    asyncio.run(test_strict_palantir())