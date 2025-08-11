#!/usr/bin/env python3
"""
Create lightweight graph schema in TerminusDB
THINK ULTRA ULTRA ULTRA - Graph Authority 복구

This script adds lightweight instance fields to ontology classes
to enable proper graph traversal while keeping payload in ES/S3
"""

import asyncio
import os
import sys
from typing import Dict, Any, List

# Add backend to path
sys.path.insert(0, "/Users/isihyeon/Desktop/SPICE HARVESTER/backend")

# Set environment variables
os.environ["TERMINUS_SERVER_URL"] = "http://localhost:6363"
os.environ["TERMINUS_USER"] = "admin"
os.environ["TERMINUS_ACCOUNT"] = "admin"
os.environ["TERMINUS_KEY"] = "admin"

from oms.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig
from shared.models.ontology import OntologyBase, Property, Relationship

# Logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def create_lightweight_ontologies():
    """Create ontology classes with lightweight instance fields"""
    
    # Connect to TerminusDB
    connection_info = ConnectionConfig(
        server_url="http://localhost:6363",
        user="admin",
        account="admin",
        key="admin"
    )
    
    service = AsyncTerminusService(connection_info)
    await service.connect()
    logger.info("✅ Connected to TerminusDB")
    
    # Database name from previous tests
    db_name = "fixed_test_20250811_144928"
    
    # Check if database exists, create if not
    databases = await service.list_databases()
    db_exists = any(db.get('name') == db_name for db in databases)
    
    if not db_exists:
        await service.create_database(db_name, "Lightweight Graph Test DB")
        logger.info(f"✅ Created database: {db_name}")
    else:
        logger.info(f"✅ Using existing database: {db_name}")
    
    # 1. Client ontology with lightweight fields
    client_ontology = OntologyBase(
        id="Client",
        label="Client (Lightweight)",
        description="Client with graph authority fields",
        properties=[
            # Core identity
            Property(name="instance_id", type="string", label="Instance ID", required=True),
            Property(name="es_doc_id", type="string", label="ES Document ID", required=True),
            Property(name="s3_uri", type="string", label="S3 URI", required=False),
            
            # Minimal business fields for graph queries
            Property(name="client_id", type="string", label="Client ID", required=True),
            Property(name="name", type="string", label="Client Name", required=False),
            
            # Metadata
            Property(name="created_at", type="datetime", label="Created At", required=False),
            Property(name="updated_at", type="datetime", label="Updated At", required=False)
        ],
        relationships=[]  # Clients don't own anything in this simple model
    )
    
    # 2. Product ontology with lightweight fields and relationships
    product_ontology = OntologyBase(
        id="Product",
        label="Product (Lightweight)",
        description="Product with graph authority fields and relationships",
        properties=[
            # Core identity
            Property(name="instance_id", type="string", label="Instance ID", required=True),
            Property(name="es_doc_id", type="string", label="ES Document ID", required=True),
            Property(name="s3_uri", type="string", label="S3 URI", required=False),
            
            # Minimal business fields
            Property(name="product_id", type="string", label="Product ID", required=True),
            Property(name="name", type="string", label="Product Name", required=False),
            
            # Metadata
            Property(name="created_at", type="datetime", label="Created At", required=False),
            Property(name="updated_at", type="datetime", label="Updated At", required=False)
        ],
        relationships=[
            # The key relationship for multi-hop
            Relationship(
                predicate="owned_by",
                label="Owned By",
                description="Product is owned by a Client",
                target="Client",
                cardinality="n:1"  # Many products to one client
            )
        ]
    )
    
    # 3. SKU ontology for 3-hop testing
    sku_ontology = OntologyBase(
        id="SKU",
        label="SKU (Lightweight)",
        description="Stock Keeping Unit with graph relationships",
        properties=[
            # Core identity
            Property(name="instance_id", type="string", label="Instance ID", required=True),
            Property(name="es_doc_id", type="string", label="ES Document ID", required=True),
            Property(name="s3_uri", type="string", label="S3 URI", required=False),
            
            # Minimal business fields
            Property(name="sku_id", type="string", label="SKU ID", required=True),
            Property(name="quantity", type="integer", label="Quantity", required=False),
            
            # Metadata
            Property(name="created_at", type="datetime", label="Created At", required=False),
            Property(name="updated_at", type="datetime", label="Updated At", required=False)
        ],
        relationships=[
            Relationship(
                predicate="belongs_to",
                label="Belongs To",
                description="SKU belongs to a Product",
                target="Product",
                cardinality="n:1"
            )
        ]
    )
    
    # 4. Supplier ontology for complex relationships
    supplier_ontology = OntologyBase(
        id="Supplier",
        label="Supplier (Lightweight)",
        description="Supplier with graph relationships",
        properties=[
            # Core identity
            Property(name="instance_id", type="string", label="Instance ID", required=True),
            Property(name="es_doc_id", type="string", label="ES Document ID", required=True),
            Property(name="s3_uri", type="string", label="S3 URI", required=False),
            
            # Minimal business fields
            Property(name="supplier_id", type="string", label="Supplier ID", required=True),
            Property(name="name", type="string", label="Supplier Name", required=False),
            
            # Metadata
            Property(name="created_at", type="datetime", label="Created At", required=False),
            Property(name="updated_at", type="datetime", label="Updated At", required=False)
        ],
        relationships=[
            Relationship(
                predicate="supplies",
                label="Supplies",
                description="Supplier supplies Products",
                target="Product",
                cardinality="n:n"  # Many to many
            )
        ]
    )
    
    # Create all ontologies
    ontologies = [
        ("Client", client_ontology),
        ("Product", product_ontology),
        ("SKU", sku_ontology),
        ("Supplier", supplier_ontology)
    ]
    
    for name, ontology in ontologies:
        try:
            result = await service.create_ontology(db_name, ontology)
            logger.info(f"✅ Created ontology: {name}")
            logger.info(f"   Properties: {len(ontology.properties)}")
            logger.info(f"   Relationships: {len(ontology.relationships)}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"⚠️  Ontology {name} already exists, skipping")
            else:
                logger.error(f"❌ Failed to create {name}: {e}")
    
    # List all ontologies to verify
    logger.info("\n📊 Verifying ontologies in database...")
    ontologies_list = await service.list_ontologies(db_name)
    
    for ont in ontologies_list:
        if ont.get('id') in ['Client', 'Product', 'SKU', 'Supplier']:
            logger.info(f"\n📌 {ont.get('id')}:")
            logger.info(f"   Label: {ont.get('label')}")
            
            # Check for our special fields
            properties = ont.get('properties', [])
            special_fields = ['instance_id', 'es_doc_id', 's3_uri']
            found_fields = [p['name'] for p in properties if p['name'] in special_fields]
            
            if found_fields:
                logger.info(f"   ✅ Lightweight fields: {', '.join(found_fields)}")
            else:
                logger.info(f"   ⚠️  No lightweight fields found")
            
            # Check relationships
            relationships = ont.get('relationships', [])
            if relationships:
                for rel in relationships:
                    logger.info(f"   → Relationship: {rel.get('predicate')} -> {rel.get('target')}")
    
    await service.disconnect()
    logger.info("\n✅ Lightweight graph schema setup complete!")
    
    return True


async def main():
    """Main execution"""
    logger.info("🔥 THINK ULTRA³: Setting up Graph Authority")
    logger.info("=" * 60)
    logger.info("Creating lightweight instance schema in TerminusDB")
    logger.info("This enables proper graph traversal with ES/S3 federation")
    logger.info("=" * 60)
    
    success = await create_lightweight_ontologies()
    
    if success:
        logger.info("\n🎯 Next steps:")
        logger.info("1. Modify Instance Worker to write lightweight nodes to TerminusDB")
        logger.info("2. Create GraphFederationService for WOQL + ES joins")
        logger.info("3. Test multi-hop queries with proper graph authority")
    
    return success


if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result else 1)