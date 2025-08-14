#!/usr/bin/env python3
"""
Migrate Elasticsearch Data to TerminusDB Lightweight Nodes
THINK ULTRA³ - Graph Authority Migration

This script migrates existing Elasticsearch documents to create
lightweight graph nodes in TerminusDB, establishing proper graph authority.
"""

import asyncio
import aiohttp
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any, List

# Add backend to path
sys.path.insert(0, "/Users/isihyeon/Desktop/SPICE HARVESTER/backend")

# Set environment variables
os.environ["TERMINUS_SERVER_URL"] = "http://localhost:6363"
os.environ["TERMINUS_USER"] = "admin"
os.environ["TERMINUS_ACCOUNT"] = "admin"
os.environ["TERMINUS_KEY"] = "admin"
os.environ["ELASTICSEARCH_HOST"] = "localhost"
os.environ["ELASTICSEARCH_PORT"] = "9201"

from oms.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ESToTerminusMigrator:
    """Migrates ES documents to TerminusDB lightweight nodes"""
    
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.es_url = "http://localhost:9201"
        self.index_name = f"instances_{db_name.replace('-', '_')}"
        self.terminus_service = None
        self.migration_stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0
        }
    
    async def initialize(self):
        """Initialize services"""
        # Connect to TerminusDB
        connection_info = ConnectionConfig(
            server_url="http://localhost:6363",
            user="admin",
            account="admin",
            key="spice123!"
        )
        
        self.terminus_service = AsyncTerminusService(connection_info)
        await self.terminus_service.connect()
        logger.info("✅ Connected to TerminusDB")
    
    async def fetch_all_documents(self) -> List[Dict[str, Any]]:
        """Fetch all documents from Elasticsearch"""
        documents = []
        
        async with aiohttp.ClientSession() as session:
            # Use scroll API for large datasets
            scroll_query = {
                "size": 100,
                "query": {"match_all": {}}
            }
            
            # Initial search
            async with session.post(
                f"{self.es_url}/{self.index_name}/_search?scroll=2m",
                json=scroll_query
            ) as resp:
                if resp.status != 200:
                    error = await resp.text()
                    logger.error(f"Failed to search ES: {error}")
                    return documents
                
                result = await resp.json()
                scroll_id = result.get("_scroll_id")
                hits = result.get("hits", {}).get("hits", [])
                
                for hit in hits:
                    doc = hit["_source"]
                    doc["_es_id"] = hit["_id"]
                    documents.append(doc)
                
                # Continue scrolling
                while hits and scroll_id:
                    async with session.post(
                        f"{self.es_url}/_search/scroll",
                        json={"scroll": "2m", "scroll_id": scroll_id}
                    ) as scroll_resp:
                        if scroll_resp.status != 200:
                            break
                        
                        scroll_result = await scroll_resp.json()
                        hits = scroll_result.get("hits", {}).get("hits", [])
                        
                        for hit in hits:
                            doc = hit["_source"]
                            doc["_es_id"] = hit["_id"]
                            documents.append(doc)
        
        logger.info(f"📊 Fetched {len(documents)} documents from Elasticsearch")
        return documents
    
    async def create_lightweight_node(self, doc: Dict[str, Any]) -> bool:
        """Create a lightweight node in TerminusDB"""
        try:
            class_id = doc.get("class_id") or doc.get("@type")
            instance_id = doc.get("instance_id") or doc.get("@id") or doc.get("_es_id")
            
            if not class_id or not instance_id:
                logger.warning(f"Skipping document without class_id or instance_id: {doc}")
                return False
            
            # Build lightweight node
            lightweight_node = {
                "@id": instance_id,
                "@type": class_id,
                "instance_id": instance_id,
                "es_doc_id": doc.get("_es_id", instance_id),
                "created_at": datetime.utcnow().isoformat()
            }
            
            # Add S3 URI if available
            if doc.get("s3_path"):
                lightweight_node["s3_uri"] = f"s3://instances/{doc['s3_path']}"
            elif doc.get("s3_uri"):
                lightweight_node["s3_uri"] = doc["s3_uri"]
            
            # Extract relationship fields based on class
            relationship_fields = self.get_relationship_fields(class_id)
            
            for field in relationship_fields:
                if field in doc and doc[field]:
                    lightweight_node[field] = doc[field]
                    logger.debug(f"  Added relationship: {field} = {doc[field]}")
            
            # Extract key identifier fields
            key_fields = self.get_key_fields(class_id)
            for field in key_fields:
                if field in doc and doc[field]:
                    lightweight_node[field] = doc[field]
            
            # Create document in TerminusDB
            result = await self.terminus_service.document_service.create_document(
                db_name=self.db_name,
                document=lightweight_node,
                graph_type="instance",
                author="migration_script",
                message=f"Migrated {class_id}/{instance_id} from Elasticsearch"
            )
            
            logger.info(f"✅ Created lightweight node: {class_id}/{instance_id}")
            return True
            
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.debug(f"Node already exists: {instance_id}")
                return True  # Consider as success if already exists
            else:
                logger.error(f"Failed to create node: {e}")
                return False
    
    def get_relationship_fields(self, class_id: str) -> List[str]:
        """Get relationship fields for a class"""
        # This should ideally query the schema, but for now use known relationships
        relationships = {
            "Product": ["owned_by", "supplied_by"],
            "SKU": ["belongs_to"],
            "Supplier": ["supplies"],
            "Client": [],
            "Order": ["client_id", "product_ids"],
            "Return": ["order_id"],
            "Exception": ["batch_id"],
            "Event": []
        }
        return relationships.get(class_id, [])
    
    def get_key_fields(self, class_id: str) -> List[str]:
        """Get key identifier fields for a class"""
        # Key fields that should be stored in graph for quick access
        key_fields = {
            "Product": ["product_id", "name"],
            "SKU": ["sku_id"],
            "Client": ["client_id", "name"],
            "Supplier": ["supplier_id", "name"],
            "Order": ["order_id"],
            "Return": ["return_id"],
            "Exception": ["exception_id"],
            "Event": ["event_id"]
        }
        return key_fields.get(class_id, [])
    
    async def migrate(self):
        """Run the migration"""
        logger.info(f"🔄 Starting migration for database: {self.db_name}")
        
        # Initialize services
        await self.initialize()
        
        # Fetch all documents
        documents = await self.fetch_all_documents()
        self.migration_stats["total"] = len(documents)
        
        if not documents:
            logger.warning("No documents to migrate")
            return
        
        # Group documents by class for better logging
        docs_by_class = {}
        for doc in documents:
            class_id = doc.get("class_id") or doc.get("@type")
            if class_id:
                if class_id not in docs_by_class:
                    docs_by_class[class_id] = []
                docs_by_class[class_id].append(doc)
        
        logger.info(f"📋 Documents by class:")
        for class_id, class_docs in docs_by_class.items():
            logger.info(f"   {class_id}: {len(class_docs)} documents")
        
        # Migrate each document
        for class_id, class_docs in docs_by_class.items():
            logger.info(f"\n🔄 Migrating {class_id} documents...")
            
            for i, doc in enumerate(class_docs, 1):
                success = await self.create_lightweight_node(doc)
                
                if success:
                    self.migration_stats["success"] += 1
                else:
                    self.migration_stats["failed"] += 1
                
                # Progress indicator
                if i % 10 == 0:
                    logger.info(f"   Progress: {i}/{len(class_docs)}")
        
        # Disconnect
        await self.terminus_service.disconnect()
        
        # Report results
        logger.info("\n📊 Migration Complete!")
        logger.info(f"   Total documents: {self.migration_stats['total']}")
        logger.info(f"   ✅ Successful: {self.migration_stats['success']}")
        logger.info(f"   ❌ Failed: {self.migration_stats['failed']}")
        logger.info(f"   ⏭️  Skipped: {self.migration_stats['skipped']}")
        
        success_rate = (self.migration_stats['success'] / self.migration_stats['total'] * 100) if self.migration_stats['total'] > 0 else 0
        logger.info(f"   Success rate: {success_rate:.1f}%")


async def verify_migration(db_name: str):
    """Verify the migration by comparing counts"""
    logger.info("\n🔍 Verifying migration...")
    
    # Connect to TerminusDB
    connection_info = ConnectionConfig(
        server_url="http://localhost:6363",
        user="admin",
        account="admin",
        key="spice123!"
    )
    
    terminus_service = AsyncTerminusService(connection_info)
    await terminus_service.connect()
    
    # Count instances in TerminusDB for each class
    classes = ["Client", "Product", "SKU", "Supplier", "Order", "Return", "Exception", "Event"]
    terminus_counts = {}
    
    for class_id in classes:
        try:
            count = await terminus_service.count_class_instances(db_name, class_id)
            terminus_counts[class_id] = count
            if count > 0:
                logger.info(f"   {class_id}: {count} instances in TerminusDB")
        except Exception as e:
            logger.debug(f"   {class_id}: No instances or error: {e}")
    
    # Count documents in Elasticsearch
    es_url = "http://localhost:9201"
    index_name = f"instances_{db_name.replace('-', '_')}"
    
    async with aiohttp.ClientSession() as session:
        for class_id in classes:
            query = {
                "query": {
                    "term": {"class_id": class_id}
                },
                "size": 0
            }
            
            async with session.post(
                f"{es_url}/{index_name}/_search",
                json=query
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    count = result.get("hits", {}).get("total", {}).get("value", 0)
                    if count > 0:
                        terminus_count = terminus_counts.get(class_id, 0)
                        match = "✅" if count == terminus_count else "⚠️"
                        logger.info(f"   {class_id}: ES={count}, TDB={terminus_count} {match}")
    
    await terminus_service.disconnect()
    logger.info("✅ Verification complete")


async def main():
    """Main execution"""
    logger.info("🔥 THINK ULTRA³: ES to TerminusDB Migration")
    logger.info("=" * 60)
    
    # Database to migrate
    db_name = "fixed_test_20250811_144928"
    
    # Check if database exists
    connection_info = ConnectionConfig(
        server_url="http://localhost:6363",
        user="admin",
        account="admin",
        key="spice123!"
    )
    
    terminus_service = AsyncTerminusService(connection_info)
    await terminus_service.connect()
    
    db_exists = await terminus_service.database_exists(db_name)
    if not db_exists:
        logger.error(f"❌ Database '{db_name}' does not exist in TerminusDB")
        logger.info("Please create the database and ontologies first")
        await terminus_service.disconnect()
        return
    
    await terminus_service.disconnect()
    
    # Run migration
    migrator = ESToTerminusMigrator(db_name)
    await migrator.migrate()
    
    # Verify migration
    await verify_migration(db_name)
    
    logger.info("\n🎯 Next steps:")
    logger.info("1. Test graph queries with migrated data")
    logger.info("2. Run A/B tests comparing direct ES vs graph federation")
    logger.info("3. Monitor performance and consistency")


if __name__ == "__main__":
    asyncio.run(main())