"""
STRICT Palantir-style Instance Worker
경량 그래프 원칙을 100% 준수하는 구현

PALANTIR RULES:
1. Graph stores ONLY: @id, @type, instance_id, es_doc_id, s3_uri, created_at + relationships
2. NO domain fields in graph (no name, price, description, etc.)
3. ALL domain data goes to ES and S3 only
4. Relationships are @id → @id references only
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Set
from uuid import uuid4
import hashlib

from confluent_kafka import Consumer, Producer, KafkaError
import redis
import boto3
from elasticsearch import AsyncElasticsearch

from shared.config.service_config import ServiceConfig
from shared.config.app_config import AppConfig
from shared.services.redis_service import create_redis_service
from shared.services.elasticsearch_service import create_elasticsearch_service
from shared.config.settings import ApplicationSettings

# ULTRA CRITICAL: Import TerminusDB service
from oms.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StrictPalantirInstanceWorker:
    """
    STRICT Palantir-style Instance Worker
    Graph는 관계와 참조만, 데이터는 ES/S3에만
    """
    
    def __init__(self):
        self.running = False
        self.kafka_servers = ServiceConfig.get_kafka_bootstrap_servers()
        self.consumer = None
        self.producer = None
        self.redis_client = None
        self.s3_client = None
        self.es_client = None
        self.terminus_service = None
        self.instance_bucket = AppConfig.INSTANCE_BUCKET
        
        # PALANTIR SYSTEM FIELDS (ONLY these go to graph)
        self.SYSTEM_FIELDS = {
            '@id', '@type', 'instance_id', 'es_doc_id', 
            's3_uri', 'created_at', 'updated_at'
        }
        
    async def initialize(self):
        """Initialize all connections"""
        logger.info("Initializing STRICT Palantir Instance Worker...")
        
        # Kafka Consumer - using new group to read from beginning
        import time
        group_id = f'strict-palantir-worker-{int(time.time())}'  # Unique group each time
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # Read from beginning
            'enable.auto.commit': True,
        })
        logger.info(f"Using consumer group: {group_id}")
        
        # Kafka Producer for events
        self.producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'client.id': 'strict-palantir-instance-worker-producer',
        })
        
        # Redis
        settings = ApplicationSettings()
        self.redis_service = create_redis_service(settings)
        await self.redis_service.connect()
        self.redis_client = self.redis_service.client
        
        # S3/MinIO
        self.s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT_URL', 'http://localhost:9000'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'admin'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'spice123!'),
            region_name='us-east-1'
        )
        
        # Ensure bucket exists
        try:
            self.s3_client.head_bucket(Bucket=self.instance_bucket)
        except:
            self.s3_client.create_bucket(Bucket=self.instance_bucket)
            
        # Elasticsearch - create directly with correct auth
        from elasticsearch import AsyncElasticsearch
        self.es_client = AsyncElasticsearch(
            hosts=[f"http://localhost:9200"],
            basic_auth=("elastic", "spice123!"),
            verify_certs=False,
            ssl_show_warn=False
        )
        # Test connection
        info = await self.es_client.info()
        logger.info(f"Connected to Elasticsearch {info['version']['number']}")
        
        # TerminusDB
        connection_info = ConnectionConfig(
            server_url=os.getenv('TERMINUS_SERVER_URL', 'http://localhost:6363'),
            user=os.getenv('TERMINUS_USER', 'admin'),
            account=os.getenv('TERMINUS_ACCOUNT', 'admin'),
            key=os.getenv('TERMINUS_KEY', 'spice123!')
        )
        self.terminus_service = AsyncTerminusService(connection_info)
        await self.terminus_service.connect()
        
        # Subscribe to Kafka topic
        self.consumer.subscribe([AppConfig.INSTANCE_COMMANDS_TOPIC])
        
        logger.info("✅ STRICT Palantir Instance Worker initialized")
        
    async def extract_relationships(self, db_name: str, class_id: str, payload: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract ONLY relationship fields from payload
        Returns: {field_name: target_id} for @id references only
        """
        relationships = {}
        
        try:
            # Get ontology to identify relationship fields
            ontology = await self.terminus_service.get_ontology(db_name, class_id)
            
            if ontology and 'relationships' in ontology:
                for rel in ontology['relationships']:
                    field_name = rel.get('predicate') or rel.get('name')
                    if field_name and field_name in payload:
                        value = payload[field_name]
                        # Only include if it's an @id reference
                        if isinstance(value, str) and '/' in value:
                            relationships[field_name] = value
                            logger.info(f"  📎 Found relationship: {field_name} → {value}")
                            
        except Exception as e:
            logger.warning(f"Could not get ontology for relationship extraction: {e}")
            
            # FALLBACK: Check for common relationship patterns
            # But NEVER include non-reference fields
            for key, value in payload.items():
                if isinstance(value, str) and '/' in value:
                    # Looks like an @id reference
                    if any(pattern in key for pattern in ['_id', '_ref', 'owned_by', 'linked_to']):
                        relationships[key] = value
                        logger.info(f"  📎 Found relationship pattern: {key} → {value}")
                        
        return relationships
        
    async def process_create_instance(self, command: Dict[str, Any]):
        """Process CREATE_INSTANCE command - STRICT Palantir style"""
        
        db_name = command.get('db_name')
        class_id = command.get('class_id')
        command_id = command.get('command_id')
        payload = command.get('payload', {})
        
        # Generate instance ID if not provided
        instance_id = command.get('instance_id') or payload.get('instance_id')
        if not instance_id:
            instance_id = f"{class_id}_inst_{datetime.now().strftime('%H%M%S%f')[:8]}"
            
        logger.info(f"🔷 STRICT Palantir: Creating {class_id}/{instance_id}")
        
        # Set command status
        await self.set_command_status(command_id, 'processing')
        
        try:
            # 1. Save FULL data to S3 (Event Store)
            s3_path = f"{db_name}/{class_id}/{instance_id}/{command_id}.json"
            s3_data = {
                'command': command,
                'payload': payload,
                'instance_id': instance_id,
                'class_id': class_id,
                'created_at': datetime.now(timezone.utc).isoformat()
            }
            
            self.s3_client.put_object(
                Bucket=self.instance_bucket,
                Key=s3_path,
                Body=json.dumps(s3_data, indent=2),
                ContentType='application/json',
                Metadata={
                    'command_id': command_id,
                    'instance_id': instance_id,
                    'class_id': class_id
                }
            )
            logger.info(f"  ✅ Saved to S3: {s3_path}")
            
            # 2. Extract ONLY relationships for graph
            relationships = await self.extract_relationships(db_name, class_id, payload)
            
            # 3. Create STRICT lightweight node for TerminusDB
            graph_node = {
                "@id": f"{class_id}/{instance_id}",
                "@type": class_id,
                "instance_id": instance_id,
                "es_doc_id": instance_id,
                "s3_uri": f"s3://{self.instance_bucket}/{s3_path}",
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            
            # Add ONLY relationships (no domain fields!)
            for rel_field, rel_target in relationships.items():
                graph_node[rel_field] = rel_target
                
            # PALANTIR PRINCIPLE: NO domain attributes in TerminusDB!
            # TerminusDB stores ONLY lightweight nodes (IDs + relationships)
            # All domain data goes to Elasticsearch
            
            # Store lightweight node in TerminusDB for graph traversal
            try:
                # Create instance using TerminusDB service
                # This will store ONLY the lightweight node with relationships
                # Note: The instance_id is already part of graph_node["@id"]
                await self.terminus_service.create_instance(
                    db_name,
                    class_id,
                    graph_node  # Contains only @id, @type, es_doc_id, s3_uri, relationships
                )
                logger.info(f"  ✅ Stored lightweight node in TerminusDB")
                logger.info(f"  📊 Relationships: {list(relationships.keys())}")
            except Exception as e:
                logger.warning(f"  ⚠️ Could not store in TerminusDB: {e}")
                # Continue anyway - ES is primary storage
            
            # 4. Index FULL data in Elasticsearch (PRIMARY storage)
            es_doc = {
                'instance_id': instance_id,
                'class_id': class_id,
                'db_name': db_name,
                'data': payload,  # ALL domain data here
                'relationships': relationships,
                'created_at': datetime.now(timezone.utc).isoformat(),
                's3_uri': f"s3://{self.instance_bucket}/{s3_path}",
                'command_id': command_id
            }
            
            index_name = f"{db_name.lower()}_instances"
            await self.es_client.index(
                index=index_name,
                id=instance_id,
                document=es_doc,
                refresh=True
            )
            logger.info(f"  ✅ Indexed full data in Elasticsearch: {index_name}/{instance_id}")
            
            # 6. Publish event
            event = {
                'event_id': str(uuid4()),
                'event_type': 'INSTANCE_CREATED',
                'aggregate_id': f"{db_name}:{class_id}:{instance_id}",
                'aggregate_type': 'Instance',
                'db_name': db_name,
                'class_id': class_id,
                'instance_id': instance_id,
                'data': {
                    'instance_id': instance_id,
                    'class_id': class_id,
                    **payload  # Include full payload in event
                },
                'occurred_at': datetime.now(timezone.utc).isoformat()
            }
            
            self.producer.produce(
                AppConfig.INSTANCE_EVENTS_TOPIC,
                key=event['aggregate_id'],
                value=json.dumps(event)
            )
            self.producer.flush()
            logger.info(f"  ✅ Published INSTANCE_CREATED event")
            
            # Set success status
            await self.set_command_status(command_id, 'completed', {
                'instance_id': instance_id,
                'es_doc_id': instance_id,
                's3_uri': f"s3://{self.instance_bucket}/{s3_path}"
            })
            
            logger.info(f"✅ STRICT Palantir: Instance created successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to create instance: {e}")
            await self.set_command_status(command_id, 'failed', {'error': str(e)})
            raise
            
    async def set_command_status(self, command_id: str, status: str, result: Dict = None):
        """Set command status in Redis"""
        if not command_id:
            return
            
        status_key = AppConfig.get_command_status_key(command_id)
        status_data = {
            'command_id': command_id,
            'status': status,
            'updated_at': datetime.now(timezone.utc).isoformat()
        }
        
        if result:
            status_data['result'] = result
            
        await self.redis_client.setex(
            status_key,
            3600,  # 1 hour TTL
            json.dumps(status_data)
        )
        
    async def run(self):
        """Main processing loop"""
        self.running = True
        logger.info("🚀 STRICT Palantir Instance Worker started")
        logger.info(f"  Subscribed to topic: {AppConfig.INSTANCE_COMMANDS_TOPIC}")
        
        poll_count = 0
        while self.running:
            msg = self.consumer.poll(timeout=1.0)
            poll_count += 1
            
            if poll_count % 10 == 0:
                logger.info(f"  Polled {poll_count} times, no messages yet...")
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
                    
            try:
                logger.info(f"📨 Received message from Kafka!")
                command = json.loads(msg.value().decode('utf-8'))
                command_type = command.get('command_type')
                
                logger.info(f"Processing command: {command_type}")
                logger.info(f"  Database: {command.get('db_name')}")
                logger.info(f"  Class: {command.get('class_id')}")
                
                if command_type == 'CREATE_INSTANCE':
                    await self.process_create_instance(command)
                elif command_type == 'UPDATE_INSTANCE':
                    # Similar but with update logic
                    pass
                elif command_type == 'DELETE_INSTANCE':
                    # Mark as deleted but keep data (Event Sourcing)
                    pass
                else:
                    logger.warning(f"Unknown command type: {command_type}")
                    
            except Exception as e:
                logger.error(f"Error processing command: {e}")
                
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down STRICT Palantir Instance Worker...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
        if self.redis_client:
            await self.redis_client.close()
        if self.terminus_service:
            await self.terminus_service.close()
        if self.es_client:
            await self.es_client.close()
            

async def main():
    """Main entry point"""
    worker = StrictPalantirInstanceWorker()
    
    try:
        await worker.initialize()
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await worker.shutdown()
        

if __name__ == "__main__":
    asyncio.run(main())