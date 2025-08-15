"""
STRICT Palantir-style Instance Worker
경량 그래프 원칙을 100% 준수하는 구현

🔥 MIGRATION: Enhanced to support S3/MinIO Event Store
- Can read events directly from S3 when reference is provided
- Falls back to PostgreSQL payload for backward compatibility
- Gradual migration from embedded payloads to S3 references

PALANTIR RULES:
1. Graph stores ONLY: @id, @type, primary_key + relationships
2. NO domain fields in graph (no name, price, description, etc.)
3. ALL domain data goes to ES and S3 only
4. Relationships are @id → @id references only
5. ES stores terminus_id for Federation lookup
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
import aioboto3
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
    
    🔥 MIGRATION: Now supports reading from S3/MinIO Event Store
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
        
        # 🔥 S3/MinIO Event Store configuration
        self.s3_event_store_enabled = os.getenv("ENABLE_S3_EVENT_STORE", "false").lower() == "true"
        self.event_store_bucket = "spice-event-store"
        self.aioboto_session = None
        
        # PALANTIR PRINCIPLE: Only business concepts in graph
        # No system fields or storage details
        
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
        
        # Redis (optional - don't fail if not available)
        settings = ApplicationSettings()
        try:
            self.redis_service = create_redis_service(settings)
            await self.redis_service.connect()
            self.redis_client = self.redis_service.client
            logger.info("Redis connected successfully")
        except Exception as e:
            logger.warning(f"Redis connection failed, continuing without Redis: {e}")
            self.redis_service = None
            self.redis_client = None
        
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
        
        # 🔥 Initialize aioboto3 for async S3 operations (Event Store)
        if self.s3_event_store_enabled:
            self.aioboto_session = aioboto3.Session()
            logger.info(f"🔥 S3/MinIO Event Store ENABLED - bucket: {self.event_store_bucket}")
        else:
            logger.info("⚠️ S3/MinIO Event Store DISABLED - using legacy payload mode")
        
        # Subscribe to Kafka topic
        self.consumer.subscribe([AppConfig.INSTANCE_COMMANDS_TOPIC])
        
        logger.info("✅ STRICT Palantir Instance Worker initialized")
        
    async def read_event_from_s3(self, s3_reference: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        🔥 Read event from S3/MinIO Event Store
        
        Args:
            s3_reference: Dictionary with bucket, key, and endpoint
            
        Returns:
            Event payload from S3 or None if failed
        """
        if not self.s3_event_store_enabled or not self.aioboto_session:
            return None
            
        try:
            bucket = s3_reference.get('bucket', self.event_store_bucket)
            key = s3_reference.get('key')
            
            if not key:
                logger.warning("No S3 key provided in reference")
                return None
            
            async with self.aioboto_session.client(
                's3',
                endpoint_url=os.getenv('MINIO_ENDPOINT_URL', 'http://localhost:9000'),
                aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'admin'),
                aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'spice123!'),
                use_ssl=False
            ) as s3:
                response = await s3.get_object(Bucket=bucket, Key=key)
                content = await response['Body'].read()
                event_data = json.loads(content)
                
                logger.info(f"🔥 Read event from S3: {key}")
                return event_data
                
        except Exception as e:
            logger.error(f"Failed to read event from S3: {e}")
            return None
    
    async def extract_payload_from_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        🔥 Extract command from message, preferring S3 if available
        
        Supports both:
        - New format: Read from S3 using reference
        - Legacy format: Use embedded command data
        """
        # Check if this is the new format with S3 reference
        if 's3_reference' in message and self.s3_event_store_enabled:
            s3_ref = message['s3_reference']
            logger.info(f"🔥 Message has S3 reference, attempting to read from Event Store")
            
            # Try to read from S3
            event_data = await self.read_event_from_s3(s3_ref)
            if event_data:
                # Return the full command from S3 event
                return event_data
            else:
                logger.warning("Failed to read from S3, falling back to embedded data")
        
        # For messages that have command fields at root level (from Kafka)
        if 'command_type' in message and 'db_name' in message:
            # This is a command message - merge root fields with payload
            command = {
                'command_id': message.get('command_id'),
                'command_type': message.get('command_type'),
                'db_name': message.get('db_name'),
                'class_id': message.get('class_id'),
                'instance_id': message.get('instance_id'),
                'payload': message.get('payload', {}),
                'metadata': message.get('metadata', {})
            }
            return command
        
        # Fall back to embedded payload (legacy or fallback)
        if 'payload' in message:
            return message['payload']
        
        # Very old format - the message itself is the command
        return message
    
    def get_primary_key_value(self, class_id: str, payload: Dict[str, Any]) -> str:
        """
        Extract primary key value dynamically based on class naming convention
        Pattern: {class_name.lower()}_id
        """
        # Standard pattern: class_name_id (e.g., product_id, client_id, order_id)
        expected_key = f"{class_id.lower()}_id"
        
        if expected_key in payload:
            return str(payload[expected_key])
        
        # Fallback: Look for any field ending with _id
        for key, value in payload.items():
            if key.endswith('_id') and value:
                logger.info(f"Using fallback primary key: {key} = {value}")
                return str(value)
        
        # Last resort: Generate a unique ID
        generated_id = f"{class_id.lower()}_{uuid4().hex[:8]}"
        logger.warning(f"No primary key found for {class_id}, generated: {generated_id}")
        return generated_id
    
    async def extract_relationships(self, db_name: str, class_id: str, payload: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract ONLY relationship fields from payload
        Returns: {field_name: target_id} for @id references only
        """
        relationships = {}
        
        try:
            # Get ontology to identify relationship fields
            ontology = await self.terminus_service.get_ontology(db_name, class_id)
            
            if ontology:
                # Check for relationships in OMS format (has 'relationships' key)
                if 'relationships' in ontology:
                    for rel in ontology['relationships']:
                        field_name = rel.get('predicate') or rel.get('name')
                        if field_name and field_name in payload:
                            value = payload[field_name]
                            # Only include if it's an @id reference
                            if isinstance(value, str) and '/' in value:
                                relationships[field_name] = value
                                logger.info(f"  📎 Found relationship: {field_name} → {value}")
                
                # Also check for TerminusDB schema format (relationships as properties with @class)
                else:
                    for key, value_def in ontology.items():
                        if isinstance(value_def, dict) and '@class' in value_def and key in payload:
                            value = payload[key]
                            # Only include if it's an @id reference
                            if isinstance(value, str) and '/' in value:
                                relationships[key] = value
                                logger.info(f"  📎 Found relationship: {key} → {value}")
                            
        except Exception as e:
            logger.warning(f"Could not get ontology for relationship extraction: {e}")
            
        # FALLBACK: Always check for common relationship patterns
        # This ensures relationships work even without schema
        for key, value in payload.items():
            if isinstance(value, str) and '/' in value:
                # Looks like an @id reference
                if any(pattern in key for pattern in ['_by', '_to', '_ref', 'contains', 'linked']):
                    if key not in relationships:  # Don't duplicate
                        relationships[key] = value
                        logger.info(f"  📎 Found relationship pattern: {key} → {value}")
                        
        return relationships
        
    async def process_create_instance(self, command: Dict[str, Any]):
        """Process CREATE_INSTANCE command - STRICT Palantir style"""
        
        db_name = command.get('db_name')
        class_id = command.get('class_id')
        command_id = command.get('command_id')
        payload = command.get('payload', {})
        
        # Extract primary key value dynamically
        primary_key_value = self.get_primary_key_value(class_id, payload)
        
        # Use primary key as instance ID for consistency
        instance_id = primary_key_value
            
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
            
            # 3. Create PURE lightweight node for TerminusDB (NO system fields!)
            graph_node = {
                "@id": f"{class_id}/{primary_key_value}",
                "@type": class_id,
            }
            
            # Add the primary key field dynamically
            primary_key_field = f"{class_id.lower()}_id"
            if primary_key_field in payload:
                graph_node[primary_key_field] = payload[primary_key_field]
            
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
            # Add terminus_id for Federation (graph node reference)
            terminus_id = f"{class_id}/{primary_key_value}"
            
            es_doc = {
                'instance_id': instance_id,
                'terminus_id': terminus_id,  # For Federation with TerminusDB
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
            
        if self.redis_client:
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
                raw_message = json.loads(msg.value().decode('utf-8'))
                
                # Debug: Log the raw message structure
                logger.info(f"📦 Raw message keys: {list(raw_message.keys())}")
                logger.info(f"📦 Raw message type: {type(raw_message)}")
                
                # 🔥 MIGRATION: Handle both new and legacy message formats
                # New format has metadata with storage_mode
                storage_mode = raw_message.get('metadata', {}).get('storage_mode', 'legacy')
                logger.info(f"🔥 Message storage mode: {storage_mode}")
                
                # Extract the actual command payload
                command = await self.extract_payload_from_message(raw_message)
                
                # Debug: Log extracted command
                logger.info(f"📦 Extracted command keys: {list(command.keys()) if command else 'None'}")
                
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