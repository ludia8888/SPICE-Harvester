"""
Kafka Configuration for EOS v2 (Exactly-Once Semantics)
THINK ULTRA³ - Production-ready Kafka settings with transactional guarantees

This module provides centralized Kafka configuration for all workers,
ensuring exactly-once processing semantics and optimal performance.
"""

import os
import uuid
from typing import Dict, Any, Optional
from shared.config.service_config import ServiceConfig


class KafkaEOSConfig:
    """
    Kafka Exactly-Once Semantics v2 Configuration
    
    Key Features:
    - Idempotent producers (automatic deduplication)
    - Transactional message delivery
    - Read-committed isolation for consumers
    - Optimized for throughput with compression
    """
    
    @staticmethod
    def get_producer_config(
        service_name: str,
        instance_id: Optional[str] = None,
        enable_transactions: bool = True
    ) -> Dict[str, Any]:
        """
        Get producer configuration with EOS v2 support
        
        Args:
            service_name: Name of the service (e.g., 'instance-worker')
            instance_id: Unique instance identifier for transactional.id
            enable_transactions: Enable transactional processing
            
        Returns:
            Producer configuration dictionary
        """
        # Generate unique instance ID if not provided
        if instance_id is None:
            instance_id = str(uuid.uuid4())[:8]
        
        config = {
            'bootstrap.servers': ServiceConfig.get_kafka_bootstrap_servers(),
            'client.id': f'{service_name}-producer',
            
            # Durability settings
            'acks': 'all',  # Wait for all in-sync replicas
            'retries': 2147483647,  # Max retries for idempotence
            
            # Idempotence (automatic deduplication)
            'enable.idempotence': True,
            
            # Performance optimization
            'compression.type': 'snappy',
            'linger.ms': 10,  # Batch messages for 10ms
            'batch.size': 16384,  # 16KB batch size
            
            # In-flight requests (max 5 for idempotence)
            'max.in.flight.requests.per.connection': 5,
            
            # Error handling
            'delivery.timeout.ms': 120000,  # 2 minutes total timeout
            'request.timeout.ms': 30000,  # 30 seconds per request
            'transaction.timeout.ms': 300000,  # 5 minutes transaction timeout
        }
        
        # Add transactional configuration if enabled
        if enable_transactions:
            config['transactional.id'] = f'{service_name}-{instance_id}'
        
        return config
    
    @staticmethod
    def get_consumer_config(
        service_name: str,
        group_id: str,
        read_committed: bool = True,
        auto_commit: bool = False
    ) -> Dict[str, Any]:
        """
        Get consumer configuration with EOS v2 support
        
        Args:
            service_name: Name of the service
            group_id: Consumer group ID
            read_committed: Only read committed messages (for EOS)
            auto_commit: Enable automatic offset commits
            
        Returns:
            Consumer configuration dictionary
        """
        config = {
            'bootstrap.servers': ServiceConfig.get_kafka_bootstrap_servers(),
            'group.id': group_id,
            'client.id': f'{service_name}-consumer',
            
            # Offset management
            'enable.auto.commit': auto_commit,
            'auto.offset.reset': 'earliest',
            
            # Session management
            'session.timeout.ms': 45000,  # 45 seconds
            'max.poll.interval.ms': 300000,  # 5 minutes
            'heartbeat.interval.ms': 3000,  # 3 seconds
            
            # Performance
            'fetch.min.bytes': 1,
            'fetch.wait.max.ms': 500,
            
            # Security
            'check.crcs': True,
        }
        
        # Set isolation level for transactional reads
        if read_committed:
            config['isolation.level'] = 'read_committed'
        else:
            config['isolation.level'] = 'read_uncommitted'
        
        return config
    
    @staticmethod
    def get_admin_config() -> Dict[str, Any]:
        """
        Get admin client configuration for topic management
        
        Returns:
            Admin configuration dictionary
        """
        return {
            'bootstrap.servers': ServiceConfig.get_kafka_bootstrap_servers(),
            'client.id': 'admin-client',
            'request.timeout.ms': 30000,
        }
    
    @staticmethod
    def get_topic_config(
        retention_ms: int = 604800000,  # 7 days default
        min_insync_replicas: int = 2,
        replication_factor: int = 3
    ) -> Dict[str, str]:
        """
        Get topic configuration for durability and performance
        
        Args:
            retention_ms: Message retention time in milliseconds
            min_insync_replicas: Minimum in-sync replicas for acks=all
            replication_factor: Number of replicas
            
        Returns:
            Topic configuration dictionary
        """
        return {
            'retention.ms': str(retention_ms),
            'min.insync.replicas': str(min_insync_replicas),
            'compression.type': 'snappy',
            'cleanup.policy': 'delete',
            'segment.ms': str(86400000),  # 1 day segments
            'segment.bytes': str(1073741824),  # 1GB segments
        }


class TransactionalProducer:
    """
    Helper class for transactional message production
    
    Provides high-level methods for transactional operations
    with automatic error handling and retry logic.
    """
    
    def __init__(self, producer, enable_transactions: bool = True):
        """
        Initialize transactional producer wrapper
        
        Args:
            producer: Confluent Kafka producer instance
            enable_transactions: Enable transactional processing
        """
        self.producer = producer
        self.enable_transactions = enable_transactions
        self.transaction_initialized = False
        
    def init_transactions(self, timeout: float = 30.0):
        """
        Initialize transactions (must be called once before any transaction)
        
        Args:
            timeout: Initialization timeout in seconds
        """
        if self.enable_transactions and not self.transaction_initialized:
            self.producer.init_transactions(timeout)  # Positional argument
            self.transaction_initialized = True
    
    def begin_transaction(self):
        """Begin a new transaction"""
        if self.enable_transactions:
            self.producer.begin_transaction()
    
    def commit_transaction(self, timeout: float = 30.0):
        """
        Commit the current transaction
        
        Args:
            timeout: Commit timeout in seconds
        """
        if self.enable_transactions:
            self.producer.commit_transaction(timeout)  # Positional argument
    
    def abort_transaction(self, timeout: float = 30.0):
        """
        Abort the current transaction
        
        Args:
            timeout: Abort timeout in seconds
        """
        if self.enable_transactions:
            self.producer.abort_transaction(timeout)  # Positional argument
    
    def send_transactional_batch(
        self,
        messages: list,
        topic: str,
        key_extractor: callable = None
    ) -> bool:
        """
        Send a batch of messages in a single transaction
        
        Args:
            messages: List of messages to send
            topic: Target topic
            key_extractor: Function to extract partition key from message
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.enable_transactions:
                self.begin_transaction()
            
            for message in messages:
                key = None
                if key_extractor:
                    key = key_extractor(message)
                    if key:
                        key = key.encode('utf-8')
                
                self.producer.produce(
                    topic=topic,
                    value=message if isinstance(message, bytes) else str(message).encode('utf-8'),
                    key=key
                )
            
            if self.enable_transactions:
                self.commit_transaction()
            else:
                self.producer.flush()
            
            return True
            
        except Exception as e:
            if self.enable_transactions:
                self.abort_transaction()
            raise e


# Export convenience functions
def create_eos_producer(service_name: str, instance_id: Optional[str] = None):
    """
    Create a Kafka producer with EOS v2 configuration
    
    Args:
        service_name: Name of the service
        instance_id: Unique instance identifier
        
    Returns:
        Configured producer instance
    """
    from confluent_kafka import Producer
    
    config = KafkaEOSConfig.get_producer_config(
        service_name=service_name,
        instance_id=instance_id,
        enable_transactions=True
    )
    
    return Producer(config)


def create_eos_consumer(service_name: str, group_id: str):
    """
    Create a Kafka consumer with EOS v2 configuration
    
    Args:
        service_name: Name of the service
        group_id: Consumer group ID
        
    Returns:
        Configured consumer instance
    """
    from confluent_kafka import Consumer
    
    config = KafkaEOSConfig.get_consumer_config(
        service_name=service_name,
        group_id=group_id,
        read_committed=True,
        auto_commit=False
    )
    
    return Consumer(config)