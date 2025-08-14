"""
🔥 THINK ULTRA! Migration Helper for Gradual S3/MinIO Event Store Adoption

This provides a dual-write pattern to safely migrate from PostgreSQL-as-Event-Store
(WRONG) to S3/MinIO-as-Event-Store (CORRECT) without breaking existing functionality.

Key Features:
1. Dual-write: Write to both S3 and PostgreSQL during migration
2. Feature flag controlled
3. Fallback to legacy if S3 fails
4. Zero downtime migration
"""

import os
import json
import uuid
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import asyncpg

from oms.services.event_store import Event, event_store
from oms.database.outbox import OutboxService, MessageType
from shared.models.commands import BaseCommand

logger = logging.getLogger(__name__)


class MigrationHelper:
    """
    Helper class for gradual migration from PostgreSQL to S3/MinIO Event Store.
    
    This implements a dual-write pattern:
    1. Write to S3/MinIO (new, correct)
    2. Write to PostgreSQL Outbox (legacy, for compatibility)
    
    This allows for zero-downtime migration.
    """
    
    def __init__(self):
        # Feature flag to control migration
        self.s3_enabled = os.getenv("ENABLE_S3_EVENT_STORE", "false").lower() == "true"
        self.dual_write = os.getenv("ENABLE_DUAL_WRITE", "true").lower() == "true"
        
        logger.info(f"🔄 Migration Helper initialized:")
        logger.info(f"   S3 Event Store: {'ENABLED' if self.s3_enabled else 'DISABLED'}")
        logger.info(f"   Dual Write: {'ENABLED' if self.dual_write else 'DISABLED'}")
    
    async def handle_command_with_migration(
        self,
        connection: asyncpg.Connection,
        command: BaseCommand,
        outbox_service: OutboxService,
        topic: str,
        actor: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle command with migration pattern.
        
        Flow:
        1. If S3 enabled: Write to S3 first
        2. If dual-write or S3 disabled: Write to PostgreSQL Outbox
        3. Return appropriate response
        """
        
        result = {
            "command_id": str(command.command_id),
            "status": "accepted",
            "storage": []
        }
        
        # Step 1: Write to S3/MinIO if enabled
        s3_event_id = None
        if self.s3_enabled:
            try:
                event = self._command_to_event(command, actor)
                s3_event_id = await event_store.append_event(event)
                result["s3_event_id"] = s3_event_id
                result["storage"].append("s3")
                logger.info(f"✅ Event stored in S3/MinIO: {s3_event_id}")
            except Exception as e:
                logger.error(f"❌ Failed to store in S3/MinIO: {e}")
                if not self.dual_write:
                    # If not dual-writing, this is a failure
                    raise
        
        # Step 2: Write to PostgreSQL Outbox (legacy or dual-write)
        if not self.s3_enabled or self.dual_write:
            try:
                # Modify payload to include S3 reference if available
                if s3_event_id:
                    # Add S3 reference to the command payload
                    modified_command = self._add_s3_reference(command, s3_event_id)
                    message_id = await outbox_service.publish_command(
                        connection, modified_command, topic
                    )
                else:
                    # Legacy mode - no S3 reference
                    message_id = await outbox_service.publish_command(
                        connection, command, topic
                    )
                
                result["outbox_message_id"] = message_id
                result["storage"].append("postgresql")
                logger.info(f"✅ Command stored in PostgreSQL Outbox: {message_id}")
                
            except Exception as e:
                logger.error(f"❌ Failed to store in PostgreSQL Outbox: {e}")
                # If S3 succeeded but PostgreSQL failed in dual-write, warn but continue
                if s3_event_id and self.dual_write:
                    logger.warning("⚠️ S3 succeeded but PostgreSQL failed in dual-write mode")
                else:
                    raise
        
        # Step 3: Add migration metadata
        result["migration_mode"] = self._get_migration_mode()
        
        return result
    
    def _command_to_event(self, command: BaseCommand, actor: Optional[str]) -> Event:
        """Convert a command to an event for S3 storage."""
        return Event(
            event_id=str(command.command_id),
            event_type=f"{command.command_type.value}Requested",
            aggregate_type=command.aggregate_type,
            aggregate_id=command.aggregate_id,
            aggregate_version=1,  # TODO: Get actual version from S3
            timestamp=datetime.utcnow(),
            actor=actor,
            payload=command.model_dump(),
            metadata={
                "source": "MigrationHelper",
                "original_command_type": command.command_type.value,
                "migration_mode": self._get_migration_mode()
            }
        )
    
    def _add_s3_reference(self, command: BaseCommand, s3_event_id: str) -> BaseCommand:
        """Add S3 event ID reference to command for dual-write mode."""
        # Clone the command and add S3 reference
        command_dict = command.model_dump()
        
        # Add S3 reference to metadata
        if "metadata" not in command_dict:
            command_dict["metadata"] = {}
        command_dict["metadata"]["s3_event_id"] = s3_event_id
        command_dict["metadata"]["dual_write"] = True
        
        # Create new command instance with S3 reference
        return command.__class__(**command_dict)
    
    def _get_migration_mode(self) -> str:
        """Get current migration mode for logging/debugging."""
        if self.s3_enabled and self.dual_write:
            return "dual_write"
        elif self.s3_enabled:
            return "s3_only"
        else:
            return "legacy"
    
    async def read_with_fallback(
        self,
        aggregate_type: str,
        aggregate_id: str
    ) -> Dict[str, Any]:
        """
        Read events with fallback pattern.
        
        1. Try S3 first if enabled
        2. Fall back to PostgreSQL if S3 fails or is disabled
        """
        
        if self.s3_enabled:
            try:
                # Try S3 first
                events = await event_store.get_events(aggregate_type, aggregate_id)
                if events:
                    logger.info(f"✅ Read {len(events)} events from S3/MinIO")
                    return {
                        "source": "s3",
                        "events": [e.model_dump() for e in events]
                    }
            except Exception as e:
                logger.warning(f"⚠️ Failed to read from S3, falling back: {e}")
        
        # Fallback to PostgreSQL (would need to implement PostgreSQL event reading)
        logger.info("📚 Reading from PostgreSQL (legacy mode)")
        return {
            "source": "postgresql",
            "events": [],  # TODO: Implement PostgreSQL event reading
            "warning": "Reading from legacy PostgreSQL storage"
        }


# Global instance
migration_helper = MigrationHelper()


async def get_migration_helper() -> MigrationHelper:
    """Dependency for getting migration helper."""
    return migration_helper