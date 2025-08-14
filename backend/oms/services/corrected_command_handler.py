"""
🔥 THINK ULTRA! Corrected Command Handler

This implements the CORRECT Event Sourcing pattern:
1. Store events in S3/MinIO (SSoT) FIRST
2. Use PostgreSQL Outbox only for delivery guarantee
3. Clear separation of concerns

PostgreSQL is NOT an event store!
"""

import uuid
from datetime import datetime
from typing import Dict, Any, Optional

from shared.utils.logging import get_logger
from .event_store import Event, event_store
from ..database.postgres import db

logger = get_logger(__name__)


class CorrectedCommandHandler:
    """
    Handles commands with the CORRECT Event Sourcing pattern.
    
    Flow:
    1. Validate command
    2. Create event
    3. Store in S3/MinIO (SSoT) ← THE REAL EVENT STORE
    4. Store reference in PostgreSQL Outbox (delivery only)
    5. Return immediately (202 Accepted)
    """
    
    def __init__(self):
        self.event_store = event_store
        self.db = db
        
    async def handle_create_instance(
        self,
        database_name: str,
        class_name: str,
        instance_data: Dict[str, Any],
        actor: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle instance creation command with CORRECT Event Sourcing.
        """
        # 1. Generate IDs
        event_id = str(uuid.uuid4())
        aggregate_id = instance_data.get('id') or str(uuid.uuid4())
        
        # 2. Get current version from S3 (SSoT)
        current_version = await self.event_store.get_aggregate_version(
            aggregate_type=f"{database_name}.{class_name}",
            aggregate_id=aggregate_id
        )
        
        # 3. Create the event
        event = Event(
            event_id=event_id,
            event_type="InstanceCreated",
            aggregate_type=f"{database_name}.{class_name}",
            aggregate_id=aggregate_id,
            aggregate_version=current_version + 1,
            timestamp=datetime.utcnow(),
            actor=actor,
            payload={
                "database_name": database_name,
                "class_name": class_name,
                "data": instance_data
            },
            metadata={
                "source": "CorrectedCommandHandler",
                "correlation_id": str(uuid.uuid4())
            }
        )
        
        # 4. CRITICAL: Store in S3/MinIO FIRST (This is the SSoT!)
        logger.info(f"🔥 Storing event in S3/MinIO (SSoT): {event_id}")
        s3_event_id = await self.event_store.append_event(event)
        
        # 5. Store REFERENCE in PostgreSQL Outbox (delivery guarantee only)
        logger.info(f"📮 Adding to Outbox for delivery guarantee: {event_id}")
        await self._add_to_outbox(event, s3_event_id)
        
        # 6. Return immediately (CQRS: async processing)
        return {
            "status": "accepted",
            "event_id": s3_event_id,
            "aggregate_id": aggregate_id,
            "message": "Event stored in S3/MinIO and queued for processing"
        }
    
    async def handle_update_instance(
        self,
        database_name: str,
        class_name: str,
        instance_id: str,
        update_data: Dict[str, Any],
        actor: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle instance update command with CORRECT Event Sourcing.
        """
        # 1. Get current version from S3 (SSoT)
        aggregate_type = f"{database_name}.{class_name}"
        current_version = await self.event_store.get_aggregate_version(
            aggregate_type=aggregate_type,
            aggregate_id=instance_id
        )
        
        # 2. Create update event
        event = Event(
            event_id=str(uuid.uuid4()),
            event_type="InstanceUpdated",
            aggregate_type=aggregate_type,
            aggregate_id=instance_id,
            aggregate_version=current_version + 1,
            timestamp=datetime.utcnow(),
            actor=actor,
            payload={
                "database_name": database_name,
                "class_name": class_name,
                "updates": update_data
            }
        )
        
        # 3. Store in S3/MinIO FIRST (SSoT)
        s3_event_id = await self.event_store.append_event(event)
        
        # 4. Add to Outbox for delivery
        await self._add_to_outbox(event, s3_event_id)
        
        return {
            "status": "accepted",
            "event_id": s3_event_id,
            "aggregate_id": instance_id
        }
    
    async def handle_delete_instance(
        self,
        database_name: str,
        class_name: str,
        instance_id: str,
        actor: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Handle instance deletion with CORRECT Event Sourcing.
        Note: We never actually delete events, just mark as deleted.
        """
        aggregate_type = f"{database_name}.{class_name}"
        current_version = await self.event_store.get_aggregate_version(
            aggregate_type=aggregate_type,
            aggregate_id=instance_id
        )
        
        event = Event(
            event_id=str(uuid.uuid4()),
            event_type="InstanceDeleted",
            aggregate_type=aggregate_type,
            aggregate_id=instance_id,
            aggregate_version=current_version + 1,
            timestamp=datetime.utcnow(),
            actor=actor,
            payload={
                "database_name": database_name,
                "class_name": class_name,
                "deleted_at": datetime.utcnow().isoformat()
            }
        )
        
        # Store in S3 (events are never deleted, just marked)
        s3_event_id = await self.event_store.append_event(event)
        
        # Add to Outbox
        await self._add_to_outbox(event, s3_event_id)
        
        return {
            "status": "accepted",
            "event_id": s3_event_id,
            "aggregate_id": instance_id
        }
    
    async def _add_to_outbox(self, event: Event, s3_event_id: str):
        """
        Add event REFERENCE to PostgreSQL Outbox.
        
        CRITICAL: This is NOT storing the event!
        It's just storing a reference for delivery guarantee.
        The actual event is already in S3/MinIO.
        """
        async with self.db.transaction() as conn:
            await conn.execute("""
                INSERT INTO spice_outbox.outbox (
                    id,
                    message_type,
                    aggregate_type,
                    aggregate_id,
                    topic,
                    payload,
                    entity_version
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
                s3_event_id,  # Reference to S3 event
                "EVENT",
                event.aggregate_type,
                event.aggregate_id,
                f"instance_events",  # Kafka topic
                json.dumps({
                    "s3_event_id": s3_event_id,  # Reference to real event
                    "event_type": event.event_type,
                    "aggregate_type": event.aggregate_type,
                    "aggregate_id": event.aggregate_id,
                    "version": event.aggregate_version,
                    "timestamp": event.timestamp.isoformat()
                }),
                event.aggregate_version
            )
            
            logger.info(
                f"✅ Outbox entry created for S3 event: {s3_event_id}"
                f" (This is just for delivery, not storage!)"
            )
    
    async def rebuild_projection(
        self,
        database_name: str,
        class_name: str,
        instance_id: Optional[str] = None
    ):
        """
        Rebuild projections from the S3/MinIO event store.
        This demonstrates that S3 is the real source of truth.
        """
        aggregate_type = f"{database_name}.{class_name}"
        
        if instance_id:
            # Rebuild single instance
            events = await self.event_store.get_events(
                aggregate_type=aggregate_type,
                aggregate_id=instance_id
            )
            
            # Apply events to rebuild state
            state = {}
            for event in events:
                state = self._apply_event(state, event)
                
            logger.info(
                f"Rebuilt {aggregate_type}/{instance_id} "
                f"from {len(events)} events in S3"
            )
            
            return state
        else:
            # Rebuild all instances of this type
            # This would iterate through all aggregates
            logger.info(f"Rebuilding all {aggregate_type} from S3 events")
            # Implementation would scan S3 for all aggregates of this type
            
    def _apply_event(self, state: Dict[str, Any], event: Event) -> Dict[str, Any]:
        """
        Apply an event to build/update state.
        This is how Event Sourcing rebuilds state from events.
        """
        if event.event_type == "InstanceCreated":
            state = event.payload.get("data", {})
            state["_version"] = event.aggregate_version
            state["_created_at"] = event.timestamp.isoformat()
            
        elif event.event_type == "InstanceUpdated":
            updates = event.payload.get("updates", {})
            state.update(updates)
            state["_version"] = event.aggregate_version
            state["_updated_at"] = event.timestamp.isoformat()
            
        elif event.event_type == "InstanceDeleted":
            state["_deleted"] = True
            state["_deleted_at"] = event.timestamp.isoformat()
            state["_version"] = event.aggregate_version
            
        return state


# Example usage showing the correct pattern
async def example_correct_flow():
    """
    Demonstrates the CORRECT Event Sourcing flow.
    """
    handler = CorrectedCommandHandler()
    
    # 1. Command comes in
    result = await handler.handle_create_instance(
        database_name="example_db",
        class_name="Product",
        instance_data={
            "product_id": "PROD-001",
            "name": "Example Product",
            "price": 99.99
        },
        actor="user@example.com"
    )
    
    print(f"✅ Event stored in S3/MinIO: {result['event_id']}")
    print(f"✅ Outbox has REFERENCE to S3 event (not the event itself)")
    
    # 2. Later, we can rebuild from S3 (the real SSoT)
    state = await handler.rebuild_projection(
        database_name="example_db",
        class_name="Product",
        instance_id=result['aggregate_id']
    )
    
    print(f"✅ Rebuilt state from S3 events: {state}")
    
    # This proves S3/MinIO is the Single Source of Truth!


import json