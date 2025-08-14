"""
🔥 THINK ULTRA! Example of How to Fix the Routers

This shows how to convert from WRONG pattern to CORRECT pattern
"""

# ============================================================================
# ❌ CURRENT WRONG IMPLEMENTATION (instance_async.py)
# ============================================================================

# WRONG - Current code treats PostgreSQL as Event Store
async def create_instance_async_WRONG(
    db_name: str,
    class_id: str,
    request: InstanceCreateRequest,
    outbox_service: OutboxService = OutboxServiceDep,  # ❌ WRONG!
):
    """WRONG: Directly publishes to PostgreSQL Outbox as if it's Event Store"""
    
    # Create command
    command = CreateInstanceCommand(
        aggregate_type=f"{db_name}.{class_id}",
        aggregate_id=request.data.get('id'),
        payload=request.data
    )
    
    # ❌ WRONG: PostgreSQL is NOT an Event Store!
    await outbox_service.publish_command(command)
    
    return {"command_id": command.command_id}


# ============================================================================
# ✅ CORRECT IMPLEMENTATION (How it should be)
# ============================================================================

from oms.services.event_store import event_store, Event
from oms.services.corrected_command_handler import CorrectedCommandHandler
from oms.database.outbox import OutboxService
import uuid
from datetime import datetime

async def create_instance_async_CORRECT(
    db_name: str,
    class_id: str,
    request: InstanceCreateRequest,
    event_store_dep = Depends(get_event_store),  # ✅ S3/MinIO
    outbox_service: OutboxService = OutboxServiceDep,  # Still needed for delivery
):
    """CORRECT: Store in S3/MinIO first, then PostgreSQL for delivery only"""
    
    # 1. Create the event
    event = Event(
        event_id=str(uuid.uuid4()),
        event_type="InstanceCreated",
        aggregate_type=f"{db_name}.{class_id}",
        aggregate_id=request.data.get('id', str(uuid.uuid4())),
        aggregate_version=1,  # Get from S3 in real implementation
        timestamp=datetime.utcnow(),
        actor=request.user_id,
        payload={
            "database_name": db_name,
            "class_name": class_id,
            "data": request.data
        }
    )
    
    # 2. ✅ CORRECT: Store in S3/MinIO FIRST (This is the SSoT!)
    s3_event_id = await event_store_dep.append_event(event)
    
    # 3. Add REFERENCE to PostgreSQL Outbox (delivery guarantee only)
    async with db.transaction() as conn:
        await conn.execute("""
            INSERT INTO spice_outbox.outbox (
                id, message_type, aggregate_type, aggregate_id, topic, payload
            ) VALUES ($1, $2, $3, $4, $5, $6)
        """,
            s3_event_id,  # Reference to S3 event
            "EVENT",
            event.aggregate_type,
            event.aggregate_id,
            "instance_events",
            json.dumps({
                "s3_event_id": s3_event_id,  # ← Key point: Reference to S3!
                "event_type": event.event_type,
                "aggregate_id": event.aggregate_id
            })
        )
    
    # 4. Return 202 Accepted (CQRS async processing)
    return {
        "status": "accepted",
        "event_id": s3_event_id,
        "message": "Event stored in S3/MinIO and queued for processing"
    }


# ============================================================================
# ✅ ALTERNATIVE: Use the CorrectedCommandHandler
# ============================================================================

async def create_instance_async_USING_HANDLER(
    db_name: str,
    class_id: str,
    request: InstanceCreateRequest,
):
    """SIMPLEST: Use the already corrected command handler"""
    
    handler = CorrectedCommandHandler()
    
    result = await handler.handle_create_instance(
        database_name=db_name,
        class_name=class_id,
        instance_data=request.data,
        actor=request.user_id
    )
    
    return result  # Already returns correct format


# ============================================================================
# 🔧 HOW TO UPDATE THE WORKER
# ============================================================================

class CorrectedInstanceWorker:
    """Worker that understands S3 is the truth"""
    
    async def process_message(self, kafka_message):
        """Process message from Kafka"""
        
        # 1. Parse the outbox message
        payload = json.loads(kafka_message.value)
        
        # 2. ✅ CRITICAL: Get the S3 event ID
        s3_event_id = payload.get('s3_event_id')
        
        if not s3_event_id:
            # Legacy message without S3 reference
            logger.error("Legacy message without S3 event ID!")
            return
        
        # 3. ✅ CORRECT: Fetch the REAL event from S3
        events = await event_store.get_events_by_id(s3_event_id)
        
        if not events:
            logger.error(f"Event {s3_event_id} not found in S3!")
            return
        
        event = events[0]
        
        # 4. Process the event (update projections)
        await self.update_terminus_graph(event)
        await self.update_elasticsearch(event)
        
        logger.info(f"Processed event {s3_event_id} from S3")


# ============================================================================
# 📝 SUMMARY OF CHANGES
# ============================================================================

"""
1. STOP using OutboxService.publish_command() directly
2. START using event_store.append_event() FIRST
3. PostgreSQL Outbox only stores REFERENCES to S3 events
4. Workers MUST fetch actual events from S3
5. S3/MinIO is the ONLY source of truth

Key Changes:
- Add event_store dependency
- Store events in S3 FIRST
- Outbox only contains S3 references
- Workers read from S3, not from Outbox payload
"""

import json