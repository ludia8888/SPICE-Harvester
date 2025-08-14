# 🔥 THINK ULTRA! Legacy Code Cleanup Required

## Critical Issues Found

### 1. ❌ WRONG: OutboxService Being Misused as Event Store

**Current (WRONG) Usage:**
```python
# In routers/instance_async.py and others
async def create_instance_async(
    outbox_service: OutboxService = OutboxServiceDep,
):
    # WRONG: Using outbox as if it's an event store
    await outbox_service.publish_command(command)
```

**Problem:** The entire codebase treats PostgreSQL Outbox as the Event Store, which is fundamentally wrong!

### 2. 🔄 Duplicate Command Handlers

Found multiple command handler implementations:
- ✅ `corrected_command_handler.py` (NEW - Correct implementation)
- ❌ Command handling in routers (OLD - Wrong pattern)
- ❌ Direct outbox usage everywhere (OLD - Wrong pattern)

### 3. 📁 Legacy Files That Need Removal/Update

| File | Issue | Action Required |
|------|-------|-----------------|
| `oms/main_legacy_backup.py` | Legacy backup | DELETE |
| `oms/routers/instance_async.py` | Uses Outbox as Event Store | UPDATE to use S3 first |
| `oms/routers/instance_sync.py` | Direct DB writes | UPDATE or DEPRECATE |
| `oms/routers/ontology.py` | Uses Outbox incorrectly | UPDATE |
| `oms/database/outbox.py` | Correct but misused | CLARIFY usage |
| `test_event_sourcing_*.py` | Multiple test files | CONSOLIDATE |

### 4. 🔀 Conflicting Patterns

**Current Flow (WRONG):**
```
Command → PostgreSQL Outbox → Kafka → Workers
         ↑
    Treated as Event Store ❌
```

**New Flow (CORRECT):**
```
Command → S3/MinIO → PostgreSQL Outbox → Kafka → Workers
         ↑                    ↑
      Event Store ✅    Delivery Only ✅
```

### 5. 📚 Services That Need Major Updates

#### A. Instance Router (`instance_async.py`)
```python
# CURRENT (WRONG)
async def create_instance_async():
    # Directly publishes to outbox
    await outbox_service.publish_command(command)
    
# SHOULD BE
async def create_instance_async():
    # Use corrected command handler
    handler = CorrectedCommandHandler()
    await handler.handle_create_instance(...)  # Stores in S3 first
```

#### B. Ontology Router (`ontology.py`)
```python
# Similar issue - needs to use S3 Event Store first
```

#### C. Database Router (`database.py`)
```python
# Check if it's also misusing outbox as event store
```

### 6. 🗑️ Test Files Proliferation

Too many test files for event sourcing:
- `test_event_sourcing_pipeline.py`
- `test_event_sourcing_perfect.py`
- `test_event_sourcing_fixed.py`
- `test_complete_system.py`
- `test_infrastructure_ultra_verification.py`
- `test_ultra_fixed_verification.py`
- `test_ultra_skeptical_verification.py`

**Action:** Keep ONE comprehensive test, delete others

### 7. 🔧 Dependencies That Need Update

```python
# oms/dependencies.py
OutboxServiceDep = Depends(OMSDependencyProvider.get_outbox_service)
# ADD:
EventStoreDep = Depends(OMSDependencyProvider.get_event_store)  # S3/MinIO
```

## Cleanup Plan

### Phase 1: Add S3/MinIO Event Store Integration
```bash
# 1. Add MinIO to docker-compose.yml
# 2. Add EventStore to dependencies
# 3. Update service initialization in main.py
```

### Phase 2: Update All Routers
```python
# Replace all instances of:
await outbox_service.publish_command(command)

# With:
await event_store.append_event(event)  # S3 first
await outbox_service.add_delivery_reference(event_id)  # Then outbox
```

### Phase 3: Update Workers
```python
# Workers should understand that:
# - Outbox contains REFERENCES to S3 events
# - Actual events are in S3/MinIO
```

### Phase 4: Clean Up Legacy Code
```bash
# Delete:
rm oms/main_legacy_backup.py
rm test_event_sourcing_perfect.py
rm test_event_sourcing_fixed.py
# ... other duplicate tests

# Consolidate into:
test_event_sourcing_s3.py  # One comprehensive test
```

### Phase 5: Update Documentation
- Remove all references to "PostgreSQL Event Store"
- Clarify that PostgreSQL is ONLY for delivery guarantee
- Update architecture diagrams

## Critical Files to Update

### 1. `oms/main.py`
```python
# ADD:
from oms.services.event_store import event_store

# In startup:
await event_store.connect()  # Connect to S3/MinIO
```

### 2. All Routers Using Async Pattern
- `instance_async.py`
- `ontology.py`
- `database.py`

Must change from:
```python
# WRONG
await outbox_service.publish_command(command)
```

To:
```python
# CORRECT
handler = CorrectedCommandHandler()
result = await handler.handle_create_instance(...)
```

### 3. Worker Files
- `instance_worker/main.py`
- `projection_worker/main.py`
- `ontology_worker/main.py`

Must understand that Outbox messages contain S3 references, not the actual events.

## Summary of Issues

1. **PostgreSQL Outbox is being used as Event Store** ❌
2. **S3/MinIO Event Store not integrated** ❌
3. **Multiple duplicate command handlers** ❌
4. **Legacy test files not cleaned up** ❌
5. **Workers reading from wrong source** ❌
6. **Documentation still shows wrong architecture** ❌

## Immediate Actions Required

1. **STOP** treating PostgreSQL Outbox as Event Store
2. **INTEGRATE** S3/MinIO as the real Event Store
3. **UPDATE** all routers to use corrected flow
4. **DELETE** legacy and duplicate code
5. **CONSOLIDATE** test files
6. **FIX** worker event consumption

---

**This is a significant architectural debt that needs immediate attention!**

The system is currently running on a fundamentally incorrect understanding of Event Sourcing.