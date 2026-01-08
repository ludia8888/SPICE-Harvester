# SPICE HARVESTER Shared Components

This package contains shared components used across all SPICE HARVESTER microservices.

## Installation

### For development (editable install):
```bash
pip install -e .
```

### For production:
```bash
pip install .
```

### With optional dependencies:
```bash
# For development tools
pip install -e ".[dev]"

# For testing
pip install -e ".[test]"

# For documentation
pip install -e ".[docs]"
```

## Components

### Services
- **RedisService**: Redis client with connection pooling
- **StorageService / EventStore**: S3/MinIO storage and event append/replay helpers
- **ElasticsearchService**: Elasticsearch client wrapper
- **CommandStatusService**: Command status tracking (async writes)
- **WebSocketService**: WebSocket connection management
- **BackgroundTaskManager**: Background task lifecycle management
- **ProcessedEventRegistry**: Idempotency + ordering guard for consumers
- **AggregateSequenceAllocator**: expected_seq allocation for OCC
- **DatasetRegistry / PipelineRegistry / ObjectifyRegistry**: control plane registries
- **ConnectorRegistry**: connector sources/mappings/sync state
- **LineageStore / AuditLogStore**: lineage graph + audit log persistence
- **LakefsClient / LakefsStorageService**: lakeFS artifacts and versioned datasets
- **Pipeline utilities**: definition/validation/locking/graph utils for Spark worker

### Models
- Common response models (BaseResponse, ApiResponse, error envelopes)
- Command/event models for CQRS pattern
- Ontology, relationship, link type models
- Pipeline/dataset/objectify registry models
- Background task + gate result models

### Validators
- Email validation (requires email-validator)
- Phone number validation (requires phonenumbers)
- URL validation
- Constraint validation (not_null, unique, range, etc.)
- Ontology and relationship validators

### Utilities
- ID generators
- Label mappers
- JSON-LD converters
- Input sanitization + auth helpers
- Key spec / mapping spec normalization helpers
- Security utilities

## Dependencies

All dependencies are now **explicitly declared** in `pyproject.toml`. 
Previously conditional imports have been removed to ensure build-time failure detection.

### Core Dependencies
- `pydantic>=2.5.0`: Data validation
- `redis[hiredis]>=5.0.1`: Redis client
- `boto3>=1.34.14`: AWS S3/MinIO storage
- `elasticsearch>=8.11.0`: Search engine
- `email-validator>=2.0.0`: Email validation
- `phonenumbers>=8.13.0`: Phone number validation

## Usage in Services

Add to your service's `requirements.txt`:
```
# Install shared package from local path
-e ../shared
```

Or if you need specific features:
```
# Install with development dependencies
-e ../shared[dev]
```

## Important Notes

1. **No Conditional Imports**: All imports are direct. Missing dependencies will fail at build time.
2. **Explicit Dependencies**: All required packages must be installed via pip.
3. **Type Safety**: Full type hints are provided for better IDE support.

## Development

Run tests:
```bash
pytest
```

Format code:
```bash
black shared/
isort shared/
```

Type checking:
```bash
mypy shared/
```
