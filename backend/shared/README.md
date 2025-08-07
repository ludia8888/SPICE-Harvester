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
- **StorageService**: S3/MinIO storage operations (requires boto3)
- **ElasticsearchService**: Elasticsearch client wrapper
- **CommandStatusService**: Command status tracking
- **WebSocketService**: WebSocket connection management
- **BackgroundTaskManager**: Background task lifecycle management

### Models
- Common data models (BaseResponse, ApiResponse, etc.)
- Command models for CQRS pattern
- Ontology models
- Background task models

### Validators
- Email validation (requires email-validator)
- Phone number validation (requires phonenumbers)
- URL validation
- Custom validators

### Utilities
- ID generators
- Label mappers
- JSON-LD converters
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