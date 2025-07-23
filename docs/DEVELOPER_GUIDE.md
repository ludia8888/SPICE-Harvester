# SPICE HARVESTER Developer Guide

## Table of Contents

1. [Getting Started](#getting-started)
2. [Development Environment Setup](#development-environment-setup)
3. [Code Structure](#code-structure)
4. [Development Workflow](#development-workflow)
5. [Testing Guidelines](#testing-guidelines)
6. [Adding New Features](#adding-new-features)
7. [Type System](#type-system)
8. [Database Schema Management](#database-schema-management)
9. [Error Handling](#error-handling)
10. [Performance Optimization](#performance-optimization)
11. [Debugging Tips](#debugging-tips)
12. [Contributing Guidelines](#contributing-guidelines)

## Getting Started

### Prerequisites

- Python 3.9 or higher
- Docker and Docker Compose
- Git
- TerminusDB v11.x
- Virtual environment tool (venv, conda, or poetry)

### Quick Start

```bash
# Clone the repository
git clone https://github.com/your-org/spice-harvester.git
cd spice-harvester

# Set up Python virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
cd backend
pip install -r requirements.txt

# Copy environment configuration
cp .env.example .env

# Start TerminusDB
docker-compose up terminusdb -d

# Start all services
python start_services.py
```

## Development Environment Setup

### 1. Project Structure

```
SPICE-HARVESTER/
├── backend/
│   ├── bff/               # Backend for Frontend service
│   ├── oms/               # Ontology Management Service
│   ├── funnel/            # Type Inference Service
│   ├── shared/            # Shared components
│   ├── tests/             # Test suites
│   ├── docs/              # Documentation
│   └── docker-compose.yml # Container orchestration
├── frontend/              # Frontend application (future)
└── docs/                  # Project-level documentation
```

### 2. Environment Variables

Create a `.env` file in the backend directory:

```bash
# Service Ports
OMS_PORT=8000
BFF_PORT=8002
FUNNEL_PORT=8003

# TerminusDB Configuration
TERMINUS_SERVER_URL=http://localhost:6363
TERMINUS_USER=admin
TERMINUS_ACCOUNT=admin
TERMINUS_KEY=admin123
TERMINUSDB_ADMIN_PASS=admin123

# Service URLs
OMS_BASE_URL=http://localhost:8000
BFF_BASE_URL=http://localhost:8002
FUNNEL_BASE_URL=http://localhost:8003

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Security
SECRET_KEY=your-secret-key-here
CORS_ORIGINS=http://localhost:3000,http://localhost:3001

# Feature Flags
ENABLE_HTTPS=false
VERIFY_SSL=false
```

### 3. Development Tools

#### IDE Configuration

**VS Code** (Recommended):
```json
{
  "python.linting.enabled": true,
  "python.linting.pylintEnabled": true,
  "python.formatting.provider": "black",
  "python.testing.pytestEnabled": true,
  "python.testing.unittestEnabled": false,
  "editor.formatOnSave": true,
  "files.exclude": {
    "**/__pycache__": true,
    "**/*.pyc": true
  }
}
```

**PyCharm**:
- Set Python interpreter to virtual environment
- Enable pytest as test runner
- Configure code style to use Black formatter

#### Required Extensions/Plugins

- Python language support
- Docker support
- REST Client (for API testing)
- GitLens (for version control)

## Code Structure

### Service Architecture

Each service follows a consistent structure:

```
service/
├── __init__.py
├── main.py              # FastAPI application entry point
├── dependencies.py      # Dependency injection
├── routers/            # API endpoints
│   ├── __init__.py
│   └── *.py
├── services/           # Business logic
│   ├── __init__.py
│   └── *.py
├── models/             # Service-specific models
├── utils/              # Utility functions
└── requirements.txt    # Service dependencies
```

### Shared Components

The `shared/` directory contains reusable components:

```
shared/
├── models/             # Common data models
│   ├── common.py      # DataType, Cardinality enums
│   ├── ontology.py    # Ontology models
│   └── requests.py    # Request/Response models
├── validators/         # Data validators
│   ├── base_validator.py
│   └── complex_type_validator.py
├── security/           # Security utilities
│   └── input_sanitizer.py
├── utils/              # Common utilities
│   ├── jsonld.py      # JSON-LD conversion
│   ├── label_mapper.py # Label-ID mapping
│   └── language.py    # Language detection
└── config/            # Configuration management
    └── service_config.py
```

## Development Workflow

### 1. Feature Development

```bash
# Create feature branch
git checkout -b feature/your-feature-name

# Make changes
# ... edit files ...

# Run tests
pytest tests/unit/

# Check code style
black backend/
pylint backend/

# Commit changes
git add .
git commit -m "feat: add new feature description"

# Push to remote
git push origin feature/your-feature-name
```

### 2. Code Style Guidelines

#### Python Style

Follow PEP 8 with these additions:

```python
# Imports order
import os  # Standard library
import sys

import httpx  # Third-party libraries
import pandas as pd
from fastapi import FastAPI

from shared.models import DataType  # Local imports
from .services import MyService

# Type hints
def process_data(
    data: List[Dict[str, Any]], 
    options: Optional[ProcessOptions] = None
) -> ProcessResult:
    """
    Process input data with given options.
    
    Args:
        data: List of data dictionaries
        options: Processing options
        
    Returns:
        ProcessResult: Processed data result
        
    Raises:
        ValidationError: If data is invalid
    """
    pass

# Async functions
async def fetch_data(url: str) -> Dict[str, Any]:
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()

# Error handling
try:
    result = await process_operation()
except ValidationError as e:
    logger.error(f"Validation failed: {e}")
    raise HTTPException(status_code=400, detail=str(e))
except Exception as e:
    logger.exception("Unexpected error")
    raise HTTPException(status_code=500, detail="Internal server error")
```

#### Naming Conventions

- **Classes**: PascalCase (`OntologyManager`)
- **Functions/Methods**: snake_case (`get_ontology_by_id`)
- **Constants**: UPPER_SNAKE_CASE (`MAX_RETRY_COUNT`)
- **Private methods**: Leading underscore (`_internal_method`)

### 3. Git Workflow

#### Branch Naming

- `feature/description` - New features
- `fix/description` - Bug fixes
- `refactor/description` - Code refactoring
- `docs/description` - Documentation updates
- `test/description` - Test additions/updates

#### Commit Messages

Follow conventional commits:

```
feat: add support for array type validation
fix: correct money type decimal precision
refactor: simplify validator chain logic
docs: update API reference for new endpoints
test: add integration tests for relationship paths
chore: update dependencies to latest versions
```

### 4. Using Git-like Features in TerminusDB

SPICE HARVESTER leverages TerminusDB's version control capabilities (85.7% Git feature coverage):

#### Implicit Commits
In TerminusDB v11.x, commits are created implicitly when modifying documents:

```python
# Traditional approach (doesn't work in v11.x)
# await terminus_service.commit(db_name, "message")

# Implicit commit approach (working)
endpoint = f"/api/document/{account}/{database}"
params = {
    "message": "Your commit message",
    "author": "username"
}
await terminus_service._make_request("POST", endpoint, documents, params)
```

#### Branch Management

```python
# List branches
branches = await terminus_service.list_branches(db_name)

# Create a new branch
await terminus_service.create_branch(db_name, "feature-branch", "main")

# Delete a branch
await terminus_service.delete_branch(db_name, "feature-branch")
```

#### Rollback with Git-style References

```python
# Rollback to previous commit
await terminus_service.rollback(db_name, "HEAD~1")

# Supported references:
# - HEAD: Latest commit
# - HEAD~1: Previous commit
# - HEAD~n: n commits back
# - Specific commit ID

# Note: Rollback creates a new branch at the target commit
```

#### Working with Commit History

```python
# Get commit history
history = await terminus_service.get_commit_history(db_name, limit=10)

# Get diff between commits
diff = await terminus_service.diff(db_name, "main", "feature-branch")
```

## Testing Guidelines

### 1. Test Structure

```
tests/
├── unit/               # Unit tests for individual components
├── integration/        # Integration tests for service interactions
├── performance/        # Performance and load tests
├── system/            # End-to-end system tests
└── conftest.py        # Pytest configuration and fixtures
```

### 2. Writing Tests

#### Unit Test Example

```python
# tests/unit/validators/test_email_validator.py
import pytest
from shared.validators.email_validator import EmailValidator

class TestEmailValidator:
    @pytest.fixture
    def validator(self):
        return EmailValidator()
    
    def test_valid_email(self, validator):
        result = validator.validate("user@example.com")
        assert result.is_valid
        assert not result.errors
    
    def test_invalid_email(self, validator):
        result = validator.validate("invalid-email")
        assert not result.is_valid
        assert "Invalid email format" in result.errors[0]
    
    @pytest.mark.parametrize("email,expected", [
        ("user@example.com", True),
        ("user+tag@example.com", True),
        ("user@subdomain.example.com", True),
        ("@example.com", False),
        ("user@", False),
        ("user@.com", False),
    ])
    def test_email_formats(self, validator, email, expected):
        result = validator.validate(email)
        assert result.is_valid == expected
```

#### Integration Test Example

```python
# tests/integration/test_ontology_creation.py
import pytest
import httpx
from tests.utils import create_test_database, cleanup_test_database

@pytest.mark.asyncio
class TestOntologyCreation:
    @pytest.fixture(autouse=True)
    async def setup_and_teardown(self):
        # Setup
        self.db_name = await create_test_database()
        self.client = httpx.AsyncClient(base_url="http://localhost:8002")
        
        yield
        
        # Teardown
        await cleanup_test_database(self.db_name)
        await self.client.aclose()
    
    async def test_create_simple_ontology(self):
        ontology_data = {
            "label": "TestClass",
            "properties": [
                {
                    "name": "name",
                    "type": "STRING",
                    "label": "Name",
                    "required": True
                }
            ]
        }
        
        response = await self.client.post(
            f"/api/v1/database/{self.db_name}/ontology",
            json=ontology_data
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["data"]["label"] == "TestClass"
```

### 3. Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/unit/validators/test_email_validator.py

# Run with coverage
pytest --cov=backend --cov-report=html

# Run specific test markers
pytest -m "unit"
pytest -m "integration"
pytest -m "not slow"

# Run with verbose output
pytest -v

# Run with parallel execution
pytest -n 4
```

### 4. Test Coverage

Maintain minimum coverage requirements:
- Unit tests: 80% coverage
- Integration tests: 60% coverage
- Critical paths: 95% coverage

## Adding New Features

### 1. Adding a New Data Type

#### Step 1: Define the Type

```python
# shared/models/common.py
class DataType(Enum):
    # ... existing types ...
    CUSTOM_TYPE = "custom_type"
```

#### Step 2: Create Validator

```python
# shared/validators/custom_type_validator.py
from typing import Any, Dict, Optional
from .base_validator import BaseValidator, ValidationResult

class CustomTypeValidator(BaseValidator):
    def __init__(self):
        super().__init__("custom_type")
    
    def validate(
        self, 
        value: Any, 
        constraints: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        errors = []
        
        # Implement validation logic
        if not self._is_valid_custom_type(value):
            errors.append("Invalid custom type format")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            normalized_value=self._normalize(value) if not errors else None
        )
    
    def _is_valid_custom_type(self, value: Any) -> bool:
        # Implement validation logic
        pass
    
    def _normalize(self, value: Any) -> Any:
        # Implement normalization logic
        pass
```

#### Step 3: Register Validator

```python
# shared/validators/__init__.py
from .custom_type_validator import CustomTypeValidator

VALIDATORS = {
    # ... existing validators ...
    "custom_type": CustomTypeValidator(),
}
```

#### Step 4: Update Type Mapping

```python
# oms/utils/terminus_schema_types.py
type_mapping = {
    # ... existing mappings ...
    "custom_type": TerminusSchemaType.STRING.value,  # or appropriate base type
}
```

### 2. Adding a New API Endpoint

#### Step 1: Define Request/Response Models

```python
# shared/models/requests.py
from pydantic import BaseModel, Field
from typing import Optional, List

class CustomOperationRequest(BaseModel):
    data: List[Dict[str, Any]] = Field(..., description="Input data")
    options: Optional[Dict[str, Any]] = Field(None, description="Operation options")

class CustomOperationResponse(BaseModel):
    status: str
    result: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None
```

#### Step 2: Create Router

```python
# bff/routers/custom.py
from fastapi import APIRouter, Depends, HTTPException
from shared.models.requests import CustomOperationRequest, CustomOperationResponse
from ..dependencies import get_oms_client

router = APIRouter(prefix="/custom", tags=["Custom Operations"])

@router.post("/operation", response_model=CustomOperationResponse)
async def perform_custom_operation(
    request: CustomOperationRequest,
    oms_client = Depends(get_oms_client)
):
    """
    Perform custom operation.
    
    Args:
        request: Operation request data
        
    Returns:
        CustomOperationResponse: Operation result
        
    Raises:
        HTTPException: If operation fails
    """
    try:
        # Implement operation logic
        result = await oms_client.custom_operation(request.data, request.options)
        
        return CustomOperationResponse(
            status="success",
            result=result,
            metadata={"processed_at": datetime.utcnow()}
        )
    except Exception as e:
        logger.error(f"Custom operation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
```

#### Step 3: Register Router

```python
# bff/main.py
from .routers import custom

app.include_router(custom.router, prefix="/api/v1")
```

## Type System

### 1. Basic Types

```python
# String types
STRING = "xsd:string"
TEXT = "xsd:string"  # Alias for string

# Numeric types
INTEGER = "xsd:integer"
DECIMAL = "xsd:decimal"
FLOAT = "xsd:float"
DOUBLE = "xsd:double"

# Boolean
BOOLEAN = "xsd:boolean"

# Temporal types
DATE = "xsd:date"
DATETIME = "xsd:dateTime"
TIME = "xsd:time"
```

### 2. Complex Types

```python
# Array type with parameterized elements
ARRAY<STRING>      # Array of strings
ARRAY<INTEGER>     # Array of integers
LIST<OBJECT>       # List of objects
SET<STRING>        # Set of unique strings

# Object type with schema
OBJECT = {
    "type": "object",
    "schema": {
        "field1": {"type": "STRING"},
        "field2": {"type": "INTEGER"}
    }
}

# Enum type
ENUM = {
    "type": "enum",
    "values": ["ACTIVE", "INACTIVE", "PENDING"]
}

# Money type
MONEY = {
    "type": "money",
    "currency": "USD",
    "minAmount": 0,
    "maxAmount": 999999.99
}

# Special types
EMAIL      # Email address validation
PHONE      # Phone number with region
URL        # Web URLs
COORDINATE # Geographic coordinates
ADDRESS    # Structured address
IMAGE      # Image URLs
FILE       # File references
```

### 3. Type Conversion

```python
# Property to Relationship conversion
property = {
    "name": "category",
    "type": "Category",  # Reference to another class
    "label": "Product Category"
}

# Automatically converts to:
relationship = {
    "predicate": "belongs_to_category",
    "target": "Category",
    "cardinality": "n:1"
}

# Array relationships
property = {
    "name": "tags",
    "type": "ARRAY<Tag>",
    "label": "Product Tags"
}

# Converts to:
relationship = {
    "predicate": "has_tags",
    "target": "Tag",
    "cardinality": "n:m"
}
```

## Database Schema Management

### 1. Schema Evolution

```python
# Version 1: Initial schema
class ProductV1:
    name: str
    price: float

# Version 2: Add new field
class ProductV2:
    name: str
    price: float
    description: Optional[str] = None  # New field with default

# Version 3: Change field type
class ProductV3:
    name: str
    price: Decimal  # Changed from float
    description: Optional[str] = None
```

### 2. Migration Strategies

```python
# Forward migration
async def migrate_v1_to_v2(db_name: str):
    """Add description field to all products."""
    products = await get_all_products(db_name)
    for product in products:
        product["description"] = None
        await update_product(db_name, product)

# Backward compatibility
def ensure_compatibility(product_data: dict) -> dict:
    """Ensure data is compatible with current version."""
    # Add default values for missing fields
    if "description" not in product_data:
        product_data["description"] = None
    
    # Convert old field names
    if "product_name" in product_data:
        product_data["name"] = product_data.pop("product_name")
    
    return product_data
```

## Error Handling

### 1. Custom Exceptions

```python
# shared/exceptions.py
class SpiceHarvesterException(Exception):
    """Base exception for all custom exceptions."""
    pass

class ValidationError(SpiceHarvesterException):
    """Raised when validation fails."""
    def __init__(self, field: str, message: str):
        self.field = field
        self.message = message
        super().__init__(f"{field}: {message}")

class NotFoundError(SpiceHarvesterException):
    """Raised when resource is not found."""
    def __init__(self, resource_type: str, identifier: str):
        self.resource_type = resource_type
        self.identifier = identifier
        super().__init__(f"{resource_type} '{identifier}' not found")

class DuplicateError(SpiceHarvesterException):
    """Raised when attempting to create duplicate resource."""
    pass

class CircularReferenceError(SpiceHarvesterException):
    """Raised when circular reference is detected."""
    pass
```

### 2. Error Handler Middleware

```python
# shared/middleware/error_handler.py
from fastapi import Request
from fastapi.responses import JSONResponse

async def error_handler_middleware(request: Request, call_next):
    try:
        response = await call_next(request)
        return response
    except ValidationError as e:
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": str(e),
                    "field": e.field
                }
            }
        )
    except NotFoundError as e:
        return JSONResponse(
            status_code=404,
            content={
                "status": "error",
                "error": {
                    "code": "NOT_FOUND",
                    "message": str(e),
                    "resource": e.resource_type
                }
            }
        )
    except Exception as e:
        logger.exception("Unhandled exception")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "An internal error occurred"
                }
            }
        )
```

## Performance Optimization

### 1. Async Best Practices

```python
# Good: Concurrent operations
async def fetch_multiple_resources(ids: List[str]) -> List[Dict]:
    tasks = [fetch_resource(id) for id in ids]
    return await asyncio.gather(*tasks)

# Bad: Sequential operations
async def fetch_multiple_resources_slow(ids: List[str]) -> List[Dict]:
    results = []
    for id in ids:
        result = await fetch_resource(id)
        results.append(result)
    return results

# Connection pooling
class ServiceClient:
    def __init__(self):
        self.client = httpx.AsyncClient(
            limits=httpx.Limits(
                max_keepalive_connections=25,
                max_connections=100,
            ),
            timeout=httpx.Timeout(30.0)
        )
    
    async def close(self):
        await self.client.aclose()
```

### 2. Caching Strategies

```python
# In-memory cache
from functools import lru_cache
from typing import Dict

@lru_cache(maxsize=1000)
def get_type_mapping(type_name: str) -> str:
    """Cache type mappings for performance."""
    return TYPE_MAPPINGS.get(type_name, "xsd:string")

# Redis cache (future)
import aioredis

class CacheService:
    def __init__(self):
        self.redis = None
    
    async def connect(self):
        self.redis = await aioredis.create_redis_pool(
            'redis://localhost'
        )
    
    async def get(self, key: str) -> Optional[str]:
        return await self.redis.get(key, encoding='utf-8')
    
    async def set(self, key: str, value: str, expire: int = 3600):
        await self.redis.setex(key, expire, value)
```

### 3. Query Optimization

```python
# Use projections to fetch only needed fields
async def get_ontology_summary(db_name: str, class_id: str):
    query = """
    SELECT ?label ?description
    WHERE {
        ?class a owl:Class ;
               rdfs:label ?label ;
               rdfs:comment ?description .
        FILTER(?class = <%s>)
    }
    """ % class_id
    
    return await terminus.query(db_name, query)

# Batch operations
async def create_multiple_ontologies(db_name: str, ontologies: List[Dict]):
    # Use transaction for atomicity
    async with terminus.transaction(db_name) as tx:
        for ontology in ontologies:
            await tx.create_ontology(ontology)
        await tx.commit()
```

## Debugging Tips

### 1. Logging Configuration

```python
# shared/utils/logging.py
import logging
import json
from pythonjsonlogger import jsonlogger

def setup_logging(level: str = "INFO"):
    """Configure structured JSON logging."""
    
    # Create formatter
    formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Configure root logger
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    
    logger = logging.getLogger()
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, level))
    
    # Add context filter
    logger.addFilter(ContextFilter())

class ContextFilter(logging.Filter):
    """Add request context to log records."""
    
    def filter(self, record):
        # Add request ID if available
        record.request_id = getattr(request_context, 'request_id', None)
        record.user_id = getattr(request_context, 'user_id', None)
        return True

# Usage
logger = logging.getLogger(__name__)
logger.info("Processing request", extra={
    "action": "create_ontology",
    "db_name": db_name,
    "class_id": class_id
})
```

### 2. Debug Endpoints

```python
# Add debug endpoints in development
if settings.DEBUG:
    @app.get("/debug/config")
    async def debug_config():
        """Show current configuration (dev only)."""
        return {
            "services": {
                "oms": settings.OMS_BASE_URL,
                "funnel": settings.FUNNEL_BASE_URL,
            },
            "features": {
                "https_enabled": settings.ENABLE_HTTPS,
                "cors_origins": settings.CORS_ORIGINS,
            }
        }
    
    @app.get("/debug/health/detailed")
    async def detailed_health():
        """Detailed health check of all services."""
        checks = {
            "oms": await check_service_health(settings.OMS_BASE_URL),
            "funnel": await check_service_health(settings.FUNNEL_BASE_URL),
            "terminus": await check_terminus_health(),
        }
        return checks
```

### 3. Request Tracing

```python
# shared/middleware/tracing.py
import uuid
from fastapi import Request

async def request_tracing_middleware(request: Request, call_next):
    # Generate request ID
    request_id = str(uuid.uuid4())
    
    # Add to request state
    request.state.request_id = request_id
    
    # Log request
    logger.info(f"Request started", extra={
        "request_id": request_id,
        "method": request.method,
        "path": request.url.path,
        "client": request.client.host
    })
    
    # Process request
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    
    # Log response
    logger.info(f"Request completed", extra={
        "request_id": request_id,
        "status_code": response.status_code,
        "duration_ms": int(duration * 1000)
    })
    
    # Add request ID to response headers
    response.headers["X-Request-ID"] = request_id
    
    return response
```

## Contributing Guidelines

### 1. Pull Request Process

1. **Fork and Clone**
   ```bash
   git clone https://github.com/your-username/spice-harvester.git
   cd spice-harvester
   git remote add upstream https://github.com/original/spice-harvester.git
   ```

2. **Create Feature Branch**
   ```bash
   git checkout -b feature/your-feature
   ```

3. **Make Changes**
   - Write code following style guidelines
   - Add tests for new functionality
   - Update documentation as needed

4. **Test Changes**
   ```bash
   # Run tests
   pytest
   
   # Check code style
   black backend/
   pylint backend/
   
   # Run type checks
   mypy backend/
   ```

5. **Submit PR**
   - Push to your fork
   - Create pull request with description
   - Link related issues
   - Wait for review

### 2. Code Review Checklist

- [ ] Code follows project style guidelines
- [ ] Tests are included and passing
- [ ] Documentation is updated
- [ ] No security vulnerabilities introduced
- [ ] Performance impact considered
- [ ] Breaking changes documented
- [ ] Error handling is appropriate
- [ ] Logging is sufficient

### 3. Release Process

1. **Version Bump**
   ```python
   # backend/__version__.py
   __version__ = "1.2.0"  # Major.Minor.Patch
   ```

2. **Update Changelog**
   ```markdown
   ## [1.2.0] - 2025-07-23
   
   ### Added
   - New feature description
   
   ### Changed
   - Modified behavior description
   
   ### Fixed
   - Bug fix description
   ```

3. **Create Release**
   ```bash
   git tag -a v1.2.0 -m "Release version 1.2.0"
   git push origin v1.2.0
   ```

## Additional Resources

### Internal Documentation
- [Architecture Overview](./ARCHITECTURE.md)
- [API Reference](./API_REFERENCE.md)
- [Security Guidelines](./SECURITY.md)
- [Operations Manual](./OPERATIONS.md)

### External Resources
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [TerminusDB Documentation](https://terminusdb.com/docs/)
- [Pydantic Documentation](https://pydantic-docs.helpmanual.io/)
- [pytest Documentation](https://docs.pytest.org/)

### Community
- GitHub Issues: Report bugs and request features
- Discussions: Ask questions and share ideas
- Wiki: Community-contributed guides and tips