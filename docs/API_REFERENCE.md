# SPICE HARVESTER API Reference

> **Last Updated**: 2025-07-26  
> **API Version**: v1.0  
> **Status**: Production Ready (90-95% Complete)

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [BFF API Endpoints](#bff-api-endpoints)
4. [OMS API Endpoints](#oms-api-endpoints)
5. [Funnel API Endpoints](#funnel-api-endpoints)
6. [Git-like Features](#git-like-features)
7. [Common Models](#common-models)
8. [Error Handling](#error-handling)
9. [Performance & Rate Limiting](#performance--rate-limiting)

## Overview

SPICE HARVESTER provides RESTful APIs across three main services:

- **BFF (Backend for Frontend)**: User-facing API with label-based operations
- **OMS (Ontology Management Service)**: Internal API with ID-based operations
- **Funnel (Type Inference Service)**: Data analysis and type inference API

### Base URLs

| Service | Development | Production |
|---------|-------------|------------|
| BFF | `http://localhost:8002/api/v1` | `https://api.spiceharvester.com/v1` |
| OMS | `http://localhost:8000/api/v1` | Internal only |
| Funnel | `http://localhost:8004/api/v1` | Internal only |

### Content Types

All APIs accept and return JSON:
```
Content-Type: application/json
Accept: application/json
```

### Response Format (ApiResponse)

All endpoints use a standardized response format:
```json
{
  "success": true,  // or false for errors
  "message": "Human-readable message",
  "data": {        // Optional response data
    // Response content
  }
}
```

## Authentication

### API Key Authentication

Include API key in request headers:
```http
X-API-Key: your-api-key-here
```

### TerminusDB Authentication

OMS requires TerminusDB credentials:
```http
Authorization: Basic base64(username:password)
```

## BFF API Endpoints

### Database Management

#### List Databases
```http
GET /databases
```

**Response:**
```json
{
  "success": true,
  "message": "데이터베이스 목록 조회 성공",
  "data": {
    "databases": [
      {"name": "db1"},
      {"name": "db2"},
      {"name": "db3"}
    ]
  }
}
```

#### Create Database
```http
POST /databases
```

**Request Body:**
```json
{
  "name": "my_database",
  "description": "Database description"
}
```

**Response:**
```json
{
  "success": true,
  "message": "데이터베이스 'my_database'가 성공적으로 생성되었습니다",
  "data": {
    "name": "my_database",
    "created_at": "2025-07-23T10:30:00Z"
  }
}
```

#### Delete Database
```http
DELETE /databases/{db_name}
```

**Response:**
```json
{
  "success": true,
  "message": "데이터베이스 'my_database'가 성공적으로 삭제되었습니다",
  "data": null
}
```

### Ontology Management

#### Create Ontology Class
```http
POST /database/{db_name}/ontology
```

**Request Body:**
```json
{
  "label": "Product",
  "description": "E-commerce product",
  "properties": [
    {
      "name": "name",
      "type": "STRING",
      "label": "Product Name",
      "required": true
    },
    {
      "name": "price",
      "type": "MONEY",
      "label": "Price",
      "required": true,
      "constraints": {
        "currency": "USD",
        "min": 0
      }
    },
    {
      "name": "images",
      "type": "ARRAY<STRING>",
      "label": "Product Images",
      "required": false
    }
  ],
  "relationships": [
    {
      "predicate": "belongs_to",
      "target": "Category",
      "label": "Belongs to Category",
      "cardinality": "n:1"
    }
  ]
}
```

**Response:**
```json
{
  "success": true,
  "message": "온톨로지가 성공적으로 생성되었습니다",
  "data": {
    "ontology": {
      "id": "Product",
      "label": "Product",
      "created_at": "2025-07-23T10:30:00Z",
      "properties": [...],
      "relationships": [...]
    }
  }
}
```

#### Get Ontology Class
```http
GET /database/{db_name}/ontology/{class_label}
```

**Response:**
```json
{
  "id": "Product",
  "label": "Product",
  "description": "E-commerce product",
  "properties": [...],
  "relationships": [...],
  "metadata": {
    "created_at": "2025-07-23T10:30:00Z",
    "updated_at": "2025-07-23T10:30:00Z"
  }
}
```

#### Update Ontology Class
```http
PUT /database/{db_name}/ontology/{class_label}
```

**Request Body:**
```json
{
  "description": "Updated description",
  "properties": [...],
  "relationships": [...]
}
```

#### Delete Ontology Class
```http
DELETE /database/{db_name}/ontology/{class_label}
```

### Query Operations

#### Query Instances
```http
POST /database/{db_name}/query
```

**Request Body:**
```json
{
  "class_label": "Product",
  "filters": [
    {
      "field": "price",
      "operator": "gte",
      "value": 100
    }
  ],
  "select": ["name", "price"],
  "limit": 20,
  "offset": 0,
  "order_by": "price",
  "order_direction": "desc"
}
```

### Mapping Management

#### Export Label Mappings
```http
POST /database/{db_name}/mappings/export
```

**Response:** CSV file download

#### Import Label Mappings
```http
POST /database/{db_name}/mappings/import
```

**Request:** Multipart form data with JSON file

**Features:**
- Enhanced security validation with input sanitization
- Schema validation using Pydantic models
- Real-time validation against actual ontology data (not hardcoded)
- Backup and rollback support
- Detailed error reporting

#### Validate Label Mappings
```http
POST /database/{db_name}/mappings/validate
```

**Request:** Multipart form data with JSON file

**Response:**
```json
{
  "status": "success|warning|error",
  "message": "매핑 검증 완료",
  "data": {
    "validation_passed": true|false,
    "details": {
      "unmapped_classes": [],
      "unmapped_properties": [],
      "conflicts": [],
      "duplicate_labels": []
    },
    "stats": {
      "classes": 2,
      "properties": 5,
      "total": 7
    }
  }
}
```

**Features:**
- Real validation against actual ontology data (not hardcoded `validation_passed: true`)
- Database existence verification
- Class and property ID validation through OMS client
- Duplicate label detection
- Conflict detection with existing mappings
- Detailed validation error reporting

## OMS API Endpoints

### Database Operations

#### List Databases
```http
GET /database/list
```

**Response:**
```json
{
  "success": true,
  "message": "데이터베이스 목록 조회 성공",
  "data": {
    "databases": [
      {"name": "db1"},
      {"name": "db2"},
      {"name": "db3"}
    ]
  }
}
```

#### Create Database
```http
POST /database/create
```

**Request Body:**
```json
{
  "name": "database_name",
  "description": "Optional description"
}
```

#### Delete Database
```http
DELETE /database/{db_name}
```

#### Check Database Existence
```http
GET /database/exists/{db_name}
```

### Ontology Operations

#### Create Ontology
```http
POST /ontology/{db_name}/create
```

**Request Body:**
```json
{
  "id": "ClassID",
  "label": "Class Label",
  "properties": [...],
  "relationships": [...],
  "parent_class": "ParentClass",
  "abstract": false
}
```

#### List Ontologies
```http
GET /ontology/{db_name}/list
```

#### Get Ontology
```http
GET /ontology/{db_name}/{class_id}
```

#### Update Ontology
```http
PUT /ontology/{db_name}/{class_id}
```

#### Delete Ontology
```http
DELETE /ontology/{db_name}/{class_id}
```

### Advanced Ontology Features

#### Analyze Relationship Network
```http
GET /ontology/{db_name}/analyze-network
```

**Query Parameters:**
- `max_depth`: Maximum traversal depth (default: 3)

#### Validate Relationships
```http
POST /ontology/{db_name}/validate-relationships
```

**Request Body:**
```json
{
  "class_id": "Product",
  "relationships": [...]
}
```

#### Detect Circular References
```http
POST /ontology/{db_name}/detect-circular-references
```

**Request Body:**
```json
{
  "class_id": "ClassA",
  "relationships": [...]
}
```

#### Find Relationship Paths
```http
GET /ontology/{db_name}/relationship-paths/{start_entity}
```

**Query Parameters:**
- `target_entity`: Target entity ID
- `max_depth`: Maximum path length
- `path_type`: "shortest" | "all"

### Branch Management

#### List Branches
```http
GET /branch/{db_name}/list
```

#### Create Branch
```http
POST /branch/{db_name}/create
```

**Request Body:**
```json
{
  "branch_name": "feature-branch",
  "from_branch": "main",
  "description": "Feature development branch"
}
```

#### Delete Branch
```http
DELETE /branch/{db_name}/branch/{branch_name}
```

#### Checkout Branch
```http
POST /branch/{db_name}/checkout
```

**Request Body:**
```json
{
  "branch_name": "feature-branch"
}
```

## Git-like Features

SPICE HARVESTER provides complete Git-like version control for ontology management. All 7 core Git features are fully implemented:

### Branch Management

#### List Branches
```http
GET /database/{db_name}/branches
```

**Response:**
```json
{
  "success": true,
  "message": "브랜치 목록 조회 성공",
  "data": {
    "branches": ["main", "feature/new-ontology", "experiment/test"],
    "current": "main"
  }
}
```

#### Create Branch
```http
POST /database/{db_name}/branch
```

**Request Body:**
```json
{
  "branch_name": "feature/new-ontology",
  "from_branch": "main"
}
```

#### Delete Branch
```http
DELETE /database/{db_name}/branch/{branch_name}
```

#### Checkout Branch
```http
POST /database/{db_name}/checkout
```

**Request Body:**
```json
{
  "target": "feature/new-ontology"  // branch name or commit ID
}
```

### Commit Management

#### Create Commit
```http
POST /database/{db_name}/commit
```

**Request Body:**
```json
{
  "message": "Add new Product ontology",
  "author": "developer@example.com"
}
```

#### Get Commit History
```http
GET /database/{db_name}/history
```

**Query Parameters:**
- `branch`: Specific branch (default: current)
- `limit`: Number of commits (default: 10)
- `offset`: Skip commits

**Response:**
```json
{
  "success": true,
  "message": "커밋 히스토리 조회 성공",
  "data": {
    "commits": [
      {
        "id": "commit_1737757890123",
        "message": "Add new Product ontology",
        "author": "developer@example.com",
        "timestamp": "2025-07-26T10:30:00Z",
        "branch": "main"
      }
    ],
    "total": 42
  }
}
```

### Diff & Merge Operations

#### Get Diff Between Branches/Commits
```http
GET /database/{db_name}/diff
```

**Query Parameters:**
- `from`: Source branch/commit
- `to`: Target branch/commit

**Response (3-Stage Diff):**
```json
{
  "success": true,
  "message": "변경사항 비교 완료",
  "data": {
    "changes": [
      {
        "type": "class_added",
        "class_id": "Product",
        "details": {...}
      },
      {
        "type": "property_modified",
        "class_id": "Customer",
        "property": "email",
        "from": {"type": "STRING"},
        "to": {"type": "EMAIL"}
      }
    ],
    "summary": {
      "classes_added": 1,
      "classes_modified": 1,
      "classes_deleted": 0,
      "properties_changed": 3
    }
  }
}
```

#### Merge Branches
```http
POST /database/{db_name}/merge
```

**Request Body:**
```json
{
  "source_branch": "feature/new-ontology",
  "target_branch": "main",
  "message": "Merge feature/new-ontology into main",
  "author": "developer@example.com"
}
```

**Response:**
```json
{
  "success": true,
  "message": "브랜치 병합 성공",
  "data": {
    "merged": true,
    "conflicts": [],
    "commit_id": "commit_1737757890456"
  }
}
```

**Conflict Response:**
```json
{
  "success": false,
  "message": "병합 충돌 발생",
  "data": {
    "merged": false,
    "conflicts": [
      {
        "class_id": "Product",
        "conflict_type": "property_type_mismatch",
        "property": "price",
        "source_value": {"type": "DECIMAL"},
        "target_value": {"type": "MONEY"}
      }
    ]
  }
}
```

### Rollback Operations

#### Rollback to Previous Commit
```http
POST /database/{db_name}/rollback
```

**Request Body:**
```json
{
  "target": "HEAD~1",  // or specific commit ID
  "author": "admin@example.com"
}
```

**Supported References:**
- `HEAD`: Latest commit
- `HEAD~1`: Previous commit
- `HEAD~n`: n commits back
- Specific commit ID: `commit_1737757890123`

## Funnel API Endpoints

### Data Analysis

#### Analyze Dataset
```http
POST /analyze
```

**Request Body:**
```json
{
  "data": [
    ["John Doe", "john@example.com", "+1-555-0123", "100.50 USD"],
    ["Jane Smith", "jane@example.com", "+1-555-0124", "200.75 USD"]
  ],
  "columns": ["name", "email", "phone", "amount"],
  "sample_size": 1000
}
```

**Response:**
```json
{
  "columns": [
    {
      "name": "name",
      "inferred_type": "STRING",
      "confidence": 1.0,
      "sample_values": ["John Doe", "Jane Smith"],
      "null_count": 0,
      "unique_count": 2
    },
    {
      "name": "email",
      "inferred_type": "EMAIL",
      "confidence": 1.0,
      "constraints": {
        "format": "email"
      }
    },
    {
      "name": "phone",
      "inferred_type": "PHONE",
      "confidence": 1.0,
      "constraints": {
        "defaultRegion": "US"
      }
    },
    {
      "name": "amount",
      "inferred_type": "MONEY",
      "confidence": 0.95,
      "constraints": {
        "currency": "USD",
        "minAmount": 100.50,
        "maxAmount": 200.75
      }
    }
  ],
  "row_count": 2,
  "analysis_metadata": {
    "duration_ms": 150,
    "sample_size": 2
  }
}
```

#### Preview Google Sheets
```http
POST /preview/google-sheets
```

**Query Parameters:**
- `sheet_url`: Google Sheets URL
- `worksheet_name`: Specific worksheet (optional)
- `sample_size`: Number of rows to analyze

**Response:**
```json
{
  "data": [...],
  "columns": [...],
  "inferred_types": {...},
  "preview_metadata": {
    "total_rows": 1000,
    "sampled_rows": 100
  }
}
```

#### Suggest Schema
```http
POST /suggest-schema
```

**Request Body:** DatasetAnalysisResponse from `/analyze`

**Response:**
```json
{
  "suggested_schema": {
    "class_name": "ImportedData",
    "properties": [
      {
        "name": "email",
        "type": "EMAIL",
        "required": true,
        "constraints": {
          "format": "email"
        }
      }
    ]
  }
}
```

## Common Models

### Property Model
```typescript
interface Property {
  name: string;
  type: string;
  label: string;
  required: boolean;
  default?: any;
  description?: string;
  constraints?: Record<string, any>;
  
  // For relationship properties
  target?: string;
  linkTarget?: string;
  isRelationship?: boolean;
  cardinality?: string;
}
```

### Relationship Model
```typescript
interface Relationship {
  predicate: string;
  target: string;
  label: string;
  cardinality: "1:1" | "1:n" | "n:1" | "n:m";
  description?: string;
  inverse_predicate?: string;
  inverse_label?: string;
}
```

### Supported Data Types

#### Basic Types
- `STRING`: Text data
- `INTEGER`: Whole numbers
- `DECIMAL`: Decimal numbers
- `BOOLEAN`: True/false values
- `DATE`: Date without time
- `DATETIME`: Date with time

#### Complex Types
- `ARRAY<T>`: Array of type T
- `OBJECT`: Nested object with schema
- `ENUM`: Enumerated values
- `MONEY`: Monetary values with currency
- `PHONE`: Phone numbers with validation
- `EMAIL`: Email addresses
- `URL`: Web URLs
- `COORDINATE`: Geographic coordinates
- `ADDRESS`: Structured addresses
- `IMAGE`: Image URLs
- `FILE`: File references

### Constraint Types

#### Value Constraints
```json
{
  "min": 0,
  "max": 1000,
  "minLength": 3,
  "maxLength": 255,
  "pattern": "^[A-Z]{3}$",
  "enum": ["ACTIVE", "INACTIVE", "PENDING"]
}
```

#### Array Constraints
```json
{
  "minItems": 1,
  "maxItems": 10,
  "uniqueItems": true,
  "itemType": "STRING"
}
```

#### Complex Type Constraints
```json
{
  "currency": "USD",
  "allowedCurrencies": ["USD", "EUR", "GBP"],
  "defaultRegion": "US",
  "allowedDomains": ["example.com"],
  "maxSize": 10485760,
  "allowedExtensions": [".jpg", ".png", ".pdf"]
}
```

## Error Handling

### Error Response Format
```json
{
  "success": false,
  "message": "Validation failed",
  "data": null
}
```

**Note:** Error details are included in the HTTP response body with appropriate status codes:
- 400 Bad Request - Validation errors
- 404 Not Found - Resource not found
- 409 Conflict - Duplicate resources
- 500 Internal Server Error - Server errors

### Common Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `VALIDATION_ERROR` | 400 | Input validation failed |
| `NOT_FOUND` | 404 | Resource not found |
| `DUPLICATE_ERROR` | 409 | Resource already exists |
| `UNAUTHORIZED` | 401 | Authentication required |
| `FORBIDDEN` | 403 | Insufficient permissions |
| `INTERNAL_ERROR` | 500 | Server error |
| `SERVICE_UNAVAILABLE` | 503 | Service temporarily unavailable |

### Error Examples

#### Validation Error (400)
```json
{
  "success": false,
  "message": "올바르지 않은 데이터베이스 이름입니다. 이름은 소문자로 시작하고 3-50자 사이여야 합니다",
  "data": null
}
```

#### Not Found Error (404)
```json
{
  "success": false,
  "message": "온톨로지 'Product'을(를) 찾을 수 없습니다",
  "data": null
}
```

#### Conflict Error (409)
```json
{
  "success": false,
  "message": "온톨로지 'Product'이(가) 이미 존재합니다",
  "data": null
}
```

## Rate Limiting

### Limits

| Endpoint Type | Rate Limit | Window |
|---------------|------------|---------|
| Read operations | 1000 req/min | 1 minute |
| Write operations | 100 req/min | 1 minute |
| Bulk operations | 10 req/min | 1 minute |

### Rate Limit Headers

```http
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 950
X-RateLimit-Reset: 1627574400
```

### Rate Limit Exceeded Response

```json
{
  "success": false,
  "message": "Rate limit exceeded. Please try again in 60 seconds",
  "data": null
}
```

## Performance & Rate Limiting

### Performance Optimizations (2025-07-26)

- **HTTP Connection Pooling**: 50 keep-alive, 100 max connections
- **Concurrent Request Control**: Semaphore(50) to prevent overload
- **Response Time**: <5s under load (improved from 29.8s)
- **Success Rate**: 95%+ (improved from 70.3%)
- **Metadata Caching**: Prevents redundant schema creation

## Best Practices

### 1. Pagination

Always use pagination for list operations:
```http
GET /database/{db_name}/ontology?limit=20&offset=0
```

### 2. Field Selection

Request only needed fields:
```json
{
  "select": ["id", "label", "created_at"]
}
```

### 3. Batch Operations

Use bulk endpoints when available:
```http
POST /database/{db_name}/ontology/bulk
```

### 4. Idempotency

Include idempotency keys for write operations:
```http
X-Idempotency-Key: unique-request-id
```

### 5. Error Handling

Always check response status and handle errors:
```python
response = await client.post("/api/v1/databases", json=data)
if response.status_code != 200:
    error = response.json()
    handle_error(error["error"]["code"])
```

## API Versioning

The API uses URL versioning:
- Current version: `v1`
- Version in URL: `/api/v1/...`

### Deprecation Policy

1. New versions announced 6 months before release
2. Old versions supported for 12 months after new release
3. Deprecation warnings in headers:
   ```http
   X-API-Deprecation: true
   X-API-Deprecation-Date: 2026-01-01
   ```

## SDK Support

Official SDKs available for:
- Python: `pip install spice-harvester-sdk`
- JavaScript: `npm install @spice-harvester/sdk`
- Go: `go get github.com/spice-harvester/go-sdk`

## Additional Resources

- [OpenAPI Specification](/api/openapi.json)
- [Postman Collection](/api/postman-collection.json)
- [GraphQL Schema](/api/graphql-schema.graphql) (planned)
- [WebSocket Events](/api/websocket-events.md) (planned)