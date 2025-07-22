# OMS Database Management API Endpoints

This document describes the database management endpoints provided by the Ontology Management Service (OMS).

**Last Updated**: 2025-07-22
**TerminusDB Version**: v11.x

## Base URL

```
http://localhost:8000/api/v1/database
```

In production or Docker environment:
```
http://oms:8000/api/v1/database
```

## Authentication

All endpoints require TerminusDB authentication configured through environment variables:
- `TERMINUS_USER`: TerminusDB username (default: admin)
- `TERMINUS_KEY`: TerminusDB password (default: admin123)
- `TERMINUS_ACCOUNT`: TerminusDB account (default: admin)

## Endpoints

### 1. List Databases

Lists all databases in the TerminusDB instance.

**Endpoint:** `GET /list`

**Response:**
```json
{
  "success": true,
  "data": {
    "databases": [
      "test_db_1", 
      "production_db",
      "_system"
    ]
  }
}
```

**Errors:**
- `500`: Failed to retrieve database list

### 2. Create Database

Creates a new database in TerminusDB.

**Endpoint:** `POST /create`

**Request Body:**
```json
{
  "name": "my_new_database",
  "description": "Optional description of the database"
}
```

**Validation Rules:**
- Database name must be 3-50 characters
- Can only contain lowercase letters, numbers, underscores, and hyphens
- Must start with a letter
- Must end with a letter or number
- Cannot contain consecutive special characters

**Response:**
```json
{
  "success": true,
  "message": "데이터베이스 'my_new_database'이(가) 생성되었습니다",
  "data": {
    "database": "my_new_database",
    "created_at": "2024-01-20T10:30:00Z"
  }
}
```

**Errors:**
- `400`: Invalid database name or missing required field
- `409`: Database already exists
- `500`: Failed to create database

### 3. Delete Database

Deletes a database from TerminusDB.

**Endpoint:** `DELETE /{db_name}`

**Path Parameters:**
- `db_name`: Name of the database to delete

**Protected Databases:**
The following system databases cannot be deleted:
- `_system`
- `_meta`

**Response:**
```json
{
  "success": true,
  "message": "데이터베이스 'my_database'이(가) 삭제되었습니다",
  "database": "my_database"
}
```

**Errors:**
- `400`: Invalid database name
- `403`: Attempted to delete protected database
- `404`: Database not found
- `500`: Failed to delete database

### 4. Check Database Existence

Checks if a database exists in TerminusDB.

**Endpoint:** `GET /exists/{db_name}`

**Path Parameters:**
- `db_name`: Name of the database to check

**Response (Exists):**
```json
{
  "success": true,
  "data": {
    "exists": true
  }
}
```

**Response (Not Exists):**
```json
{
  "success": false,
  "error": "Not Found",
  "detail": "데이터베이스 'nonexistent_db'을(를) 찾을 수 없습니다"
}
```

**Errors:**
- `400`: Invalid database name
- `404`: Database not found
- `500`: Failed to check database existence

## Error Response Format

All error responses follow this format:

```json
{
  "success": false,
  "error": "Error Type",
  "detail": "Detailed error message in Korean"
}
```

## Database Name Validation

Database names must comply with the following rules:
1. Length: 3-50 characters
2. Allowed characters: lowercase letters (a-z), numbers (0-9), underscores (_), hyphens (-)
3. Must start with a letter
4. Must end with a letter or number
5. Cannot contain consecutive special characters (__, --, _-, -_)

Examples of valid database names:
- `my_database`
- `test-db-123`
- `ontology_v2`

Examples of invalid database names:
- `My_Database` (uppercase not allowed)
- `2_database` (cannot start with number)
- `db__test` (consecutive special characters)
- `db-` (cannot end with special character)
- `db` (too short)

## Integration with Docker Compose

When running with Docker Compose, the OMS service is accessible at:
- From host machine: `http://localhost:8000`
- From other containers: `http://oms:8000`

Environment variables are configured in `docker-compose.yml`:
```yaml
environment:
  - TERMINUS_SERVER_URL=${TERMINUS_SERVER_URL:-http://terminusdb:6363}
  - TERMINUS_USER=${TERMINUS_USER:-admin}
  - TERMINUS_ACCOUNT=${TERMINUS_ACCOUNT:-admin}
  - TERMINUS_KEY=${TERMINUS_KEY:-${TERMINUSDB_ADMIN_PASS:-admin123}}
```

## Schema Management Features (v11.x)

### New Features:
1. **Property-to-Relationship Conversion**: Automatically converts class properties to relationships when they reference other classes
2. **Advanced Constraint System**: Supports detailed constraints including min/max values, patterns, cardinality
3. **Complex Schema Types**: Full support for TerminusDB v11.x types including:
   - OneOfType (Union types)
   - Foreign keys
   - GeoPoint, GeoTemporalPoint
   - Enum types
   - Set, List, Array with dimensions
   - Optional types

### Schema Type Changes:
- `sys:JSON` is now replaced with `xsd:string` for metadata fields
- Complex types are fully validated before storage
- Automatic constraint extraction from schema definitions

## Usage Examples

### Using curl

```bash
# List databases
curl http://localhost:8000/api/v1/database/list

# Create a database
curl -X POST http://localhost:8000/api/v1/database/create \
  -H "Content-Type: application/json" \
  -d '{"name": "test_database", "description": "Test database for development"}'

# Check if database exists
curl http://localhost:8000/api/v1/database/exists/test_database

# Delete a database
curl -X DELETE http://localhost:8000/api/v1/database/test_database
```

### Using Python httpx

```python
import httpx

# List databases
async with httpx.AsyncClient() as client:
    response = await client.get("http://localhost:8000/api/v1/database/list")
    databases = response.json()["data"]["databases"]

# Create a database
async with httpx.AsyncClient() as client:
    response = await client.post(
        "http://localhost:8000/api/v1/database/create",
        json={"name": "new_database", "description": "My new database"}
    )
    result = response.json()
```

## Security Considerations

1. **Input Validation**: All database names are validated and sanitized to prevent injection attacks
2. **Protected Databases**: System databases cannot be modified or deleted
3. **Authentication**: TerminusDB authentication is required for all operations
4. **Error Messages**: Sensitive information is not exposed in error messages
5. **Logging**: All database operations are logged for audit purposes