# OMS Database Management API Endpoints

This document describes the database management endpoints provided by the Ontology Management Service (OMS), including Git-like version control features.

**Last Updated**: 2025-07-25
**TerminusDB Version**: v11.x
**Git Features**: Fully implemented (7/7 features working)

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

## Git-like Version Control Endpoints (NEW)

### Branch Management

#### 1. List Branches

**Endpoint:** `GET /{db_name}/branches`

**Response:**
```json
{
  "success": true,
  "data": {
    "branches": [
      {
        "name": "main",
        "is_current": true,
        "created_at": "2025-01-01T00:00:00Z",
        "commit_count": 15
      },
      {
        "name": "experiment/feature-a",
        "is_current": false,
        "created_at": "2025-01-15T10:30:00Z",
        "commit_count": 3
      }
    ]
  }
}
```

#### 2. Create Branch

**Endpoint:** `POST /{db_name}/branches`

**Request Body:**
```json
{
  "name": "experiment/new-feature",
  "source_branch": "main"
}
```

**Response:**
```json
{
  "success": true,
  "message": "Branch 'experiment/new-feature' created from 'main'",
  "data": {
    "branch_name": "experiment/new-feature",
    "source_branch": "main",
    "created_at": "2025-01-20T14:30:00Z"
  }
}
```

#### 3. Delete Branch

**Endpoint:** `DELETE /{db_name}/branches/{branch_name}`

**Response:**
```json
{
  "success": true,
  "message": "Branch 'experiment/old-feature' deleted successfully"
}
```

### Diff and Comparison

#### 4. Compare Branches

**Endpoint:** `GET /{db_name}/diff`

**Query Parameters:**
- `from_branch`: Source branch name
- `to_branch`: Target branch name

**Response:**
```json
{
  "success": true,
  "data": {
    "changes": [
      {
        "type": "class_modified",
        "class_id": "Product",
        "path": "schema/Product",
        "property_changes": [
          {
            "property": "description",
            "change": "added",
            "new_definition": {
              "@class": "xsd:string",
              "@type": "Optional"
            }
          }
        ]
      }
    ],
    "summary": {
      "total_changes": 1,
      "classes_added": 0,
      "classes_modified": 1,
      "classes_removed": 0
    }
  }
}
```

### Merge Operations

#### 5. Merge Branches

**Endpoint:** `POST /{db_name}/merge`

**Request Body:**
```json
{
  "source_branch": "experiment/feature-a",
  "target_branch": "main",
  "message": "Merge feature-a into main",
  "author": "developer"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "merged": true,
    "message": "Successfully merged experiment/feature-a into main",
    "commit_id": "commit_1737757890123"
  }
}
```

### Pull Request System

#### 6. Create Pull Request

**Endpoint:** `POST /{db_name}/pull-requests`

**Request Body:**
```json
{
  "source_branch": "experiment/feature-b",
  "target_branch": "main",
  "title": "Add new Product features",
  "description": "This PR adds description and category fields to Product class",
  "author": "developer"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "id": "pr_1737757890124",
    "title": "Add new Product features",
    "status": "open",
    "can_merge": true,
    "conflicts": [],
    "stats": {
      "total_changes": 2,
      "classes_modified": 1
    },
    "created_at": "2025-01-20T15:00:00Z"
  }
}
```

#### 7. Merge Pull Request

**Endpoint:** `POST /{db_name}/pull-requests/{pr_id}/merge`

**Request Body:**
```json
{
  "merge_message": "Merged PR: Add new Product features",
  "author": "maintainer"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "merged": true,
    "commit_id": "commit_1737757890125",
    "message": "PR merged successfully"
  }
}
```

### Commit History

#### 8. Get Commit History

**Endpoint:** `GET /{db_name}/commits`

**Query Parameters:**
- `branch`: Branch name (optional, defaults to current branch)
- `limit`: Number of commits to return (optional, default: 10)

**Response:**
```json
{
  "success": true,
  "data": {
    "commits": [
      {
        "id": "commit_1737757890125",
        "message": "Add description field to Product",
        "author": "developer",
        "timestamp": "2025-01-20T15:30:00Z",
        "branch": "main"
      }
    ]
  }
}
```

### Rollback Operations

#### 9. Rollback to Commit

**Endpoint:** `POST /{db_name}/rollback`

**Request Body:**
```json
{
  "commit_id": "commit_1737757890123",
  "branch": "main",
  "author": "admin",
  "message": "Rollback to stable version"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "rolled_back": true,
    "target_commit": "commit_1737757890123",
    "new_commit_id": "commit_1737757890126"
  }
}
```

## Standard Database Endpoints

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

### Git Operations with curl

```bash
# List branches
curl http://localhost:8000/api/v1/database/my_db/branches

# Create a new branch
curl -X POST http://localhost:8000/api/v1/database/my_db/branches \
  -H "Content-Type: application/json" \
  -d '{"name": "experiment/new-feature", "source_branch": "main"}'

# Compare branches
curl "http://localhost:8000/api/v1/database/my_db/diff?from_branch=main&to_branch=experiment/new-feature"

# Create a pull request
curl -X POST http://localhost:8000/api/v1/database/my_db/pull-requests \
  -H "Content-Type: application/json" \
  -d '{
    "source_branch": "experiment/new-feature",
    "target_branch": "main",
    "title": "Add new features",
    "description": "This PR adds important new features"
  }'

# Merge branches directly
curl -X POST http://localhost:8000/api/v1/database/my_db/merge \
  -H "Content-Type: application/json" \
  -d '{
    "source_branch": "experiment/feature-a",
    "target_branch": "main",
    "message": "Merge feature-a",
    "author": "developer"
  }'

# Get commit history
curl "http://localhost:8000/api/v1/database/my_db/commits?branch=main&limit=5"
```

### Standard Database Operations with curl

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

# Git operations
async with httpx.AsyncClient() as client:
    # List branches
    response = await client.get("http://localhost:8000/api/v1/database/my_db/branches")
    branches = response.json()["data"]["branches"]
    
    # Create branch
    response = await client.post(
        "http://localhost:8000/api/v1/database/my_db/branches",
        json={"name": "experiment/test", "source_branch": "main"}
    )
    
    # Compare branches
    response = await client.get(
        "http://localhost:8000/api/v1/database/my_db/diff",
        params={"from_branch": "main", "to_branch": "experiment/test"}
    )
    diff_result = response.json()["data"]
    
    # Create pull request
    response = await client.post(
        "http://localhost:8000/api/v1/database/my_db/pull-requests",
        json={
            "source_branch": "experiment/test",
            "target_branch": "main",
            "title": "Test feature",
            "description": "Testing new functionality"
        }
    )
    pr = response.json()["data"]
    
    # Merge pull request
    response = await client.post(
        f"http://localhost:8000/api/v1/database/my_db/pull-requests/{pr['id']}/merge",
        json={"merge_message": "Merged test feature", "author": "developer"}
    )

# Standard database operations
async with httpx.AsyncClient() as client:
    # List databases
    response = await client.get("http://localhost:8000/api/v1/database/list")
    databases = response.json()["data"]["databases"]

    # Create a database
    response = await client.post(
        "http://localhost:8000/api/v1/database/create",
        json={"name": "new_database", "description": "My new database"}
    )
    result = response.json()
```

## Git Features Implementation Details

### Technical Architecture
1. **3-Stage Diff System**: Commit-based, schema-level, and property-level comparison
2. **Rebase-based Merging**: Uses TerminusDB's rebase API instead of traditional merge
3. **NDJSON Parsing**: Handles TerminusDB's newline-delimited JSON responses
4. **Conflict Detection**: Identifies schema conflicts during PR creation
5. **Branch Isolation**: Branches share data but maintain separate commit histories

### Multi-Branch Experiments
1. **Unlimited Branches**: Create as many experimental branches as needed
2. **A/B Testing**: Compare different schema variants simultaneously  
3. **Integration Testing**: Merge multiple experiments into integration branches
4. **Performance Metrics**: Collect data on experiment effectiveness
5. **Safe Merging**: Only merge successful experiments to production

### Supported Git Operations
- ✅ **Branch Management**: Create, list, delete, switch branches
- ✅ **Commit System**: Full history with messages and authors  
- ✅ **Diff Operations**: Real differences between any two branches
- ✅ **Merge Operations**: Conflict-aware merging with rebase
- ✅ **Pull Requests**: Complete review workflow with conflict detection
- ✅ **Rollback**: Reset to any previous commit safely
- ✅ **Version History**: Complete audit trail of all changes

## Security Considerations

1. **Input Validation**: All database names and branch names are validated to prevent injection attacks
2. **Protected Databases**: System databases cannot be modified or deleted
3. **Authentication**: TerminusDB authentication is required for all operations
4. **Branch Protection**: Main branch can be protected from direct commits
5. **Conflict Safety**: Merge operations are atomic and can be safely rolled back
6. **Error Messages**: Sensitive information is not exposed in error messages
7. **Audit Logging**: All database and git operations are logged for compliance
8. **Experiment Isolation**: Experimental branches cannot affect production data