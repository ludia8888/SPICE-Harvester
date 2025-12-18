# SPICE HARVESTER API Reference

> **Last Updated**: 2025-12-18  
> **API Version**: v1  
> **Status**: Event Sourcing steady state (docs are partially curated; OpenAPI is the source of truth)

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [BFF API Endpoints](#bff-api-endpoints)
4. [OMS API Endpoints](#oms-api-endpoints)
   - [Asynchronous API](#asynchronous-api)
5. [Funnel API Endpoints](#funnel-api-endpoints)
6. [Git-like Features](#git-like-features)
7. [Command Status Tracking](#command-status-tracking) ⭐ NEW
8. [WebSocket Real-time Updates](#websocket-real-time-updates) ⭐ NEW
9. [Common Models](#common-models)
10. [Error Handling](#error-handling)
11. [Performance & Rate Limiting](#performance--rate-limiting)
12. [AI (LLM-assisted)](#ai-llm-assisted)

## Overview

SPICE HARVESTER provides RESTful APIs across three main services with advanced Command/Event architecture:

- **BFF (Backend for Frontend)**: User-facing API with label-based operations
- **OMS (Ontology Management Service)**: Internal API with ID-based operations + Command/Event Sourcing
- **Funnel (Type Inference Service)**: Data analysis and type inference API

### Recent Additions (steady state)
- **Asynchronous writes**: Command/Event append-only to S3/MinIO Event Store (SSoT)
- **Command status API**: async processing observability via `GET /api/v1/commands/{command_id}/status`
- **Optimistic concurrency (OCC)**: `expected_seq` required on update/delete (409 on mismatch)

### Base URLs

| Service | Development | Production |
|---------|-------------|------------|
| BFF | `http://localhost:8002/api/v1` | `https://api.spiceharvester.com/v1` |
| OMS | `http://localhost:8000/api/v1` | Internal only |
| Funnel | `http://localhost:8003/api/v1` | Internal only |

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

### Admin Operations (Protected)

⚠️ These endpoints are for operators/admins only.

**Auth contract**
- Admin endpoints are disabled unless `BFF_ADMIN_TOKEN` is configured on the BFF service.
- Required header: `X-Admin-Token: <BFF_ADMIN_TOKEN>` (or `Authorization: Bearer <token>`)
- Optional header: `X-Admin-Actor: <name/email>` (recorded into audit metadata)

**Rate limit**
- Strict by default: 10 requests / 60s per IP.

#### Recompute Projection (Versioning + Recompute)
Rebuild an Elasticsearch read model by replaying immutable domain events (preferred production recovery path).

```http
POST /admin/recompute-projection
```

**Request Body:**
```json
{
  "db_name": "production",
  "projection": "instances",
  "branch": "main",
  "from_ts": "2025-01-01T00:00:00Z",
  "to_ts": "2025-01-02T00:00:00Z",
  "promote": false,
  "allow_delete_base_index": false,
  "max_events": 100000
}
```

**Response (202 Accepted):**
```json
{
  "task_id": "b9c9b3d3-2b21-4e2a-9f9f-50b8f5d2d2a2",
  "status": "accepted",
  "message": "Projection recompute task started: instances",
  "status_url": "/api/v1/tasks/b9c9b3d3-2b21-4e2a-9f9f-50b8f5d2d2a2"
}
```

#### Get Recompute Result
```http
GET /admin/recompute-projection/{task_id}/result
```

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

<a id="ai-llm-assisted"></a>
### AI (LLM-assisted Queries) ⭐ NEW

자연어 질문을 LLM으로 **“안전한 쿼리 계획(JSON)”**으로 변환한 뒤, 서버가 결정론적 엔진(`/database/{db_name}/query` 또는 `/graph-query/{db_name}`)으로 실행하고, 결과를 다시 자연어로 요약합니다.

핵심 안전장치:
- LLM은 **실행/쓰기(write)를 하지 않고**, 오직 “계획”과 “요약”만 생성합니다.
- 입력/결과는 **마스킹/샘플링**되어 LLM에 전달됩니다.
- LLM 응답은 **JSON 스키마 검증**을 통과해야 합니다.

#### 자연어 → 쿼리 계획(실행 없음)
```http
POST /ai/translate/query-plan/{db_name}
```

**Request Body (AIQueryRequest):**
```json
{
  "question": "Name이 Alice인 고객 찾아줘",
  "branch": "main",
  "mode": "auto",
  "limit": 50,
  "include_provenance": true,
  "include_documents": true
}
```

**Response:**
```json
{
  "plan": {
    "tool": "label_query",
    "interpretation": "Customer에서 Name이 Alice인 항목을 조회합니다.",
    "confidence": 0.9,
    "query": {
      "class_id": "Customer",
      "filters": [{"field": "Name", "operator": "eq", "value": "Alice"}],
      "select": ["Name"],
      "limit": 20,
      "offset": 0,
      "order_by": null,
      "order_direction": "asc"
    },
    "graph_query": null,
    "warnings": []
  },
  "llm": {
    "provider": "openai_compat",
    "model": "gpt-4.1-mini",
    "cache_hit": false,
    "latency_ms": 120
  }
}
```

#### 자연어 질의 실행 + 자연어 답변
```http
POST /ai/query/{db_name}
```

**Request Body:** 위와 동일(AIQueryRequest)

**Response (AIQueryResponse):**
```json
{
  "answer": {
    "answer": "Name이 Alice인 Customer는 1건입니다.",
    "confidence": 0.8,
    "rationale": null,
    "follow_ups": []
  },
  "plan": { "tool": "label_query", "interpretation": "...", "confidence": 0.9, "query": {...}, "graph_query": null, "warnings": [] },
  "execution": { "results": [{"Name": "Alice"}], "total": 1, "query": {...} },
  "llm": {
    "plan": { "provider": "openai_compat", "model": "gpt-4.1-mini", "cache_hit": false, "latency_ms": 120 },
    "answer": { "provider": "openai_compat", "model": "gpt-4.1-mini", "cache_hit": false, "latency_ms": 90 }
  },
  "warnings": []
}
```

> 운영 설정/가드레일/감사 정책은 `docs/LLM_INTEGRATION.md`를 참고하세요.

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

### AI Schema Suggestion (Data Import)

#### Suggest Schema From Data (Rows + Columns)
```http
POST /database/{db_name}/suggest-schema-from-data
```

**Request Body:**
```json
{
  "data": [["1", "Shirt", "15,000원"]],
  "columns": ["id", "name", "price"],
  "class_name": "MyImportedData",
  "include_complex_types": true
}
```

#### Suggest Schema From Google Sheets
```http
POST /database/{db_name}/suggest-schema-from-google-sheets
```

**Request Body:**
```json
{
  "sheet_url": "https://docs.google.com/spreadsheets/d/xxxx/edit#gid=0",
  "worksheet_name": "Sheet1",
  "class_name": "MyImportedData",
  "api_key": null,
  "table_id": "table_1",
  "table_bbox": {"top": 4, "left": 0, "bottom": 120, "right": 8}
}
```

#### Suggest Schema From Excel Upload (.xlsx/.xlsm)
```http
POST /database/{db_name}/suggest-schema-from-excel
```

**Multipart Form Data:**
- `file`: Excel file
- Query params (optional): `sheet_name`, `class_name`, `max_tables`, `max_rows`, `max_cols`, `include_complex_types`, `table_id`, `table_top`, `table_left`, `table_bottom`, `table_right`

**Notes:**
- 구조 분석 기반으로 멀티테이블/전치/폼 문서도 처리합니다.
- 응답에는 `preview_data`와 함께 `structure`(tables/key_values)가 포함되어, 프론트에서 테이블 선택 UI를 붙일 수 있습니다.

### AI Mapping Suggestion (Data Import)

#### Suggest Mappings From Google Sheets → Target Schema
```http
POST /database/{db_name}/suggest-mappings-from-google-sheets
```

**Request Body:**
```json
{
  "sheet_url": "https://docs.google.com/spreadsheets/d/xxxx/edit#gid=0",
  "worksheet_name": "Sheet1",
  "api_key": null,
  "target_class_id": "OrderItem",
  "target_schema": [
    {"name": "name", "type": "xsd:string"},
    {"name": "qty", "type": "xsd:integer"},
    {"name": "unit_price", "type": "xsd:decimal"}
  ],
  "table_id": "table_1",
  "table_bbox": {"top": 4, "left": 0, "bottom": 120, "right": 8},
  "include_relationships": false,
  "enable_semantic_hints": false
}
```

#### Suggest Mappings From Excel Upload → Target Schema
```http
POST /database/{db_name}/suggest-mappings-from-excel
```

**Multipart Form Data:**
- `file`: Excel file
- `target_schema_json`: JSON array string (e.g. `[{"name":"qty","type":"xsd:integer"}]`)
- Query params:
  - required: `target_class_id`
  - optional: `sheet_name`, `table_id`, `table_top`, `table_left`, `table_bottom`, `table_right`, `include_relationships`, `enable_semantic_hints`, `max_tables`, `max_rows`, `max_cols`

**Notes:**
- 두 엔드포인트 모두 응답에 `preview_data` + `structure` + `source_schema/target_schema` + `mappings`를 포함합니다.
- `enable_semantic_hints=false`가 기본값이며, 도메인 중립 동작을 우선합니다.
- 현재는 OMS 연동을 비활성화한 상태라, `target_schema`(또는 `target_schema_json`)를 클라이언트가 직접 전달해야 합니다.

### AI Import (Dry-run / Commit)

#### Dry-run Import From Google Sheets
```http
POST /database/{db_name}/import-from-google-sheets/dry-run
```

**Request Body:**
```json
{
  "sheet_url": "https://docs.google.com/spreadsheets/d/xxxx/edit#gid=0",
  "worksheet_name": "Sheet1",
  "api_key": null,
  "target_class_id": "OrderItem",
  "target_schema": [
    {"name": "name", "type": "xsd:string"},
    {"name": "qty", "type": "xsd:integer"},
    {"name": "unit_price", "type": "xsd:decimal"}
  ],
  "table_id": "table_1",
  "table_bbox": {"top": 4, "left": 0, "bottom": 120, "right": 8},
  "mappings": [
    {"source_field": "상품명", "target_field": "name"},
    {"source_field": "수량", "target_field": "qty"},
    {"source_field": "단가", "target_field": "unit_price"}
  ],
  "dry_run_rows": 100,
  "options": {}
}
```

#### Commit Import From Google Sheets (Prepare-only, OMS disabled)
```http
POST /database/{db_name}/import-from-google-sheets/commit
```

**Request Body (same shape as dry-run + commit options):**
```json
{
  "sheet_url": "https://docs.google.com/spreadsheets/d/xxxx/edit#gid=0",
  "worksheet_name": "Sheet1",
  "api_key": null,
  "target_class_id": "OrderItem",
  "target_schema": [{"name": "name", "type": "xsd:string"}],
  "mappings": [{"source_field": "상품명", "target_field": "name"}],
  "allow_partial": false,
  "max_import_rows": null,
  "batch_size": 500,
  "return_instances": false,
  "max_return_instances": 1000,
  "options": {}
}
```

#### Dry-run Import From Excel Upload
```http
POST /database/{db_name}/import-from-excel/dry-run
```

**Multipart Form Data:**
- `file`: Excel file
- `target_class_id`: string
- `target_schema_json`: JSON array string (e.g. `[{"name":"name","type":"xsd:string"}]`)
- `mappings_json`: JSON array string (e.g. `[{"source_field":"A","target_field":"name"}]`)
- Optional: `sheet_name`, `table_id`, `table_top`, `table_left`, `table_bottom`, `table_right`, `dry_run_rows`, `max_import_rows`, `options_json`

#### Commit Import From Excel Upload (Prepare-only, OMS disabled)
```http
POST /database/{db_name}/import-from-excel/commit
```

**Multipart Form Data:**
- `file`: Excel file
- `target_class_id`: string
- `target_schema_json`: JSON array string
- `mappings_json`: JSON array string
- Optional: `sheet_name`, `table_id`, `table_top`, `table_left`, `table_bottom`, `table_right`, `allow_partial`, `max_import_rows`, `batch_size`, `return_instances`, `max_return_instances`, `options_json`

**Notes:**
- Import는 Funnel의 구조 분석 결과(멀티테이블/전치/병합셀 등)를 기반으로 “선택된 테이블”을 정규화한 뒤 적용됩니다.
- `allow_partial=false`(기본)인 경우, 변환/검증 에러가 있으면 커밋을 거부하고 에러를 반환합니다.
- 현재는 OMS 연동을 비활성화한 상태라, `commit`은 실제 저장을 수행하지 않고 `prepared`(배치/옵션에 따라 생성된 instances)를 반환합니다.

### Graph Query (Federated Multi-hop) ⭐ NEW

TerminusDB(그래프 관계) + Elasticsearch(문서 payload)를 결합한 멀티홉 조회 API입니다.

#### Multi-hop Graph Query
```http
POST /graph-query/{db_name}?branch=<branch>
```

**Query Params:**
- `branch`: Target branch (default: `main`)

**Branch semantics (copy-on-write):**
- TerminusDB query runs on the requested branch (graph/schema authority).
- Elasticsearch payload is resolved with an overlay strategy:
  1) branch index (if present)
  2) fallback to main index (if missing)
  - branch deletes are tombstoned to prevent “fallback resurrection”.
  - tombstoned nodes return `data_status=MISSING` with `index_status.tombstoned=true` (intentional hide, not ES lag).

**Request Body:**
```json
{
  "start_class": "Product",
  "hops": [
    {"predicate": "owned_by", "target_class": "Customer"}
  ],
  "filters": {"product_id": "PROD-1"},
  "limit": 100,
  "offset": 0,
  "max_nodes": 500,
  "max_edges": 2000,
  "include_documents": true,
  "include_paths": false,
  "max_paths": 100,
  "no_cycles": false,
  "include_provenance": false
}
```

**Response (핵심 필드):**
- `nodes[].display`: ES가 없어도 UI가 안정적으로 렌더링할 수 있는 최소 표시 필드(그래프에서 조회)
- `nodes[].data_status`: `FULL|PARTIAL|MISSING`
  - `FULL`: ES payload 포함
  - `PARTIAL`: 요청에서 payload를 제외(`include_documents=false`)
  - `MISSING`: payload 요청했지만 ES 문서가 없음
- `nodes[].es_ref`: `{index,id}` (BFF/FE가 batch resolve 가능)
- `paths`: `include_paths=true`일 때만 포함 (비용이 크므로 기본 false)
- `index_summary`: 응답 내 ES payload 커버리지/age 요약 (best-effort)

**Example Response (ES 누락 시 UX 안정성):**
```json
{
  "nodes": [
    {
      "id": "Customer/CUST-1",
      "type": "Customer",
      "data_status": "MISSING",
      "display": {"primary_key": "CUST-1", "name": "Alice", "summary": "Alice"},
      "data": null
    }
  ],
  "edges": [
    {"from_node": "Product/PROD-1", "to_node": "Customer/CUST-1", "predicate": "owned_by"}
  ]
}
```

**Safety Guards (운영 안전장치):**
- `GRAPH_QUERY_MAX_HOPS` / `GRAPH_QUERY_MAX_LIMIT` / `GRAPH_QUERY_MAX_PATHS` 환경변수로 상한을 강제합니다.
- `max_nodes`/`max_edges`/`no_cycles`로 폭발/순환을 제어합니다.
- 관계 의미(도메인/레인지) 검증은 기본 ON(`GRAPH_QUERY_ENFORCE_SEMANTICS=true`)입니다.

### Lineage (Provenance) ⭐ NEW

#### Get Lineage Graph
```http
GET /lineage/graph?root=<event_id_or_node_id>&direction=both&max_depth=5
```

#### Get Lineage Impact (Downstream Artifacts)
```http
GET /lineage/impact?root=<event_id_or_node_id>&direction=downstream&max_depth=10
```

#### Get Lineage Metrics (Lag / Missing Ratio)
```http
GET /lineage/metrics?db_name=<db_name>&window_minutes=60
```

### Audit Logs ⭐ NEW

#### List Audit Logs
```http
GET /audit/logs?partition_key=db:<db_name>&limit=100
```

#### Get Audit Chain Head
```http
GET /audit/chain-head?partition_key=db:<db_name>
```

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
**Notes:**
- Best-effort cleanup: deletes any stale Elasticsearch overlay indices for the new branch name (prevents leaking old docs if a previous branch with the same name was deleted while ES was down).

#### Delete Branch
```http
DELETE /branch/{db_name}/branch/{branch_name}
```
**Notes:**
- Deletes the TerminusDB branch (graph/schema authority).
- Best-effort cleanup: deletes Elasticsearch overlay indices for that branch (`instances` + `ontologies`) to avoid cost drift.

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
**Notes:**
- Best-effort cleanup: deletes any stale Elasticsearch overlay indices for the new branch name.

#### Delete Branch
```http
DELETE /database/{db_name}/branch/{branch_name}
```
**Notes:**
- Best-effort cleanup: deletes Elasticsearch overlay indices for that branch (copy-on-write payload layer).

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
POST /version/{db_name}/rollback?branch=<branch>
```

⚠️ **Safety**:
- Rollback is **disabled by default**. Enable explicitly in non-production only:
  - `ENABLE_OMS_ROLLBACK=true`
- Protected branches (e.g. `main`, `production`) are blocked by default.
- Prefer “Versioning + Recompute” (rebuild read models by replay) for production recovery.

**Request Body:**
```json
{
  "target": "HEAD~1"
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

### Structure Analysis (Spreadsheet)

Funnel은 “엑셀/구글시트 같은 격자 데이터”를 `grid + merged_cells`로 표준화한 뒤,
멀티 테이블/전치 표/키-값 폼을 자동으로 분해해 줍니다.

#### Analyze Raw Sheet Grid
```http
POST /api/v1/funnel/structure/analyze
```

**Request Body:**
```json
{
  "grid": [["상품", "수량", "가격"], ["셔츠", "2", "15,000원"]],
  "merged_cells": [{"top": 1, "left": 0, "bottom": 2, "right": 0}],
  "include_complex_types": true,
  "max_tables": 5,
  "options": {}
}
```

#### Analyze Excel Upload (.xlsx/.xlsm)
```http
POST /api/v1/funnel/structure/analyze/excel
```

**Multipart Form Data:**
- `file`: Excel file
- `sheet_name` (optional)
- `max_rows`/`max_cols` (optional)
- `options_json` (optional, JSON string)

#### Analyze Google Sheets URL (End-to-End)
```http
POST /api/v1/funnel/structure/analyze/google-sheets
```

**Request Body:**
```json
{
  "sheet_url": "https://docs.google.com/spreadsheets/d/xxxx/edit#gid=0",
  "worksheet_name": "Sheet1",
  "api_key": null,
  "max_rows": 5000,
  "max_cols": 200,
  "include_complex_types": true,
  "max_tables": 5,
  "options": {}
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
- 409 Conflict - Duplicate resources or optimistic concurrency conflicts (`expected_seq` mismatch)
- 500 Internal Server Error - Server errors

### Common Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `VALIDATION_ERROR` | 400 | Input validation failed |
| `NOT_FOUND` | 404 | Resource not found |
| `DUPLICATE_ERROR` | 409 | Resource already exists |
| `OPTIMISTIC_CONCURRENCY_CONFLICT` | 409 | `expected_seq` mismatch (write rejected; no command appended) |
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

#### Conflict Error (409) — Optimistic Concurrency (OCC)

```json
{
  "detail": {
    "error": "optimistic_concurrency_conflict",
    "aggregate_id": "db:class:instance",
    "expected_seq": 41,
    "actual_seq": 42
  }
}
```

## Rate Limiting

> 참고: 레이트 리미팅은 Redis 백엔드를 사용하지만, Redis 장애 시에는 **서비스 가용성을 우선하여 fail-open(요청 허용)** 으로 동작합니다.

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

## Asynchronous API

Write operations are command-based and append-only to the Event Store.

- Write endpoints return **202 Accepted** with a `command_id`.
- Read endpoints return **200 OK** with the current read-side view.

### Submit commands (examples)

#### Create database

```http
POST /api/v1/database/create
Content-Type: application/json

{
  "name": "mydb",
  "description": "Database description"
}
```

#### Create ontology class

```http
POST /api/v1/database/{db_name}/ontology?branch=<branch>
Content-Type: application/json

{
  "id": "Product",
  "label": "Product",
  "description": "E-commerce product",
  "properties": [...],
  "relationships": [...]
}
```

#### Create instance (async)

```http
POST /api/v1/instances/{db_name}/async/{class_id}/create?branch=<branch>
Content-Type: application/json

{
  "data": {
    "...": "..."
  }
}
```

### Update/Delete require `expected_seq` (OCC)

Update/delete endpoints require an `expected_seq` query param. If the current aggregate sequence does not match, OMS returns **409 Conflict** and **does not append** the command.

Tip: you can obtain the current `expected_seq` from federated reads:
- `POST /graph-query/{db_name}` returns `nodes[].index_status.event_sequence` (best-effort, but works for UI-level OCC).

Examples:

```http
PUT /api/v1/database/{db_name}/ontology/{class_id}?branch=<branch>&expected_seq=42
DELETE /api/v1/database/{db_name}/ontology/{class_id}?branch=<branch>&expected_seq=42

PUT /api/v1/instances/{db_name}/async/{class_id}/{instance_id}/update?branch=<branch>&expected_seq=42
DELETE /api/v1/instances/{db_name}/async/{class_id}/{instance_id}/delete?branch=<branch>&expected_seq=42
```

409 response example:

```json
{
  "detail": {
    "error": "optimistic_concurrency_conflict",
    "aggregate_id": "db:class:instance",
    "expected_seq": 41,
    "actual_seq": 42
  }
}
```

### Check command status

```http
GET /api/v1/commands/{command_id}/status
```

## Command Status Tracking ⭐ NEW

### Status Values

| Status | Description |
|--------|-------------|
| `PENDING` | Command received, awaiting processing |
| `PROCESSING` | Command being executed by worker |
| `COMPLETED` | Command executed successfully |
| `FAILED` | Command execution failed |
| `CANCELLED` | Command was cancelled |
| `RETRYING` | Command being retried after failure |

### Status History

Each status change is tracked with:
- Timestamp
- Status message
- Optional worker ID
- Progress percentage (0-100)
- Error details (for failures)

### TTL and Cleanup

- Commands are automatically cleaned up after 24 hours (Redis TTL; currently fixed in code).
- There is no manual cleanup endpoint; use Redis key expiration/eviction or operational tooling.

## WebSocket Real-time Updates ⭐ NEW

Real-time command status updates via WebSocket connections.

### Base URL
```
ws://localhost:8002/api/v1/ws  (Development)
wss://api.spiceharvester.com/api/v1/ws  (Production)
```

### Connection Endpoints

#### Subscribe to Specific Command
```
ws://localhost:8002/api/v1/ws/commands/{command_id}?client_id={client_id}&user_id={user_id}
```

**Parameters:**
- `command_id` (required): Command ID to subscribe to
- `client_id` (optional): Unique client identifier (auto-generated if not provided)
- `user_id` (optional): User identifier for authentication

#### Subscribe to All User Commands
```
ws://localhost:8002/api/v1/ws/commands?user_id={user_id}&client_id={client_id}
```

**Parameters:**
- `user_id` (required): User ID to subscribe to all their commands
- `client_id` (optional): Unique client identifier

### Client Messages

Clients can send these message types:

#### Subscribe to Command
```json
{
  "type": "subscribe",
  "command_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

#### Unsubscribe from Command
```json
{
  "type": "unsubscribe",
  "command_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

#### Get Current Subscriptions
```json
{
  "type": "get_subscriptions"
}
```

#### Ping (Keep-alive)
```json
{
  "type": "ping",
  "timestamp": "2025-08-05T10:30:00Z"
}
```

### Server Messages

#### Connection Established
```json
{
  "type": "connection_established",
  "client_id": "client_abc123",
  "command_id": "550e8400-e29b-41d4-a716-446655440000",
  "message": "Subscribed to command updates"
}
```

#### Command Status Update
```json
{
  "type": "command_update",
  "command_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-08-05T10:30:05Z",
  "data": {
    "status": "PROCESSING",
    "progress": 75,
    "message": "Validating ontology schema...",
    "updated_at": "2025-08-05T10:30:05Z",
    "worker_id": "worker-12345"
  }
}
```

#### Subscription Result
```json
{
  "type": "subscription_result",
  "action": "subscribe",
  "command_id": "550e8400-e29b-41d4-a716-446655440000",
  "success": true
}
```

#### Error Message
```json
{
  "type": "error",
  "message": "Invalid JSON format"
}
```

#### Pong Response
```json
{
  "type": "pong",
  "timestamp": "2025-08-05T10:30:00Z"
}
```

### Notes

- WebSocket 테스트용 HTTP 엔드포인트(`/ws/test`, `/ws/stats` 등)는 제거되었습니다.
- 로컬 수동 테스트는 `wscat`/`websocat` 같은 도구로 WebSocket 엔드포인트에 직접 연결하세요.

### JavaScript Client Example

```javascript
// Connect to specific command updates
const socket = new WebSocket('ws://localhost:8002/api/v1/ws/commands/my-command-123');

socket.onopen = function() {
    console.log('WebSocket connected');
    
    // Subscribe to additional commands
    socket.send(JSON.stringify({
        type: 'subscribe',
        command_id: 'another-command-456'
    }));
};

socket.onmessage = function(event) {
    const data = JSON.parse(event.data);
    
    switch(data.type) {
        case 'command_update':
            console.log(`Command ${data.command_id} status: ${data.data.status}`);
            console.log(`Progress: ${data.data.progress}%`);
            updateUI(data.command_id, data.data);
            break;
            
        case 'connection_established':
            console.log('Connected:', data.message);
            break;
            
        case 'error':
            console.error('WebSocket error:', data.message);
            break;
    }
};

socket.onclose = function() {
    console.log('WebSocket disconnected');
};

socket.onerror = function(error) {
    console.error('WebSocket error:', error);
};

// Send ping to keep connection alive
setInterval(() => {
    if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({
            type: 'ping',
            timestamp: new Date().toISOString()
        }));
    }
}, 30000); // Every 30 seconds
```

### Connection Lifecycle

1. **Connect**: Client connects to WebSocket endpoint
2. **Subscribe**: Automatically subscribe to specified command or user commands
3. **Receive Updates**: Real-time status updates are pushed to client
4. **Manage Subscriptions**: Add/remove command subscriptions dynamically
5. **Heartbeat**: Ping/pong to maintain connection
6. **Disconnect**: Clean disconnection with automatic cleanup

### Error Handling

- **Connection Failures**: Automatic cleanup of failed connections
- **Invalid Messages**: Error responses for malformed JSON
- **Subscription Errors**: Clear error messages for failed subscriptions
- **Resource Cleanup**: Automatic cleanup when clients disconnect

## Enhanced Error Handling ⭐ NEW

### 408 Request Timeout

For sync API operations that exceed the timeout:

```json
{
  "detail": {
    "message": "Operation timed out after 30 seconds", 
    "command_id": "550e8400-e29b-41d4-a716-446655440000",
    "hint": "You can check the status using the async API",
    "retry_after": 5
  }
}
```

### Command Failure Details

```json
{
  "command_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "FAILED",
  "result": {
    "error": "Database connection timeout",
    "retry_count": 2,
    "last_retry_at": "2025-08-05T10:30:10Z",
    "can_retry": true
  }
}
```

## Additional Resources

- [OpenAPI Specification](/api/openapi.json)
- [Postman Collection](/api/postman-collection.json)
- [Command/Event Pattern Guide](/docs/command-event-pattern.md) ⭐ NEW
- [Architecture Documentation](/docs/architecture/README.md)
- [GraphQL Schema](/api/graphql-schema.graphql) (planned)
- [WebSocket Real-time Updates](#websocket-real-time-updates) ✅ COMPLETED
