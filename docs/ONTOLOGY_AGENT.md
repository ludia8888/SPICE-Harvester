# Ontology Agent (Single Autonomous Loop + MCP Tools)

이 문서는 "사용자 자연어 요청 → 온톨로지 스키마 생성/매핑"을 위한 **Ontology Agent** 개발자 가이드입니다.

핵심 철학:
- MCP는 **도구 제공**(스키마 빌더 + 검증 + 추론 tools)
- Ontology Agent는 **단일 에이전트 루프**(BuildPrompt → Inference → Tool → Observation → … → Finish)
- 서버는 의미를 바꾸는 **무음(heuristic) 수정**을 하지 않습니다. 수정은 반드시 tool call로 명시합니다.

---

## 1) 구성요소 한 눈에 보기

### Entry point (BFF)
- Ontology Agent 실행: `POST /api/v1/ontology-agent/runs`
  - 라우터: `backend/bff/routers/ontology_agent.py`
  - 런타임(단일 루프): `backend/bff/services/ontology_agent_autonomous_loop.py`
  - 모델 정의: `backend/bff/services/ontology_agent_models.py`

### Tool provider (MCP)
- Ontology MCP server: 스키마 빌더 + 검증 + 추론 도구 제공
  - 코드: `backend/mcp_servers/ontology_mcp_server.py`
  - 도구 카테고리:
    - **초기화**: `ontology_new`, `ontology_load`, `ontology_reset`
    - **클래스 메타데이터**: `ontology_set_class_meta`, `ontology_set_abstract`
    - **속성 관리**: `ontology_add_property`, `ontology_update_property`, `ontology_remove_property`, `ontology_set_primary_key`
    - **관계 관리**: `ontology_add_relationship`, `ontology_update_relationship`, `ontology_remove_relationship`
    - **스키마 추론**: `ontology_infer_schema_from_data`, `ontology_suggest_mappings`
    - **검증**: `ontology_validate`, `ontology_check_relationships`, `ontology_check_circular_refs`
    - **조회**: `ontology_list_classes`, `ontology_get_class`, `ontology_search_classes`
    - **저장**: `ontology_create`, `ontology_update`, `ontology_preview`

### 기존 서비스 재사용
| 서비스 | 용도 | 연동 도구 |
|--------|------|-----------|
| `FunnelClient` | 데이터 타입 추론 | `ontology_infer_schema_from_data` |
| `MappingSuggestionService` | 필드 매핑 제안 (7가지 알고리즘) | `ontology_suggest_mappings` |
| `OMSClient` | 온톨로지 CRUD | `ontology_create`, `ontology_update`, `ontology_load` |

---

## 2) 단일 에이전트 루프 동작 방식

Ontology Agent는 한 요청 안에서 반복적으로 도구를 호출하는 **런타임 컨트롤러**입니다.

고수준 플로우:
1. Append-only JSONL 상태 로그에 "현재 상태"를 추가(Goal + db_name + working_ontology + 마지막 관측)
   - 로그가 너무 커지면 서버가 **결정론 snapshot**으로 compaction(LLM 요약 금지)합니다.
2. LLM이 다음 액션 선택(JSON):
   - `call_tool`: MCP tool 호출
     - 한 번의 inference에서 여러 tool call을 `tool_calls`로 배치 실행할 수 있습니다.
   - `clarify`: 필요한 질문 생성
   - `finish`: 결과 종료(생성된 온톨로지 반환)
3. Tool 결과를 관측으로 저장하고 다시 1)로 반복

---

## 3) 사용 예시

### 3.1) 새 클래스 생성
```
"이 데이터셋으로 Customer 클래스 만들어줘"
```

에이전트 동작:
1. `ontology_new` → 새 온톨로지 초기화
2. `ontology_infer_schema_from_data` → 데이터셋에서 타입 추론
3. `ontology_set_class_meta` → 클래스 이름/설명 설정
4. `ontology_add_property` × N → 속성 추가
5. `ontology_validate` → 스키마 검증
6. `ontology_create` → OMS에 저장
7. `finish` → 결과 반환

### 3.2) 기존 클래스에 필드 매핑
```
"email, name, phone 필드를 기존 Person 클래스에 매핑해줘"
```

에이전트 동작:
1. `ontology_load` → 기존 클래스 로드
2. `ontology_suggest_mappings` → 매핑 제안 생성
3. `ontology_add_property` or `ontology_update_property` → 매핑 적용
4. `ontology_validate` → 검증
5. `ontology_update` → 저장
6. `finish` → 결과 반환

### 3.3) 관계 생성
```
"Customer와 Order 사이에 hasOrders 관계 만들어줘"
```

에이전트 동작:
1. `ontology_load` → 소스/타겟 클래스 로드
2. `ontology_add_relationship` → 관계 추가
3. `ontology_check_relationships` → 관계 검증
4. `ontology_update` → 저장
5. `finish` → 결과 반환

---

## 4) API 스펙

### POST /api/v1/ontology-agent/runs

#### Request
```json
{
  "goal": "name, email, phone 컬럼으로 Customer 클래스 만들어줘",
  "db_name": "my_database",
  "branch": "main",
  "target_class_id": null,
  "dataset_sample": {
    "columns": ["name", "email", "phone"],
    "data": [["홍길동", "hong@test.com", "010-1234-5678"]]
  },
  "answers": null,
  "selected_model": null
}
```

| 필드 | 타입 | 필수 | 설명 |
|------|------|------|------|
| `goal` | string | ✅ | 자연어 목표 |
| `db_name` | string | ✅ | 대상 데이터베이스 |
| `branch` | string | - | 브랜치 (기본: "main") |
| `target_class_id` | string | - | 수정 대상 클래스 ID |
| `dataset_sample` | object | - | 스키마 추론용 샘플 데이터 |
| `answers` | object | - | clarify 응답 |
| `selected_model` | string | - | 선호 LLM 모델 |

#### Response
```text
{
  "status": "success",
  "message": "Ontology agent completed successfully",
  "data": {
    "run_id": "ont-run-abc123",
    "ontology": {
      "class_id": "entity_customer",
      "label": "Customer",
      "properties": [...],
      "relationships": [...]
    },
    "questions": [],
    "validation_errors": [],
    "validation_warnings": [],
    "mapping_suggestions": null,
    "planner": {
      "confidence": 0.95,
      "notes": ["Created new Customer class with 3 properties"]
    },
    "llm": {
      "provider": "anthropic",
      "model": "claude-sonnet-4-20250514",
      "latency_ms": 1234
    }
  }
}
```

#### Status 값
| Status | 설명 |
|--------|------|
| `success` | 온톨로지 생성/수정 완료 |
| `clarification_required` | 추가 정보 필요 (questions 필드 확인) |
| `partial` | 부분 성공 |
| `error` | 실패 |

---

## 5) MCP 도구 상세

### 초기화 도구

#### ontology_new
새 온톨로지 작업 세션을 시작합니다.
```json
{
  "session_id": "sess-123",
  "db_name": "my_db",
  "branch": "main"
}
```

#### ontology_load
기존 온톨로지 클래스를 로드합니다.
```json
{
  "session_id": "sess-123",
  "class_id": "entity_customer"
}
```

### 속성 관리 도구

#### ontology_add_property
클래스에 새 속성을 추가합니다.
```json
{
  "session_id": "sess-123",
  "property_id": "prop_email",
  "label": "이메일",
  "data_type": "xsd:string",
  "description": "고객 이메일 주소",
  "required": true
}
```

#### ontology_set_primary_key
클래스의 기본 키를 설정합니다.
```json
{
  "session_id": "sess-123",
  "property_ids": ["prop_customer_id"]
}
```

### 스키마 추론 도구

#### ontology_infer_schema_from_data
샘플 데이터에서 스키마를 추론합니다.
```json
{
  "session_id": "sess-123",
  "columns": ["name", "email", "age"],
  "sample_data": [
    ["홍길동", "hong@test.com", 30],
    ["김철수", "kim@test.com", 25]
  ]
}
```

반환:
```json
{
  "inferred_properties": [
    {"name": "name", "type": "xsd:string", "nullable": false},
    {"name": "email", "type": "xsd:string", "nullable": false},
    {"name": "age", "type": "xsd:integer", "nullable": false}
  ]
}
```

#### ontology_suggest_mappings
기존 클래스에 대한 필드 매핑을 제안합니다. 7가지 알고리즘 사용:
- Exact match
- Case-insensitive match
- Prefix/Suffix match
- Semantic similarity
- Type compatibility
- Fuzzy match
- Pattern match

---

## 6) 설정

### MCP 서버 등록 (mcp-config.json)
```json
{
  "mcpServers": {
    "ontology": {
      "command": "python",
      "args": ["backend/mcp_servers/ontology_mcp_server.py"],
      "config": {
        "capabilities": [
          "ontology_management",
          "schema_inference",
          "mapping_suggestions",
          "validation"
        ]
      }
    }
  }
}
```

### 환경 변수
| 변수 | 설명 | 기본값 |
|------|------|--------|
| `LLM_PROVIDER` | LLM 제공자 | `anthropic` |
| `LLM_MODEL` | LLM 모델 | `claude-sonnet-4-20250514` |
| `OMS_BASE_URL` | OMS 서비스 URL | `http://localhost:8001` |

---

## 7) 아키텍처 다이어그램

```
┌─────────────────────────────────────────────────────────────────┐
│                         BFF Layer                                │
├─────────────────────────────────────────────────────────────────┤
│  POST /api/v1/ontology-agent/runs                               │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           Ontology Agent Autonomous Loop                 │    │
│  │  ┌─────────────────────────────────────────────────┐    │    │
│  │  │  1. Build Prompt (goal + state + observations)  │    │    │
│  │  │  2. LLM Inference → Decision                    │    │    │
│  │  │  3. Execute Tool(s) via MCP                     │    │    │
│  │  │  4. Append Observation                          │    │    │
│  │  │  5. Loop until finish/clarify                   │    │    │
│  │  └─────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │  LLM Gateway    │  │  OMS Client     │  │  Funnel Client  │  │
│  │  (Anthropic/    │  │  (CRUD)         │  │  (Type Infer)   │  │
│  │   OpenAI)       │  │                 │  │                 │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        MCP Layer                                 │
├─────────────────────────────────────────────────────────────────┤
│  Ontology MCP Server (ontology_mcp_server.py)                   │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐             │
│  │ Schema       │ │ Validation   │ │ Inference    │             │
│  │ Builder      │ │ Tools        │ │ Tools        │             │
│  │ Tools        │ │              │ │              │             │
│  └──────────────┘ └──────────────┘ └──────────────┘             │
└─────────────────────────────────────────────────────────────────┘
```

---

## 8) 관련 문서

- [Pipeline Agent](PIPELINE_AGENT.md) - 데이터 ETL용 에이전트
- [Ontology](Ontology.md) - 온톨로지 개념 및 스키마 설계
- [API Reference](API_REFERENCE.md) - 전체 API 참조
- [Architecture](ARCHITECTURE.md) - 시스템 아키텍처

<!-- DOC_SYNC: 2026-02-13 Foundry pipeline parity + runtime consistency sweep -->
