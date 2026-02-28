# 첫 API 호출 따라하기

> Spice OS가 실행된 상태에서 직접 API를 호출하며 플랫폼을 체험하는 실습 가이드예요. 각 단계에서 "뒤에서 무슨 일이 일어나는지"도 함께 설명해 드릴게요.

**사전 준비:** [로컬 환경 설정](03-LOCAL-SETUP.md)을 완료하고, 전체 스택이 실행 중이어야 해요.

---

## 0단계: 관리자 토큰 확인

> API 호출에 필요한 인증 토큰을 먼저 준비해요.

`.env` 파일에서 `ADMIN_TOKEN` 값을 확인하세요.

```bash
# .env에서 토큰 확인
grep ADMIN_TOKEN .env
```

이후 모든 curl 명령에서 이 토큰을 사용할 거예요:

```bash
# 편의를 위해 환경 변수로 설정
export TOKEN="여기에_ADMIN_TOKEN_값"
```

---

## 1단계: 헬스 체크

> 먼저 BFF가 정상적으로 동작하고 있는지 확인해 봐요.

```bash
curl -s http://localhost:8002/api/v1/health | python3 -m json.tool
```

**예상 응답:**
```json
{
    "status": "healthy",
    "version": "1.0.0",
    "checks": {
        "database": "ok",
        "elasticsearch": "ok",
        "redis": "ok",
        "kafka": "ok"
    }
}
```

> 💡 **뒤에서 일어나는 일:** BFF가 PostgreSQL, Elasticsearch, Redis, Kafka에 각각 연결을 시도해요. 모두 응답하면 "healthy"를 반환해요. 코드 위치는 `backend/bff/main.py`의 `/health` 엔드포인트예요.

---

## 2단계: 데이터베이스(온톨로지 컨테이너) 생성

> 모든 데이터를 담을 그릇을 먼저 만들어 볼까요.

Spice OS에서 "데이터베이스"는 온톨로지의 최상위 컨테이너예요. 하나의 데이터베이스 안에 여러 객체 유형과 인스턴스가 들어가요.

```bash
curl -s -X POST http://localhost:8002/api/v1/databases \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "tutorial-db",
    "displayName": "튜토리얼 데이터베이스",
    "description": "온보딩 튜토리얼을 위한 테스트 데이터베이스"
  }' | python3 -m json.tool
```

**예상 응답:**
```json
{
    "status": "success",
    "data": {
        "name": "tutorial-db",
        "displayName": "튜토리얼 데이터베이스",
        "description": "온보딩 튜토리얼을 위한 테스트 데이터베이스"
    }
}
```

> 💡 **뒤에서 일어나는 일:**
> 1. BFF가 요청을 받아 인증을 확인해요.
> 2. OMS에 전달해서 PostgreSQL에 데이터베이스 메타데이터를 저장해요.
> 3. Elasticsearch에 해당 데이터베이스용 인덱스를 생성해요.
> 4. LakeFS에 해당 데이터베이스용 리포지토리를 생성해요.

---

## 3단계: 객체 유형 생성

> 데이터 구조(스키마)를 정의하는 단계예요. SQL의 `CREATE TABLE`에 해당해요.

"Employee"라는 객체 유형을 만들어 볼까요:

```bash
curl -s -X POST "http://localhost:8002/api/v2/ontologies/tutorial-db/objectTypes" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "apiName": "Employee",
    "displayName": "직원",
    "pluralDisplayName": "직원들",
    "primaryKey": "employeeId",
    "properties": {
      "employeeId": {
        "dataType": { "type": "string" },
        "displayName": "직원 ID"
      },
      "fullName": {
        "dataType": { "type": "string" },
        "displayName": "이름"
      },
      "department": {
        "dataType": { "type": "string" },
        "displayName": "부서"
      },
      "salary": {
        "dataType": { "type": "integer" },
        "displayName": "연봉"
      }
    }
  }' | python3 -m json.tool
```

> 💡 **뒤에서 일어나는 일:**
> 1. BFF가 Foundry v2 API 형식을 OMS 내부 형식으로 변환해요.
> 2. OMS가 스키마를 검증해요. 타입이 유효한지, primaryKey가 존재하는지 확인하는 거죠.
> 3. 이벤트("ONTOLOGY_CLASS_CREATED")가 이벤트 스토어(S3)에 저장돼요.
> 4. Kafka를 통해 Projection Worker가 ES 인덱스 매핑을 갱신해요.

---

## 4단계: 객체 유형 목록 조회

방금 만든 객체 유형이 조회되는지 확인합니다:

```bash
curl -s "http://localhost:8002/api/v2/ontologies/tutorial-db/objectTypes" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

**예상 응답:**
```json
{
    "data": [
        {
            "apiName": "Employee",
            "displayName": "직원",
            "primaryKey": "employeeId",
            "properties": {
                "employeeId": { "dataType": { "type": "string" } },
                "fullName": { "dataType": { "type": "string" } },
                "department": { "dataType": { "type": "string" } },
                "salary": { "dataType": { "type": "integer" } }
            }
        }
    ]
}
```

---

## 5단계: 인스턴스 생성 (액션)

이제 실제 데이터(인스턴스)를 넣어봅시다. Spice OS에서는 **액션(Action)**을 통해 데이터를 변경합니다:

```bash
curl -s -X POST "http://localhost:8002/api/v2/ontologies/tutorial-db/actions/createObject" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "parameters": {
      "objectType": "Employee",
      "properties": {
        "employeeId": "EMP-001",
        "fullName": "김철수",
        "department": "Engineering",
        "salary": 70000000
      }
    }
  }' | python3 -m json.tool
```

두 명 더 추가합니다:

```bash
# 두 번째 직원
curl -s -X POST "http://localhost:8002/api/v2/ontologies/tutorial-db/actions/createObject" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "parameters": {
      "objectType": "Employee",
      "properties": {
        "employeeId": "EMP-002",
        "fullName": "이영희",
        "department": "Marketing",
        "salary": 65000000
      }
    }
  }' | python3 -m json.tool

# 세 번째 직원
curl -s -X POST "http://localhost:8002/api/v2/ontologies/tutorial-db/actions/createObject" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "parameters": {
      "objectType": "Employee",
      "properties": {
        "employeeId": "EMP-003",
        "fullName": "박지민",
        "department": "Engineering",
        "salary": 80000000
      }
    }
  }' | python3 -m json.tool
```

> **뒤에서 일어나는 일:**
> 1. BFF가 액션 요청을 검증합니다
> 2. OMS가 온톨로지 스키마에 맞는지 확인합니다
> 3. `INSTANCE_CREATED` 이벤트가 이벤트 스토어(S3)에 저장됩니다
> 4. Kafka에 이벤트가 게시됩니다
> 5. Instance Worker가 PostgreSQL에 인스턴스를 저장합니다
> 6. Projection Worker가 Elasticsearch에 인덱싱합니다
>
> Elasticsearch 인덱싱에는 1~2초 정도 소요될 수 있습니다.

---

## 6단계: 인스턴스 검색

Engineering 부서 직원을 검색해봅시다:

```bash
curl -s -X POST "http://localhost:8002/api/v2/ontologies/tutorial-db/objects/Employee/search" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "where": {
      "type": "eq",
      "field": "department",
      "value": "Engineering"
    },
    "pageSize": 10
  }' | python3 -m json.tool
```

**예상 응답:**
```json
{
    "data": [
        {
            "__primaryKey": "EMP-001",
            "__apiName": "Employee",
            "properties": {
                "employeeId": "EMP-001",
                "fullName": "김철수",
                "department": "Engineering",
                "salary": 70000000
            }
        },
        {
            "__primaryKey": "EMP-003",
            "__apiName": "Employee",
            "properties": {
                "employeeId": "EMP-003",
                "fullName": "박지민",
                "department": "Engineering",
                "salary": 80000000
            }
        }
    ],
    "nextPageToken": null
}
```

> **뒤에서 일어나는 일:**
> 1. BFF가 검색 쿼리를 SearchJsonQueryV2 형식으로 파싱합니다
> 2. OMS에 전달하면 Elasticsearch DSL 쿼리로 변환됩니다
> 3. Elasticsearch에서 인덱싱된 데이터를 검색합니다
> 4. OMS가 Foundry v2 응답 형식으로 변환하여 반환합니다
>
> 코드 경로: `backend/bff/routers/foundry_ontology_v2.py` → `backend/oms/routers/query.py`

---

## 7단계: 특정 인스턴스 조회

기본키로 특정 인스턴스를 직접 조회합니다:

```bash
curl -s "http://localhost:8002/api/v2/ontologies/tutorial-db/objects/Employee/EMP-001" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

**예상 응답:**
```json
{
    "__primaryKey": "EMP-001",
    "__apiName": "Employee",
    "properties": {
        "employeeId": "EMP-001",
        "fullName": "김철수",
        "department": "Engineering",
        "salary": 70000000
    }
}
```

---

## 8단계: 인스턴스 수정 (Edit Action)

김철수의 부서를 변경해봅시다:

```bash
curl -s -X POST "http://localhost:8002/api/v2/ontologies/tutorial-db/actions/editObject" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "parameters": {
      "objectType": "Employee",
      "primaryKey": "EMP-001",
      "properties": {
        "department": "Management"
      }
    }
  }' | python3 -m json.tool
```

변경 확인:

```bash
curl -s "http://localhost:8002/api/v2/ontologies/tutorial-db/objects/Employee/EMP-001" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

`department`가 `"Management"`로 변경된 것을 확인할 수 있습니다.

> **뒤에서 일어나는 일:**
> 이번에는 `INSTANCE_UPDATED` 이벤트가 생성됩니다. 이전 `INSTANCE_CREATED` 이벤트는 그대로 남아있고, 새 이벤트가 추가됩니다. 이것이 **Event Sourcing**입니다 - 변경이 아니라 새 이벤트의 추가!

---

## 요약: 지금까지 일어난 일

```
여러분이 한 일                 시스템 내부에서 일어난 일
──────────────                 ──────────────────────
1. 데이터베이스 생성     →     PostgreSQL에 메타데이터 저장
                               ES에 인덱스 생성
                               LakeFS에 리포지토리 생성

2. 객체 유형 정의        →     스키마 검증 → 이벤트 저장 (S3)
                               → Kafka 게시 → ES 매핑 갱신

3. 인스턴스 3개 생성     →     액션 검증 → 이벤트 3개 저장 (S3)
                               → Kafka 게시 → PostgreSQL 저장
                               → ES 인덱싱

4. 검색                  →     ES에서 빠른 검색

5. 수정                  →     새 이벤트 추가 (기존 이벤트 유지)
                               → PostgreSQL 갱신 → ES 재인덱싱
```

## 다음으로 읽을 문서

- [아키텍처 이해하기](05-ARCHITECTURE-EXPLAINED.md) - 방금 체험한 것을 아키텍처 관점에서 이해합니다
- [데이터 흐름 추적](06-DATA-FLOW-WALKTHROUGH.md) - 더 다양한 시나리오의 데이터 흐름을 따라갑니다
