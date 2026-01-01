# SPICE Harvester — BFF API 레퍼런스 (v1)

> **최종 업데이트**: 2025-12-19  
> **프론트엔드 계약**: ✅ 프론트엔드에서 지원되는 API 표면은 BFF만  
> **기본 URL(로컬)**: `http://localhost:8002/api/v1`  
> **Swagger UI(로컬)**: `http://localhost:8002/docs`  
> **OpenAPI JSON(로컬)**: `http://localhost:8002/openapi.json`

## 범위 (프론트엔드 계약)

- 프론트엔드는 **반드시** `/api/v1` 이하의 BFF 엔드포인트만 호출한다.
- OMS / Funnel / 워커는 **내부 의존성**이며 프론트엔드 계약에 포함되지 않는다.
- 엔드포인트 표기는 다음과 같다:
  - **안정**: 프론트엔드 연동에 안전
  - **운영자 전용**: 관리자 토큰 필요
  - **🚧 작업 중**: API 표면에는 있으나 준비되지 않음(프론트엔드 사용 금지)

## 빠른 시작 (로컬)

```bash
docker compose -f docker-compose.full.yml up -d --build
```

이후 접속:
- Swagger UI: `http://localhost:8002/docs`

## 공통 규칙

### 콘텐츠 타입

- 요청: `Content-Type: application/json`
- 응답: `application/json` (파일 업로드 엔드포인트 제외)

### 인증 / 권한

- 기본적으로 BFF는 **인증 필수**로 동작한다.
- 인증 토큰 전달 방식 (둘 중 하나):
  - `X-Admin-Token: <token>`
  - `Authorization: Bearer <token>`
- WebSocket은 `?token=<token>` 쿼리 또는 헤더 방식 사용 가능.
- 운영자 전용(Admin) 엔드포인트는 위 토큰이 **반드시** 필요하며, 미제공 시 `401/403`.

### 레이트 리밋 (429)

- 일부 엔드포인트는 레이트 리밋이 적용된다(특히 AI/데이터 커넥터).
- 초과 시 HTTP `429` + `Retry-After` 헤더 반환.
- 표준 헤더:
  - `X-RateLimit-Limit`
  - `X-RateLimit-Remaining`
  - `X-RateLimit-Reset`
  - `Retry-After`

### 에러 응답 패턴

- FastAPI 기본 에러: `{"detail": "..."}`
- 도메인 에러 상세 예시:
  - `unknown_label_keys`:
    ```json
    { "detail": { "error": "unknown_label_keys", "labels": ["..."], "class_id": "..." } }
    ```
  - OCC 충돌:
    ```json
    { "detail": { "error": "optimistic_concurrency_conflict", "expected_seq": 3, "actual_seq": 4 } }
    ```
- 흔한 HTTP 코드:
  - `400` 입력 검증 실패
  - `401/403` 인증/권한 실패
  - `404` 리소스 없음
  - `409` OCC 충돌
  - `429` 레이트 리밋
  - `5xx` 서버/의존성 오류

### 페이징 / 정렬

- 목록 조회는 보통 `limit` + `offset`을 사용한다.
- 기본값은 엔드포인트마다 다르며, 상한은 100~1000 사이로 제한된다.

### 브랜치 컨텍스트

- 많은 쓰기/그래프/온톨로지 API는 `?branch=`를 사용한다(기본 `main`).
- 일부 읽기 API는 **branch를 받지 않거나 무시**한다(예: 인스턴스 리스트/샘플값).
- Google Sheets/Excel 커밋은 현재 `branch=main` 고정이다.

### 경로

BFF의 DB 스코프 리소스는 `/api/v1/databases/{db_name}/...` 형식을 사용한다.

- 컬렉션: `/api/v1/databases`, `/api/v1/databases/{db_name}/branches`
- DB 스코프: `/api/v1/databases/{db_name}/ontology`, `/api/v1/databases/{db_name}/query`, `/api/v1/databases/{db_name}/instances`, `/api/v1/databases/{db_name}/mappings`

### 시간

- 모든 타임스탬프는 ISO8601 형식이다.
- UTC(`...Z`) 사용을 권장한다. (일부 내부 응답은 오프셋이 포함될 수 있음)

### 핵심 ID

- `db_name`: 소문자 + 숫자 + `_`/`-` (검증됨)
- `branch`: 영숫자 + `_`/`-`/`/`
- `class_id`: ID 형식(영숫자 + `_`/`-`/`:`), **내부용**
- `class_label`: BFF 비동기 인스턴스 엔드포인트에서 쓰는 사람이 읽는 라벨(BFF가 `class_id`로 매핑)
- `instance_id`: 영숫자 + `_`/`-`/`:` (검증됨)
- `command_id`: UUID

브랜치가 path 파라미터로 쓰이는 엔드포인트(예: `/databases/{db_name}/branches/{branch_name}`)는
`{branch_name:path}`로 `/` 포함 브랜치를 허용한다.

### 용어 정리 (헷갈리기 쉬운 항목)

| 용어 | 의미 | 안정성 |
|------|------|--------|
| `class_id` | 클래스(스키마)의 내부 고정 식별자. 그래프 문서/관계 참조에 사용. | **안정** |
| `label` | UI에 표시되는 라벨(다국어 가능). 키로 사용하지 말 것. | **변경 가능** |
| `class_label` | 일부 BFF 엔드포인트가 받는 편의용 라벨 문자열. BFF가 라벨 매핑으로 `class_id`에 매핑. | **편의** |
| `instance_id` | 인스턴스(엔티티)의 내부 고정 식별자. 보통 `{class_id.lower()}_id`에서 파생. | **안정** |
| `expected_seq` | 쓰기 명령용 OCC 토큰(불일치 시 409). | **안정** |

### 언어 / 다국어 (EN + KO)

- 지원 언어: `en`, `ko`
- UI용 텍스트 필드는 **둘 중 하나** 형식을 지원:
  - 평문 문자열(레거시), 또는
  - 언어 맵: `{"en": "...", "ko": "..."}` (권장)
- 출력 언어 선택:
  - 권장: `?lang=en|ko` (쿼리 파라미터 우선)
  - 지원: `Accept-Language: en-US,en;q=0.9,ko;q=0.8`
  - 둘 다 있으면 `?lang`이 우선한다.
- 번역이 없으면 다른 지원 언어로 폴백한다.
- 기본값(둘 다 없을 때): `ko`

### 쓰기 모드 (202 대 200/201)

지원되는 운영 모드에서 핵심 쓰기(데이터베이스/온톨로지/인스턴스)는 **비동기**다:

- 대부분의 쓰기 요청은 HTTP `202` + `command_id`를 반환한다(폴링 필요).
- 배치형 임포트 커밋(예: Google Sheets/Excel)은 HTTP `200`으로 여러 command를 반환하며, 각 항목의 `command_id`/`status_url`을 따라 폴링한다.
- 브랜치 생성/삭제, 라벨 매핑 import 등 일부 관리성 작업은 동기 응답(200/201)일 수 있다.
- 직접 쓰기 모드(`ENABLE_EVENT_SOURCING=false`)는 핵심 쓰기 경로에서 **지원하지 않는다**(시도 시 `5xx` 가능).

비고:
- 이 레퍼런스는 지원되는 운영 형태를 **이벤트 소싱 모드**로만 가정한다.

### 표준 응답: `ApiResponse`

대부분의 BFF HTTP 엔드포인트(특히 DB/온톨로지 쓰기)는 다음 형태를 반환한다:

```json
{
  "status": "success|created|accepted|warning|partial|error",
  "message": "사람이 읽을 수 있는 메시지",
  "data": { "..." : "..." },
  "errors": ["..."]
}
```

비고:
- `status="accepted"` + HTTP `202`는 “커맨드가 수락되었고 작업은 비동기적으로 진행됨”을 의미한다.
- 일부 엔드포인트는 도메인 전용 형태(예: 그래프 질의)를 반환하며, 일부는 `CommandResult`를 반환한다.

### 비동기 커맨드 결과: `CommandResult`

일부 쓰기 엔드포인트(특히 비동기 인스턴스 명령)는 다음 형태를 반환한다:

```json
{
  "command_id": "uuid",
  "status": "PENDING|PROCESSING|COMPLETED|FAILED|CANCELLED|RETRYING",
  "result": { "..." : "..." },
  "error": "오류 메시지",
  "completed_at": "2025-01-01T00:00:00Z",
  "retry_count": 0
}
```

### 자동 생성 `class_id` (BFF에서 `id` 생략 시)

일부 BFF 온톨로지 엔드포인트는 `id`가 없을 때 `label`로부터 `id`를 생성한다.

현재 구현 상세(충돌 예측용):
- 텍스트 소스 선택:
  - `label`이 문자열이면 그것을 사용
  - `label`이 객체이면 `en` → `ko` → 첫 번째 값 순으로 선택
- 정규화:
  - 구두점/특수문자 제거
  - 공백 분리 단어를 `CamelCase`로 변환
  - 숫자로 시작하면 `Class` 접두어 추가
  - 약 50자 내로 절단
- 한글 라벨:
  - 간단 로마자화(소규모 매핑) + **타임스탬프 접미사**로 충돌 감소

충돌 정책:
- 생성된 `id`가 이미 존재하면 생성 요청은 `409 Conflict`로 실패한다.
- 운영 스키마에서는 명시적이고 안정적인 `id`를 제공하고, `label`은 UI 문자열로 취급하는 것을 권장한다.

## 신뢰성 계약 (비동기 쓰기)

### 전달 보장

- 퍼블리셔/Kafka 전달은 **최소 1회** 보장이다.
- 컨슈머(워커/프로젝션)는 **`event_id` 기준 멱등성**을 구현한다.
- 순서는 애그리게이트 단위로 `seq`에 의해 보장된다(오래된 이벤트는 무시됨).

참고:
- `docs/IDEMPOTENCY_CONTRACT.md`

### 비동기 쓰기 관측

엔드포인트가 **HTTP `202`(수락)**와 `data.command_id`를 반환하면, 프론트엔드는 다음을 수행해야 한다:

1. `command_id` 저장
2. 폴링:
   - `GET /api/v1/commands/{command_id}/status`
3. 상태 전이 표시:
   - `PENDING → PROCESSING → COMPLETED | FAILED`

### 실패/재시도 UX (사용자 행동 가이드)

`GET /api/v1/commands/{command_id}/status` 응답 상태 예시:

- `RETRYING`: 워커가 **일시적 장애**를 감지하고 백오프로 재시도 중.
  - 프론트엔드는 “재시도 중” 표시 후 계속 폴링.
- `FAILED`: 재시도 불가 오류 또는 최대 재시도 초과로 **종결 실패**.
  - 프론트엔드는 `error`(문자열)를 노출하고 다음 행동을 제공:
    - 입력 수정 또는 시스템 복구 **이후에만** “재시도”(커맨드 재제출)
    - 가능한 경우 “감사/라인리지 보기” 링크 제공(운영 흐름)
- `CANCELLED`: 명시적 취소 플로우용(환경에 따라 아직 발생하지 않을 수 있음).

운영자 가이드(요약):
- `400` 계열: 입력/스키마 불일치 → 페이로드/스키마 수정 후 재제출.
- `409`: OCC 불일치(`expected_seq`) → 최신 상태 갱신 후 올바른 토큰으로 재시도.
- `5xx`/타임아웃: 인프라 일시 장애 → 대기 후 재시도(자동 회복 가능).

### 낙관적 동시성 제어 (OCC)

일부 업데이트/삭제 엔드포인트는 `expected_seq`를 요구한다:

- `expected_seq`가 현재 애그리게이트 시퀀스와 다르면 HTTP `409`
- 오류 형식(일반적 예시):

```json
{
  "detail": {
    "error": "optimistic_concurrency_conflict",
    "aggregate_id": "...",
    "expected_seq": 3,
    "actual_seq": 4
  }
}
```

## 관계 참조 형식 (인스턴스 → 그래프)

### 참조 문자열

인스턴스 페이로드의 관계 필드는 TerminusDB `@id` 참조 문자열로 저장된다:

- **문자열 참조**: `"<TargetClassID>/<instance_id>"`
  - 예: `"Customer/cust_001"`

브랜치 동작:
- 참조는 명령/쿼리의 **브랜치 컨텍스트**(`?branch=...`) 안에서만 해석된다.
- 값 형식에 `branch` 접두어가 없으므로 **크로스 브랜치 참조는 지원하지 않는다**.

### 카디널리티 → JSON 형태 (권장)

혼동을 줄이기 위해 카디널리티별 JSON 형태를 고정한다:

- `1:1`, `n:1`: **단일** 문자열 참조  
  - `"owned_by": "Customer/cust_001"`
- `1:n`, `n:m`: **배열** 문자열 참조  
  - `"employees": ["Person/p1", "Person/p2"]`

참조 무결성 정책(현재 시스템 모드):
- 관계 참조는 **완전 보장되지 않는 그래프 링크**로 취급된다.
- 대상 엔티티가 아직 프로젝션에 반영되거나 인덱싱되지 않아도 링크가 허용될 수 있다.
- 읽기 시에는 프로젝션 지연에 따라 `data_status=PARTIAL|MISSING`으로 나타날 수 있다.

누락 대상 거부 등 **엄격 무결성**이 필요하면 명시적인 제품 요구사항으로 별도 요청해야 한다(현재 기본적으로 강제되지 않음).

## 인증

### BFF 인증 기본 정책

- BFF는 기본적으로 **모든 HTTP 요청에 인증을 요구**한다.
- 기본 면제 경로: `/api/v1/health`, `/api/v1/` (변경하려면 `BFF_AUTH_EXEMPT_PATHS` 설정)
- 인증 토큰은 `X-Admin-Token` 또는 `Authorization: Bearer <token>`으로 전달한다.
- 인증이 요구되는데 토큰이 미설정이면 HTTP `503`을 반환한다.
- 인증 비활성화는 `BFF_REQUIRE_AUTH=false`와 `ALLOW_INSECURE_BFF_AUTH_DISABLE=true`가 동시에 필요하다(보안상 비권장).

| 구분 | 헤더 | 비고 |
|------|------|------|
| BFF (일반) | `X-Admin-Token` 또는 `Authorization: Bearer` | 기본적으로 모든 엔드포인트 인증 필요(기본 면제: `/api/v1/health`, `/api/v1/`) |
| BFF (관리자) | `X-Admin-Token` 또는 `Authorization: Bearer` | 운영자 전용 엔드포인트 포함 |
| OMS / Terminus | *(내부 전용)* | 프론트엔드는 호출 금지, 통상 TerminusDB 기본 인증 |

### 관리자 엔드포인트 (운영자 전용)

- `BFF_ADMIN_TOKEN`(또는 `ADMIN_API_KEY` / `ADMIN_TOKEN`)이 설정되지 않으면 관리자 엔드포인트는 비활성(HTTP `403`)이다.
- 필수 헤더:
  - `X-Admin-Token: <token>` **또는** `Authorization: Bearer <token>`
- 선택 헤더:
  - `X-Admin-Actor: <name/email>` (감사/추적 메타데이터에 기록)

## 엔드포인트 인덱스 (BFF)

이 섹션은 현재 노출된 **모든** BFF HTTP 엔드포인트를 나열한다(웹소켓 경로 제외).

### AI (**안정**)
- `POST /api/v1/ai/query/{db_name}` — DB 대상 LLM 보조 질의 실행. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: JSON(OpenAPI 스키마).
- `POST /api/v1/ai/translate/query-plan/{db_name}` — 쿼리 플랜을 실행 가능한 질의로 변환. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: JSON(OpenAPI 스키마).

### Summary (**안정**)
- `GET /api/v1/summary?db=<db_name>&branch=<branch>` — 프론트엔드용 시스템 요약(컨텍스트/보호 브랜치 정책/Redis·ES 헬스/Terminus 브랜치 정보). 응답: `ApiResponse`.

### 관리자 작업 (**운영자 전용**)
- `POST /api/v1/admin/replay-instance-state` — 이벤트 재생으로 인스턴스 상태 재구성 작업 생성. 권한: 운영자 전용. 요청: JSON 바디(OpenAPI 스키마). 응답: 작업 ID/상태 포함 JSON(OpenAPI 스키마).
- `GET /api/v1/admin/replay-instance-state/{task_id}/result` — 재생 작업 결과 조회. 권한: 운영자 전용. 요청: 경로 `task_id`. 응답: 결과 JSON(OpenAPI 스키마).
- `GET /api/v1/admin/replay-instance-state/{task_id}/trace` — 재생 이력을 감사/라인리지로 추적한 결과 조회. 권한: 운영자 전용. 요청: 경로 `task_id`. 응답: 트레이스 JSON(OpenAPI 스키마).
- `POST /api/v1/admin/recompute-projection` — ES 리드모델 재계산 작업 생성. 권한: 운영자 전용. 요청: JSON 바디(OpenAPI 스키마). 응답: 작업 ID/상태 JSON(OpenAPI 스키마).
- `GET /api/v1/admin/recompute-projection/{task_id}/result` — 재계산 결과 조회. 권한: 운영자 전용. 요청: 경로 `task_id`. 응답: 결과 JSON(OpenAPI 스키마).
- `POST /api/v1/admin/cleanup-old-replays` — Redis에 저장된 재생 결과 정리 작업 실행. 권한: 운영자 전용. 요청: JSON 바디(OpenAPI 스키마). 응답: 실행 결과 JSON(OpenAPI 스키마).
- `GET /api/v1/admin/system-health` — 시스템 상태 요약 조회. 권한: 운영자 전용. 응답: 상태 요약 JSON(OpenAPI 스키마).

### 비동기 인스턴스 관리 (**안정**)
- `POST /api/v1/databases/{db_name}/instances/{class_label}/create` — 인스턴스 생성 명령 제출(비동기, **HTTP 202**). 요청: 경로 `db_name`, `class_label`; JSON 바디(`data`, `metadata` 등; OpenAPI 스키마). 응답: `CommandResult` + `command_id`.
- `PUT /api/v1/databases/{db_name}/instances/{class_label}/{instance_id}/update` — 인스턴스 업데이트 명령 제출(비동기, **HTTP 202**). 요청: 경로 `db_name`, `class_label`, `instance_id`; `expected_seq` 필요(OCC, 위치/형식 OpenAPI 참고). 응답: `CommandResult` + `command_id`.
- `DELETE /api/v1/databases/{db_name}/instances/{class_label}/{instance_id}/delete` — 인스턴스 삭제 명령 제출(비동기, **HTTP 202**). 요청: 경로 `db_name`, `class_label`, `instance_id`; `expected_seq` 필요(OCC, 위치/형식 OpenAPI 참고). 응답: `CommandResult` + `command_id`.
- `POST /api/v1/databases/{db_name}/instances/{class_label}/bulk-create` — 인스턴스 다건 생성 명령 제출(비동기, **HTTP 202**). 요청: 경로 `db_name`, `class_label`; JSON 바디(OpenAPI 스키마). 응답: `CommandResult` + `command_id`.

비고:
- `data` 키는 **속성 라벨**(사람이 읽는 라벨)입니다. BFF가 LabelMapper로 내부 `property_id`에 매핑합니다.
- 라벨을 해석할 수 없으면 HTTP `400`과 `detail.error="unknown_label_keys"`를 반환합니다.
- 라벨 매핑은 보통 온톨로지 생성/수정 흐름 또는 라벨 매핑 가져오기 API로 채워집니다.

공통 쿼리: `branch` (default: `main`)

### 감사 (**안정**)
- `GET /api/v1/audit/logs` — 감사 로그 목록 조회. 요청: 쿼리 파라미터(`partition_key=db:<db_name>` 등; OpenAPI 스키마). 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/audit/chain-head` — 감사 체인 헤드 검증. 요청: 쿼리 `partition_key` 필수. 응답: 검증 결과 JSON(OpenAPI 스키마).

### 백그라운드 작업 (**안정**)
- `GET /api/v1/tasks/` — 백그라운드 작업 목록 조회. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/tasks/{task_id}` — 작업 상태 조회. 요청: 경로 `task_id`. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/tasks/{task_id}/result` — 작업 결과 조회(저장된 경우). 요청: 경로 `task_id`. 응답: JSON(OpenAPI 스키마).
- `DELETE /api/v1/tasks/{task_id}` — 작업 취소 요청. 요청: 경로 `task_id`. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/tasks/metrics/summary` — 작업/큐 메트릭 요약 조회. 응답: JSON(OpenAPI 스키마).

비고:
- 작업 수동 재시도는 의도적으로 **노출하지 않습니다**(작업 스펙이 내구성 있게 저장되지 않음). 새 커맨드를 재제출해야 합니다.

### 커맨드 상태 (**안정**)
- `GET /api/v1/commands/{command_id}/status` — 비동기 커맨드 상태/결과 폴링. 요청: 경로 `command_id`. 응답: `CommandResult` (상태: `PENDING|PROCESSING|COMPLETED|FAILED|CANCELLED|RETRYING`).

### 구성 모니터링 (**운영자 전용**)
- `GET /api/v1/config/config/current` — 현재 구성 조회. 권한: 운영자 전용. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/config/config/report` — 구성 리포트 조회. 권한: 운영자 전용. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/config/config/validation` — 구성 유효성 검증 결과 조회. 권한: 운영자 전용. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/config/config/changes` — 최근 구성 변경 내역 조회. 권한: 운영자 전용. 응답: JSON(OpenAPI 스키마).
- `POST /api/v1/config/config/check-changes` — 구성 변경 체크 트리거. 권한: 운영자 전용. 요청: JSON 바디(OpenAPI 스키마). 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/config/config/drift-analysis` — 환경 드리프트 분석 조회. 권한: 운영자 전용. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/config/config/security-audit` — 보안 감사용 구성 뷰 조회. 권한: 운영자 전용. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/config/config/health-impact` — 구성 변경의 헬스 영향 조회. 권한: 운영자 전용. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/config/config/monitoring-status` — 구성 모니터링 상태 조회. 권한: 운영자 전용. 응답: JSON(OpenAPI 스키마).

### 데이터 커넥터 (**안정**)
- `POST /api/v1/data-connectors/google-sheets/grid` — 시트 그리드/병합 정보 추출. 요청: JSON 바디(OpenAPI 스키마). 응답: JSON(OpenAPI 스키마).
- `POST /api/v1/data-connectors/google-sheets/preview` — 시트 데이터 미리보기(Funnel/임포트용). 요청: JSON 바디(OpenAPI 스키마). 응답: JSON(OpenAPI 스키마).
- `POST /api/v1/data-connectors/google-sheets/register` — 시트 모니터링 등록. 요청: JSON 바디(OpenAPI 스키마). 응답: 등록 결과 JSON(OpenAPI 스키마).
- `GET /api/v1/data-connectors/google-sheets/registered` — 등록된 시트 목록 조회. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/data-connectors/google-sheets/{sheet_id}/preview` — 등록된 시트 미리보기. 요청: 경로 `sheet_id`. 응답: JSON(OpenAPI 스키마).
- `DELETE /api/v1/data-connectors/google-sheets/{sheet_id}` — 시트 모니터링 해제. 요청: 경로 `sheet_id`. 응답: JSON(OpenAPI 스키마).

### 데이터베이스 관리 (**안정**)
- `GET /api/v1/databases` — 데이터베이스 목록 조회. 응답: JSON(OpenAPI 스키마).
- `POST /api/v1/databases` — 데이터베이스 생성(비동기, **HTTP 202**). 요청: JSON 바디(`name`, `description` 등; OpenAPI 스키마). 응답: `ApiResponse` + `data.command_id`.
- `GET /api/v1/databases/{db_name}` — 데이터베이스 정보 조회. 요청: 경로 `db_name`. 응답: JSON(OpenAPI 스키마).
- `DELETE /api/v1/databases/{db_name}` — 데이터베이스 삭제(비동기, **HTTP 202**). 요청: 경로 `db_name`, `expected_seq` 필요(OCC, 위치/형식 OpenAPI 참고). 응답: `ApiResponse` + `command_id`.
- `GET /api/v1/databases/{db_name}/expected-seq` — 데이터베이스 OCC 토큰(`expected_seq`) 조회(프론트 편의). 응답: `ApiResponse` + `data.expected_seq`.
- `GET /api/v1/databases/{db_name}/branches` — 브랜치 목록 조회. 요청: 경로 `db_name`. 응답: JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/branches` — 브랜치 생성. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/databases/{db_name}/branches/{branch_name}` — 브랜치 정보 조회. 요청: 경로 `db_name`, `branch_name`. 응답: JSON(OpenAPI 스키마).
- `DELETE /api/v1/databases/{db_name}/branches/{branch_name}` — 브랜치 삭제. 요청: 경로 `db_name`, `branch_name`. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/databases/{db_name}/classes` — 클래스 목록 조회. 요청: 경로 `db_name`. 응답: JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/classes` — 클래스 생성(레거시 JSON-LD 임포트). 요청: 경로 `db_name`, JSON-LD 바디(OpenAPI 스키마). 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/databases/{db_name}/classes/{class_id}` — 클래스 정보 조회. 요청: 경로 `db_name`, `class_id`. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/databases/{db_name}/versions` — 버전 정보 조회. 요청: 경로 `db_name`. 응답: JSON(OpenAPI 스키마).

### 그래프 (**안정**)
- `GET /api/v1/graph-query/health` — 그래프 서비스 헬스 체크. 응답: JSON(OpenAPI 스키마).
- `POST /api/v1/graph-query/{db_name}` — 멀티홉 그래프 질의(ES 연합 포함). 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 그래프 결과 JSON(OpenAPI 스키마).
- `POST /api/v1/graph-query/{db_name}/simple` — 단일 클래스 간단 조회. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 결과 JSON(OpenAPI 스키마).
- `POST /api/v1/graph-query/{db_name}/multi-hop` — 멀티홉 전용 헬퍼 질의. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 결과 JSON(OpenAPI 스키마).
- `GET /api/v1/graph-query/{db_name}/paths` — 클래스 간 관계 경로 탐색. 요청: 경로 `db_name`, 쿼리 파라미터(OpenAPI 스키마). 응답: 경로 JSON(OpenAPI 스키마).

공통 쿼리: `branch` (default: `main`). `start_class`/`target_class`/`predicate`는 내부 `class_id`/관계 ID 기준.

### 헬스 (**안정**)
- `GET /api/v1/` — 루트 엔드포인트(서비스 기본 정보/헬스 확인용). 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/health` — 헬스 체크. 응답: JSON(OpenAPI 스키마).

### 인스턴스 관리 (**안정**)
- `GET /api/v1/databases/{db_name}/class/{class_id}/instances` — 인스턴스 목록 조회(ES 우선, TerminusDB 폴백). 요청: 경로 `db_name`, `class_id`, 쿼리 파라미터(OpenAPI 스키마). 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/databases/{db_name}/class/{class_id}/instance/{instance_id}` — 인스턴스 단건 조회(ES 우선, TerminusDB 폴백). 요청: 경로 `db_name`, `class_id`, `instance_id`. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/databases/{db_name}/class/{class_id}/sample-values` — 필드 샘플 값 조회(UI 필터용). 요청: 경로 `db_name`, `class_id`. 응답: JSON(OpenAPI 스키마).

### 라벨 매핑 (**안정**)
- `GET /api/v1/databases/{db_name}/mappings/` — 라벨 매핑 요약 조회. 요청: 경로 `db_name`. 응답: JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/mappings/export` — 라벨 매핑 내보내기. 요청: 경로 `db_name` (바디 없음). 응답: JSON 파일 다운로드(`Content-Disposition`).
- `POST /api/v1/databases/{db_name}/mappings/import` — 라벨 매핑 가져오기. 요청: `multipart/form-data`의 `file`(JSON). 응답: JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/mappings/validate` — 라벨 매핑 검증(쓰기 없음). 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 검증 결과 JSON(OpenAPI 스키마).
- `DELETE /api/v1/databases/{db_name}/mappings/` — 라벨 매핑 전체 삭제. 요청: 경로 `db_name`. 응답: JSON(OpenAPI 스키마).

### 라인리지 (**안정**)
- `GET /api/v1/lineage/graph` — 라인리지 그래프 조회. 요청: 쿼리 `root` 필수(예: `event:<uuid>`), `db_name` 권장. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/lineage/impact` — 영향 분석 결과 조회. 요청: 쿼리 `root` 필수, `db_name` 권장. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/lineage/metrics` — 라인리지 메트릭 조회. 요청: 쿼리 `db_name`(선택), `window_minutes`(기본 60). 응답: JSON(OpenAPI 스키마).

### 머지 충돌 해결 (**안정**)
- `POST /api/v1/databases/{db_name}/merge/simulate` — 머지 충돌 시뮬레이션. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 시뮬레이션 결과 JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/merge/resolve` — 머지 충돌 해결. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 해결 결과 JSON(OpenAPI 스키마).

### 모니터링 (**운영자 전용**)
- `GET /api/v1/monitoring/health` — 기본 헬스 상태 조회. 권한: 운영자 전용. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/monitoring/health/detailed` — 상세 헬스 상태 조회. 권한: 운영자 전용. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/monitoring/health/liveness` — k8s Liveness 프로브용 상태. 권한: 운영자 전용. 응답: JSON/OpenAPI 스키마.
- `GET /api/v1/monitoring/health/readiness` — k8s Readiness 프로브용 상태. 권한: 운영자 전용. 응답: JSON/OpenAPI 스키마.
- `GET /api/v1/monitoring/status` — 서비스 상태 요약. 권한: 운영자 전용. 응답: JSON/OpenAPI 스키마.
- `GET /api/v1/monitoring/metrics` — Prometheus 스크레이프용 `/metrics` 리다이렉트. 권한: 운영자 전용. 요청: 없음. 응답: `/metrics`로 리다이렉트.
- `GET /api/v1/monitoring/config` — 구성 요약 조회. 권한: 운영자 전용. 응답: JSON/OpenAPI 스키마.
- `GET /api/v1/monitoring/background-tasks/active` — 활성 백그라운드 작업 조회. 권한: 운영자 전용. 응답: JSON/OpenAPI 스키마.
- `GET /api/v1/monitoring/background-tasks/health` — 백그라운드 작업 헬스 조회. 권한: 운영자 전용. 응답: JSON/OpenAPI 스키마.
- `GET /api/v1/monitoring/background-tasks/metrics` — 백그라운드 작업 메트릭 조회. 권한: 운영자 전용. 응답: JSON/OpenAPI 스키마.

비고:
- 의존성 그래프 및 “서비스 재시작” API는 의도적으로 **노출하지 않습니다**(가짜 제어 방지 목적). 대신 `/health/detailed`, `/status`를 사용하세요.

### 온톨로지 관리 (**안정**)
- `POST /api/v1/databases/{db_name}/ontology?branch=<branch>` — 온톨로지 클래스 생성(비동기, **HTTP 202**). 요청: 경로 `db_name`, 쿼리 `branch`; JSON 바디(OpenAPI 스키마). 응답: `ApiResponse` + `command_id`.
- `POST /api/v1/databases/{db_name}/ontology/validate?branch=<branch>` — 온톨로지 생성 검증(린트, 쓰기 없음). 요청: 경로 `db_name`, 쿼리 `branch`; JSON 바디(OpenAPI 스키마). 응답: 검증 리포트 JSON(OpenAPI 스키마).
- `GET /api/v1/databases/{db_name}/ontology/{class_label}?branch=<branch>` — 온톨로지 클래스 조회. 요청: 경로 `db_name`, `class_label`, 쿼리 `branch`. 응답: JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/ontology/{class_label}/validate?branch=<branch>` — 온톨로지 업데이트 검증(린트+diff, 쓰기 없음). 요청: 경로 `db_name`, `class_label`, 쿼리 `branch`; JSON 바디(OpenAPI 스키마). 응답: 검증 리포트 JSON(OpenAPI 스키마).
- `PUT /api/v1/databases/{db_name}/ontology/{class_label}?branch=<branch>&expected_seq=...` — 온톨로지 업데이트(비동기, **HTTP 202**). 요청: 경로 `db_name`, `class_label`, 쿼리 `branch` + `expected_seq`(OCC). 응답: `ApiResponse` + `command_id`.
- `DELETE /api/v1/databases/{db_name}/ontology/{class_label}?branch=<branch>&expected_seq=...` — 온톨로지 삭제(비동기, **HTTP 202**). 요청: 경로 `db_name`, `class_label`, 쿼리 `branch` + `expected_seq`(OCC). 응답: `ApiResponse` + `command_id`.
- `GET /api/v1/databases/{db_name}/ontology/list?branch=<branch>` — 온톨로지 목록 조회. 요청: 경로 `db_name`, 쿼리 `branch`. 응답: JSON(OpenAPI 스키마).
- `GET /api/v1/databases/{db_name}/ontology/{class_id}/schema?branch=<branch>&format=json|jsonld|owl` — 스키마 내보내기(형식 선택). 요청: 경로 `db_name`, `class_id`, 쿼리 `branch`, `format`. 응답: JSON/JSON-LD/OWL.
- `POST /api/v1/databases/{db_name}/ontology-advanced` — 고급 관계 검증(비동기, 202). 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: `ApiResponse` + `command_id`. 비고: `auto_generate_inverse=true`면 `501`.
- `POST /api/v1/databases/{db_name}/validate-relationships` — 관계 검증(사전 검증, 쓰기 없음). 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 검증 결과 JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/check-circular-references` — 순환 참조 탐지(사전 검증). 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 분석 결과 JSON(OpenAPI 스키마).
- `GET /api/v1/databases/{db_name}/relationship-network/analyze` — 관계 네트워크 분석. 요청: 경로 `db_name`, 쿼리 파라미터(OpenAPI 스키마). 응답: 분석 결과 JSON(OpenAPI 스키마).
- `GET /api/v1/databases/{db_name}/relationship-paths` — 관계 경로 탐색. 요청: 경로 `db_name`, 쿼리 파라미터(OpenAPI 스키마). 응답: 결과 JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/suggest-schema-from-data` — 샘플 데이터 기반 스키마 제안. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 제안 스키마 JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/suggest-schema-from-google-sheets` — 구글 시트 기반 스키마 제안. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 제안 스키마 JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/suggest-schema-from-excel` — 엑셀 기반 스키마 제안. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 제안 스키마 JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/suggest-mappings` — 스키마 간 매핑 제안. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 제안 매핑 JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/suggest-mappings-from-google-sheets` — 구글 시트 기반 매핑 제안. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 제안 매핑 JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/suggest-mappings-from-excel` — 엑셀 기반 매핑 제안. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 제안 매핑 JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/import-from-google-sheets/dry-run` — 구글 시트 임포트 드라이런(쓰기 없음). 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 미리보기/검증 JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/import-from-google-sheets/commit` — 구글 시트 임포트 커밋(OMS 비동기 쓰기 제출, 배치). 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 배치 제출 결과 JSON(여러 `command_id` 포함).
- `POST /api/v1/databases/{db_name}/import-from-excel/dry-run` — 엑셀 임포트 드라이런(쓰기 없음). 요청: `multipart/form-data`(파일 + 폼 필드; OpenAPI 참고). 응답: 미리보기/검증 JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/import-from-excel/commit` — 엑셀 임포트 커밋(OMS 비동기 쓰기 제출, 배치). 요청: `multipart/form-data`(파일 + 폼 필드). 응답: 배치 제출 결과 JSON(여러 `command_id` 포함).
- `POST /api/v1/databases/{db_name}/ontology/{class_id}/mapping-metadata` — 매핑 메타데이터 저장. 요청: 경로 `db_name`, `class_id`, JSON 바디(OpenAPI 스키마). 응답: 저장 결과 JSON(OpenAPI 스키마).

비고: Google Sheets/Excel 커밋은 배치로 OMS bulk-create를 호출하며 응답에 `write.commands[]`가 포함된다. 현재 커밋은 `branch=main` 고정이다.

**보호 브랜치 정책(스키마 안전)**
- 기본 보호 브랜치: `main`, `master`, `production`, `prod` (`ONTOLOGY_PROTECTED_BRANCHES`로 변경 가능)
- 보호 브랜치에서 **고위험 스키마 변경**(예: 속성 제거/타입 변경) 및 삭제는 아래 헤더가 필요:
  - `X-Change-Reason: <text>` (필수)
  - `X-Admin-Token: <secret>` 또는 `Authorization: Bearer <secret>` (필수)
  - 선택: `X-Admin-Actor: <name>` (감사/추적용)

### 쿼리 (**안정**)
- `POST /api/v1/databases/{db_name}/query` — 쿼리 실행. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 쿼리 결과 JSON(OpenAPI 스키마).
- `GET /api/v1/databases/{db_name}/query/builder` — 쿼리 빌더 정보 조회. 요청: 경로 `db_name`. 응답: 빌더 정보 JSON(OpenAPI 스키마).
- `POST /api/v1/databases/{db_name}/query/raw` — 원시 쿼리 실행. 요청: 경로 `db_name`, JSON 바디(OpenAPI 스키마). 응답: 결과 JSON(OpenAPI 스키마).

## 상세 API 가이드 (FE 구현용)

이 섹션은 프론트엔드가 **API 목록만 보고도 구현**할 수 있도록 요청/응답/역할/주의사항을 상세히 정리한다.

### 공통 데이터 구조

**LocalizedText**
- 문자열 또는 언어 맵을 허용한다.
- 예: `"제품"` 또는 `{"ko":"제품","en":"Product"}`

**Ontology: Class Create/Update**
| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| id | string | N | 클래스 ID. 없으면 `label` 기반 자동 생성 |
| label | LocalizedText | Y | UI 라벨 |
| description | LocalizedText | N | 설명 |
| parent_class | string | N | 부모 클래스 ID |
| abstract | boolean | N | 추상 클래스 여부 |
| properties | Property[] | N | 속성 정의 |
| relationships | Relationship[] | N | 관계 정의 |
| metadata | object | N | 임의 메타데이터 |

**Property**
| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| name | string | Y | 속성 ID |
| type | string | Y | 데이터 타입 (`xsd:string`, `xsd:date`, `xsd:decimal` 등) |
| label | LocalizedText | Y | UI 라벨 |
| required | boolean | N | 필수 여부 |
| primary_key | boolean | N | PK 여부 |
| description | LocalizedText | N | 설명 |
| constraints | object | N | 제약(도메인별) |
| target / linkTarget | string | N | 관계형 속성일 때 대상 클래스 |
| cardinality | string | N | 관계형 속성일 때 카디널리티 |

**Relationship**
| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| predicate | string | Y | 관계 ID |
| target | string | Y | 대상 클래스 ID |
| label | LocalizedText | Y | UI 라벨 |
| cardinality | string | N | `1:1`, `1:n`, `n:1`, `n:m` |
| description | LocalizedText | N | 설명 |
| inverse_predicate | string | N | 역관계 ID |
| inverse_label | LocalizedText | N | 역관계 라벨 |

**Import 공통 구조**
| 구조 | 설명 |
|---|---|
| ImportFieldMapping | `{ "source_field": "컬럼명", "target_field": "속성ID" }` |
| ImportTargetField | `{ "name": "속성ID", "type": "xsd:string" }` |
| BoundingBox | `{ "top":0,"left":0,"bottom":10,"right":5 }` (0-based, inclusive) |

**ImportFromGoogleSheetsRequest**
| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| sheet_url | string | Y | Google Sheet URL |
| worksheet_name | string | N | 시트 탭 이름 |
| api_key | string | N | Google API key |
| target_class_id | string | Y | 타겟 클래스 ID |
| target_schema | ImportTargetField[] | Y | 타입 정보 |
| mappings | ImportFieldMapping[] | N | 컬럼 → 속성 매핑 |
| table_id | string | N | 선택된 테이블 ID |
| table_bbox | BoundingBox | N | 테이블 범위 |
| max_tables | number | N | 테이블 탐색 상한 |
| max_rows/max_cols | number | N | 샘플 제한 |
| trim_trailing_empty | boolean | N | trailing empty 제거 |
| allow_partial | boolean | N | 오류 행 스킵 허용 |
| dry_run_rows | number | N | dry-run 샘플 행 수 |
| max_import_rows | number | N | 커밋 시 최대 행 수 |
| batch_size | number | N | 커밋 배치 크기 |
| return_instances | boolean | N | 응답에 인스턴스 포함 |
| max_return_instances | number | N | 응답 인스턴스 상한 |
| options | object | N | Funnel 옵션 |

---

### 1) Summary / Health

**GET /api/v1/summary**
- 역할: 현재 컨텍스트 + 보호 브랜치 정책 + Redis/ES 헬스 요약을 **단일 응답**으로 제공.
- 쿼리: `db`, `branch`
- 응답(`ApiResponse.data`):
  - `context`: `{ db_name, branch }`
  - `policy`: `{ protected_branches, is_protected_branch }`
  - `services`: `{ redis: { ok }, elasticsearch: { ok, health, error } }`
  - `terminus`: OMS 브랜치 info (있을 때만)

**GET /api/v1/**
- 역할: 서비스 기본 정보(서비스명/버전).

**GET /api/v1/health**
- 역할: BFF + OMS 연결 상태 헬스 체크.
- 응답 예시:
  ```json
  { "status":"success", "data": { "service":"BFF", "version":"2.0.0", "oms_connected": true } }
  ```

---

### 2) 데이터베이스 / 브랜치

**GET /api/v1/databases**
- 역할: DB 이름 목록 조회.
- 응답: `ApiResponse.data.databases`는 **문자열 배열**(DB 이름).

**POST /api/v1/databases**
- 역할: DB 생성 (비동기).
- 바디:
  | 필드 | 타입 | 필수 | 설명 |
  |---|---|---|---|
  | name | string | Y | DB 이름 |
  | description | string | N | 설명 |
- 응답: `202` + `data.command_id` (Command 추적).

**GET /api/v1/databases/{db_name}**
- 역할: DB 존재 여부 조회.
- 응답: `ApiResponse.data.exists` (boolean)

**DELETE /api/v1/databases/{db_name}?expected_seq=...**
- 역할: DB 삭제 (비동기, OCC).
- 응답: `202` + `command_id` (또는 legacy 200)

**GET /api/v1/databases/{db_name}/expected-seq**
- 역할: DB 삭제용 OCC 토큰 획득.
- 응답: `ApiResponse.data.expected_seq`

**GET /api/v1/databases/{db_name}/branches**
- 역할: 브랜치 목록 조회.
- 응답: `{ branches: [...], count }`

**POST /api/v1/databases/{db_name}/branches**
- 역할: 브랜치 생성.
- 바디: `{ "name": "feature/x", "from_branch": "main" }`
- 응답: `{ status: "success", name: "...", data: <OMS 응답> }`

**GET /api/v1/databases/{db_name}/branches/{branch_name}**
- 역할: 브랜치 상세 조회 (OMS branch info pass-through).
- `branch_name`은 `/` 포함 가능. URL 인코딩 필요.

**DELETE /api/v1/databases/{db_name}/branches/{branch_name}?force=false**
- 역할: 브랜치 삭제.

**GET /api/v1/databases/{db_name}/classes**
- 역할: 클래스 목록 조회.
- 응답: `{ classes: [...], count }`
- 비고: OMS 전체 온톨로지 목록을 반환하므로 FE는 `type=="Class"` 필터를 권장.

**POST /api/v1/databases/{db_name}/classes**
- 역할: 레거시 JSON-LD 클래스 생성.
- 바디: JSON-LD(`@id` 필수). BFF가 `@id` → `id`로 변환.

**GET /api/v1/databases/{db_name}/classes/{class_id}**
- 역할: 특정 클래스 조회 (OMS pass-through).

**GET /api/v1/databases/{db_name}/versions**
- 역할: DB 버전/커밋 히스토리 조회.
- 응답: `{ versions: [...], count }`

---

### 3) 온톨로지 CRUD

**POST /api/v1/databases/{db_name}/ontology?branch=...**
- 역할: 클래스 생성(비동기). `id` 없으면 자동 생성.
- 바디: `OntologyCreateRequest`
- 응답: `202` + `command_id`

**POST /api/v1/databases/{db_name}/ontology/validate?branch=...**
- 역할: 생성 요청 사전 검증(쓰기 없음).
- 바디: `OntologyCreateRequest`
- 응답: 검증 리포트(JSON)

**GET /api/v1/databases/{db_name}/ontology/{class_label}?branch=...**
- 역할: 클래스 조회 (라벨 기반). `class_label`은 라벨 또는 class_id.

**POST /api/v1/databases/{db_name}/ontology/{class_label}/validate?branch=...**
- 역할: 업데이트 검증(린트+diff).
- 바디: `OntologyUpdateRequest`

**PUT /api/v1/databases/{db_name}/ontology/{class_label}?branch=...&expected_seq=...**
- 역할: 클래스 업데이트(비동기, OCC).
- 바디: `OntologyUpdateRequest`
- 응답: `202` + `command_id`

**DELETE /api/v1/databases/{db_name}/ontology/{class_label}?branch=...&expected_seq=...**
- 역할: 클래스 삭제(비동기, OCC).

**GET /api/v1/databases/{db_name}/ontology/list?branch=...**
- 역할: 클래스 목록(라벨 매핑 포함).

**GET /api/v1/databases/{db_name}/ontology/{class_id}/schema?branch=...&format=json|jsonld|owl**
- 역할: 스키마 내보내기.

---

### 4) 관계/스키마 검증 (고급)

**POST /api/v1/databases/{db_name}/ontology-advanced**
- 역할: 관계 검증/순환 참조 체크를 포함한 생성(비동기).
- 쿼리: `branch`, `auto_generate_inverse`(미구현, true면 501), `validate_relationships`, `check_circular_references`
- 바디: `OntologyCreateRequest`

**POST /api/v1/databases/{db_name}/validate-relationships**
- 역할: 관계 검증(쓰기 없음).

**POST /api/v1/databases/{db_name}/check-circular-references**
- 역할: 순환 참조 검사(쓰기 없음).

**GET /api/v1/databases/{db_name}/relationship-network/analyze**
- 역할: 관계 네트워크 분석.

**GET /api/v1/databases/{db_name}/relationship-paths**
- 역할: 관계 경로 탐색(라벨 기반 힌트 제공용).

---

### 5) 스키마/매핑 제안 (Funnel)

**POST /api/v1/databases/{db_name}/suggest-schema-from-data**
- 역할: 샘플 데이터 기반 클래스/타입 제안.
- 바디:
  | 필드 | 타입 | 필수 | 설명 |
  |---|---|---|---|
  | data | array[] | Y | row 배열 |
  | columns | string[] | Y | 컬럼명 |
  | class_name | string | N | 클래스 라벨 힌트 |
  | include_complex_types | boolean | N | 복합 타입 추론 |
- 응답: `suggested_schema`, `analysis_summary`, `detailed_analysis`

**POST /api/v1/databases/{db_name}/suggest-schema-from-google-sheets**
- 역할: Google Sheets 기반 스키마 제안.
- 바디: `sheet_url`, `worksheet_name`, `api_key`, `table_id`, `table_bbox` 등.

**POST /api/v1/databases/{db_name}/suggest-schema-from-excel**
- 역할: Excel 업로드 기반 스키마 제안.
- 요청: `multipart/form-data` (파일 + 폼 필드)

**POST /api/v1/databases/{db_name}/suggest-mappings**
- 역할: 소스/타겟 스키마 간 매핑 제안.
- 바디:
  | 필드 | 타입 | 필수 | 설명 |
  |---|---|---|---|
  | source_schema | object[] | Y | `{name,type}` 리스트 |
  | target_schema | object[] | Y | `{name,type}` 리스트 |
  | sample_data | object[] | N | 샘플 값 |
  | target_sample_data | object[] | N | 타겟 샘플 값 |

**POST /api/v1/databases/{db_name}/suggest-mappings-from-google-sheets**
- 역할: Google Sheets → 타겟 클래스 매핑 제안.
- 바디: `sheet_url`, `worksheet_name`, `target_class_id`, `target_schema`, `table_id`, `table_bbox`,
  `include_relationships`, `enable_semantic_hints`

**POST /api/v1/databases/{db_name}/suggest-mappings-from-excel**
- 역할: Excel → 타겟 클래스 매핑 제안.

---

### 6) Import (Google Sheets / Excel)

**POST /api/v1/databases/{db_name}/import-from-google-sheets/dry-run**
- 역할: 타입 변환/매핑 검증(쓰기 없음).
- 바디: `ImportFromGoogleSheetsRequest`
- 응답: `stats`, `errors`, `preview_data`, `structure`, `sample_instances`

**POST /api/v1/databases/{db_name}/import-from-google-sheets/commit**
- 역할: OMS bulk-create로 비동기 쓰기 제출(배치).
- 바디: `ImportFromGoogleSheetsRequest`
- 응답: `write.commands[]` (각 항목에 `command_id`, `status_url`)
- 비고: 현재 `branch=main` 고정.

**POST /api/v1/databases/{db_name}/import-from-excel/dry-run**
- 역할: Excel 파일 기반 dry-run.
- 요청: `multipart/form-data`
  - `file` (xlsx/xlsm)
  - `target_class_id` (string)
  - `target_schema_json` (JSON string, ImportTargetField[])
  - `mappings_json` (JSON string, ImportFieldMapping[])
  - `sheet_name`, `table_id`, `table_top/left/bottom/right`, `dry_run_rows`, `options_json` 등

**POST /api/v1/databases/{db_name}/import-from-excel/commit**
- 역할: Excel 파일 기반 커밋(배치).
- 응답: `write.commands[]` 포함.

**POST /api/v1/databases/{db_name}/ontology/{class_id}/mapping-metadata**
- 역할: 매핑 이력/통계 메타데이터를 클래스에 저장.
- 바디: 임의 JSON (권장 키: `sourceFile`, `mappingsCount`, `averageConfidence`, `mappingDetails`, `timestamp`)

---

### 7) 라벨 매핑

**GET /api/v1/databases/{db_name}/mappings/**
- 역할: 매핑 요약 통계(언어별 집계).

**POST /api/v1/databases/{db_name}/mappings/export**
- 역할: 전체 매핑 번들 다운로드(JSON).
- 응답: `Content-Disposition` 첨부파일.

**POST /api/v1/databases/{db_name}/mappings/import**
- 역할: 매핑 번들 업로드/적용.
- 요청: `multipart/form-data`의 `file`(JSON).

**POST /api/v1/databases/{db_name}/mappings/validate**
- 역할: 업로드 매핑 검증(쓰기 없음).
- 요청: `multipart/form-data`의 `file`(JSON).
- 응답: `validation_passed`, `details`(unmapped/충돌 목록)

**DELETE /api/v1/databases/{db_name}/mappings/**
- 역할: 매핑 전체 삭제.

---

### 8) 인스턴스 쓰기 (비동기)

**POST /api/v1/databases/{db_name}/instances/{class_label}/create?branch=...**
- 역할: 단건 생성(라벨 기반).
- 바디: `{ data: { <label>: <value> }, metadata?: object }`
- 응답: `CommandResult` (`command_id`, `status` 등)
- 에러: `unknown_label_keys`, `409`(OCC), `400`(검증 실패)

**PUT /api/v1/databases/{db_name}/instances/{class_label}/{instance_id}/update?branch=...&expected_seq=...**
- 역할: 단건 업데이트(OCC 필요).
- 바디: `{ data: {...}, metadata?: {...} }`

**DELETE /api/v1/databases/{db_name}/instances/{class_label}/{instance_id}/delete?branch=...&expected_seq=...**
- 역할: 단건 삭제(OCC 필요).

**POST /api/v1/databases/{db_name}/instances/{class_label}/bulk-create?branch=...**
- 역할: 다건 생성(라벨 기반).
- 바디: `{ instances: [ {<label>:<value>}, ... ], metadata?: {...} }`

- **expected_seq 주의**: 인스턴스용 expected_seq 조회 엔드포인트는 없으므로,
  최신 조회 응답의 `version`(ES) 또는 `409` 응답의 `actual_seq`를 사용한다.

---

### 9) 인스턴스 읽기 (ES 우선, TerminusDB 폴백)

**GET /api/v1/databases/{db_name}/class/{class_id}/instances**
- 쿼리: `limit`(<=1000), `offset`, `search`(문자열)
- 응답: `{ class_id, total, limit, offset, search, instances }`
- 비고: 현재 `branch` 미지원.
  - ES 응답에는 `version`, `event_timestamp`, `class_id`, `instance_id` 등이 포함될 수 있다.

**GET /api/v1/databases/{db_name}/class/{class_id}/instance/{instance_id}**
- 응답: `{ status: "success", data: <instance> }`

**GET /api/v1/databases/{db_name}/class/{class_id}/sample-values**
- 쿼리: `property_name` (선택)
- 응답:
  - `property_name` 있을 때: `{ values: [...] }`
  - 없을 때: `{ property_values: { field: [...] } }`

---

### 10) 그래프 쿼리 (Terminus + ES 페더레이션)

**POST /api/v1/graph-query/{db_name}?branch=...**
- 역할: 멀티홉 그래프 쿼리 + ES 문서 결합.
- 바디: `GraphQueryRequest`
  | 필드 | 타입 | 설명 |
  |---|---|---|
  | start_class | string | 시작 클래스 ID |
  | hops | GraphHop[] | `{predicate,target_class}` |
  | filters | object | 시작 클래스 필터(베스트에포트, **내부 property_id 기준**) |
  | limit/offset | number | 페이지네이션 |
  | max_nodes/max_edges | number | 폭발 방지 |
  | include_documents | boolean | ES 문서 포함 |
  | include_paths | boolean | 경로 포함 |
  | include_provenance | boolean | provenance/lag 정보 |
  | include_audit | boolean | audit 최소 정보 |
- 응답: `GraphQueryResponse`
  - `nodes[]`: `{ id, type, data_status(FULL|PARTIAL|MISSING), data?, display?, provenance?, index_status? }`
  - `edges[]`: `{ from_node, to_node, predicate }`
- `data_status`:
  - `FULL`: ES 문서 포함
  - `PARTIAL`: ES 일부 누락/지연 가능
  - `MISSING`: ES 문서 미존재(프로젝션 지연 또는 미인덱스)

**POST /api/v1/graph-query/{db_name}/simple?branch=...**
- 역할: 단일 클래스 간단 조회(필터).
- 바디: `{ class_name, filters?, limit? }`
- 비고: 현재 limit이 엄격히 적용되지 않을 수 있음.

**POST /api/v1/graph-query/{db_name}/multi-hop?branch=...**
- 역할: 멀티홉 헬퍼(legacy).

**GET /api/v1/graph-query/{db_name}/paths?source_class=...&target_class=...&max_depth=...&branch=...**
- 역할: 클래스 간 가능한 경로 목록.
- 응답: `{ source_class, target_class, paths, count, max_depth }`

---

### 11) 머지 충돌

**POST /api/v1/databases/{db_name}/merge/simulate**
- 역할: 충돌 미리보기.
- 바디: `{ source_branch, target_branch, message?, strategy? }`
- 응답: `merge_preview` + `conflicts[]`

**POST /api/v1/databases/{db_name}/merge/resolve**
- 역할: 충돌 해결 + 병합 실행.
- 바디:
  - `source_branch`, `target_branch`, `strategy`, `message`, `author`
  - `resolutions[]`: `{ path, resolution_type, resolved_value, metadata? }`

---

### 12) 감사 (Audit)

**GET /api/v1/audit/logs**
- 역할: 감사 로그 조회.
- 쿼리: `partition_key=db:<db_name>` 권장, `action`, `status`, `resource_type`, `event_id`, `command_id`, `since`, `until`, `limit`, `offset`

**GET /api/v1/audit/chain-head**
- 역할: 해시 체인 헤드 확인.
- 쿼리: `partition_key` 필수

---

### 13) 라인리지 (Lineage)

**GET /api/v1/lineage/graph**
- 쿼리: `root` 필수, `db_name` 권장, `direction`, `max_depth`, `max_nodes`, `max_edges`

**GET /api/v1/lineage/impact**
- 쿼리: `root` 필수, `artifact_kind`(es|s3|terminus 등)

**GET /api/v1/lineage/metrics**
- 쿼리: `db_name`(선택), `window_minutes`(기본 60)

---

### 14) 백그라운드 작업

**GET /api/v1/tasks/**
- 역할: 작업 목록 조회.
- 쿼리: `status`, `task_type`, `limit`

**GET /api/v1/tasks/{task_id}**
- 역할: 작업 상태/진행률 조회.

**GET /api/v1/tasks/{task_id}/result**
- 역할: 완료된 작업 결과 조회.

**DELETE /api/v1/tasks/{task_id}**
- 역할: 작업 취소.

**GET /api/v1/tasks/metrics/summary**
- 역할: 작업/큐 메트릭 요약.

---

### 15) 커맨드 상태

**GET /api/v1/commands/{command_id}/status**
- 역할: 202 커맨드 상태 폴링.
- 응답: `CommandResult`
- 비고: 상태 TTL(기본 24h) 이후 404 가능.

---

### 16) 데이터 커넥터 (Google Sheets)

**POST /api/v1/data-connectors/google-sheets/grid**
- 역할: 시트 그리드 + merge 정보 추출.
- 바디: `{ sheet_url, worksheet_name?, api_key?, max_rows?, max_cols?, trim_trailing_empty? }`
- 응답: `SheetGrid` (grid + merged_cells + metadata)

**POST /api/v1/data-connectors/google-sheets/preview**
- 역할: 컬럼 + 샘플 데이터 미리보기.
- 쿼리: `limit` (기본 10)
- 바디: `{ sheet_url, worksheet_name?, api_key? }`

**POST /api/v1/data-connectors/google-sheets/register**
- 역할: 시트 모니터링 등록(레지스트리 저장).
- 바디: `{ sheet_url, worksheet_name?, polling_interval?, database_name?, branch?, class_label?, auto_import?, max_import_rows?, api_key? }`
- 응답: `ApiResponse.data.registered_sheet`

**GET /api/v1/data-connectors/google-sheets/registered**
- 역할: 등록된 시트 목록.
- 쿼리: `database_name`(필터)
- 응답(`ApiResponse.data.sheets[]`): `sheet_id`, `sheet_url`, `worksheet_name`, `polling_interval`,
  `database_name`, `branch`, `class_label`, `auto_import`, `last_polled`, `last_hash`, `is_active`, `registered_at`

**GET /api/v1/data-connectors/google-sheets/{sheet_id}/preview**
- 역할: 등록된 시트 미리보기.
- 쿼리: `worksheet_name`, `limit`

**DELETE /api/v1/data-connectors/google-sheets/{sheet_id}**
- 역할: 등록 해제.

---

### 17) 쿼리 (Label 기반)

**POST /api/v1/databases/{db_name}/query**
- 역할: 라벨 기반 쿼리 실행.
- 바디 예시:
  ```json
  {
    "class_label": "제품",
    "filters": [
      { "field": "가격", "operator": "ge", "value": 10000 },
      { "field": "카테고리", "operator": "eq", "value": "전자제품" }
    ],
    "select": ["이름", "가격"],
    "order_by": "가격",
    "order_direction": "desc",
    "limit": 10
  }
  ```
- operator 허용값: `eq`, `ne`, `gt`, `ge`, `lt`, `le`, `like`, `in`, `not_in`, `is_null`, `is_not_null`
- UI 빌더에서 제공되는 기호(`=`, `!=`, `>=` 등)는 위 연산자 키로 매핑해서 전송해야 한다.

**GET /api/v1/databases/{db_name}/query/builder**
- 역할: UI 빌더용 연산자/예시 제공.

**POST /api/v1/databases/{db_name}/query/raw**
- 역할: 제한된 원시 쿼리. 허용 타입: `select|count|exists`

---

### 18) AI (NLQ)

**POST /api/v1/ai/translate/query-plan/{db_name}**
- 역할: 자연어 → 실행 계획(JSON) 생성 (실행 없음).
- 바디: `{ question, branch?, mode?, limit?, include_provenance?, include_documents? }`

**POST /api/v1/ai/query/{db_name}**
- 역할: 자연어 → 실행 → 답변 생성.
- 바디: 동일 (AIQueryRequest)
- 응답: `{ answer, plan, execution, llm, warnings }`

---

### 19) 운영자 전용 (Admin)

**POST /api/v1/admin/replay-instance-state**
- 역할: 이벤트 스토어 리플레이로 인스턴스 상태 복원(백그라운드).
- 바디: `{ db_name, class_id, instance_id, store_result?, result_ttl? }`
- 응답: `{ task_id, status_url }`

**GET /api/v1/admin/replay-instance-state/{task_id}/result**
- 역할: 리플레이 결과 조회.

**GET /api/v1/admin/replay-instance-state/{task_id}/trace**
- 역할: 리플레이 감사/라인리지 추적 결과 조회.

**POST /api/v1/admin/recompute-projection**
- 역할: ES 프로젝션 재계산(백그라운드).
- 바디: `{ db_name, projection, branch, from_ts, to_ts?, promote?, allow_delete_base_index?, max_events? }`

**GET /api/v1/admin/recompute-projection/{task_id}/result**
- 역할: 재계산 결과 조회.

**POST /api/v1/admin/cleanup-old-replays**
- 역할: Redis 리플레이 결과 정리.

**GET /api/v1/admin/system-health**
- 역할: 시스템 상태 요약.

## 웹소켓 (실시간 업데이트)

웹소켓 경로는 OpenAPI에 포함되지 않는다.

### 커맨드 구독

- `WS /api/v1/ws/commands/{command_id}`
  - 인증이 활성화되어 있으면 `?token=<admin_token>` 또는 `X-Admin-Token` 헤더를 전달한다.

클라이언트 수신 이벤트:
- `connection_established`
- `command_update` (상태 변경 시, `data`에 CommandResult payload 포함)
- `pong` (ping 응답)
- `subscription_result` (subscribe/unsubscribe 결과)
- `error`

클라이언트 → 서버 메시지:
```json
{ "type": "ping", "timestamp": "..." }
{ "type": "subscribe", "command_id": "uuid" }
{ "type": "unsubscribe", "command_id": "uuid" }
{ "type": "get_subscriptions" }
```

### 사용자 전체 커맨드 구독

- `WS /api/v1/ws/commands?user_id=...`
  - 인증이 활성화되어 있으면 `?token=<admin_token>` 또는 `X-Admin-Token` 헤더를 전달한다.
  - 연결 후 동일한 메시지 프로토콜(ping/subscribe/unsubscribe)을 사용한다.

## 핵심 플로우 (프론트엔드 레시피)

### 성공 기준 (“완료” 판단 기준)

사용자는 “어디서 성공을 확인하나”에 자주 막힌다. 아래 기준을 사용한다:

- **DB 생성**
  - 비동기일 때: `GET /api/v1/commands/{command_id}/status` → `COMPLETED`
  - 이후: `GET /api/v1/databases`에 DB 이름이 포함
- **온톨로지 클래스 생성**
  - 비동기일 때: 커맨드 상태 → `COMPLETED`
  - 이후: `GET /api/v1/databases/{db_name}/ontology/list`에 클래스 포함
- **인스턴스 생성**
  - 커맨드 상태 → `COMPLETED`
  - 이후: `POST /api/v1/graph-query/{db_name}`가 노드를 반환
  - 참고: ES 페이로드는 지연될 수 있으므로 `data_status=PARTIAL|MISSING` 처리 필요

### 1) 데이터베이스 생성 (비동기)

```bash
curl -sS -X POST 'http://localhost:8002/api/v1/databases' \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo_db","description":"demo"}'
```

예상(이벤트 소싱 모드): HTTP `202`

```json
{
  "status": "accepted",
  "message": "데이터베이스 'demo_db' 생성 명령이 접수되었습니다",
  "data": {
    "command_id": "uuid",
    "database_name": "demo_db",
    "status": "processing",
    "mode": "event_sourcing"
  }
}
```

이후:
- `GET /api/v1/commands/{command_id}/status`

### 2) 온톨로지 클래스 생성 (비동기)

```bash
curl -sS -X POST 'http://localhost:8002/api/v1/databases/demo_db/ontology?branch=main' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "Product",
    "label": "Product",
    "properties": [
      {"name":"product_id","type":"xsd:string","label":"Product ID","required":true},
      {"name":"name","type":"xsd:string","label":"Name"}
    ]
  }'
```

예상(이벤트 소싱 모드): HTTP `202` + `data.command_id`, 이후 커맨드 상태 폴링.

### 3) 인스턴스 생성 (비동기 커맨드)

**결정적** `instance_id`가 필요하면 ID 계열 필드를 포함한다:
- OMS는 `{class_id}_id`(예: `product_id`) 또는 `*_id` 필드에서 `instance_id`를 파생한다.

```bash
curl -sS -X POST 'http://localhost:8002/api/v1/databases/demo_db/instances/Product/create?branch=main' \
  -H 'Content-Type: application/json' \
  -d '{
    "data": {"Product ID": "PROD-1", "Name": "Apple"},
    "metadata": {}
  }'
```

예상: HTTP `202` + `command_id` 포함 `CommandResult`, 이후 폴링:
- `GET /api/v1/commands/{command_id}/status`

### 4) 멀티홉 그래프 쿼리

안전 기본값이 중요하다. 멀티홉은 폭발적으로 커질 수 있으므로 다음 값을 권장:
- `max_nodes`, `max_edges`, `max_paths`, `no_cycles=true`, `include_paths=false`(필요할 때만 변경)

```bash
curl -sS -X POST 'http://localhost:8002/api/v1/graph-query/demo_db' \
  -H 'Content-Type: application/json' \
  -d '{
    "start_class": "Product",
    "hops": [{"predicate": "owned_by", "target_class": "Client"}],
    "filters": {"product_id": "PROD-1"},
    "limit": 10,
    "max_nodes": 200,
    "max_edges": 500,
    "no_cycles": true,
    "include_documents": true
  }'
```

## 보류 (프론트엔드 미사용)

- RBAC(권한 제어)/테넌시가 아직 강제되지 않는다.
- 필드 단위 프로비넌스의 전체 흐름은 아직 완성되지 않았다.

## 관련 문서

- `docs/ARCHITECTURE.md`
- `docs/IDEMPOTENCY_CONTRACT.md`
- `docs/OPERATIONS.md`
