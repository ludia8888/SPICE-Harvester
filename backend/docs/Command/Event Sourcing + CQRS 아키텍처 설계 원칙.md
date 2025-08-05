# command/Event Sourcing + CQRS 아키텍처 설계 원칙

## 개요

분산 트랜잭션 문제를 해결하기 위해 Command/Event Sourcing 패턴을 구현했습니다. 이 패턴은 "작업 의도(Command)"와 "작업 결과(Event)"를 분리하여 시스템의 일관성과 신뢰성을 보장합니다.

## 아키텍처

### 이전 아키텍처의 문제점

```
OMS → TerminusDB (트랜잭션 A)
    → PostgreSQL Outbox (트랜잭션 B)  // 💥 원자성 보장 불가!
```

두 개의 독립적인 데이터베이스 시스템 간의 트랜잭션은 원자성을 보장할 수 없습니다.

### 새로운 아키텍처

```
┌─────────┐     ┌──────────┐     ┌─────────┐     ┌──────────┐     ┌────────┐
│   OMS   │────▶│PostgreSQL│────▶│ Message │────▶│  Kafka   │────▶│ Worker │
│         │     │ (Outbox) │     │  Relay  │     │          │     │        │
└─────────┘     └──────────┘     └─────────┘     └──────────┘     └───┬────┘
    │                                                                   │
    │ 1. Command만 저장                                        3. 실제 실행 │
    │ (단일 트랜잭션)                                                     ▼
    │                                                            ┌────────────┐
    └───────────────────────────────────────────────────────────│TerminusDB │
                        2. 결과 Event 발행                         └────────────┘
```

## 구성 요소

### 1. Redis 기반 Command 상태 추적 (✅ 새로 추가)

- **Redis Service** (`shared/services/redis_service.py`): Redis 연결 및 기본 작업
- **Command Status Service** (`shared/services/command_status_service.py`): Command 생명주기 관리
- **상태 저장**: Command ID를 키로 사용하여 상태, 진행률, 결과 저장
- **TTL**: 기본 24시간 (설정 가능)
- **Pub/Sub**: 실시간 상태 업데이트 지원

### 2. Command 모델 (`shared/models/commands.py`)

```python
class CommandType(str, Enum):
    CREATE_ONTOLOGY_CLASS = "CREATE_ONTOLOGY_CLASS"
    UPDATE_ONTOLOGY_CLASS = "UPDATE_ONTOLOGY_CLASS"
    DELETE_ONTOLOGY_CLASS = "DELETE_ONTOLOGY_CLASS"

class OntologyCommand(BaseCommand):
    command_id: UUID
    command_type: CommandType
    aggregate_id: str
    payload: Dict[str, Any]
    metadata: Dict[str, Any]
```

### 2. Event 모델 (`shared/models/events.py`)

```python
class EventType(str, Enum):
    ONTOLOGY_CLASS_CREATED = "ONTOLOGY_CLASS_CREATED"
    ONTOLOGY_CLASS_UPDATED = "ONTOLOGY_CLASS_UPDATED"
    ONTOLOGY_CLASS_DELETED = "ONTOLOGY_CLASS_DELETED"

class OntologyEvent(BaseEvent):
    event_id: UUID
    event_type: EventType
    command_id: UUID  # 원인 Command 참조
    data: Dict[str, Any]
```

### 3. 비동기 API (`oms/routers/ontology_async.py`)

```python
@router.post("/ontology/{db_name}/async/create")
async def create_ontology_async(request: OntologyCreateRequest):
    # TerminusDB를 호출하지 않음!
    # Command만 저장 (단일 트랜잭션)
    command = OntologyCommand(
        command_type=CommandType.CREATE_ONTOLOGY_CLASS,
        payload=request.dict()
    )
    
    async with postgres_db.transaction() as conn:
        command_id = await outbox_service.publish_command(conn, command)
    
    return {"command_id": command_id, "status": "PENDING"}
```

### 4. Ontology Worker Service

Kafka에서 Command를 읽어 실제 작업을 수행합니다:

```python
async def handle_create_ontology(command_data):
    # 실제 TerminusDB 작업
    result = await terminus.create_ontology_class(...)
    
    # 성공 Event 발행
    event = OntologyEvent(
        event_type=EventType.ONTOLOGY_CLASS_CREATED,
        command_id=command_data['command_id'],
        data=result
    )
    await publish_event(event)
```

## 장점

1. **완벽한 원자성**: Command 저장은 단일 PostgreSQL 트랜잭션
2. **재시도 가능**: 실패한 Command는 자동으로 재시도
3. **감사 추적**: 모든 의도와 결과가 기록됨
4. **확장성**: Worker를 수평 확장 가능
5. **분리된 관심사**: OMS는 요청 접수, Worker는 실행

### 4. WebSocket 실시간 업데이트 (✅ 구현 완료)

실시간 Command 상태 업데이트를 위한 WebSocket 서비스:

```python
# WebSocket 연결 관리자
class WebSocketConnectionManager:
    async def connect(self, websocket: WebSocket, client_id: str, user_id: str = None)
    async def subscribe_command(self, client_id: str, command_id: str)
    async def broadcast_command_update(self, command_id: str, update_data: Dict)

# Redis Pub/Sub → WebSocket 브리지
class WebSocketNotificationService:
    async def start(self)  # Redis 채널 구독 시작
    async def _listen_redis_updates(self)  # command_updates:* 채널 수신
```

**WebSocket 엔드포인트**:
- `/ws/commands/{command_id}` - 특정 Command 구독
- `/ws/commands?user_id={user_id}` - 사용자 모든 Command 구독
- `/ws/test` - 테스트 페이지
- `/ws/stats` - 연결 통계

## 사용 방법

### 1. 비동기 API 사용

```bash
# 온톨로지 생성 요청
POST /api/v1/ontology/mydb/async/create
{
  "id": "Person",
  "label": "Person",
  "properties": [...]
}

# 응답
{
  "command_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "PENDING",
  "result": {
    "message": "Ontology creation command accepted for 'Person'"
  }
}
```

### 2. Command 상태 확인 (✅ 구현 완료)

```bash
GET /api/v1/ontology/mydb/async/command/550e8400-e29b-41d4-a716-446655440000/status

# 응답
{
  "command_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "COMPLETED",
  "result": {
    "message": "Command is COMPLETED",
    "command_type": "CREATE_ONTOLOGY_CLASS",
    "aggregate_id": "Person",
    "created_at": "2025-08-05T10:30:00Z",
    "updated_at": "2025-08-05T10:30:05Z",
    "progress": 100,
    "history": [
      {
        "status": "PENDING",
        "timestamp": "2025-08-05T10:30:00Z",
        "message": "Command created"
      },
      {
        "status": "PROCESSING",
        "timestamp": "2025-08-05T10:30:01Z",
        "message": "Processing started by worker worker-12345"
      },
      {
        "status": "COMPLETED",
        "timestamp": "2025-08-05T10:30:05Z",
        "message": "Command completed successfully"
      }
    ],
    "result": {
      "class_id": "Person",
      "message": "Successfully created ontology class: Person",
      "terminus_result": {...}
    }
  }
}
```

### 3. 동기 API 래퍼 (✅ 구현 완료)

```bash
# 동기식 온톨로지 생성 (기본 30초 타임아웃)
POST /api/v1/ontology/mydb/sync/create
{
  "id": "Person",
  "label": "Person"
}

# 응답 (성공)
{
  "status": "success",
  "message": "Successfully created ontology class 'Person'",
  "data": {
    "command_id": "550e8400-e29b-41d4-a716-446655440000",
    "class_id": "Person",
    "execution_time": 2.5,
    "result": {...}
  }
}

# 타임아웃 설정 및 polling 간격 조정
POST /api/v1/ontology/mydb/sync/create?timeout=60&poll_interval=1.0
{
  "id": "Person",
  "label": "Person"
}

# 응답 (타임아웃)
HTTP 408 Request Timeout
{
  "detail": {
    "message": "Operation timed out after 60 seconds",
    "command_id": "550e8400-e29b-41d4-a716-446655440000",
    "hint": "You can check the status using GET /api/v1/ontology/mydb/async/command/550e8400.../status"
  }
}

# 기존 Command 대기
GET /api/v1/ontology/mydb/sync/command/550e8400-e29b-41d4-a716-446655440000/wait?timeout=30

# 응답
{
  "status": "success",
  "message": "Command completed successfully",
  "data": {
    "command_id": "550e8400-e29b-41d4-a716-446655440000",
    "execution_time": 25.3,
    "progress_history": [
      {
        "timestamp": "2025-08-05T10:30:00Z",
        "elapsed_time": 0.0,
        "status": "PENDING",
        "progress": 0
      },
      {
        "timestamp": "2025-08-05T10:30:01Z",
        "elapsed_time": 1.0,
        "status": "PROCESSING",
        "progress": 50
      },
      {
        "timestamp": "2025-08-05T10:30:25Z",
        "elapsed_time": 25.0,
        "status": "COMPLETED",
        "progress": 100
      }
    ],
    "result": {...}
  }
}
```

### 4. WebSocket 실시간 업데이트 (✅ 구현 완료)

```javascript
// 특정 Command 구독
const socket = new WebSocket('ws://localhost:8002/ws/commands/550e8400-e29b-41d4-a716-446655440000');

socket.onopen = () => {
    console.log('WebSocket 연결됨');
};

socket.onmessage = (event) => {
    const data = JSON.parse(event.data);
    if (data.type === 'command_update') {
        console.log(`Command ${data.command_id} 상태:`, data.data.status);
        console.log('진행률:', data.data.progress + '%');
    }
};

// 추가 Command 구독
socket.send(JSON.stringify({
    type: 'subscribe',
    command_id: 'another-command-id'
}));

// 구독 해제
socket.send(JSON.stringify({
    type: 'unsubscribe', 
    command_id: 'command-id-to-unsubscribe'
}));

// 현재 구독 목록 확인
socket.send(JSON.stringify({
    type: 'get_subscriptions'
}));
```

**WebSocket 메시지 형식**:
```json
{
  "type": "command_update",
  "command_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-08-05T10:30:05Z",
  "data": {
    "status": "PROCESSING",
    "progress": 75,
    "message": "Validating schema...",
    "updated_at": "2025-08-05T10:30:05Z"
  }
}
```

## 마이그레이션 전략

1. **단계적 마이그레이션**: 
   - 기존 동기 API는 유지
   - 새로운 비동기 API를 `/async` 경로에 추가
   - 클라이언트가 점진적으로 이전

2. **하이브리드 모드**:
   - 중요한 작업: 비동기 API (일관성 보장)
   - 읽기 작업: 기존 동기 API (빠른 응답)

## 향후 개선 사항

1. **Command 상태 추적** (✅ 구현 완료):
   - ✅ Redis에 Command 상태 저장
   - ✅ Command 실행 이력 추적
   - ✅ 진행률(progress) 표시
   - ✅ WebSocket으로 실시간 상태 업데이트 (완료)

2. **동기 API 래퍼** (✅ 구현 완료):
   - ✅ Command 제출 후 결과를 기다리는 동기 API
   - ✅ 설정 가능한 타임아웃 (1-300초)
   - ✅ 설정 가능한 polling 간격 (0.1-5초)
   - ✅ 408 Request Timeout 응답
   - ✅ 진행률 이력 추적
   - ✅ 기존 Command 대기 엔드포인트

3. **Saga 패턴**:
   - 여러 Command를 연결한 복잡한 워크플로우
   - 보상 트랜잭션 지원

4. **Event Store**:
   - 모든 Event를 영구 저장
   - 시점 복원(Point-in-time recovery) 가능

## 구현 완료 현황

### ✅ 완전 구현된 기능

1. **Command/Event Sourcing 패턴**
   - ✅ Command와 Event 모델 분리 (`shared/models/commands.py`, `shared/models/events.py`)
   - ✅ PostgreSQL Outbox 테이블에 message_type 필드 추가
   - ✅ OMS 서비스를 Command 저장 전용으로 리팩토링
   - ✅ Ontology Worker Service 구현 (Command 처리 + Event 발행)
   - ✅ Message Relay Service 업데이트 (Command/Event 큐분 처리)

2. **Redis 기반 Command 상태 추적**
   - ✅ Redis 서비스 인프라 구축 (`shared/services/redis_service.py`)
   - ✅ Command 상태 관리 서비스 (`shared/services/command_status_service.py`)
   - ✅ 실시간 상태 업데이트 (PENDING → PROCESSING → COMPLETED/FAILED)
   - ✅ 진행률 추적 및 이력 관리
   - ✅ TTL 기반 자동 정리 (기본 24시간)
   - ✅ Pub/Sub 지원 (WebSocket 연동 준비)

3. **동기 API 래퍼**
   - ✅ SyncWrapperService 구현 (`shared/services/sync_wrapper_service.py`)
   - ✅ 동기 API 엔드포인트 (`oms/routers/ontology_sync.py`)
   - ✅ 설정 가능한 타임아웃 (1-300초)
   - ✅ 설정 가능한 polling 간격 (0.1-5초)
   - ✅ 408 Request Timeout 응답
   - ✅ 진행률 이력 추적
   - ✅ 기존 Command 대기 기능

4. **Docker 및 인프라**
   - ✅ Redis 서비스 Docker Compose 설정
   - ✅ 모든 서비스 간 네트워크 연결 구성
   - ✅ Health check 및 재시작 정책

5. **에러 처리 및 모니터링**
   - ✅ Command 실패 시 Redis 상태 업데이트
   - ✅ 재시도 로직 (Message Relay)
   - ✅ 상세한 에러 메시지 및 힌트 제공
   - ✅ 실행 시간 추적

6. **WebSocket 실시간 업데이트**
   - ✅ WebSocket 연결 관리 서비스 (`shared/services/websocket_service.py`)
   - ✅ Redis Pub/Sub → WebSocket 브리지
   - ✅ 클라이언트별 Command 구독 관리
   - ✅ 실시간 상태 업데이트 브로드캐스팅
   - ✅ WebSocket 라우터 및 엔드포인트 (`bff/routers/websocket.py`)
   - ✅ 테스트 페이지 및 완전한 테스트 커버리지

### 🚧 진행 중인 작업

*현재 모든 우선순위 기능이 완료되었습니다.*

### 📋 계획된 기능

1. **Saga 패턴**
   - 복잡한 워크플로우 오케스트레이션
   - 보상 트랜잭션 지원
   - 워크플로우 상태 관리

2. **Event Store**
   - 모든 Event 영구 저장
   - 시점 복원 기능
   - Event 재생 기능

## 기술적 성과

- **원자성 보장**: 단일 PostgreSQL 트랜잭션으로 Command 저장
- **확장성**: Worker 서비스 수평 확장 가능
- **관측 가능성**: 완전한 Command 추적 및 이력 관리
- **사용성**: 비동기와 동기 API 모두 제공
- **신뢰성**: 자동 재시도 및 에러 처리
- **성능**: Redis 기반 빠른 상태 조회
# ✅ Command/Event Sourcing + CQRS 아키텍처 설계 원칙

본 문서는 메타데이터와 객체 데이터를 분리 저장하면서도 **일관된 시스템 상태와 추론 가능성**을 보장하기 위해 반드시 지켜야 할 아키텍처 원칙을 정의합니다. 본 원칙은 Palantir Foundry 스타일의 일관성과 추적 가능성을 실현하기 위한 핵심 가이드입니다.

---

## 1. 핵심 아키텍처 원칙

| 번호 | 원칙 | 설명 |
|------|------|------|
| **1** | **모든 변경은 Command로만 시작한다** | TerminusDB, PostgreSQL, S3 등 어떠한 저장소도 직접 쓰지 않는다. 모든 변경은 Command를 통해 시작되어야 한다. |
| **2** | **모든 Command는 Event를 반드시 발행해야 한다** | Command가 성공했지만 Event가 누락되면 시스템 상태는 추론 불가해진다. Event 생성은 필수이며 실패 시 재생성되어야 한다. |
| **3** | **객체와 메타데이터는 `aggregate_id`로 연결한다** | 모든 저장소에 동일한 `aggregate_id`, `command_id`, `event_id`, `version`을 포함시켜 추적 가능성을 유지해야 한다. |
| **4** | **읽기 모델은 오직 Event로부터 생성한다** | Projection(PostgreSQL, Elasticsearch)은 오직 Kafka Event를 기반으로 갱신되며, 직접 쓰기 금지다. |
| **5** | **메타데이터와 객체는 동기화되지 않지만 논리적으로 일관되어야 한다** | 즉각적으로 일치할 필요는 없으나, 상태 불일치가 발생해서는 안 된다. |
| **6** | **쓰기 경로는 단일, 읽기 경로는 자유** | 쓰기 = Command → Event → Worker만 허용. 읽기는 자유롭게 허용하되 변경은 금지한다. |
| **7** | **객체의 원본은 TerminusDB 또는 S3이다** | Projection은 캐시이자 읽기 최적화 전용이며, 최종 진실(SOT)은 객체 저장소이다. |
| **8** | **모든 객체에는 `source_command_id`, `event_id`가 포함되어야 한다** | 이 정보가 있어야 추적, 감사, 롤백이 가능하다. 누락 금지. |
| **9** | **Command/Event는 append-only로만 관리된다** | 삭제/수정 없이 오직 추가만 가능해야 한다. 이벤트 로그는 절대 변조하지 않는다. |
| **10** | **Event Replay로 상태를 재현할 수 있어야 한다** | Projection이나 상태가 손상되어도, Event log만으로 전체 상태를 복원할 수 있어야 한다. |

---

## 2. 고급 확장 원칙 (Foundry 스타일)

| 번호 | 원칙 | 설명 |
|------|------|------|
| **11** | **객체에는 항상 `version` 필드를 포함한다** | 같은 `aggregate_id`의 다중 버전이 공존 가능해야 하며, 변경 이력은 version 기반으로 추적된다. |
| **12** | **객체-메타간 Reconciliation 절차를 갖춘다** | 일관성이 깨졌을 경우, Event Store 기준으로 상태를 다시 계산하는 복원 루틴이 있어야 한다. |
| **13** | **Worker는 Idempotent 해야 한다** | 동일 Command가 여러 번 실행되더라도 시스템 상태가 중복 변경되지 않아야 한다. |
| **14** | **Fail Fast + Retry 원칙을 따른다** | 실패는 즉시 감지되어 재시도되며, 조용한 실패는 금지한다. Dead Letter Queue 활용 권장. |
| **15** | **모든 객체는 “누가, 언제, 어떤 명령으로 만들었는가”를 설명할 수 있어야 한다** | 이것이 Palantir가 가진 데이터 추론력의 본질이며, 시스템 신뢰성의 핵심이다. |

---

## 3. 보안 및 운영 원칙

| 번호 | 원칙 | 설명 |
|------|------|------|
| **16** | **Command-level ACL 적용** | Command 호출자는 해당 리소스에 대한 권한이 있어야 하며, 인증 및 권한 검사를 통과해야 한다. |
| **17** | **Command immutability 보장** | 이미 기록된 Command는 절대 수정/삭제 불가. 동일 작업은 새로운 Command로 요청해야 한다. |
| **18** | **WebSocket 상태 Push에도 ACL을 적용한다** | 실시간 상태 알림도 해당 사용자의 Command일 경우에만 전달한다. |
| **19** | **Event Replay는 관리자 승인 하에만 수행** | Event Replay는 강력한 복원 기능이지만, 운영환경에서는 권한 보호 및 절차가 필요하다. |

---

## 4. 시스템 워크플로우 요약 (전형적인 흐름)

```mermaid
graph TD
  A[API 요청 (Command)] --> B[PostgreSQL Outbox 저장]
  B --> C[Kafka 발행]
  C --> D[Worker 처리]
  D --> E[객체 저장 (TerminusDB/S3)]
  D --> F[Projection 갱신 (PostgreSQL/ES)]
  F --> G[WebSocket 실시간 상태 푸시]

  subgraph 일관성 유지
    B --> E
    B --> F
    E --> F
    F --> E
  end
  단계: 인프라 준비 및 모델 정의 (가장 먼저, 가장 중요)

  > 목표: 코드를 작성하기 전에 필요한 모든 인프라를 갖추고, 데이터의
  기본 설계도를 완성합니다.

   1. `docker-compose.databases.yml`에 S3 서비스 추가:
       * Action: 실제 AWS S3를 사용하기 전에, 로컬 개발 환경에서 S3 API와
         호환되는 MinIO 서비스를 추가합니다. 이렇게 하면 AWS 비용 없이,
         네트워크 연결 없이도 로컬에서 완벽하게 S3 연동 개발 및 테스트가
         가능합니다.
       * Code to Add (in `docker-compose.databases.yml`):

    1         services:
    2           # ... 기존 서비스들
    3           minio:
    4             image: minio/minio:RELEASE.2023-03-20T20-16-18Z
    5             container_name: spice-harvester-minio
    6             command: server /data --console-address ":9001"
    7             ports:
    8               - "9000:9000"  # S3 API Port
    9               - "9001:9001"  # MinIO Console Port
   10             environment:
   11               - MINIO_ROOT_USER=minioadmin
   12               - MINIO_ROOT_PASSWORD=minioadmin
   13             volumes:
   14               - minio-data:/data
   15             healthcheck:
   16               test: ["CMD", "curl", "-f",
      "http://localhost:9000/minio/health/live"]
   17               interval: 30s
   18               timeout: 20s
   19               retries: 3
   20 
   21         volumes:
   22           # ... 기존 볼륨들
   23           minio-data:

   2. `shared/models/`에 `Instance` Command/Event 모델 정의:
       * Action: 이전 답변의 Phase 1과 동일합니다. commands.py와
         events.py에 Instance 관련 Enum과 Pydantic 모델을 먼저 정의합니다.
          이것이 모든 후속 개발의 기준이 됩니다.

  2단계: API 엔드포인트 및 Command 발행 구현

  > 목표: 사용자가 인스턴스 데이터를 생성/수정/삭제하려는 '의도'를
  시스템이 안정적으로 접수할 수 있게 합니다.

   1. BFF/OMS에 `instance_async.py` 라우터 생성:
       * Action: 이전 답변의 Phase 1과 동일합니다. POST 
         /api/v1/instances/{db_name}/{class_id}/create 와 같은 비동기 API
         엔드포인트를 만듭니다.
       * 핵심: 이 엔드포인트는 오직 `InstanceCommand`를 생성하여 
         PostgreSQL의 `outbox` 테이블에 저장하는 역할만 합니다. 아직 S3나
         TerminusDB에 접근하지 않습니다.

  3단계: '쓰기 모델' 워커 구현 (S3 & TerminusDB)

  > 목표: 접수된 Command를 실제로 처리하여, 데이터의 원본(Source of
  Truth)을 S3와 TerminusDB에 기록합니다.

   1. `instance_worker` 서비스 생성:
       * Action: ontology_worker를 템플릿으로 삼아 instance_worker
         서비스를 만듭니다.
       * Action: requirements.txt 또는 pyproject.toml에 boto3 (S3
         클라이언트 라이브러리)를 추가합니다.
       * Action: docker-compose.yml에 instance_worker 서비스를 추가하고,
         MinIO 접속에 필요한 환경 변수(Endpoint URL, Access Key, Secret
         Key)를 주입합니다.

   2. `instance_worker/main.py`의 Command 핸들러 구현:
       * Action: handle_create_instance 메서드를 구현합니다. 이 메서드는 두 
         가지 핵심 쓰기 작업을 수행합니다.
           1. S3에 이벤트 페이로드 저장: Command의 전체 내용을 JSON 파일로
              만들어 MinIO(S3)에 업로드합니다. 이것이 불변의 이벤트 로그가
              됩니다.
           2. TerminusDB에 인스턴스 문서 생성: Command의 데이터를 사용하여
              TerminusDB에 실제 문서를 생성합니다.
       * Action: 위 두 작업이 모두 성공했을 때만, `InstanceEvent`를 
         생성하여 Kafka의 `instance_events` 토픽으로 발행합니다.
       * Action: Redis의 CommandStatusService를 호출하여 Command의 상태를
         PROCESSING -> COMPLETED로 업데이트합니다.

  ---

  여기까지가 1차 목표입니다.

  위 3단계까지 완료하면, "모든 인스턴스 데이터의 변경 이력이 S3에 
  영구적으로 기록되고, 최신 상태는 TerminusDB에서 조회 가능한" 안정적인
  쓰기 모델이 완성됩니다. Elasticsearch가 없어도 시스템의 핵심 데이터는
  안전하게 보관됩니다.

  ---

  다음 단계 (ES 도입 준비)

  위 1차 목표가 완벽하게 작동하는 것을 확인한 후에, 비로소 Elasticsearch
   도입을 시작합니다.

  4단계: Elasticsearch 인프라 추가

   1. `docker-compose.databases.yml`에 Elasticsearch 서비스 추가:
       * Action: redis나 postgres 서비스처럼, Elasticsearch와
         Kibana(시각화 도구) 서비스를 추가합니다.

  5단계: '읽기 모델' 프로젝션 워커 구현

  > 목표: instance_events를 읽어 검색에 최적화된 읽기 모델을 ES에
  구축합니다.

   1. `projection_worker` 서비스 생성:
       * Action: instance_worker와 유사하게, Kafka 컨슈머를 포함하는
         projection_worker를 만듭니다.
       * Action: requirements.txt에 elasticsearch-py 라이브러리를
         추가하고, docker-compose.yml에 서비스를 등록합니다.

   2. `projection_worker/main.py`의 Event 핸들러 구현:
       * Action: instance_events 토픽을 구독합니다.
       * Action: INSTANCE_CREATED, INSTANCE_UPDATED, INSTANCE_DELETED
         이벤트를 받아 Elasticsearch의 문서를 생성, 수정, 삭제하는 로직을
         구현합니다.

  이처럼 단계를 나누어 진행하면, 각 단계마다 명확한 목표를 가지고
  개발하고 테스트할 수 있어 복잡성이 크게 줄어듭니다. 특히 MinIO를 
  사용한 로컬 S3 환경 구축은 초기 개발 속도와 안정성을 높이는 데
  결정적인 역할을 할 것입니다.