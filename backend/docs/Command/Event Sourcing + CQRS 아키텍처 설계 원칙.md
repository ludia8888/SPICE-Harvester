# 아키텍처 설계 원칙: Event Sourcing + CQRS

## 1. 개요

본 문서는 **인스턴스(Instance) 데이터** 관리에 적용된 **이벤트 소싱(Event Sourcing) 및 CQRS(Command Query Responsibility Segregation)** 아키텍처의 설계 원칙과 실제 구현 흐름을 설명합니다. 이 아키텍처는 데이터의 모든 변경 이력을 완벽하게 보존하고, 시스템의 확장성과 안정성을 극대화하는 것을 목표로 합니다.

이 방식은 **"데이터는 불변하며, 파생만 있을 뿐이다"**라는 원칙을 따릅니다.

---

## 2. 핵심 아키텍처 및 구성 요소

### 아키텍처 다이어그램

```mermaid
graph TD
    subgraph "API 계층 (BFF/OMS)"
        A[API Endpoint] -->|1. Command 생성| B(Outbox Service)
        B -->|2. Command 저장| C[PostgreSQL Outbox]
    end

    subgraph "메시지 릴레이"
        D[Message Relay] -->|3. Command 폴링| C
        D -->|4. Kafka 발행| E[Kafka Event Bus]
    end

    subgraph "쓰기 모델 (Instance Worker)"
        F(Instance Worker) -->|5. Command 구독| E
        F -->|6. S3에 Command Log 저장<br/><b>(Source of Truth)</b>| G[S3/MinIO]
        F -->|7. TerminusDB 상태 업데이트<br/>(Write-Model Cache)| H[TerminusDB]
        F -->|8. Domain Event 발행| E
    end

    subgraph "읽기 모델 (Projection Worker)"
        I(Projection Worker) -->|9. Event 구독| E
        I -->|10. ES 문서 생성/수정<br/>(Read Model)| J[Elasticsearch]
    end

    style G fill:#f8d7da,stroke:#333,stroke-width:2px
```

### 구성 요소 역할

| 컴포넌트 | 역할 | 핵심 기술 |
| :--- | :--- | :--- |
| **Outbox Service** | 사용자의 요청(Command)을 원자적으로 데이터베이스에 저장하여 처리의 안정성을 보장합니다. | FastAPI, PostgreSQL |
| **Message Relay** | Outbox 테이블의 커맨드를 Kafka로 안정적으로 전달하는 브릿지 역할을 합니다. | `asyncio`, `confluent-kafka` |
| **Instance Worker** | **쓰기 모델(Write Model)**의 핵심. Kafka에서 커맨드를 받아 실제 데이터 변경 작업을 수행합니다. | `confluent-kafka`, `boto3` |
| **S3 (MinIO)** | **이벤트 저장소(Event Store)**. 모든 인스턴스 커맨드의 원본 로그를 불변의 상태로 영구 저장하는 **진실의 원천(Source of Truth)** 입니다. | S3 API |
| **TerminusDB** | 인스턴스의 **최신 상태를 저장하는 캐시** 역할을 하는 쓰기 모델의 일부입니다. 빠른 직접 조회를 지원합니다. | Graph DB |
| **Projection Worker** | **읽기 모델(Read Model)**을 생성하는 역할. 도메인 이벤트를 구독하여 검색에 최적화된 문서를 Elasticsearch에 저장(Projection)합니다. | `confluent-kafka`, `elasticsearch-py` |
| **Elasticsearch** | 검색 및 목록 조회에 특화된 고성능 읽기 모델입니다. | Search Engine |

---

## 3. 데이터 흐름: 인스턴스 생성의 생명주기

1.  **Command 생성 및 저장 (at OMS)**
    -   사용자가 인스턴스 생성 API (`/instances/{db_name}/async/{class_id}/create`)를 호출합니다.
    -   `InstanceCommand` 객체가 생성됩니다.
    -   `OutboxService`가 이 커맨드를 PostgreSQL의 `outbox` 테이블에 **원자적으로 저장**합니다. 이 시점에서 클라이언트에게는 `202 Accepted`와 함께 `command_id`가 즉시 반환됩니다.

2.  **Command 발행 (at Message Relay)**
    -   `MessageRelay` 서비스가 `outbox` 테이블을 폴링하여 처리되지 않은 커맨드를 가져옵니다.
    -   가져온 커맨드를 Kafka의 `INSTANCE_COMMANDS_TOPIC` 토픽으로 발행합니다.

3.  **Command 처리 (at Instance Worker)**
    -   `InstanceWorker`가 `INSTANCE_COMMANDS_TOPIC`을 구독하다가 커맨드를 수신합니다.
    -   `handle_create_instance` 메소드가 호출됩니다.
    -   **[핵심 동작 1] S3에 커맨드 로그 저장**: 수신한 커맨드 객체 전체를 JSON 파일 형태로 S3 버킷(`spice-commands-{db_name}`)의 `/{class_id}/{instance_id}/{command_id}.json` 경로에 저장합니다. **이 파일이 바로 이 인스턴스의 모든 것을 증명하는 진실의 원천(Source of Truth)입니다.**
    -   **[핵심 동작 2] TerminusDB 상태 업데이트**: S3 저장 후, TerminusDB에 해당 인스턴스의 최신 상태 문서를 생성/업데이트합니다. 이는 빠른 조회를 위한 캐시 역할을 합니다.
    -   **[핵심 동작 3] 도메인 이벤트 발행**: 모든 쓰기 작업이 성공하면, `INSTANCE_CREATED` 라는 도메인 이벤트를 생성하여 Kafka의 `INSTANCE_EVENTS_TOPIC`으로 발행합니다. 이 이벤트는 "인스턴스가 성공적으로 생성되었음"을 시스템 전체에 알립니다.

4.  **프로젝션 (at Projection Worker)**
    -   `ProjectionWorker`가 `INSTANCE_EVENTS_TOPIC`을 구독하다가 `INSTANCE_CREATED` 이벤트를 수신합니다.
    -   `handle_instance_created` 메소드가 호출됩니다.
    -   이벤트 데이터를 가공하여 검색에 최적화된 JSON 문서를 만듭니다.
    -   `ElasticsearchService`를 통해 해당 문서를 Elasticsearch에 색인(저장)합니다.

---

## 4. 핵심 기능 구현 방식

### 시간 여행 (Time Travel) 및 상태 재현 (Replay)

-   **구현 주체**: `shared/services/storage_service.py`의 `replay_instance_state` 메소드.
-   **동작 방식**:
    1.  특정 인스턴스의 S3 경로 (`/{class_id}/{instance_id}/`) 아래에 있는 **모든 커맨드 로그 파일**을 시간순으로 읽어옵니다.
    2.  빈 초기 상태에서 시작하여, 첫 번째 커맨드부터 마지막 커맨드까지 순차적으로 메모리상에서 적용하며 상태를 재구성합니다.
    3.  이 과정을 통해 언제든지 인스턴스의 특정 시점 또는 최종 상태를 100% 정확하게 재현할 수 있습니다.
-   **스냅샷 최적화 (현재 미구현, 향후 계획)**: 커맨드 로그가 매우 많아질 경우, 주기적으로 특정 버전의 상태(스냅샷)를 S3에 저장하고, 리플레이 시 가장 최신 스냅샷부터 그 이후의 커맨드만 적용하여 재현 속도를 최적화할 수 있습니다.

### 데이터 계보 추적 (Lineage & Provenance)

-   **구현 주체**: `replay_instance_state` 메소드 내부.
-   **동작 방식**: 상태를 재현하는 과정에서, 적용된 모든 커맨드의 ID, 타입, 타임스탬프 등의 정보를 최종 상태 객체의 `_metadata.command_history` 필드에 리스트 형태로 저장합니다.
-   **효과**: 이를 통해 "이 인스턴스의 현재 상태가 어떤 변경 과정(커맨드)을 거쳐 만들어졌는가"를 완벽하게 추적할 수 있습니다.

---

## 5. 설계 원칙 준수

-   **모든 변경은 Command로**: `InstanceWorker`만이 S3와 TerminusDB에 데이터를 쓸 수 있으며, 이는 오직 Kafka를 통해 전달된 Command에 의해서만 촉발됩니다.
-   **읽기 모델은 Event로부터**: `ProjectionWorker`는 오직 `INSTANCE_EVENTS_TOPIC`의 이벤트를 통해서만 Elasticsearch의 데이터를 변경합니다.
-   **Idempotency (멱등성)**: `InstanceWorker`는 동일한 `command_id`를 가진 커맨드가 중복 수신되더라도, S3에 동일한 이름의 파일이 이미 존재하는지 확인하는 등의 방법으로 시스템 상태가 중복 변경되지 않도록 설계될 수 있습니다. (현재는 Kafka의 Exactly-once Semantics에 일부 의존)
