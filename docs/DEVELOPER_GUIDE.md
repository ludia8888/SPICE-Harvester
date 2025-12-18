# SPICE HARVESTER 개발자 가이드

> **최종 업데이트**: 2025-12-17
> **상태**: Event Sourcing steady state

> NOTE (2025-12): 현재 실사용 경로는 **S3/MinIO Event Store(SSoT) + EventPublisher(S3 tail → Kafka)** 이며, PostgreSQL은 **`processed_events`(멱등 레지스트리) + write-side seq allocator** 용도로만 사용합니다.  
> 멱등/순서 계약은 `docs/IDEMPOTENCY_CONTRACT.md`를 기준으로 합니다.

## 목차

1.  [시작하기](#1-시작하기)
2.  [핵심 아키텍처 이해](#2-핵심-아키텍처-이해)
3.  [개발 환경 설정](#3-개발-환경-설정)
4.  [주요 개발 워크플로우](#4-주요-개발-워크플로우)
5.  [테스트 가이드라인](#5-테스트-가이드라인)
6.  [디버깅 팁](#6-디버깅-팁)

---

## 1. 시작하기

### 사전 요구사항

-   Python 3.9 이상
-   Docker 및 Docker Compose
-   Git

### 설치 및 실행

```bash
# 1. 저장소 복제
git clone https://github.com/your-org/spice-harvester.git
cd spice-harvester

# 2. 파이썬 가상 환경 설정 및 의존성 설치
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
cd backend
pip install -r requirements.txt

# 3. 환경 변수 설정
cp .env.example .env
# .env 파일에 필요한 설정을 입력합니다.

# 4. 모든 서비스 실행 (Docker Compose 사용)
# 이 명령어로 TerminusDB, PostgreSQL, Redis, Kafka 등 모든 인프라가 실행됩니다.
cd ..
docker compose -f docker-compose.full.yml up -d
cd backend

# 5. 백엔드 애플리케이션 서비스 실행 (별도 터미널)
python -m oms.main &
python -m bff.main &
python -m message_relay.main &
python -m instance_worker.main &
python -m ontology_worker.main &
python -m projection_worker.main &

# 6. 서비스 상태 확인
curl http://localhost:8002/health # BFF
```

---

## 2. 핵심 아키텍처 이해

개발자는 **반드시 `Ontology`와 `Instance` 데이터가 서로 다른 아키텍처로 관리된다는 점을 이해해야 합니다.**

### 아키텍처 비교

| 구분 | **Ontology (스키마) 데이터** | **Instance (실 데이터)** |
| :--- | :--- | :--- |
| **아키텍처 모델** | **상태 저장 (State-Store)** | **이벤트 소싱 (Event Sourcing) + CQRS** |
| **진실의 원천 (SSoT)** | **TerminusDB** 에 저장된 최신 상태의 문서 | **S3/MinIO Event Store** 에 저장된 불변 EventEnvelope 스트림 |
| **데이터 변경 방식** | `UPDATE` 쿼리를 통해 기존 문서를 **덮어씀** | 새로운 `EventEnvelope(kind=command)`를 Event Store에 **append-only** |
| **시간 여행/상태 재현** | **불가능** (TerminusDB 자체 버전 관리에만 제한적으로 의존) | **완벽하게 가능** (S3 로그를 리플레이하여 재현) |
| **핵심 처리 워커** | `OntologyWorker` | `InstanceWorker` |

### 개발 시 고려사항

-   **`Ontology` 개발 시**: 전통적인 CRUD 개발 방식과 유사합니다. `OntologyWorker`가 TerminusDB의 문서를 직접 수정하고, 그 결과를 이벤트로 발행합니다. 데이터의 최종 상태를 중심으로 개발합니다.
-   **`Instance` 개발 시**: 이벤트 소싱 패러다임을 따라야 합니다. **SSoT는 S3/MinIO Event Store**이고, TerminusDB/Elasticsearch/Redis는 **파생(derived) 모델**입니다. 개발의 중심은 '상태'가 아닌 '발생한 이벤트(EventEnvelope)'가 되어야 합니다.

---

## 3. 개발 환경 설정

### 프로젝트 구조

```
SPICE-HARVESTER/
├── backend/                # 모든 백엔드 서비스
│   ├── bff/              # API 게이트웨이
│   ├── oms/              # Command 접수 및 Event Store append
│   ├── message_relay/    # EventPublisher (S3 tail → Kafka)
│   ├── instance_worker/  # Instance 쓰기 모델
│   ├── ontology_worker/  # Ontology 쓰기 모델
│   ├── projection_worker/ # 읽기 모델 생성
│   ├── shared/           # 공통 모듈 (매우 중요)
│   └── tests/            # 통합 테스트
└── docs/                   # 전체 프로젝트 문서
```

### `shared` 디렉토리

모든 서비스가 공유하는 핵심 로직이 담겨있습니다. 새로운 기능을 추가할 때 가장 먼저 살펴봐야 할 곳입니다.

-   `shared/models/`: `commands.py`, `events.py` 등 핵심 데이터 모델 정의
-   `shared/services/`: `storage_service.py` (S3), `elasticsearch_service.py` 등 공용 서비스 클라이언트
-   `shared/config/`: `app_config.py` 등 서비스 전반의 설정값 (Kafka 토픽 이름 등)

---

## 4. 주요 개발 워크플로우

### 워크플로우 1: 새로운 `Instance` 관련 기능 추가 (예: "인스턴스 태그 추가" 기능)

1.  **Command 정의**: `shared/models/commands.py`에 `ADD_TAGS_TO_INSTANCE`와 같은 새로운 `CommandType`과 관련 `InstanceCommand` 모델을 추가합니다.
2.  **API 엔드포인트 추가**: `oms/routers/instance_async.py`에 `/instances/{db_name}/async/{instance_id}/add-tags` 와 같은 비동기 API를 추가합니다. 이 API는 오직 `ADD_TAGS_TO_INSTANCE` 커맨드를 생성하여 **S3/MinIO Event Store(SSoT)**에 `EventEnvelope(kind=command)`로 append하는 역할만 합니다.
3.  **Worker 로직 추가**: `instance_worker/main.py`의 `process_command` 메소드에 `ADD_TAGS_TO_INSTANCE` 타입을 처리하는 분기문을 추가하고, `handle_add_tags`와 같은 새로운 핸들러 메소드를 구현합니다.
4.  **핸들러 메소드 구현 (`handle_add_tags`)**:
    a.  `processed_events` 레지스트리로 `event_id`를 **claim**하여 멱등성을 확보합니다.
    b.  TerminusDB에서 인스턴스 문서를 가져와 태그 정보를 추가하고 **업데이트**합니다.
    c.  `INSTANCE_TAGS_ADDED`와 같은 새로운 도메인 이벤트를 `EventEnvelope(kind=domain)`로 **Event Store에 append**합니다. (EventPublisher가 Kafka로 전달)
5.  **프로젝션 로직 추가**: `projection_worker/main.py`에 `INSTANCE_TAGS_ADDED` 이벤트를 처리하는 로직을 추가하여, Elasticsearch 문서에 `tags` 필드를 추가/업데이트합니다.
6.  **테스트 작성**: 위 모든 흐름을 검증하는 통합 테스트를 작성합니다.

### 워크플로우 2: 새로운 `Ontology` 관련 기능 추가 (예: "온톨로지 동의어 추가" 기능)

1.  **Command 정의**: `shared/models/commands.py`에 `ADD_SYNONYM_TO_CLASS` 커맨드 타입을 추가합니다.
2.  **API 엔드포인트 추가**: `oms/routers/ontology.py`에 관련 비동기 API를 추가하여 커맨드를 생성하고 **S3/MinIO Event Store(SSoT)**에 `EventEnvelope(kind=command)`로 append합니다.
3.  **Worker 로직 추가**: `ontology_worker/main.py`에 `ADD_SYNONYM_TO_CLASS` 커맨드를 처리하는 핸들러(`handle_add_synonym`)를 추가합니다.
4.  **핸들러 메소드 구현 (`handle_add_synonym`)**:
    a.  TerminusDB에서 온톨로지 클래스 문서를 가져와 `synonyms` 필드를 **직접 수정**합니다.
    b.  `ONTOLOGY_CLASS_SYNONYM_ADDED` 이벤트를 `EventEnvelope(kind=domain)`로 **Event Store에 append**합니다. (EventPublisher가 Kafka로 전달)
5.  **프로젝션 로직 추가**: `projection_worker`에서 해당 이벤트를 받아 Elasticsearch 문서를 업데이트합니다.

---

## 5. 테스트 가이드라인

-   **단위 테스트**: `shared` 디렉토리의 유틸리티, 서비스, 모델 등은 반드시 단위 테스트를 작성해야 합니다.
-   **통합 테스트**: `Instance`나 `Ontology`의 전체 생성/수정/삭제 흐름(API 호출 -> Worker 처리 -> 프로젝션)을 검증하는 통합 테스트는 `backend/tests/`에 작성합니다.
-   **실행 방법**:
    ```bash
    # 전체 테스트 실행
    pytest backend/tests

    # 특정 테스트 파일 실행
    pytest backend/tests/test_core_functionality.py -q

    # 커버리지 리포트 생성
    pytest backend/tests --cov=backend --cov-report=html
    ```

---

## 6. 디버깅 팁

-   **로그 레벨 조정**: `.env` 파일에서 `LOG_LEVEL=DEBUG`로 설정하면 모든 서비스에서 상세한 로그를 확인할 수 있습니다.
-   **Kafka 메시지 확인**: `kafka-console-consumer.sh` (Kafka 제공) 또는 `kcat`과 같은 도구를 사용하여 특정 토픽에 어떤 메시지가 흐르는지 실시간으로 확인할 수 있습니다. 이는 워커가 이벤트를 제대로 받는지 확인할 때 매우 유용합니다.
    ```bash
    # instance_events 토픽 메시지 확인 예시
    kcat -b localhost:29092 -t instance_events -C -o beginning
    ```
-   **S3 파일 확인**: MinIO를 사용 중이라면, 웹 콘솔(`http://localhost:9001`)에 접속하여 `spice-event-store` 버킷의 `events/`(이벤트), `indexes/`(인덱스), `checkpoints/`(publisher 체크포인트) 경로가 정상적으로 생성/증가하는지 확인합니다.
-   **Redis 상태 확인**: `redis-cli`를 사용하여 `command:{command_id}:status` 키를 조회하면 특정 커맨드의 현재 처리 상태를 직접 확인할 수 있습니다.
