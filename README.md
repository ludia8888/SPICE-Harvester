# SPICE HARVESTER

SPICE HARVESTER는 엔터프라이즈급 온톨로지 관리 및 데이터 거버넌스 플랫폼입니다. 다국어 지원, 복합 데이터 타입, 고급 관계 관리와 더불어, 데이터의 모든 변경 이력을 완벽하게 추적하고 재현하는 강력한 기능을 제공합니다.

## 목차

- [개요](#개요)
- [아키텍처](#아키텍처)
- [주요 기능](#주요-기능)
- [기술 스택](#기술-스택)
- [시작하기](#시작하기)
- [문서](#문서)

---

## 개요

SPICE HARVESTER는 복잡한 데이터 스키마, 관계, 다국어 콘텐츠를 관리하기 위한 완벽한 솔루션을 제공하며, 보안, 확장성, 개발자 경험에 중점을 두고 설계되었습니다.

### 핵심 역량

-   **하이브리드 데이터 아키텍처**: 데이터의 특성에 따라 최적의 관리 모델을 적용합니다.
    -   **이벤트 소싱 (Event Sourcing)**: 모든 변경 이력의 추적이 중요한 **인스턴스(Instance) 데이터**에 적용되어, 완벽한 감사 추적과 시간 여행(Time Travel) 기능을 제공합니다.
    -   **상태 저장 (State-Store)**: 스키마와 같이 최신 상태가 중요한 **온톨로지(Ontology) 데이터**에 적용되어, 빠르고 일관된 조회를 보장합니다.
-   **CQRS (Command Query Responsibility Segregation)**: 쓰기(Command)와 읽기(Query)의 책임을 분리하여 시스템 성능과 확장성을 극대화합니다.
-   **Git과 유사한 버전 관리**: 브랜치, 커밋, 비교(Diff), 병합(Merge) 등 강력한 버전 관리 기능을 온톨로지 스키마에 제공합니다.
-   **AI 기반 타입 추론**: 데이터 소스를 분석하여 자동으로 온톨로지 스키마 생성을 제안합니다.

## 아키텍처

본 시스템은 CQRS와 이벤트 소싱 원칙을 기반으로 한 마이크로서비스 아키텍처를 따릅니다. 쓰기 경로와 읽기 경로가 명확하게 분리되어 있으며, 모든 데이터 변경은 Kafka를 통해 비동기 이벤트로 처리됩니다.

### 아키텍처 다이어그램

```mermaid
graph TD
    subgraph "사용자 / 클라이언트"
        A[Web UI / API Clients]
    end

    subgraph "API 계층"
        B(BFF - Backend for Frontend)
    end

    subgraph "명령 처리 (Write Path)"
        C(OMS - Command Gateway)
        D[PostgreSQL - Outbox]
        E[Message Relay]
        F[Kafka - Event Bus]
        G[Instance Worker]
        H[Ontology Worker]
        I[S3/MinIO - Event Store for Instances]
        J[TerminusDB - State Store for Ontologies]
    end

    subgraph "조회 처리 (Read Path)"
        K[Projection Worker]
        L[Elasticsearch - Search Read Model]
        M[TerminusDB - Direct Read Model]
    end

    %% 흐름 정의
    A -->|API Request| B
    B -->|Command| C
    C -->|Save to Outbox| D
    D -->|Poll & Publish| E
    E -->|Push to Kafka| F

    F -- Instance Commands --> G
    F -- Ontology Commands --> H

    G -->|Save to S3 (SSoT)| I
    G -->|Update Cache| J
    G -->|Publish Event| F

    H -->|Update TerminusDB (SSoT)| J
    H -->|Publish Event| F

    F -- Domain Events --> K
    K -->|Project to| L

    B -->|Search Query| L
    B -->|Direct Get Query| M
```

더 자세한 내용은 [전체 아키텍처 문서](./docs/ARCHITECTURE.md)를 참고하세요.

## 주요 기능

### 데이터 아키텍처
-   ✅ **CQRS**: 읽기/쓰기 경로를 완벽하게 분리하여 확장성 확보.
-   ✅ **이벤트 소싱**: `Instance` 데이터의 모든 변경 이력을 S3에 불변 로그로 저장.
-   ✅ **상태 재현 (Replay)**: S3의 로그를 기반으로 특정 시점의 `Instance` 상태를 완벽하게 재구성.
-   ✅ **데이터 계보 (Lineage)**: `Instance` 상태가 어떤 커맨드들로 만들어졌는지 완벽히 추적 가능.
-   ✅ **아웃박스 패턴**: PostgreSQL을 활용하여 데이터베이스 변경과 메시지 발행의 원자성을 보장.
-   ✅ **프로젝션**: 도메인 이벤트를 기반으로 검색에 최적화된 읽기 모델을 Elasticsearch에 구축.

### Git 버전 관리 (온톨로지)
-   ✅ **브랜치 관리**: 생성, 목록 조회, 삭제, 체크아웃 기능.
-   ✅ **커밋 시스템**: 전체 변경 이력, 메시지, 작성자, 타임스탬프 기록.
-   ✅ **비교 및 병합**: 두 브랜치 또는 커밋 간의 차이점 비교 및 병합/리베이스 수행.
-   ✅ **롤백**: 특정 커밋 시점으로 안전하게 되돌리기.

### AI 타입 추론
-   ✅ **고급 타입 탐지**: 문자열, 숫자 등 기본 타입은 물론 이메일, 전화번호, 날짜 등 복합 타입까지 정확하게 추론.
-   ✅ **다국어 지원**: "이메일", "Email" 등 다양한 언어의 컬럼명을 이해하고 분석.
-   ✅ **스키마 자동 생성**: 분석 결과를 바탕으로 OMS와 호환되는 온톨로지 스키마를 자동으로 제안.

## 기술 스택

-   **프로그래밍 언어**: Python 3.9+
-   **웹 프레임워크**: FastAPI
-   **데이터베이스 및 저장소**:
    -   **TerminusDB**: 그래프 DB, 온톨로지 스키마의 SSoT.
    -   **S3 (MinIO)**: 오브젝트 스토리지, 인스턴스 이벤트 로그의 SSoT.
    -   **PostgreSQL**: 아웃박스 패턴 구현을 위한 RDBMS.
    -   **Elasticsearch**: 검색 및 분석을 위한 읽기 모델.
    -   **Redis**: 커맨드 상태 추적 및 캐시.
-   **메시지 브로커**: Apache Kafka
-   **핵심 설계 패턴**: 마이크로서비스, CQRS, 이벤트 소싱, 아웃박스, 프로젝션, 어댑터 패턴.
-   **컨테이너**: Docker, Docker Compose

## 시작하기

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
docker-compose -f docker-compose.full.yml up -d

# 5. 서비스 상태 확인
curl http://localhost:8002/health # BFF
```

## 문서

-   **[전체 아키텍처](./docs/ARCHITECTURE.md)**: 시스템 설계 및 컴포넌트 상세 설명.
-   **[개발자 가이드](./docs/DEVELOPER_GUIDE.md)**: 개발 환경 설정 및 워크플로우 가이드.
-   **[API 레퍼런스](./docs/API_REFERENCE.md)**: 전체 API 명세.
-   **[이벤트 소싱 설계 원칙](./backend/docs/Command/Event%20Sourcing%20+%20CQRS%20아키텍처%20설계%20원칙.md)**: CQRS 및 이벤트 소싱 패턴 심층 가이드.