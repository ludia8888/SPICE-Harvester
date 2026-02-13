0. 중복구현 없는지 철저 확인(핵심!)
1. SOLID 원칙 철저 준수.
2. 클린코드, 클린 아키텍처 준수.
3. 항상 시니어 아키텍트로써 디자인 패턴 어떤것을 써야할지 고려하시오. 

디자인 패턴  규칙: GoF(객체지향) 디자인 패턴 23종

가장 유명한 분류입니다(“Gang of Four” 책 기반). 크게 3가지로 나뉩니다.

A. 생성(Creational) 패턴: “어떻게 만들 것인가”
	•	Singleton: 인스턴스 1개만 유지
	•	Factory Method: 생성 책임을 하위 클래스/구현체로 위임
	•	Abstract Factory: “관련 있는 객체 묶음”을 통째로 생성
	•	Builder: 복잡한 생성 과정을 단계적으로 분리
	•	Prototype: 복제로 생성(클론)

핵심 효과: 객체 생성 로직을 숨기고, 결합도를 낮춤.

B. 구조(Structural) 패턴: “어떻게 조립/연결할 것인가”
	•	Adapter: 인터페이스 호환(변환기)
	•	Bridge: 추상/구현 분리(확장 방향 2축)
	•	Composite: 트리 구조(부분-전체 동일 취급)
	•	Decorator: 기능을 “겹겹이” 동적으로 추가
	•	Facade: 복잡한 서브시스템에 단순 창구 제공
	•	Flyweight: 공유로 메모리 절약(대량 객체)
	•	Proxy: 대리 객체로 접근 제어/지연 로딩/캐싱

핵심 효과: 구조를 바꿔도 외부 영향 최소화, 재사용/확장 용이.

C. 행위(Behavioral) 패턴: “어떻게 행동/협력할 것인가”
	•	Strategy: 알고리즘 교체(정책 분리)
	•	Observer: 구독/발행(이벤트)
	•	Command: 요청을 객체로 캡슐화(undo/queue)
	•	Chain of Responsibility: 처리자 체인으로 책임 전달
	•	State: 상태에 따라 행동 변경(조건문 제거)
	•	Template Method: 골격은 상위, 변하는 부분은 하위
	•	Iterator: 순회 로직 분리
	•	Mediator: 객체 간 복잡한 상호작용을 중재자에 집중
	•	Memento: 상태 스냅샷 저장/복원
	•	Visitor: 데이터 구조는 고정, 연산을 확장
	•	Interpreter: DSL 문법 해석
	•	(GoF 목록에 포함: 위 항목들이 대표)

핵심 효과: 조건문/분기 폭발을 줄이고, 변경 지점을 고립.

⸻

2) 아키텍처(Architectural) 패턴: “시스템 전체 구조”

GoF가 “클래스/객체 설계” 중심이라면, 여긴 서비스/레이어/컴포넌트 구성입니다.
	•	Layered Architecture(계층형): Presentation / Domain / Data 등
	•	MVC / MVP / MVVM: UI 책임 분리
	•	Hexagonal(Ports & Adapters): 도메인 중심, 외부 의존성 격리
	•	Clean Architecture: 의존성 규칙(안쪽으로만)
	•	Microservices: 서비스 분해 + 독립 배포
	•	Event-Driven Architecture: 이벤트 중심 비동기 결합

포인트: 팀 규모·배포 방식·변경 빈도·테스트 전략과 직결됩니다.

⸻

3) 엔터프라이즈/데이터·분산 시스템 패턴(실무에서 매우 자주 등장)

특히 백엔드/플랫폼에서 “디자인 패턴”이라고 부르며 쓰는 경우가 많습니다.
	•	CQRS: 조회/명령 모델 분리
	•	Event Sourcing: 상태 대신 이벤트를 저장(감사/재현 강점, 복잡도 증가)
	•	Saga: 분산 트랜잭션 대안(보상 트랜잭션)
	•	Outbox: DB 트랜잭션과 메시지 발행 일관성
	•	Circuit Breaker: 장애 전파 차단
	•	Bulkhead: 격벽(리소스 분리)로 장애 격리
	•	Retry + Backoff: 일시 장애 대응(무한 재시도는 독)
	•	Idempotency Key: 중복 요청 안전 처리
	•	Cache-Aside / Write-Through / Write-Back: 캐시 전략

왜 별도 분류가 필요하냐?
객체지향 패턴만 알아도 분산 환경의 “일관성/재시도/부분 실패” 문제는 해결이 안 됩니다. 그래서 실무는 이 패턴들을 더 자주 씁니다.

⸻

4) 동시성(Concurrency) 패턴
	•	Producer–Consumer(Queue): 생산/소비 분리
	•	Thread Pool: 스레드 재사용으로 비용 절감
	•	Reactor / Proactor: 이벤트 루프 기반 I/O 처리(서버에서 흔함)
	•	Actor Model: 메시지로 상태 격리(경합 줄임)

⸻

5) 도메인 설계(DDD) 패턴
	•	Aggregate / Entity / Value Object
	•	Repository
	•	Domain Service
	•	Factory(도메인 팩토리)
	•	Anti-Corruption Layer: 외부 시스템 오염 방지
<!-- DOC_SYNC: 2026-02-13 Foundry pipeline parity + runtime consistency sweep -->
