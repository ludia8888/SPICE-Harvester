객체 타입과 Backing 데이터셋 연결 (KeySpec: 기본 키 및 타이틀 키) 제품 요구사항(Requirements) + 체크리스트(Checklist)
⸻

요구사항: 객체 타입 ↔ 백킹 데이터셋 연결 계층

용어 정의
	•	ObjectType: 도메인 엔티티/이벤트를 표현하는 타입(스키마)
	•	BackingDataSource: ObjectType에 실제 값을 공급하는 데이터셋(또는 제한 뷰)
	•	BackingDataSourceVersion: 커밋/스냅샷으로 고정된 BackingDataSource의 특정 버전
	•	PropertyMapping: 데이터셋 컬럼 → ObjectType 속성 매핑 정의
	•	KeySpec: PrimaryKey / TitleKey / 제약(Nullability/Uniqueness 등)
	•	Indexing Pipeline: BackingDataSourceVersion → Object 인스턴스 재구성/갱신을 수행하는 백엔드 작업

⸻

RQ-OD-001 BackingDataSource 연결 선언

설명: ObjectType은 반드시 하나 이상의 BackingDataSource를 명시적으로 연결할 수 있어야 한다.
	•	ObjectType 생성 시, BackingDataSource를 지정할 수 있어야 한다.
	•	ObjectType에는 “어떤 데이터가 이 ObjectType의 truth인가”가 명확히 기록되어야 한다.
	•	(권장) 기본 모드에서는 ObjectType ↔ BackingDataSource를 1:1로 제한하고, 멀티 소스는 별도 모드로 취급한다.

Acceptance Criteria
	•	ObjectType 조회 시, 연결된 BackingDataSource와 현재 활성 BackingDataSourceVersion을 확인 가능
	•	연결 변경(교체)은 별도 “마이그레이션 절차/검증”을 통과해야만 가능

⸻

RQ-OD-002 PropertyMapping 자동 생성 및 수동 매핑 지원

설명: BackingDataSource를 연결하면, 컬럼 스키마 기반으로 PropertyMapping을 자동 생성하거나 수동으로 재매핑할 수 있어야 한다.
	•	자동 생성: 컬럼명/타입 기반으로 속성 후보 생성
	•	수동 매핑: 특정 컬럼을 특정 속성에 연결/변경 가능
	•	매핑 결과는 BackingDataSourceVersion에 대해 결정적(deterministic)이어야 함

Acceptance Criteria
	•	동일한 데이터셋 스키마에 대해 자동 매핑 결과가 재현 가능
	•	수동 매핑 변경 시 변경 이력과 영향 범위가 기록됨

⸻

RQ-OD-003 스키마 호환성 검증(하드게이트)

설명: PropertyMapping은 스키마 검증을 통과하지 못하면 저장/적용 불가해야 한다.

3.1 타입 호환 검증
	•	컬럼 타입과 ObjectType 속성 타입이 호환되지 않으면 매핑 불가(하드 실패)
	•	허용되는 캐스팅 규칙이 있다면 명시적(화이트리스트)이어야 함

3.2 금지 타입/구조 제한
	•	백킹 컬럼으로 사용할 수 없는 타입(예: 복잡한 중첩 타입/맵/중첩 구조 등)은 명시적으로 차단
	•	“지원 안 함”이면 무조건 실패(조용한 string 변환 금지)

Acceptance Criteria
	•	매핑 저장 시점 또는 배포 시점에 검증 실패하면 4xx(또는 명시적 실패 상태)로 차단
	•	실패 원인(컬럼명/타입/요구 타입/정책)을 에러 메시지로 제공

⸻

RQ-OD-004 KeySpec 강제: PrimaryKey + TitleKey 필수

설명: 각 ObjectType은 최소한 아래 KeySpec을 가져야 한다.
	•	PrimaryKey: 인스턴스 유일 식별자
	•	TitleKey: 표시용 대표 이름(유일성은 요구하지 않음)

Acceptance Criteria
	•	KeySpec 없이 ObjectType을 “활성/서빙 상태”로 만들 수 없음
	•	TitleKey는 null 허용 여부를 정책으로 결정할 수 있으나, UI 표시 목적상 비권장 경고 제공

⸻

RQ-OD-005 PrimaryKey 유일성/결정성 검증(하드게이트)

설명: PrimaryKey는 BackingDataSourceVersion 단위로 반드시 유일해야 하며, 중복 시 인덱싱/빌드/배포는 실패해야 한다.
	•	PK 중복 검출 시:
	•	인덱싱 파이프라인은 즉시 실패 처리
	•	어떤 PK 값이 몇 번 중복되었는지 샘플/통계를 제공(운영 디버깅용)
	•	PK 결측(null/empty)도 정책상 허용하지 않는다면 동일하게 실패 처리

Acceptance Criteria
	•	PK 중복이 있는 버전은 “인스턴스 생성/갱신 0건”이 보장됨(부분 반영 금지)
	•	실패 후 재시도 시 동일 조건에서 동일 결과(결정성)

⸻

RQ-OD-006 Indexing Pipeline 자동 연결 및 버전 기반 재구성

설명: BackingDataSource와 ObjectType 연결이 확정되면, 백엔드는 다음을 자동 수행해야 한다.
	•	(1) BackingDataSourceVersion 커밋 발생 시
	•	(2) 해당 버전을 입력으로
	•	(3) Object 인스턴스 재구성/갱신을 수행

즉, 데이터 버전이 올라가면 Object 인스턴스가 따라 움직이는 “버전 동기화”가 필요하다.

Acceptance Criteria
	•	어떤 인스턴스가 “어떤 BackingDataSourceVersion에서 만들어졌는지” 추적 가능
	•	인덱싱 결과가 실패/성공/부분 등 상태로 관리됨(부분 성공은 정책적으로 금지 권장)

⸻

RQ-OD-007 스키마 변경 감지 및 마이그레이션 게이트

설명: BackingDataSource 스키마가 바뀌면, ObjectType과의 계약 위반 여부를 자동 감지해야 한다.

7.1 계약 위반(하드 실패) 예시
	•	PrimaryKey 컬럼 삭제
	•	PrimaryKey 컬럼 타입 변경
	•	매핑된 필수 컬럼 삭제/타입 변경(Required 속성)

7.2 마이그레이션 프레임워크
	•	계약 위반이 발생하면:
	•	자동 반영 금지
	•	“스키마 마이그레이션” 절차를 통해서만 반영 가능
	•	일부 변경은 자동 지원 가능(예: 비필수 컬럼 추가)
	•	PrimaryKey 변경은 자동 마이그레이션을 금지하고, 별도 수동 절차를 강제

Acceptance Criteria
	•	계약 위반 버전은 인덱싱 파이프라인이 차단됨
	•	마이그레이션 승인/적용 전까지 기존 안정 버전 유지

⸻

RQ-OD-008 사용자 편집(Writeback/Edits) 이력의 마이그레이션/정리 정책

설명: ObjectType에 사용자 편집 데이터(수정 레이어)가 존재할 수 있으며, 스키마 변경 시 이를 어떻게 처리할지 정책이 필요하다.

8.1 속성 삭제/이름 변경 처리
	•	삭제되는 속성에 대한 편집 기록은:
	•	(A) 다른 속성으로 이동(move) 가능하면 이동
	•	(B) 이동 불가하면 삭제(drop) 또는 무효화(invalidate)
	•	해당 정책은 마이그레이션 계획에 명시되어야 함

8.2 PrimaryKey 변경 처리(가장 위험)
	•	PK 변경 시 편집 이력을 안전하게 보존하기 어려우므로:
	•	기본 정책: **모든 편집 기록 삭제(Reset/Drop all edits)**를 요구
	•	예외 허용 시, 별도 “ID 재매핑 계획”을 강제

Acceptance Criteria
	•	마이그레이션 수행 전: 영향 받는 편집 레코드 수/범위를 사전에 산출
	•	PK 변경이 포함된 마이그레이션은 기본적으로 “편집 이력 초기화” 체크를 통과해야만 완료

⸻

체크리스트

A. 연결/정의(Defined)
	•	ObjectType에 BackingDataSource를 명시적으로 연결할 수 있다.
	•	연결 정보에 BackingDataSourceVersion(커밋/스냅샷)이 포함된다.
	•	PropertyMapping을 자동 생성할 수 있다.
	•	PropertyMapping을 수동으로 수정/재매핑할 수 있다.
	•	KeySpec(PrimaryKey, TitleKey)를 저장할 수 있다.

B. 검증(Validated)
	•	컬럼 타입 ↔ 속성 타입 호환 검증이 존재하며 실패 시 차단된다.
	•	중첩/맵 등 금지 타입 컬럼은 백킹 컬럼으로 매핑할 수 없다.
	•	PrimaryKey가 누락된 ObjectType은 활성화/서빙 불가하다.
	•	PrimaryKey 유일성 검증이 있다(중복/결측 검출).

C. 강제(Enforced)
	•	PK 중복/결측이 있는 BackingDataSourceVersion은 인덱싱/빌드가 실패한다.
	•	실패 시 “부분 인스턴스 생성”이 발생하지 않는다(0건 보장).
	•	데이터셋 커밋(새 버전) 시 인덱싱 파이프라인이 자동 트리거된다.
	•	인스턴스는 어떤 BackingDataSourceVersion에서 생성됐는지 추적 가능하다.

D. 진화/마이그레이션(Governed Evolution)
	•	BackingDataSource 스키마 변경을 감지한다.
	•	계약 위반(특히 PK 삭제/타입 변경)은 자동 반영되지 않고 차단된다.
	•	스키마 마이그레이션 절차(승인/적용)가 존재한다.
	•	PrimaryKey 변경은 자동 마이그레이션을 허용하지 않는다(수동 절차 강제).
	•	사용자 편집(Writeback/Edits) 이력에 대해 move/drop/invalid 정책이 있다.
	•	PK 변경 시 기본 정책은 “편집 이력 초기화”이며 예외는 특별 승인 필요.

요구사항: 관계 매핑 계층 (RelationshipSpec / Link Types)

용어 정의
	•	ObjectType: 도메인 엔티티/이벤트 타입
	•	Instance: ObjectType의 개별 객체
	•	LinkType: 두 ObjectType 간 관계 스키마(1:1, 1:N, N:M)
	•	LinkInstance: 두 Instance 사이의 관계 1건
	•	RelationshipSpec: LinkType을 생성/유지하기 위한 “연결 규칙” 정의(FK 매핑, 조인테이블 매핑 등)
	•	ForeignKeySpec: FK 컬럼(속성) ↔ 대상 ObjectType의 PrimaryKey 매핑
	•	JoinTableSpec: N:M 관계를 위한 조인 테이블(데이터셋) 기반 매핑
	•	LinkIndexing Pipeline: RelationshipSpec을 기반으로 LinkInstance를 생성/갱신하는 백엔드 작업

⸻

RQ-RS-001 LinkType 1급 리소스

설명: 시스템은 관계를 1급으로 표현하기 위해 LinkType을 별도의 리소스로 제공해야 한다.
	•	LinkType은 Source ObjectType, Target ObjectType, Cardinality(1:1/1:N/N:M)를 포함해야 한다.
	•	LinkType은 RelationshipSpec(연결 규칙)을 반드시 포함해야 한다.
	•	LinkType 정의는 “도메인 관계”이며, 실제 생성 로직은 RelationshipSpec이 담당한다(책임 분리).

Acceptance Criteria
	•	LinkType을 생성/조회/수정/비활성화할 수 있다.
	•	LinkType 조회 시 RelationshipSpec과 인덱싱 상태(성공/실패/지연)를 확인할 수 있다.

⸻

RQ-RS-002 ForeignKey 기반 관계 정의 (FK → PK 매핑)

설명: 1:1 또는 1:N 관계는 FK 기반으로 정의할 수 있어야 한다.
	•	사용자는 다음을 지정해야 한다:
	•	source ObjectType
	•	target ObjectType
	•	source의 FK 속성(또는 컬럼)
	•	target의 PrimaryKey 속성(또는 컬럼)
	•	FK 속성과 target PK 속성의 타입 호환성이 충족되어야 한다.
	•	FK 값이 target PK 값을 “참조”한다는 의미는, 값 기반 매칭으로 LinkInstance가 생성됨을 뜻한다.

Acceptance Criteria
	•	FK → PK 타입이 비호환이면 RelationshipSpec 생성이 실패(하드게이트).
	•	인덱싱 결과, FK == PK인 경우에만 LinkInstance가 생성된다.
	•	FK 컬럼명이 PK와 동일하거나 유사할 때 자동 제안은 가능하되, 최종 결정은 명시적 저장으로 확정된다(자동 적용 금지 권장).

⸻

RQ-RS-003 JoinTable 기반 N:M 관계 정의 (조인테이블 매핑)

설명: N:M 관계는 조인테이블(연결 테이블) 데이터셋을 backing하여 정의할 수 있어야 한다.
	•	사용자는 다음을 지정해야 한다:
	•	source ObjectType
	•	target ObjectType
	•	join table dataset
	•	join table 내 source PK 컬럼
	•	join table 내 target PK 컬럼
	•	join table의 각 row는 (source_pk, target_pk) 한 쌍의 관계를 의미한다.
	•	시스템은 join table 기반으로 LinkInstance를 생성/갱신한다.

Acceptance Criteria
	•	join table에 source/target PK 컬럼이 없으면 실패(하드게이트).
	•	join table의 두 컬럼 타입이 각 ObjectType의 PK 타입과 일치하지 않으면 실패.
	•	join table에 중복 row가 있을 경우:
	•	기본 정책: 중복은 경고 또는 실패 중 선택 가능(권장: dedupe 후 처리)
	•	단, 결과 LinkInstance는 중복 생성되지 않아야 한다.

⸻

RQ-RS-004 JoinTable 자동 생성 옵션

설명: 사용자가 join table을 사전에 준비하지 않은 경우, 시스템이 “빈 조인테이블”을 자동 생성할 수 있어야 한다(옵션).
	•	자동 생성 시:
	•	스키마는 (source_pk, target_pk) 컬럼을 포함해야 한다.
	•	타입은 각 ObjectType의 PK 타입과 동일해야 한다.
	•	생성된 join table은 이후 파이프라인/사용자 편집/외부 업로드로 채울 수 있어야 한다.

Acceptance Criteria
	•	자동 생성된 join table이 즉시 RelationshipSpec에 연결 가능
	•	join table이 채워질 때마다 LinkIndexing Pipeline이 반영(버전/스냅샷 기반이면 그 규칙에 따름)

⸻

RQ-RS-005 Object-backed Link 지원 (선택적 고급 기능)

설명: 관계 자체에 추가 메타데이터가 필요하면, 관계를 별도 ObjectType(예: Assignment)으로 모델링할 수 있어야 한다.
	•	관계 ObjectType은 최소한:
	•	source PK 참조 속성
	•	target PK 참조 속성
을 포함해야 한다.
	•	관계 ObjectType에는 start_date, role 등 추가 속성 확장이 가능해야 한다.
	•	이 경우 LinkType은 “단순 링크”가 아니라 “관계 객체를 통한 관계”로 표현될 수 있다.

Acceptance Criteria
	•	관계 객체(Assignment) 인스턴스를 생성하면 해당 source/target 사이 관계가 유도(derived)되거나, 쿼리에서 일관되게 탐색 가능해야 한다.
	•	관계 객체가 삭제되면 관계도 제거되는 일관성이 보장되어야 한다.

⸻

RQ-RS-006 RelationshipSpec 검증(하드게이트) 정책

설명: 잘못된 관계 정의는 조용히 넘어가면 데이터 의미가 망가진다. 따라서 RelationshipSpec은 강한 검증을 통과해야만 활성화된다.

필수 검증 항목:
	•	FK 기반:
	•	FK 필드 존재
	•	target PK 존재
	•	타입 호환
	•	JoinTable 기반:
	•	join table 존재
	•	source/target PK 컬럼 존재
	•	타입 일치
	•	공통:
	•	source/target ObjectType 존재
	•	Cardinality 설정 유효성

Acceptance Criteria
	•	검증 실패 시 LinkIndexing Pipeline을 실행하지 않는다(0건 보장).
	•	실패 사유를 구조화된 에러로 반환(어떤 컬럼/어떤 타입/어떤 규칙 위반인지).

⸻

RQ-RS-007 LinkIndexing Pipeline (링크 인스턴스 생성/갱신)

설명: RelationshipSpec이 확정되면, 백엔드는 데이터를 기반으로 LinkInstance를 생성/갱신해야 한다.
	•	FK 기반:
	•	source 인스턴스의 FK 값과 target 인스턴스의 PK 값을 매칭하여 링크 생성
	•	JoinTable 기반:
	•	각 row의 (source_pk, target_pk)로 링크 생성
	•	갱신 규칙은 명시되어야 한다:
	•	source/target 데이터가 업데이트되거나 join table이 업데이트될 때 링크가 재계산되는지
	•	삭제/부활(재등장) 정책은 무엇인지

Acceptance Criteria
	•	LinkInstance는 “어떤 데이터 버전/조인테이블 버전”에서 생성되었는지 추적 가능(라인리지).
	•	링크 생성은 멱등이어야 한다(같은 입력이면 같은 결과).

⸻

RQ-RS-008 Dangling Reference(참조 무결성) 정책

설명: FK 또는 join table이 가리키는 대상 PK가 실제로 존재하지 않을 수 있다. 정책이 필요하다.

권장 정책(기본):
	•	대상이 존재하지 않는 관계는 링크를 만들지 않음 + 오류/통계로 보고
	•	“허용 목록” 또는 “동일 배치에서 함께 생성되는 대상” 같은 예외 정책이 있다면 명시적으로만 허용

Acceptance Criteria
	•	dangling 비율/개수를 인덱싱 결과에 리포트로 제공
	•	정책에 따라 실패 처리(엄격 모드) 또는 경고 처리(유연 모드) 선택 가능

⸻

RQ-RS-009 사용자 편집(링크 추가/삭제) 지원을 위한 저장소 요구

설명: 링크를 사용자 편집으로 추가/삭제하려면, 변경을 기록할 저장소가 필요하다.
	•	JoinTable 기반 링크는 “관계 자체가 데이터 자산”이므로 편집/추적에 유리하다.
	•	FK 기반 링크에서 사용자 편집을 허용하려면:
	•	FK 값을 수정하는 방식(= source object property edit)으로만 허용하거나
	•	별도의 “override/edits layer”를 설계해야 한다.

Acceptance Criteria
	•	링크 편집이 가능하다고 선언된 LinkType은 반드시 편집 변경이 기록되는 저장소가 존재해야 한다.
	•	편집은 원본을 덮지 않고 writeback/edits layer에 기록(권장)되며, 링크 재계산 시 반영 규칙이 명시되어야 한다.

⸻

체크리스트

A. 연결/정의(Defined)
	•	LinkType을 1급 리소스로 생성/조회/수정 가능하다.
	•	LinkType에 Cardinality(1:1/1:N/N:M)가 정의된다.
	•	LinkType에 RelationshipSpec가 포함된다.
	•	FK 기반 RelationshipSpec를 정의할 수 있다.
	•	JoinTable 기반 RelationshipSpec를 정의할 수 있다.
	•	Object-backed link(관계 객체 모델)을 지원한다.

B. 검증(Validated)
	•	FK 기반: FK 필드 존재/타입 호환/target PK 존재를 검증한다.
	•	JoinTable 기반: join table 존재/PK 컬럼 존재/타입 일치를 검증한다.
	•	검증 실패 시 RelationshipSpec 활성화가 차단된다.
	•	dangling reference 정책(실패/경고)이 명시되어 있다.

C. 강제(Enforced)
	•	RelationshipSpec 검증 실패 시 링크 인덱싱 파이프라인은 실행되지 않는다(0건 보장).
	•	링크 인덱싱은 멱등이며 재시도 안전하다.
	•	링크 인덱싱 결과에 생성/실패/누락 통계가 남는다.
	•	LinkInstance가 어떤 데이터 버전에서 생성되었는지 추적 가능하다.

D. 운영/편집(Governed Operations)
	•	N:M은 기본적으로 join table 기반 링크를 지원한다.
	•	join table 자동 생성 옵션이 있다(없어도 됨, 있으면 MVP 가속).
	•	링크 편집(추가/삭제)을 지원하는 경우 저장소(override/edits layer)가 존재한다.
	•	FK 기반 링크 편집은 FK 속성 수정 방식 또는 별도 override 정책 중 하나로 명확히 정의되어 있다.

요구사항: 도메인 모델과 데이터 운영 계약의 분리

용어 정의
	•	DomainModel: 비즈니스 개체/관계/행동을 표현하는 추상 계층(= 온톨로지 계층)
	•	DataContractLayer: DomainModel에 실제 값을 공급하는 데이터 운영 계약 계층(= 백킹 데이터소스 계층)
	•	DataAsset: 데이터셋/뷰/제한 뷰 등 실데이터 저장/서빙 단위
	•	DataAssetVersion: 커밋/스냅샷 기반으로 고정된 데이터 버전
	•	Indexing Pipeline: DataAssetVersion → DomainModel Instance(객체/관계)로 투영하는 백엔드 작업
	•	Proposal/Branch: 안전한 변경을 위해 분리된 변경 공간(도메인 모델 또는 데이터 자산에 적용 가능)
	•	AccessPolicy: 접근 제어(행 필터/컬럼 마스킹/분류 태그/권한)

⸻

RQ-SEP-001 계층 분리 원칙(Separation of Concerns)

설명: DomainModel(도메인 타입/관계)과 DataContractLayer(데이터 준비/운영 계약)는 서로 다른 1급 계층으로 분리되어야 한다.
	•	DomainModel은 “개체/관계/행동의 의미”만 담당한다.
	•	DataContractLayer는 “어떤 데이터가 truth인지, 어떤 버전이 활성인지, 어떤 품질/보안 계약이 적용되는지”를 담당한다.
	•	두 계층은 “매핑/계약”으로만 연결되며, 데이터 물리 저장이 DomainModel 내부로 들어오면 안 된다.

Acceptance Criteria
	•	DomainModel 리소스만으로는 실데이터가 변경/교체되지 않는다.
	•	DataContractLayer 변경만으로 DomainModel 정의(타입/관계)가 바뀌지 않는다.
	•	두 계층의 변경 이력/승인 흐름이 독립적으로 존재한다.

⸻

RQ-SEP-002 독립적 변경 관리(버전/브랜치/프로포절)

설명: DomainModel과 DataContractLayer는 서로 다른 수명주기를 가지므로 변경 관리도 분리되어야 한다.

2.1 DomainModel 변경 흐름
	•	타입/속성/관계 변경은 “안전한 변경 공간(브랜치/프로포절)”에서 수행되어야 한다.
	•	검토/승인 후에만 메인/운영 버전에 반영된다.

2.2 DataContractLayer 변경 흐름
	•	데이터 자산은 파이프라인/ETL로 지속적으로 업데이트되며, 커밋/스냅샷 단위로 버전이 생성된다.
	•	최신 버전은 Indexing Pipeline을 통해 자동 반영되되, 계약 위반 시 자동 반영이 차단되어야 한다.

Acceptance Criteria
	•	DomainModel 변경은 승인 없이는 운영 반영 불가
	•	DataAssetVersion 변경은 자동 반영 가능하나 “계약 위반”이면 차단
	•	두 계층의 변경은 서로를 강제 동기화하지 않음(느슨한 결합)

⸻

RQ-SEP-003 Indexing Pipeline 기반 “자동 반영” 계약

설명: DataAssetVersion이 생성되면, DomainModel 인스턴스(객체/관계)는 Indexing Pipeline을 통해 재구성/갱신되어야 한다.
	•	“데이터가 바뀌면 객체가 자동으로 최신화된다”는 운영 계약을 제공해야 한다.
	•	단, DomainModel 스키마가 깨지는 변경(예: PK 컬럼 삭제/타입 변경)은 자동 반영이 아니라 마이그레이션 게이트로 넘어가야 한다.

Acceptance Criteria
	•	DataAssetVersion → 인덱싱 결과(성공/실패/지연) 상태 추적 가능
	•	어떤 객체/관계가 어떤 DataAssetVersion에서 만들어졌는지 라인리지 제공
	•	계약 위반 데이터는 인덱싱 실행 자체가 차단되거나 실패 처리되어 “부분 반영”이 발생하지 않음

⸻

RQ-SEP-004 거버넌스/책임 분리(조직 역할 모델)

설명: 시스템은 역할 분리를 전제로 해야 하며, 최소한 아래 역할들이 충돌 없이 운영 가능해야 한다.
	•	Data Engineering 역할: 소스 연결/정제/데이터 자산 생성/버전 관리
	•	Domain Modeling 역할: 객체 타입/관계 정의 및 매핑 선언
	•	Security/Governance 역할: 제한 뷰/분류/정책/마스킹/감사 요건 관리

Acceptance Criteria
	•	각 역할이 소유한 리소스의 변경 권한이 분리되어 있다.
	•	도메인 모델러가 데이터 파이프라인을 직접 바꾸지 않아도 객체 모델링 가능
	•	데이터 엔지니어가 도메인 모델을 직접 바꾸지 않아도 데이터 자산 운영 가능

⸻

RQ-SEP-005 이중 거버넌스: 도메인 권한 vs 원천 데이터 권한 분리

설명: “DomainModel 접근 권한”과 “원천 데이터 접근 권한”을 분리해 이중 통제를 제공해야 한다.
	•	사용자가 객체 타입을 볼 권한이 있어도,
	•	원천 데이터 권한이 없으면 일부 값이 마스킹(null/placeholder)될 수 있어야 한다.
	•	또는 행 자체가 제한되어 객체가 보이지 않을 수 있어야 한다.
	•	이는 DataContractLayer의 AccessPolicy(행 정책/컬럼 마스크/분류 태그 등)에 의해 결정된다.

Acceptance Criteria
	•	객체 조회 시, 데이터 권한 미충족 컬럼은 정책에 따라 마스킹된다.
	•	행 정책 미충족이면 객체 자체가 노출되지 않는다.
	•	도메인 권한만으로 원천 데이터 정책을 우회할 수 없다.

⸻

RQ-SEP-006 호환 계층(마이그레이션/폴백) 제공

설명: DomainModel 스키마가 진화하거나, DataAsset 스키마가 진화할 때 충돌이 발생한다. 이를 안전하게 처리하는 호환 계층이 필요하다.
	•	계약 위반이 감지되면 자동 반영 금지
	•	마이그레이션 플랜을 통해서만 반영 가능
	•	일부 변경은 자동 허용(예: 비필수 컬럼 추가)
	•	핵심 변경(예: PK 변경)은 강한 수동 절차 요구(편집/정정 레이어 초기화 포함 가능)

Acceptance Criteria
	•	스키마 변경 감지 및 영향 범위(깨지는 매핑/관계/편집 데이터) 사전 산출
	•	승인된 마이그레이션만 운영 반영
	•	실패 시 즉시 이전 안정 상태로 유지(롤포워드/롤백 정책 명시)

⸻

RQ-SEP-007 확장성과 재사용성: 데이터 교체(Swap)와 파생 재사용

설명: DataContractLayer가 분리되어 있으므로 아래가 가능해야 한다.
	•	동일한 원천을 다양한 파생 데이터 자산(뷰/파생 데이터셋)으로 만들어 서로 다른 ObjectType에 사용 가능
	•	DomainModel을 바꾸지 않고 Backing DataSource를 **교체(swap)**할 수 있음(단, 계약/스키마 요건을 만족해야 함)

Acceptance Criteria
	•	Backing DataSource 교체 기능 존재
	•	교체 시 스키마 계약(KeySpec/필수 속성/관계 규칙) 검증을 통과해야만 활성화
	•	교체 후 인덱싱 파이프라인이 재실행되어 객체가 재구성됨

⸻

체크리스트

A. 구조 분리(Separation)
	•	DomainModel과 DataContractLayer가 1급 리소스로 분리되어 있다.
	•	DomainModel은 의미(타입/관계)만 담당하고 물리 데이터 저장을 포함하지 않는다.
	•	DataContractLayer는 데이터 운영 계약(버전/품질/보안)을 담당한다.

B. 변경 관리(Change Management)
	•	DomainModel 변경은 브랜치/프로포절로 격리되고 승인 후 반영된다.
	•	DataAsset은 커밋/스냅샷 기반 버전 관리가 된다.
	•	DataAssetVersion 업데이트 시 인덱싱 파이프라인이 자동 트리거된다.
	•	계약 위반이면 자동 반영이 차단된다.

C. 거버넌스(Governance)
	•	도메인 권한과 원천 데이터 권한이 분리되어 있다.
	•	원천 데이터 권한 미충족 시 컬럼 마스킹/행 제한이 적용된다.
	•	도메인 권한만으로 원천 정책을 우회할 수 없다.
	•	역할(데이터/도메인/보안)의 변경 권한이 분리되어 있다.

D. 호환/진화(Compatibility)
	•	스키마 변경 감지 및 영향 분석이 가능하다.
	•	마이그레이션 플랜 없이는 계약 위반 변경이 운영 반영되지 않는다.
	•	PK 변경 등 위험 변경은 수동 절차/강한 게이트를 가진다.
	•	실패 시 이전 안정 상태를 유지하는 정책이 있다.

E. 재사용/교체(Reusability & Swap)
	•	파생 데이터 자산(뷰/파생 데이터셋)로 재사용 가능하다.
	•	Backing DataSource 교체(swap)가 가능하다.
	•	교체 시 KeySpec/필수 속성/관계 규칙 검증을 통과해야 한다.
	•	교체 후 객체 재구성(인덱싱)이 자동 수행된다. 

⸻

구현 상태/증거 매트릭스 (Defined / Validated / Enforced / E2E)

※ E2E는 실제 테스트 실행으로 재현된 항목만 “Proven”으로 표시.

RQ-OD-001 BackingDataSource 연결 선언
	•	Defined: `backend/shared/services/dataset_registry.py` (backing_datasources/backing_datasource_versions), `backend/bff/routers/governance.py`
	•	Validated: `backend/oms/services/ontology_resource_validator.py` (object_type backing_source 필수), `backend/bff/routers/object_types.py` (_resolve_backing)
	•	Enforced: `backend/objectify_worker/main.py` (object_type contract 필수 + pk_spec 검증)
	•	E2E Proven: `backend/bff/tests/test_object_types_backing_retrieval.py` (backing datasource/version 조회), `backend/bff/tests/test_object_types_migration_gate.py` (swap/migration gate)

RQ-OD-002 PropertyMapping 자동/수동 매핑
	•	Defined: `backend/shared/services/objectify_registry.py` (mapping spec 버전), `backend/bff/services/mapping_suggestion_service.py`
	•	Validated: `backend/bff/routers/objectify.py` (source/target/type preflight + change_summary/impact_scope 기록), `backend/bff/routers/object_types.py` (auto_generate_mapping)
	•	Enforced: `backend/objectify_worker/main.py` (mapping spec/필수 필드/타입 검증)
	•	E2E Proven: `backend/bff/tests/test_objectify_mapping_spec_preflight.py` (change_summary), `backend/bff/tests/test_mapping_suggestion_service.py` (determinism)

RQ-OD-003 스키마 호환성 검증(하드게이트)
	•	Defined: `backend/bff/routers/objectify.py` (허용 타입/캐스트)
	•	Validated: `backend/bff/routers/objectify.py` (존재/타입 불일치 차단)
	•	Enforced: `backend/objectify_worker/main.py` (MAPPING_SPEC_* 오류로 실패)
	•	E2E Proven: `backend/bff/tests/test_objectify_mapping_spec_preflight.py` (type mismatch/unsupported detail 검증)

RQ-OD-004 KeySpec 강제(Primary/Title)
	•	Defined: `backend/shared/utils/key_spec.py`, `backend/shared/services/dataset_registry.py`
	•	Validated: `backend/oms/services/ontology_resource_validator.py`, `backend/bff/routers/object_types.py`
	•	Enforced: `backend/objectify_worker/main.py` (pk_spec/title_spec 필수)
	•	E2E Proven: `backend/bff/tests/test_object_types_key_spec_required.py` (primary/title 필수)

RQ-OD-005 PrimaryKey 유일성/결정성
	•	Defined: `_scan_key_constraints` (`backend/objectify_worker/main.py`)
	•	Validated: 동일 함수 내 PRIMARY_KEY_DUPLICATE/MISSING 검출
	•	Enforced: validation_failed 시 objectify 실패 → write 0건
	•	E2E Proven: `backend/tests/unit/workers/test_objectify_worker_pk_uniqueness.py` (중복 시 job 실패 + write 차단)

RQ-OD-006 Indexing Pipeline 자동 연결
	•	Defined: `backend/pipeline_worker/main.py` (_maybe_enqueue_objectify_job)
	•	Validated: mapping_spec schema_hash 매칭 필수
	•	Enforced: schema mismatch 시 job 미발행 + gate result 기록
	•	E2E Proven: `backend/tests/unit/workers/test_pipeline_worker_objectify_auto_enqueue.py` (auto enqueue + schema gate), `backend/tests/unit/workers/test_objectify_worker_lineage_dataset_version.py` (dataset_version lineage), `make backend-prod-full` (core flow)

RQ-OD-007 스키마 변경 감지/마이그레이션 게이트
	•	Defined: `backend/pipeline_worker/main.py` (schema_hash mismatch + schema_diff), `backend/shared/services/dataset_registry.py` (schema_migration_plans), `backend/bff/routers/governance.py`
	•	Validated: object_type update 시 migration.approved 필수 (`backend/bff/routers/object_types.py`)
	•	Enforced: gate result FAIL + auto index 차단
	•	E2E Proven: `backend/tests/unit/workers/test_pipeline_worker_objectify_auto_enqueue.py` (schema_diff gate), `backend/bff/tests/test_object_types_migration_gate.py` (approval gate + plan)

RQ-OD-008 Writeback/Edits 마이그레이션 정책
	•	Defined: `backend/shared/services/dataset_registry.py` (instance_edits, remap_instance_edits), `backend/instance_worker/main.py` (edit 기록)
	•	Validated: pk 변경 시 reset_edits 또는 id_remap 없으면 409 (`backend/bff/routers/object_types.py`)
	•	Enforced: reset_edits 시 편집 이력 삭제, id_remap 시 편집 재매핑
	•	E2E Proven: `backend/tests/test_access_policy_link_indexing_e2e.py` (reset_edits gate), `backend/bff/tests/test_object_types_edit_migration.py` (move/drop/invalidate + id_remap plan)

RQ-RS-001 LinkType 1급 리소스
	•	Defined: `backend/bff/routers/link_types.py`, `backend/oms/services/ontology_resource_validator.py`
	•	Validated: link_type 필수 필드 + relationship_spec 필수 검증
	•	Enforced: relationship_spec 미충족 시 4xx
	•	E2E Proven: `backend/bff/tests/test_link_types_retrieval.py` (relationship_spec + index status), `make backend-prod-full` (OpenAPI smoke 경유)

RQ-RS-002/003 FK/Join 관계 정의
	•	Defined: `backend/bff/routers/link_types.py` (FK/Join spec)
	•	Validated: 타입/컬럼 존재성/PK 호환성 검증
	•	Enforced: mapping spec 생성 실패 → 링크 인덱싱 차단
	•	E2E Proven: `backend/bff/tests/test_link_types_fk_validation.py` (FK 타입 검증), `backend/bff/tests/test_link_types_join_table_validation.py` (join 컬럼/타입 검증), `backend/tests/unit/workers/test_objectify_worker_link_index_dangling.py` (FK 매칭 링크 생성 + join dedupe)

RQ-RS-004 JoinTable 자동 생성 옵션
	•	Defined: `backend/bff/routers/link_types.py` (_ensure_join_dataset, auto_create)
	•	Validated: join_dataset_name/branch/schema_hash 검증
	•	Enforced: auto_create 활성 시 join dataset/version 자동 생성
	•	E2E Proven: `backend/bff/tests/test_link_types_auto_join_table.py` (unit)

RQ-RS-005 Object-backed Link 지원
	•	Defined: `backend/bff/routers/link_types.py` (ObjectBackedRelationshipSpec), `backend/oms/services/ontology_resource_validator.py`
	•	Validated: relationship_object_type 필수 검증 + relationship backing resolve
	•	Enforced: join table 검증/매핑 생성 실패 시 차단
	•	E2E Proven: `backend/tests/unit/workers/test_objectify_worker_link_index_dangling.py` (object_backed 링크 생성/삭제 full_sync)

RQ-RS-006 RelationshipSpec 검증(하드게이트) 정책
	•	Defined: `backend/oms/services/ontology_resource_validator.py` (_collect_relationship_spec_issues), `backend/bff/routers/link_types.py` (_build_mapping_request)
	•	Validated: `backend/bff/routers/link_types.py` (필수 필드/컬럼/타입 검증), `backend/oms/services/ontology_resource_validator.py` (relationship_spec 필수/유형 검증)
	•	Enforced: 검증 실패 시 4xx로 LinkType 생성/수정 차단 (`backend/bff/routers/link_types.py`)
	•	E2E Proven: `backend/tests/unit/services/test_ontology_resource_validator.py` (relationship_spec 누락/유형/필수 필드)

RQ-RS-007 LinkIndexing Pipeline
	•	Defined: `backend/objectify_worker/main.py` (_run_link_index_job), `backend/shared/services/dataset_registry.py` (relationship_index_results, last_index_*)
	•	Validated: pk/row key/relationship 값 검증 + 통계/라인리지 기록
	•	Enforced: dangling_policy=FAIL 시 전체 실패 (0건) + status 기록
	•	E2E Proven: `backend/tests/unit/workers/test_objectify_worker_link_index_dangling.py` (PASS/WARN/FAIL 기록 + lineage)

RQ-RS-008 Dangling Reference 정책
	•	Defined: link_index options.dangling_policy, relationship_index_results.stats
	•	Validated: WARN/FAIL 분기 + 대상 인스턴스 존재성 확인 + dangling 통계 기록
	•	Enforced: FAIL 시 전체 실패
	•	E2E Proven: `backend/tests/unit/workers/test_objectify_worker_link_index_dangling.py` (WARN/FAIL 분기 + 통계 기록)

RQ-RS-009 링크 편집(override) 저장소
	•	Defined: `backend/shared/services/dataset_registry.py` (link_edits), `backend/bff/routers/link_types.py` (edits API)
	•	Validated: edits_enabled flag 확인
	•	Enforced: `backend/objectify_worker/main.py` (ADD/REMOVE overlay 적용)
	•	E2E Proven: `backend/bff/tests/test_link_types_link_edits.py` (edits API), `backend/tests/unit/workers/test_objectify_worker_link_index_dangling.py` (overlay 적용)

RQ-SEP-001/003 계층 분리 + 자동 반영
	•	Defined: DomainModel(ontology) ↔ DataContract(object_type + backing + mapping_spec) 분리
	•	Validated: object_type spec 필수 필드 검증
	•	Enforced: objectify/pipeline gate
	•	E2E Proven: `backend/tests/unit/workers/test_pipeline_worker_objectify_auto_enqueue_nospark.py` (자동 enqueue + schema gate), `backend/tests/unit/workers/test_objectify_worker_lineage_dataset_version.py` (dataset_version lineage)

RQ-SEP-002 독립적 변경 관리(버전/브랜치/프로포절)
	•	Defined: `backend/bff/routers/ontology_extensions.py` (ontology branch/proposal), `backend/shared/services/pipeline_registry.py` (proposal_status/fields), `backend/bff/routers/pipeline.py` (proposal submit/approve)
	•	Validated: `backend/bff/routers/pipeline.py` (_pipeline_requires_proposal, protected branch gate), `backend/shared/utils/branch_utils.py`
	•	Enforced: protected branch 업데이트/배포 시 승인된 proposal 없으면 409 (`backend/bff/routers/pipeline.py`)
	•	E2E Proven: `backend/bff/tests/test_pipeline_proposal_governance.py` (unit), `backend/bff/tests/test_pipeline_router_helpers.py` (unit)

RQ-SEP-004 거버넌스/책임 분리(조직 역할 모델)
	•	Defined: `backend/shared/services/pipeline_registry.py` (pipeline_permissions), `backend/bff/routers/database.py` (database_access roles)
	•	Validated: `backend/bff/routers/pipeline.py` (_ensure_pipeline_permission), `backend/bff/routers/database.py` (Owner 체크)
	•	Enforced: 권한 부족 시 403 (`backend/bff/routers/pipeline.py`, `backend/bff/routers/database.py`)
	•	E2E Proven: `backend/bff/tests/test_pipeline_permissions_enforced.py` (unit)

RQ-SEP-005 AccessPolicy (행/컬럼 마스킹)
	•	Defined: `backend/shared/services/dataset_registry.py` (access_policies), `backend/bff/routers/governance.py`
	•	Validated: 정책 저장 시 DB/대상 검증
	•	Enforced: `backend/bff/routers/query.py`, `backend/bff/routers/graph.py`, `backend/bff/routers/instances.py` (마스킹/필터 적용)
	•	E2E Proven: `backend/tests/unit/utils/test_access_policy.py`, `backend/bff/tests/test_instances_access_policy.py`

RQ-SEP-006 호환 계층(마이그레이션/폴백) 제공
	•	Defined: `backend/bff/routers/pipeline.py` (_detect_breaking_schema_changes), `backend/pipeline_worker/main.py` (schema_hash mismatch gate), `backend/shared/services/dataset_registry.py` (schema_migration_plans), `backend/bff/routers/object_types.py` (migration gate)
	•	Validated: breaking schema change 탐지 + migration.approved 요구 (`backend/bff/routers/pipeline.py`, `backend/bff/routers/object_types.py`)
	•	Enforced: REPLAY_REQUIRED/OBJECT_TYPE_MIGRATION_REQUIRED 409 + gate_result 기록 (`backend/bff/routers/pipeline.py`, `backend/pipeline_worker/main.py`, `backend/bff/routers/object_types.py`)
	•	E2E Proven: `backend/bff/tests/test_pipeline_promotion_semantics.py` (unit)

RQ-SEP-007 확장성과 재사용성: 데이터 교체(Swap)와 파생 재사용
	•	Defined: `backend/shared/services/dataset_registry.py` (backing_datasources/backing_datasource_versions), `backend/bff/routers/governance.py` (backing datasource APIs), `backend/bff/routers/object_types.py` (_resolve_backing)
	•	Validated: backing datasource/version/schema_hash 검증 (`backend/bff/routers/object_types.py`)
	•	Enforced: backing 변경 시 migration.approved 필요 + swap 후 objectify reindex enqueue (`backend/bff/routers/object_types.py`)
	•	E2E Proven: `backend/bff/tests/test_object_types_swap_reindex.py` (swap 후 reindex enqueue)
