> Updated: 2026-01-08  
> Status: Foundry reference notes. 이 문서는 SPICE HARVESTER의 현재 구현을 설명하지 않습니다.

아래는 팔란티어 Foundry 문서에 나온 정의/제약만으로, “온톨로지 타입(설계도 부품들)”, “프로퍼티 기본 데이터 타입(Base types)”, “Value type(의미 래퍼)”를 분해해서 설명한 것입니다.

⸻

0) 먼저, Foundry가 말하는 “타입”의 큰 분류

Foundry 문서는 타입을 크게 두 범주로 나눕니다.  ￼
	•	Ontology types: 현실 세계를 “개체/관계/행동”으로 모델링하는 의미 레이어의 부품들
	•	Data types: 속성 값이 실제로 가지는 데이터 값의 타입들(RDF/OWL/XSD에서 영감을 받았다고 명시)  ￼

그리고 “Ontology는 데이터셋/모델을 Object types, properties, link types, action types로 매핑해서 조직의 ‘semantic layer’를 만든다”고 설명합니다.  ￼

⸻

1) Ontology를 구성하는 “Ontology 타입들”

(= 설계도를 만드는 부품들)

Foundry에서 “Ontology라는 아티팩트가 저장하는 리소스”로 공식적으로 나열된 항목은 Object types / Link types / Action types / Interfaces / Shared properties / Object type groups 입니다.  ￼
여기에 실무적으로 함께 쓰이는 Properties(Object type 내부 구성요소)와, 별도 관리되는 Value types(의미 래퍼)를 같이 설명하겠습니다.  ￼

⸻

1-1) Object type

무엇인가?

Object type은 “조직이 다루는 현실 세계의 엔티티/이벤트”를 표현하는 최상위 설계 단위입니다(예: Employee, Flight, Airport). 그리고 사용자 애플리케이션이 실제로 보여주는 ‘객체 화면’의 중심이 됩니다.

Object type이 꼭 가져야 하는 핵심 2개: Primary key / Title key

Object type은 최소 1개 property가 필요하고, 그 이유는 “고유하게 식별하기 위한 primary key가 필요하기 때문”이라고 문서에서 명시합니다.  ￼
	•	Primary key: 각 인스턴스를 유일하게 식별하는 키. backing datasource의 각 row에서 중복이 있으면 안 됨.  ￼
	•	Title key: 객체를 화면에 표시할 때 쓰는 “표시 이름(display name) 역할”의 키.  ￼

특히 Object Storage v2에서는 primary key 중복이 빌드 실패로 이어질 수 있음을 문서가 경고합니다.  ￼
→ 이게 시현님 시스템에서 말하는 “의미/정합성 게이트”와 유사한, 온톨로지 레벨에서 ‘ID 정합성’이 강제되는 지점입니다.

Object type에 붙는 메타데이터(왜 중요하냐?)

Object type은 단순 스키마가 아니라, “조직 UX/운영/거버넌스”를 위해 풍부한 메타데이터를 가집니다. 예를 들면: ID, API name, 상태(active/experimental/deprecated), visibility, 아이콘, 그룹, reindex 상태, writeback 여부 같은 것들이 공식 메타데이터로 나열됩니다.  ￼
	•	여기서 Status(active/experimental/deprecated), Index status(success/failed) 같은 필드는 “이 타입이 운영에 안전한가/갱신이 정상인가”를 표현하는 운영 신호입니다.  ￼

⸻

1-2) Property

무엇인가?

Property는 Object type의 “특성(특징)을 표현하는 스키마”입니다. 그리고 property value는 개별 객체(인스턴스)에서 그 속성이 실제로 갖는 값입니다.  ￼

Property가 중요한 이유: “키”와 “표현”을 결정함

Property 메타데이터에는 “이 프로퍼티가 title key냐 primary key냐”가 포함될 수 있고, 문서는 이 Key의 의미를 명확히 정의합니다.  ￼
	•	title key: 표시용 이름
	•	primary key: 유일 식별자(각 row 고유)

즉, Foundry에서 property는 단순 컬럼이 아니라 객체의 정체성/표현을 결정하는 요소입니다.

⸻

1-3) Shared property

무엇인가?

Shared property는 “여러 object type에서 재사용 가능한 property”입니다. 핵심은 메타데이터를 중앙에서 공유한다는 점입니다.  ￼

문서가 굉장히 중요한 단서를 하나 박습니다:
	•	공유되는 건 ‘property metadata’이고
	•	실제 underlying object data(각 객체의 값)는 공유되지 않는다  ￼

즉, “start date”라는 속성을 Employee와 Contractor가 같이 쓰게 만들면:
	•	이름/설명/타입/포맷 같은 정의는 한 군데서 관리(일관성)
	•	각 객체의 start date 값은 각자의 데이터에서 그대로 유지(데이터가 섞이지 않음)

이게 왜 중요하냐면, 대기업/전사 모델링에서 가장 자주 터지는 문제가 “같은 의미인데 팀마다 다른 이름/설명/포맷으로 난립”하는 것이고, shared property는 그걸 구조적으로 막습니다. (이 부분은 문서 정의로부터의 논리적 귀결입니다.)

⸻

1-4) Link type

무엇인가?

Link type은 두 object type 간 관계의 스키마 정의입니다. 그리고 “link”는 그 관계의 **단일 인스턴스(한 쌍의 객체 사이 관계 한 건)**를 뜻한다고 문서가 구분합니다.  ￼

또한 링크는 같은 타입끼리도 가능(예: Employee의 Direct Report ↔ Manager)하다고 명시합니다.  ￼

Link type이 데이터 관점에서 무엇과 닮았나?

문서는 link type을 데이터셋의 join에 비유합니다:
	•	link type 정의 ≈ 두 데이터셋을 어떤 키로 join할지의 정의
	•	link(관계 한 건) ≈ join 결과에서 “이 row와 저 row가 연결됐다”는 한 매칭  ￼

이건 시현님이 pipeline에서 join을 만들고, 그 결과를 ontology link로 “승격(objectify)”시키는 흐름을 설계할 때 매우 직접적인 대응관계가 됩니다.

제약/구조적 함의
	•	서로 다른 Ontology 간 link는 지원되지 않는다고 명시합니다. 필요하면 “shared Ontology”를 권장합니다.  ￼
	•	many-to-many 관계에서는 “link type 자체를 backing datasource로 뒷받침”해야 한다는 설명이 있습니다(관계 테이블이 필요하다는 뜻).  ￼

⸻

1-5) Action type

무엇인가? (Foundry에서 “행동/트랜잭션”의 설계도)

Action은 Ontology에서 객체/속성/링크를 변경하는 단일 트랜잭션입니다. 그리고 action type은 사용자가 한 번에 수행할 수 있는 변경(편집)의 정의 + 제출 시 발생하는 side effects까지 포함한다고 명시합니다.  ￼

즉, Action type은 단순 “버튼”이 아니라:
	•	어떤 객체들을
	•	어떤 규칙으로
	•	무엇을 바꾸고
	•	제출 시 어떤 부수 효과를 일으킬지
를 묶은 업무 트랜잭션 스펙입니다.  ￼

Action rule(룰): 실제로 뭘 바꿀 수 있나?

문서에서는 Ontology rule이 객체/링크를 생성·수정·삭제할 수 있다고 명시합니다.  ￼
또한 1:N이나 1:1 링크를 만들거나 삭제하려면 “object rule을 쓰고 foreign key property를 수정해야 한다”고 설명합니다.  ￼
→ 즉, Foundry에서 “관계 변경”은 결국 키/속성 변경과 결합되어 모델링됩니다.

Submission criteria(검증/거버넌스 게이트)

Action은 아무나/아무 때나 제출되는 게 아니라, “제출 가능 조건(Submission criteria)”을 정의할 수 있고, 이것이 데이터 품질 및 편집 거버넌스를 보장한다고 명시합니다.  ￼
이 criteria는 객체/관계/사용자 정보까지 포함해서 논리식으로 구성할 수 있다고 설명합니다.  ￼
→ 이게 팔란티어식 “Hard gate(실패하면 못 바꿈)”의 대표적인 구현 축입니다.

⸻

1-6) Object type groups

무엇인가?

Object type group은 **온톨로지 탐색/검색을 돕는 분류(라벨)**이고, “검색/탐색을 더 잘 하게 하는 classification primitive”라고 명시합니다.  ￼
Ontology Manager에서 생성/관리되며, 검색바/필터/Explorer 홈에서 그룹이 활용된다고 나옵니다.  ￼

이건 데이터 모델 자체를 바꾸지 않으면서도, “전사 온톨로지 커질수록 발생하는 탐색 비용”을 줄이는 UX 장치입니다.

⸻

1-7) Interfaces

무엇인가?

Interface는 “object type의 shape(모양)와 capabilities(능력)”을 설명하는 Ontology type입니다. 여러 object type이 공통 형태를 가질 때, 일관된 모델링/상호작용을 가능하게 한다고 정의합니다.  ￼
예시로 Facility 인터페이스를 만들고 Airport/Plant/Hangar 같은 타입들이 이를 구현(implement)할 수 있다고 설명합니다.  ￼

이건 전통적 OOP 상속과 완전히 동일하다고 단정하긴 어렵지만, 문서 정의대로라면 “공통 스키마/공통 상호작용을 강제하는 다형성 레이어”로 쓰입니다.

⸻

1-8) Value types

Value type은 문서에서 “object type/property/link type 같은 Ontology 구성요소와 달리, space에 귀속되는 타입”이라고 선명하게 선을 긋습니다.  ￼

핵심 정의
	•	Value type은 field type(또는 base type) 위에
	•	**metadata + constraints(제약)**를 얹어서
	•	도메인 의미 + 재사용 가능한 검증을 제공하는 “semantic wrapper”라고 정의합니다.  ￼

또한 value type은:
	•	생성된 해당 space 안에서만 사용 가능
	•	Default ontology에서는 사용 불가
라고 명시합니다.  ￼

버전 관리(중요)

Value type은 “breaking/non-breaking 편집을 다루기 위해 버전이 있다”고 하고, 버전은 metadata + constraints로 구성된다고 말합니다.  ￼
특히:
	•	name/description/apiName 같은 메타는 바꿀 수 있지만
	•	base type 메타데이터와 validation rule(제약)은 immutable(불변) 이라고 못 박습니다.  ￼
→ 이건 “의미 타입이 흔들리면 전사 데이터가 흔들린다”를 방지하는 강한 안전장치입니다.

어디에 쓸 수 있나?

문서에 명시된 사용처는 3가지입니다.  ￼
	•	object type property에 value type 지정
	•	shared property에 value type 지정
	•	Pipeline Builder에서 logical type cast로 value type을 지정한 뒤 objects target에 쓸 때 사용

즉, Value type은 “온톨로지”와 “파이프라인(쓰기)” 사이에 걸쳐서 의미를 전달하는 연결 부품으로 쓰입니다.  ￼

⸻

2) Property 값에 들어갈 수 있는 “Base type(기본 데이터 타입)”

(= 속성 값의 실제 저장 타입)

Foundry의 properties 문서에는 base type별 지원/제약이 표 형태로 정리되어 있고, 특히 Array/Struct의 제약이 명시돼 있습니다.  ￼

아래는 “실무에서 어떤 의미인지”까지 붙여서 설명하겠습니다.

⸻

2-1) 기본 스칼라(원자) 타입
	•	String
	•	Boolean
	•	Integer / Short / Long / Byte
	•	Float / Double / Decimal
	•	Date / Timestamp
(이 목록은 properties/base types 체계에서 일반적으로 다루는 primitive군이며, Foundry는 data types가 RDF/OWL/XSD에서 영감을 받았다고 설명합니다.)  ￼

여기서 실무적으로 중요한 포인트는:
	•	Decimal은 “돈/정산”처럼 정밀도가 필요한 곳에 쓰는 경향이 강하고,
	•	Float/Double은 계산/센서/수치 모델에서 흔합니다.
(이건 일반적 해석이며, 구체 제약은 조직 규칙에 따릅니다.)

⸻

2-2) Vector

문서에서 Vector는 “semantic search에 쓰기 위한 벡터 저장 타입”이라고 명시합니다.  ￼
즉, 임베딩 벡터(예: 텍스트/이미지 임베딩)를 객체 속성으로 저장해 “의미 기반 검색”을 가능하게 하는 용도입니다.

⸻

2-3) Array

Array는 “배열 속성”이고, 문서 제약이 매우 중요합니다:
	•	Array 요소는 null을 포함할 수 없다  ￼
	•	Object Storage v2에서는 중첩 배열(nested arrays)을 지원하지 않는다  ￼

이 제약이 의미하는 바:
	•	“리스트 안에 비어있는 값(null)”을 허용하지 않으니, 결측치는 다른 방식(예: 빈 배열, 별도 플래그, 혹은 struct로 감싸기)으로 설계해야 합니다.
	•	nested array를 못 쓰면, “배열의 배열” 같은 JSON 구조는 Struct 또는 별도 객체/링크 모델로 풀어야 합니다.

⸻

2-4) Struct

Struct는 “여러 필드를 가진 schema-based property”라고 정의합니다.  ￼
또한 struct property는 dataset의 struct 타입 컬럼에서 만들어진다고 설명합니다.  ￼
즉, 대충 아무 컬럼 여러 개를 묶는 게 아니라, 파이프라인에서 구조화된 컬럼으로 만든 다음 온톨로지 속성으로 승격시키는 흐름입니다.

그리고 문서가 명시하는 제약:
	•	Struct는 nesting(중첩)을 지원하지 않는다
	•	Struct의 fields는 arrays가 될 수 없다  ￼

이게 의미하는 바:
	•	“객체 안 객체” 같은 깊은 JSON은 Struct로 끝까지 못 가져가고, 모델을 분해해야 합니다.
	•	Struct 안에 리스트가 필요하면, 그 리스트를 별도 객체 타입 + 링크로 빼거나, 데이터 모델을 평탄화해야 합니다.

⸻

2-5) Media reference / Time series / Attachment

base types 페이지는 이들을 “특수 목적 타입”으로 분리해서 설명합니다.  ￼
	•	Media reference: 미디어 파일에 대한 참조 타입  ￼
	•	Time series: time series property로 정의할 수 있는 타입  ￼
	•	Attachment: 객체에 파일을 저장해 “functions on objects”에서 사용 가능하다고 명시  ￼

⸻

2-6) Geopoint / Geoshape

base types로 존재하며, geospatial 문서가 “값이 어떤 문자열 포맷이어야 하는지”까지 규정합니다.  ￼
	•	Geopoint: latitude,longitude 또는 geohash 문자열  ￼
	•	Geoshape: GeoJSON geometry 문자열(지원 geometry 종류와 금지되는 구조가 명시됨)  ￼

즉, 단순히 “지도 타입”이 아니라 입력 포맷 게이트가 있는 엄격한 타입입니다.

⸻

2-7) Marking

Properties의 supported types 표에 base type으로 포함되어 있습니다.  ￼
다만 “Marking이 정확히 어떤 포맷/의미 체계를 갖는지”는 지금 제가 펼친 문서 조각만으로는 충분히 단정하기 어렵습니다. (여기서는 **‘지원 타입 목록에 포함’**까지만 확실하게 말할 수 있습니다.)  ￼

⸻

2-8) Cipher (Cipher text)

base types 페이지에서 Cipher text는 “Cipher로 인코딩된 문자열을 저장하는 타입”이라고 명시합니다.  ￼
즉, 민감정보를 평문 string으로 두지 않고 암호화된 형태로 저장/취급하는 쪽으로 설계된 타입입니다.

⸻

3) Value type: “의미 + 제약(검증)”을 얹는 커스텀 타입

(= Base type 위에 도메인 타입을 만든다)

Value type은 문서 정의대로, primitive/base type이 줄 수 없는 “도메인 의미”를 부여합니다.  ￼

3-1) 왜 필요한가? (문서가 말하는 문제의식)

문서는 “dataset field types / property base types는 primitive이고 domain-agnostic이라 맥락이 없다”고 말합니다.  ￼
반대로 value type은:
	•	의미(semantic meaning)를 캡슐화하고
	•	검증(validation)을 중앙화해서
	•	주변 정보(컬럼명/설명)에 의존하지 않게 한다
고 설명합니다.  ￼

3-2) 제약(Constraints)

각 value type은 선택적으로 constraint를 정의해 데이터 validation을 강제할 수 있고, 어떤 base type에 어떤 constraint를 적용할 수 있는지 표로 제공한다고 안내합니다.  ￼

3-3) 버전 정책(불변성)

다시 강조하지만, Foundry는 value type 버전에서:
	•	name/description/apiName 같은 메타는 변경 가능
	•	base type과 constraints(검증 규칙)는 immutable
이라고 명시합니다.  ￼
→ 이건 “의미 타입을 바꿔서 과거 데이터 해석이 흔들리는 사고”를 구조적으로 막는 설계입니다.

3-4) 파이프라인과의 연결(시현님 관심 포인트)

value type은 Ontology 속성에만 붙는 게 아니라,
Pipeline Builder에서 logical type cast로 지정해 objects target에 쓸 때도 사용할 수 있다고 문서에 명시돼 있습니다.  ￼
즉, “ETL 단계에서 의미 타입을 주입한 뒤 → 온톨로지에 기록”이라는 설계가 Foundry에서도 가능한 형태로 드러납니다.

⸻

마지막으로: 시현님 시스템에 바로 대입되는 핵심만 압축
	•	Object type + (primary key/title key): “객체의 정체성과 화면 표현”이 온톨로지에서 강제됨  ￼
	•	Link type: 관계는 데이터 join 개념과 대응되며, many-to-many는 관계 데이터소스가 필요할 수 있음  ￼
	•	Action type: “업무 트랜잭션”을 스키마로 정의하고, submission criteria로 게이트를 걸 수 있음  ￼
	•	Value type: primitive 위에 “의미+검증”을 얹고, 버전에서 base/constraint는 불변이라 의미 안정성을 보장  ￼
	•	Array/Struct 제약: OSv2에서 nested array 불가, array null 불가, struct nesting 불가/fields array 불가 → 모델링을 분해해야 하는 강제 조건  ￼
알겠습니다, 시현님. **Action type(액션 타입)**은 Foundry Ontology에서 “사용자가 버튼 한 번으로 일으키는 업무 트랜잭션”을 정의(설계)하는 타입입니다. 단순히 “속성 하나 수정”이 아니라, 여러 객체/속성/링크를 한 번에 바꾸고 + 제출 시 부수효과까지 포함하는 것이 핵심입니다.  ￼

아래는 Action type을 구성요소 → 실행 순서 → 운영상 함정까지 최대한 촘촘하게 풀어쓴 설명입니다.

⸻

1) Action vs Action type: 먼저 용어를 분리
	•	Action type: “어떤 입력을 받고(파라미터), 어떤 규칙으로 바꾸고(룰), 어떤 검증/권한을 통과해야 하고(Submission criteria/permissions), 어떤 부수효과를 낼지(side effects)”를 정의한 설계도  ￼
	•	Action(실행): 사용자가 UI(Workshop/Slate/Object View 등)에서 실제로 제출해 발생하는 단일 트랜잭션(한 번의 실행)  ￼

즉, “액션 타입은 PRD, 액션은 실제 배포/실행”에 가까운 관계입니다.

⸻

2) Action type의 4대 구성요소

(1) Parameters: 액션의 입력(폼 필드)

Parameters는 Action type의 입력값이고, Rules와 앱(Workshop/Slate/Object Views 등) 사이를 잇는 “인터페이스”라고 문서가 명시합니다.  ￼
	•	파라미터는 “변수”처럼 동작하며, 타입(type) 이 있어 어떤 값이 들어올 수 있는지 결정됩니다.  ￼
	•	실전에서 가장 중요한 포인트는: Object 파라미터(“현재 보고 있는 Ticket 객체”) 같은 입력을 받는다는 점입니다. 이게 있으면 “어떤 객체를 바꿀지”가 파라미터로 결정됩니다. (Object View에서 특히 중요)

UI 관점 디테일:
Object View에 Action을 붙이면 기본적으로 파라미터가 폼에 다 노출되는데, “현재 객체를 자동 주입하고 사용자가 못 바꾸게” 하려면 폼에서 숨기고 default value로 현재 객체를 설정하라고 문서가 구체적으로 안내합니다.  ￼
→ 이게 없으면 사용자가 다른 객체를 골라서 엉뚱한 레코드를 수정할 수 있습니다(거버넌스 사고 포인트).

⸻

(2) Rules: 실제로 무엇을 어떻게 바꾸는가

Foundry 문서는 “Ontology rule은 Ontology의 특정 요소를 바꾸며, 기존 타입의 objects/links를 생성·수정·삭제할 수 있다”고 못 박습니다.  ￼

여기서 중요한 세부 규칙이 하나 있습니다:
	•	1:1 또는 1:N 링크를 생성/삭제하려면 “link rule만으로 끝나는 게 아니라”, object rule로 ‘foreign key property’를 수정해야 한다고 명시합니다.  ￼

이 문장 하나가 굉장히 실무적입니다.
즉, Foundry에서 “관계(link)”는 그냥 선을 그리는 게 아니라, 데이터 모델 상 FK(키) 갱신과 결합된 방식으로 안전하게 관리됩니다.

⸻

(3) Submission criteria & Permissions: “제출”을 통과시키는 게이트

Action은 아무나 못 누르게 만들 수 있어야 합니다. 문서는 submission criteria가 “누가 액션을 실행할 수 있는지에 대한 fine-grained control”이라고 설명합니다.  ￼

특히 submission criteria는 단순 권한 체크가 아니라:
	•	**현재 사용자(Current User)**의 속성(사용자 ID, 그룹 ID, 기타 멀티패스 속성 등)을 기준으로 조건을 걸 수 있고  ￼
	•	파라미터에 들어온 값(예: “승인자 ID 파라미터”)과도 조합해서 조건을 만들 수 있다고 문서가 말합니다.  ￼

실무적으로는 “이 액션은 특정 그룹만 가능”, “이 액션은 요청자가 아닌 승인자만 가능” 같은 정책을 여기에 넣습니다.

⸻

(4) Side effects: 제출 이후 외부/부수 동작(웹훅 등)

Action type은 “제출 시 함께 발생하는 side effect behavior”까지 포함한다고 공식 Overview에 명시돼 있습니다.  ￼

한국어 문서(웹훅)에는 실행 순서도 나옵니다:
	•	웹훅을 writeback으로 설정하면 “다른 모든 규칙 평가 전에 실행”  ￼
	•	웹훅을 부작용(side effect) 으로 설정하면 “규칙 평가 후 실행”, 사용자에게는 Foundry 오브젝트 수정 후 성공 메시지가 먼저 보일 수 있고, 부작용은 그 뒤에 실행될 수도 있다고 설명합니다.  ￼

즉, side effect는 “트랜잭션 내부 동작”이냐 “트랜잭션 이후 비동기 후처리”냐를 구분해서 설계해야 합니다.

⸻

3) “Writeback”과 Action type: 어디에 기록되나?

Action은 결국 “사용자가 데이터를 편집하는 행위”인데, Foundry는 이를 원본 backing dataset에 바로 쓰지 않고 별도의 writeback dataset으로 적재하는 구조를 권장/강제합니다.  ￼

문서가 매우 명확히 말합니다:
	•	사용자가 action을 수행하려면 writeback dataset이 필요하고  ￼
	•	편집 내용은 backing dataset이 아니라 writeback dataset에 기록되며  ￼
	•	이렇게 하면 사용자는 분석에서 원본 데이터와 편집된 데이터를 모두 접근할 수 있어 안전하다고 설명합니다.  ￼

이게 “의미론적 시스템의 안전장치”인 이유는 간단합니다:
	•	원본 데이터(수집/정산/회계 등)는 보호
	•	사용자 운영 편집(현업 수정)은 별도 레이어로 누적
	•	최신 상태는 Ontology가 “원본+편집”을 결합해 보여주는 구조로 갑니다  ￼

⸻

4) Function-backed Actions: 룰로 표현이 안 되면 “함수 기반 액션”

문서에서 “많은 액션은 단순 rules로 충분하지만, 어떤 경우엔 rules만으로는 부족하고 function-backed actions가 필요하다”고 분리합니다.  ￼

이건 보통 이런 상황입니다:
	•	외부 시스템 호출/복잡한 계산/다단계 로직/조건 분기 폭발
	•	단순히 “필드 바꾸기”가 아니라 “정책 엔진/최적화/추천/워크플로우”를 태워야 하는 경우

(문서는 “rules가 충분하지 않을 수 있다”는 프레이밍까지는 명시하지만, 구체 케이스는 조직 구현에 따라 달라집니다.)  ￼

⸻

5) 어디서 실행되나: Workshop/Slate/Object Views 등
	•	Workshop 문서: Actions는 사용자가 객체 데이터를 편집/생성/삭제/링크할 수 있게 해주며, 이는 “action types(룰셋)”로 정의된다고 명시합니다.  ￼
	•	Slate 문서: interactive app에서 writeback을 할 때 Ontology Actions가 권장 방식이라고 말합니다.  ￼
	•	Object Views: 액션 폼과 파라미터 기본값/숨김 설정 같은 UX가 구체적으로 문서화돼 있습니다.  ￼

⸻

6) (중요) 일관성/반영 타이밍: “즉시 보이냐?” 문제

Foundry의 SDK 문서에서는 액션 적용 후 Object Storage V1의 변경은 eventually consistent일 수 있어 반영에 시간이 걸릴 수 있다고 명시합니다.  ￼
→ 운영 UX에서 “눌렀는데 왜 아직 안 바뀌지?”가 발생할 수 있는 지점이라, 사용자 피드백/로딩/재조회 정책이 중요해집니다.

⸻

7) 시현님 스타일로 한 줄 정의

Action type = “입력(Parameters) + 변경 규칙(Rules) + 제출 게이트(Submission criteria/Permissions) + 부수효과(Side effects)”로 구성된, 온톨로지 기반 업무 트랜잭션 템플릿입니다.  ￼

⸻

부록) 문서 대비 코드 구현 현황 (증거 기반)

상태 정의
	•	Defined: 모델/리소스 타입 존재(저장 가능)
	•	Validated: 생성/업데이트 시 검증으로 4xx 차단
	•	Enforced: 실행 경로(objectify/pipeline/instance)에서 실패 시 write=0
	•	E2E Proven: 재현 가능한 테스트/로그로 증명됨
	•	미구현: 위 기준을 충족하지 못함(특히 테스트 부재)
	•	본 부록 표기는 Defined/Validated/미구현만 사용 (Enforced/E2E는 정의만 유지)
	•	테스트/로그 증거가 없는 항목은 미구현으로 표기

증거 3종 세트 (각 항목에 필수로 명시)
	1) 정의/스키마 변환
	2) 검증 적용 지점(콜스택)
	3) 실패 재현(테스트/로그)

1) Ontology 타입
	•	Object type
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology.py, backend/oms/services/terminus/ontology.py
			2) 검증 적용 지점: backend/oms/routers/ontology.py (create/update -> lint/interface)
			3) 실패 재현: backend/tests/unit/services/test_ontology_linter_pk_branching.py
	•	Primary key
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology.py (Property.primary_key)
			2) 검증 적용 지점: backend/shared/services/ontology_linter.py, backend/objectify_worker/main.py
			3) 실패 재현: backend/tests/unit/services/test_ontology_linter_pk_branching.py, backend/tests/unit/workers/test_objectify_worker_p0_gates.py
	•	Title key
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology.py (Property.title_key)
			2) 검증 적용 지점: backend/shared/services/ontology_linter.py
			3) 실패 재현: backend/tests/unit/services/test_ontology_linter_pk_branching.py
	•	Property
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology.py
			2) 검증 적용 지점: backend/objectify_worker/main.py (_validate_value_constraints)
			3) 실패 재현: backend/tests/unit/workers/test_objectify_worker_p0_gates.py
	•	Shared property
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology_resources.py
			2) 검증 적용 지점: backend/oms/routers/ontology.py (_apply_shared_properties), backend/oms/services/ontology_resource_validator.py
			3) 실패 재현: backend/tests/unit/services/test_ontology_router_helpers.py, backend/tests/unit/services/test_ontology_resource_validator.py
	•	Link type
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology_resources.py, backend/oms/services/ontology_resources.py
			2) 검증 적용 지점: backend/oms/services/ontology_resource_validator.py (link_type)
			3) 실패 재현: backend/tests/unit/services/test_ontology_resource_validator.py
		- 비고: 링크 인스턴스 실행 경로는 미구현
	•	Action type
		- 상태: Validated (정의/검증까지만)
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology_resources.py
			2) 검증 적용 지점: backend/oms/services/ontology_resource_validator.py, backend/oms/routers/ontology_extensions.py
			3) 실패 재현: backend/tests/unit/services/test_ontology_resource_validator.py
		- 미구현: Submission criteria/side effects 실행 엔진
	•	Interfaces
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology_resources.py
			2) 검증 적용 지점: backend/oms/services/ontology_interface_contract.py, backend/oms/routers/ontology.py
			3) 실패 재현: backend/tests/unit/services/test_ontology_interface_contract.py
	•	Object type groups
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology_resources.py
			2) 검증 적용 지점: backend/oms/routers/ontology.py (_validate_group_refs)
			3) 실패 재현: backend/tests/unit/services/test_ontology_router_helpers.py
	•	Value types
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology_resources.py, backend/shared/utils/ontology_type_normalization.py
			2) 검증 적용 지점: backend/oms/routers/ontology.py (_validate_value_type_refs), backend/oms/routers/ontology_extensions.py (_validate_value_type_immutability), backend/objectify_worker/main.py
			3) 실패 재현: backend/tests/unit/services/test_ontology_router_helpers.py, backend/tests/unit/services/test_ontology_value_type_immutability.py, backend/tests/unit/workers/test_objectify_worker_p0_gates.py
	•	(문서 범위 밖) Function 리소스
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology_resources.py
			2) 검증 적용 지점: backend/oms/services/ontology_resource_validator.py
			3) 실패 재현: backend/tests/unit/services/test_ontology_resource_validator.py

2) Base types/제약
	•	기본 scalar + enum/format/length/pattern
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/utils/ontology_type_normalization.py, backend/shared/validators/constraint_validator.py
			2) 검증 적용 지점: backend/objectify_worker/main.py (_validate_value_constraints)
			3) 실패 재현: backend/tests/unit/workers/test_objectify_worker_p0_gates.py
	•	Array/Struct
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/validators/array_validator.py, backend/shared/validators/struct_validator.py
			2) 검증 적용 지점: backend/objectify_worker/main.py (base_constraints)
			3) 실패 재현: backend/tests/unit/validators/test_base_type_validators.py
	•	Vector
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/validators/vector_validator.py
			2) 검증 적용 지점: backend/objectify_worker/main.py (get_validator)
			3) 실패 재현: backend/tests/unit/validators/test_base_type_validators.py
	•	Geopoint/Geoshape
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/validators/geopoint_validator.py, backend/shared/validators/geoshape_validator.py
			2) 검증 적용 지점: backend/objectify_worker/main.py (get_validator)
			3) 실패 재현: backend/tests/unit/validators/test_base_type_validators.py
	•	Marking/Cipher
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/validators/marking_validator.py, backend/shared/validators/cipher_validator.py
			2) 검증 적용 지점: backend/objectify_worker/main.py (get_validator)
			3) 실패 재현: backend/tests/unit/validators/test_base_type_validators.py
	•	Media/Attachment/Time series
		- 상태: Validated (string 수준)
		- 증거:
			1) 정의/스키마 변환: backend/shared/utils/ontology_type_normalization.py, backend/shared/validators/__init__.py
			2) 검증 적용 지점: backend/objectify_worker/main.py (get_validator -> StringValidator)
			3) 실패 재현: backend/tests/unit/validators/test_base_type_validators.py
		- 비고: 전용 포맷 규칙은 미구현

3) 거버넌스/게이트
	•	Primary key 게이트
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology.py
			2) 검증 적용 지점: backend/shared/services/ontology_linter.py, backend/objectify_worker/main.py
			3) 실패 재현: backend/tests/unit/services/test_ontology_linter_pk_branching.py, backend/tests/unit/workers/test_objectify_worker_p0_gates.py
	•	Title key 게이트
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology.py
			2) 검증 적용 지점: backend/shared/services/ontology_linter.py
			3) 실패 재현: backend/tests/unit/services/test_ontology_linter_pk_branching.py
	•	Interface 계약 게이트
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology_resources.py
			2) 검증 적용 지점: backend/oms/routers/ontology.py (_collect_interface_issues)
			3) 실패 재현: backend/tests/unit/services/test_ontology_interface_contract.py
	•	Value type ref 게이트
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/shared/models/ontology.py, backend/shared/models/ontology_resources.py
			2) 검증 적용 지점: backend/oms/routers/ontology.py (_validate_value_type_refs)
			3) 실패 재현: backend/tests/unit/services/test_ontology_router_helpers.py
	•	Object type 계약(pk_spec/backing_source) 게이트
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/oms/services/ontology_resource_validator.py
			2) 검증 적용 지점: backend/oms/routers/ontology_extensions.py (validate_resource)
			3) 실패 재현: backend/tests/unit/services/test_ontology_resource_validator.py
	•	Relationship 검증 게이트
		- 상태: Validated
		- 증거:
			1) 정의/스키마 변환: backend/oms/validators/relationship_validator.py
			2) 검증 적용 지점: backend/oms/services/async_terminus.py, backend/oms/routers/ontology.py (_validate_relationships_gate)
			3) 실패 재현: backend/tests/unit/services/test_ontology_router_helpers.py
	•	Action submission criteria/side effects
		- 상태: 미구현
		- 증거:
			1) 정의/스키마 변환: 없음
			2) 검증 적용 지점: 없음
			3) 실패 재현: 없음
