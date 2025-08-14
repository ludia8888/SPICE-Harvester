# 🚨 CRITICAL: Legacy Architecture Issues

## 답변: 네, 심각한 레거시 문제가 있습니다!

### 1. 🔴 가장 심각한 문제: PostgreSQL을 Event Store로 오용

**현재 모든 라우터가 잘못된 패턴 사용 중:**

```python
# ❌ WRONG - 현재 코드 (instance_async.py, ontology.py 등)
async def create_instance_async(
    outbox_service: OutboxService = OutboxServiceDep,
):
    # PostgreSQL Outbox를 Event Store처럼 사용 중!
    await outbox_service.publish_command(command)
```

**이것은 근본적으로 잘못되었습니다!**
- PostgreSQL Outbox는 Event Store가 아닙니다
- 단지 Kafka로의 전달 보증 메커니즘일 뿐입니다

### 2. 🔄 중복된 Command Handler들

| 파일 | 상태 | 문제 |
|------|------|------|
| `corrected_command_handler.py` | ✅ 새로 만든 올바른 구현 | - |
| `instance_async.py` | ❌ 레거시 | Outbox를 Event Store로 사용 |
| `ontology.py` | ❌ 레거시 | Outbox를 Event Store로 사용 |
| `database.py` | ❌ 레거시 | 직접 DB 조작 |

### 3. 📁 정리 안 된 레거시 파일들

**삭제 필요:**
- `oms/main_legacy_backup.py` - 백업 파일
- 수많은 중복 테스트 파일들:
  - `test_event_sourcing_perfect.py`
  - `test_event_sourcing_fixed.py`
  - `test_event_sourcing_pipeline.py`
  - `test_ultra_fixed_verification.py`
  - `test_ultra_skeptical_verification.py`

### 4. 🔀 아키텍처 충돌

**현재 시스템은 두 가지 상충하는 패턴이 공존:**

```
1. 레거시 (잘못된) 플로우:
   Command → PostgreSQL (Event Store로 착각) → Kafka

2. 새로운 (올바른) 플로우:
   Command → S3/MinIO (진짜 Event Store) → PostgreSQL (전달용) → Kafka
```

### 5. 🛠️ MinIO는 설정되어 있지만 사용 안 함

```yaml
# docker-compose.yml에 MinIO 있음 ✅
minio:
  image: minio/minio:latest
  environment:
    - MINIO_ROOT_USER=admin
    - MINIO_ROOT_PASSWORD=spice123!
```

**하지만:**
- Event Store로 사용하는 코드 없음 ❌
- 모든 라우터가 여전히 PostgreSQL 사용 중 ❌
- Workers가 S3에서 읽지 않음 ❌

## 즉시 필요한 조치

### 1단계: 기존 라우터 수정
```python
# ❌ 현재 (모든 라우터)
await outbox_service.publish_command(command)

# ✅ 수정 필요
from oms.services.corrected_command_handler import CorrectedCommandHandler

handler = CorrectedCommandHandler()
await handler.handle_create_instance(...)  # S3 먼저!
```

### 2단계: Dependencies 업데이트
```python
# oms/dependencies.py에 추가
from oms.services.event_store import event_store

EventStoreDep = Depends(lambda: event_store)
```

### 3단계: Main.py 수정
```python
# oms/main.py startup에 추가
from oms.services.event_store import event_store

async def startup():
    await event_store.connect()  # S3/MinIO 연결
```

### 4단계: Worker 수정
Workers가 Outbox 메시지를 받으면:
1. S3 event_id를 추출
2. S3에서 실제 이벤트를 읽음
3. 이벤트 처리

### 5단계: 레거시 삭제
```bash
# 레거시 파일 삭제
rm oms/main_legacy_backup.py
rm test_event_sourcing_*.py  # 중복된 테스트들
```

## 영향 범위

### 🔴 높은 우선순위 (즉시 수정 필요)
1. `instance_async.py` - 모든 인스턴스 생성이 잘못된 패턴 사용
2. `ontology.py` - 온톨로지 생성이 잘못된 패턴 사용
3. Workers - S3에서 읽도록 수정 필요

### 🟡 중간 우선순위
1. 테스트 파일 정리
2. 문서 업데이트
3. 의존성 정리

### 🟢 낮은 우선순위
1. 동기 라우터 업데이트
2. 성능 최적화

## 결론

**심각한 레거시 문제가 있습니다:**

1. **전체 시스템이 PostgreSQL을 Event Store로 오용 중** ❌
2. **S3/MinIO 설정은 되어있지만 사용 안 함** ❌
3. **새로 만든 올바른 코드와 레거시가 공존** ❌
4. **중복된 파일들 정리 안 됨** ❌

**이는 단순한 리팩토링이 아니라 근본적인 아키텍처 수정이 필요합니다!**

THINK ULTRA 원칙: 겉만 수정하지 말고 근본 원인을 해결해야 합니다.