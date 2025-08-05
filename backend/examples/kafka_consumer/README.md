# Kafka Consumer Example

이 예제는 Outbox Pattern을 통해 발행된 온톨로지 이벤트를 구독하고 처리하는 방법을 보여줍니다.

## 실행 방법

1. 필요한 의존성 설치:
```bash
pip install confluent-kafka
```

2. 환경 변수 설정 (선택사항):
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

3. 컨슈머 실행:
```bash
python consumer_example.py
```

## 이벤트 유형

### ONTOLOGY_CLASS_CREATED
- 새로운 온톨로지 클래스가 생성될 때 발행
- 데이터: class_id, label, description, properties, relationships

### ONTOLOGY_CLASS_UPDATED
- 기존 온톨로지 클래스가 수정될 때 발행
- 데이터: class_id, updates, merged_data

### ONTOLOGY_CLASS_DELETED
- 온톨로지 클래스가 삭제될 때 발행
- 데이터: class_id

## 커스터마이징

`OntologyEventConsumer` 클래스를 상속하여 자신만의 이벤트 처리 로직을 구현할 수 있습니다:

```python
class MyCustomConsumer(OntologyEventConsumer):
    def handle_class_created(self, class_id: str, data: dict):
        # 커스텀 로직 구현
        super().handle_class_created(class_id, data)
        # ElasticSearch 인덱싱
        # 캐시 갱신
        # 외부 서비스 호출
```

## 프로덕션 고려사항

1. **에러 처리**: Dead Letter Queue 구현
2. **모니터링**: 메트릭 수집 및 알림
3. **스케일링**: 여러 컨슈머 인스턴스 실행
4. **재시도**: 실패한 메시지 재처리 로직
5. **멱등성**: 중복 메시지 처리 방지