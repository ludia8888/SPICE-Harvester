
# Spice Harvester Synthetic 3PL Dataset (역직구 시나리오)

기간: 2025-05-01 ~ 2025-07-31
특징:
- 역직구(한국 -> 해외) B2C 물류 흐름을 가정한 합성 데이터
- 의도적으로 생성된 데이터 이상치(재고 불일치, mis-pick, 중복 트래킹, 통관 지연 등)
- Event/Outbox 포함(원인-결과 연결: cause_event_id)

## 파일 목록
- warehouses.csv, locations.csv
- suppliers.csv, clients.csv
- products.csv, skus.csv
- inbounds.csv, receipts.csv
- orders.csv, order_items.csv
- tracking.csv
- returns.csv, exceptions.csv
- inventory_snapshots.csv
- events.csv, outbox.csv
- ontology.jsonld

## 주요 테스트 시나리오
1) **재고 불일치 감지**
   - `inbounds/receipts` vs `orders/order_items` vs `inventory_snapshots` 비교로 음수 재고/오차 검출

2) **출고 오류(MIS-PICK) 추적**
   - `exceptions`의 MIS-PICK 발생 주문 → `order_items` 실제 SKU와 비교

3) **배송 지연/통관 이슈**
   - `tracking`의 status=EXCEPTION 또는 `exceptions`의 DELIVERY_DELAY

4) **중복 트래킹 번호**
   - `tracking.tracking_number` 중복 값 존재 → 중복 처리 로직 검증

5) **Outbox 패턴 검증**
   - `events`의 event_id ↔ `outbox.cause_event_id` 인과 연결

## 스키마(요약)
각 CSV 헤더가 컬럼이며, 상세 속성은 `ontology.jsonld` 참고.

예) order_items.csv
- order_id (FK: orders.order_id)
- line_no (int)
- sku_id (FK: skus.sku_id)
- qty (int)
- unit_price (float)

## 크기(대략)
- Products: 300
- SKUs: 1064
- Orders: 3000
- Order items: 6016
- Inbounds: 2639
- Events: 2500 / Outbox: 2500

