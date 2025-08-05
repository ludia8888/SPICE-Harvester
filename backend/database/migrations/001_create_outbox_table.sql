-- Outbox Pattern 테이블 생성
-- 이 테이블은 모든 이벤트 메시지를 임시로 저장하여 
-- 트랜잭션의 원자성을 보장합니다.

CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- 어떤 종류의 엔티티에 대한 이벤트인지 (e.g., 'OntologyClass', 'Property', 'User')
    aggregate_type VARCHAR(255) NOT NULL,
    -- 해당 엔티티의 고유 ID
    aggregate_id VARCHAR(255) NOT NULL,
    -- 발행할 Kafka 토픽 이름
    topic VARCHAR(255) NOT NULL,
    -- 이벤트의 상세 내용 (메시지 본문)
    payload JSONB NOT NULL,
    -- 이벤트 생성 시각
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 이벤트가 Kafka로 전송된 시각 (NULL이면 아직 미처리)
    processed_at TIMESTAMPTZ,
    -- 재시도 횟수 (실패 시 증가)
    retry_count INTEGER DEFAULT 0,
    -- 마지막 처리 시도 시각
    last_retry_at TIMESTAMPTZ
);

-- 빠른 조회를 위한 인덱스 생성
-- 미처리된 이벤트를 created_at 순서로 조회하기 위한 인덱스
CREATE INDEX idx_outbox_unprocessed ON outbox (created_at) 
WHERE processed_at IS NULL;

-- aggregate_type과 aggregate_id로 특정 엔티티의 이벤트를 조회하기 위한 인덱스
CREATE INDEX idx_outbox_aggregate ON outbox (aggregate_type, aggregate_id);

-- 토픽별 이벤트 조회를 위한 인덱스
CREATE INDEX idx_outbox_topic ON outbox (topic);

-- 재시도가 필요한 이벤트 조회를 위한 인덱스
CREATE INDEX idx_outbox_retry ON outbox (retry_count, last_retry_at) 
WHERE processed_at IS NULL AND retry_count > 0;