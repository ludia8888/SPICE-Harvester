-- PostgreSQL 초기화 스크립트
-- 데이터베이스 및 스키마 설정

-- UUID 확장 활성화
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 애플리케이션용 스키마 생성
CREATE SCHEMA IF NOT EXISTS spice_outbox;

-- 기본 스키마 설정
SET search_path TO spice_outbox, public;

-- Outbox Pattern 테이블 생성 (Command/Event 지원)
CREATE TABLE IF NOT EXISTS spice_outbox.outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_type VARCHAR(50) NOT NULL DEFAULT 'EVENT',
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    retry_count INTEGER DEFAULT 0,
    last_retry_at TIMESTAMPTZ,
    CONSTRAINT chk_message_type CHECK (message_type IN ('COMMAND', 'EVENT'))
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed ON spice_outbox.outbox (created_at) 
WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_outbox_message_type ON spice_outbox.outbox (message_type, processed_at) 
WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_outbox_aggregate ON spice_outbox.outbox (aggregate_type, aggregate_id);

CREATE INDEX IF NOT EXISTS idx_outbox_topic ON spice_outbox.outbox (topic);

CREATE INDEX IF NOT EXISTS idx_outbox_retry ON spice_outbox.outbox (retry_count, last_retry_at) 
WHERE processed_at IS NULL AND retry_count > 0;

-- 권한 설정
GRANT ALL PRIVILEGES ON SCHEMA spice_outbox TO admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA spice_outbox TO admin;