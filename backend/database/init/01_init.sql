-- PostgreSQL 초기화 스크립트
-- 데이터베이스 및 스키마 설정 (processed_events registry)

-- UUID 확장 활성화
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Durable idempotency registry schema (Worker/Projection)
CREATE SCHEMA IF NOT EXISTS spice_event_registry;

-- 기본 스키마 설정
SET search_path TO spice_event_registry, public;

-- processed_events: idempotency + lease/heartbeat (핵심 방어선)
CREATE TABLE IF NOT EXISTS spice_event_registry.processed_events (
    handler TEXT NOT NULL,
    event_id TEXT NOT NULL,
    aggregate_id TEXT,
    sequence_number BIGINT,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    owner TEXT,
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 1,
    last_error TEXT,
    PRIMARY KEY (handler, event_id)
);

-- aggregate_versions: per-aggregate ordering guard
CREATE TABLE IF NOT EXISTS spice_event_registry.aggregate_versions (
    handler TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    last_sequence BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (handler, aggregate_id)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_processed_events_aggregate
ON spice_event_registry.processed_events(handler, aggregate_id);

CREATE INDEX IF NOT EXISTS idx_processed_events_status
ON spice_event_registry.processed_events(status);

CREATE INDEX IF NOT EXISTS idx_processed_events_lease
ON spice_event_registry.processed_events(handler, status, heartbeat_at)
WHERE status = 'processing';
