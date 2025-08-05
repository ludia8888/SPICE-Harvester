-- Outbox 테이블에 message_type 필드 추가
-- Command와 Event를 구분하기 위함

ALTER TABLE spice_outbox.outbox 
ADD COLUMN IF NOT EXISTS message_type VARCHAR(50) DEFAULT 'EVENT';

-- 기존 데이터는 모두 EVENT로 간주
UPDATE spice_outbox.outbox 
SET message_type = 'EVENT' 
WHERE message_type IS NULL;

-- message_type에 NOT NULL 제약 추가
ALTER TABLE spice_outbox.outbox 
ALTER COLUMN message_type SET NOT NULL;

-- message_type에 대한 인덱스 추가 (빠른 필터링을 위해)
CREATE INDEX IF NOT EXISTS idx_outbox_message_type 
ON spice_outbox.outbox (message_type, processed_at) 
WHERE processed_at IS NULL;

-- CHECK 제약 추가 (COMMAND 또는 EVENT만 허용)
ALTER TABLE spice_outbox.outbox 
ADD CONSTRAINT chk_message_type 
CHECK (message_type IN ('COMMAND', 'EVENT'));