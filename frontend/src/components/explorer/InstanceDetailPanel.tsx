import { useCallback } from 'react'
import { Button, Icon, Spinner, Tag } from '@blueprintjs/core'
import type { Instance, Relationship } from '../../api/bff'

type InstanceDetailPanelProps = {
  instance: Instance | null
  relationships: Relationship[]
  isLoading?: boolean
  onClose: () => void
  onRelationshipClick?: (relationship: Relationship) => void
}

const formatPropertyValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '-'
  }
  if (typeof value === 'string') {
    return value
  }
  if (typeof value === 'number') {
    return value.toLocaleString('ko-KR')
  }
  if (typeof value === 'boolean') {
    return value ? '예' : '아니오'
  }
  if (value instanceof Date) {
    return value.toLocaleDateString('ko-KR')
  }
  if (Array.isArray(value)) {
    return value.map((v) => formatPropertyValue(v)).join(', ')
  }
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return String(value)
  }
}

export const InstanceDetailPanel = ({
  instance,
  relationships,
  isLoading = false,
  onClose,
  onRelationshipClick,
}: InstanceDetailPanelProps) => {
  const handleRelationshipClick = useCallback(
    (rel: Relationship) => {
      onRelationshipClick?.(rel)
    },
    [onRelationshipClick],
  )

  if (!instance && !isLoading) {
    return null
  }

  return (
    <div className="instance-detail-panel is-open">
      <div className="instance-detail-header">
        <div className="instance-detail-title">
          {isLoading ? (
            '불러오는 중...'
          ) : (
            <>
              <Icon icon="cube" size={16} />
              <span>{instance?.label}</span>
            </>
          )}
        </div>
        <Button minimal icon="cross" onClick={onClose} aria-label="닫기" />
      </div>

      <div className="instance-detail-body">
        {isLoading ? (
          <div className="instance-detail-loading">
            <Spinner size={24} />
          </div>
        ) : instance ? (
          <>
            <div className="instance-detail-section">
              <div className="instance-detail-section-title">기본 정보</div>
              <div>
                <div className="instance-detail-row">
                  <span className="instance-detail-label">ID</span>
                  <span className="instance-detail-value">{instance.id}</span>
                </div>
                <div className="instance-detail-row">
                  <span className="instance-detail-label">클래스</span>
                  <span className="instance-detail-value">{instance.classId}</span>
                </div>
                {instance.createdAt && (
                  <div className="instance-detail-row">
                    <span className="instance-detail-label">생성일</span>
                    <span className="instance-detail-value">
                      {new Date(instance.createdAt).toLocaleDateString('ko-KR')}
                    </span>
                  </div>
                )}
              </div>
            </div>

            <div className="instance-detail-section">
              <div className="instance-detail-section-title">속성</div>
              <div className="instance-detail-properties">
                {Object.entries(instance.properties).map(([key, value]) => (
                  <div key={key} className="instance-detail-row">
                    <span className="instance-detail-label">{key}</span>
                    <span className="instance-detail-value">
                      {formatPropertyValue(value)}
                    </span>
                  </div>
                ))}
                {Object.keys(instance.properties).length === 0 && (
                  <div className="instance-detail-empty">속성이 없습니다</div>
                )}
              </div>
            </div>

            <div className="instance-detail-section">
              <div className="instance-detail-section-title">
                관계 ({relationships.length}개)
              </div>
              <div className="instance-detail-relationships">
                {relationships.length === 0 ? (
                  <div className="instance-detail-empty">관계가 없습니다</div>
                ) : (
                  relationships.map((rel) => (
                    <button
                      key={rel.id}
                      type="button"
                      className="instance-detail-relationship"
                      onClick={() => handleRelationshipClick(rel)}
                    >
                      <div className="instance-detail-relationship-main">
                        <Tag minimal className="instance-detail-relationship-predicate">
                          {rel.predicateLabel || rel.predicate}
                        </Tag>
                        <span className="instance-detail-relationship-target">
                          {rel.targetLabel}
                        </span>
                      </div>
                      <span className="instance-detail-relationship-class">
                        {rel.targetClass}
                      </span>
                    </button>
                  ))
                )}
              </div>
            </div>
          </>
        ) : null}
      </div>
    </div>
  )
}
