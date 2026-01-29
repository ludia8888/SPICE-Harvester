import { Icon } from '@blueprintjs/core'
import type { ActionTypeDetail as ActionTypeDetailType } from '../../api/bff'

type ActionTypeDetailProps = {
  actionType: ActionTypeDetailType | null
  isLoading?: boolean
}

export const ActionTypeDetail = ({ actionType, isLoading = false }: ActionTypeDetailProps) => {
  if (isLoading) {
    return (
      <div className="action-type-detail is-loading">
        <span>불러오는 중...</span>
      </div>
    )
  }

  if (!actionType) {
    return (
      <div className="action-type-detail is-empty">
        <Icon icon="flash" size={24} />
        <span>Action을 선택해주세요</span>
      </div>
    )
  }

  return (
    <div className="action-type-detail">
      <div className="action-type-detail-header">
        <div className="action-type-detail-icon">
          <Icon icon="flash" size={20} />
        </div>
        <div className="action-type-detail-title-area">
          <h3 className="action-type-detail-name">{actionType.name}</h3>
          {actionType.targetClass && (
            <span className="action-type-detail-target">
              <Icon icon="cube" size={12} />
              {actionType.targetClass}
            </span>
          )}
        </div>
      </div>

      {actionType.description && (
        <p className="action-type-detail-description">{actionType.description}</p>
      )}

      <div className="action-type-detail-section">
        <h4 className="action-type-detail-section-title">
          필수 입력 ({actionType.parameters.length}개)
        </h4>
        <div className="action-type-detail-params">
          {actionType.parameters.map((param) => (
            <div key={param.name} className="action-type-detail-param">
              <div className="action-type-detail-param-header">
                <span className="action-type-detail-param-name">{param.label}</span>
                {param.required && (
                  <span className="action-type-detail-param-required">필수</span>
                )}
              </div>
              <span className="action-type-detail-param-type">{param.type}</span>
              {param.description && (
                <span className="action-type-detail-param-desc">{param.description}</span>
              )}
            </div>
          ))}
        </div>
      </div>

      {actionType.examples && actionType.examples.length > 0 && (
        <div className="action-type-detail-section">
          <h4 className="action-type-detail-section-title">사용 예시</h4>
          <div className="action-type-detail-examples">
            {actionType.examples.map((example, index) => (
              <div key={index} className="action-type-detail-example">
                <span>{example.description}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
