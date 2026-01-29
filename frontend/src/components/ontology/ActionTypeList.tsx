import { Icon, Spinner, Button } from '@blueprintjs/core'
import type { ActionType } from '../../api/bff'

type ActionTypeListProps = {
  actionTypes: ActionType[]
  selectedId: string | null
  onSelect: (actionType: ActionType) => void
  onCreate?: () => void
  isLoading?: boolean
}

export const ActionTypeList = ({
  actionTypes,
  selectedId,
  onSelect,
  onCreate,
  isLoading = false,
}: ActionTypeListProps) => {
  if (isLoading) {
    return (
      <div className="action-type-list is-loading">
        <Spinner size={24} />
        <span>불러오는 중...</span>
      </div>
    )
  }

  return (
    <div className="action-type-list">
      <div className="action-type-list-header">
        <span className="action-type-list-title">Action 목록</span>
        <span className="action-type-list-count">{actionTypes.length}개</span>
      </div>
      <div className="action-type-list-items">
        {actionTypes.length === 0 ? (
          <div className="action-type-list-empty">
            <Icon icon="flash" size={20} />
            <span>정의된 Action이 없습니다</span>
          </div>
        ) : (
          actionTypes.map((actionType) => (
            <button
              key={actionType.id}
              type="button"
              className={`action-type-item ${selectedId === actionType.id ? 'is-selected' : ''}`}
              onClick={() => onSelect(actionType)}
            >
              <Icon icon="flash" size={14} className="action-type-item-icon" />
              <div className="action-type-item-content">
                <span className="action-type-item-name">{actionType.name}</span>
                {actionType.description && (
                  <span className="action-type-item-desc">{actionType.description}</span>
                )}
              </div>
              <Icon icon="chevron-right" size={12} className="action-type-item-arrow" />
            </button>
          ))
        )}
      </div>
      {onCreate && (
        <div className="action-type-list-footer">
          <Button icon="add" text="새 Action" onClick={onCreate} fill />
        </div>
      )}
    </div>
  )
}
