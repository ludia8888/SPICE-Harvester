import { Icon, Spinner } from '@blueprintjs/core'
import type { OntologyClass } from '../../api/bff'

type ObjectTypeListProps = {
  objectTypes: OntologyClass[]
  selectedId: string | null
  onSelect: (objectType: OntologyClass) => void
  isLoading?: boolean
}

export const ObjectTypeList = ({
  objectTypes,
  selectedId,
  onSelect,
  isLoading = false,
}: ObjectTypeListProps) => {
  if (isLoading) {
    return (
      <div className="object-type-list is-loading">
        <Spinner size={24} />
        <span>불러오는 중...</span>
      </div>
    )
  }

  return (
    <div className="object-type-list">
      {objectTypes.length === 0 ? (
        <div className="object-type-list-empty">
          <Icon icon="cube" size={20} />
          <span>정의된 Object type이 없습니다</span>
        </div>
      ) : (
        objectTypes.map((objectType) => (
          <button
            key={objectType.id}
            type="button"
            className={`object-type-item ${selectedId === objectType.id ? 'is-selected' : ''}`}
            onClick={() => onSelect(objectType)}
          >
            <Icon icon="cube" size={14} className="object-type-item-icon" />
            <div className="object-type-item-content">
              <span className="object-type-item-name">{objectType.label}</span>
              {objectType.description && (
                <span className="object-type-item-desc">{objectType.description}</span>
              )}
            </div>
            <div className="object-type-item-meta">
              {objectType.propertyCount !== undefined && (
                <span className="object-type-item-count">{objectType.propertyCount}개 속성</span>
              )}
              {objectType.instanceCount !== undefined && (
                <span className="object-type-item-count">
                  {objectType.instanceCount}개 인스턴스
                </span>
              )}
            </div>
          </button>
        ))
      )}
    </div>
  )
}
