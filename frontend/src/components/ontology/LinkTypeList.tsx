import { Icon, Spinner } from '@blueprintjs/core'
import type { LinkType } from '../../api/bff'

type LinkTypeListProps = {
  linkTypes: LinkType[]
  selectedId: string | null
  onSelect: (linkType: LinkType) => void
  isLoading?: boolean
}

export const LinkTypeList = ({
  linkTypes,
  selectedId,
  onSelect,
  isLoading = false,
}: LinkTypeListProps) => {
  if (isLoading) {
    return (
      <div className="link-type-list is-loading">
        <Spinner size={24} />
        <span>불러오는 중...</span>
      </div>
    )
  }

  return (
    <div className="link-type-list">
      {linkTypes.length === 0 ? (
        <div className="link-type-list-empty">
          <Icon icon="link" size={20} />
          <span>정의된 Link type이 없습니다</span>
        </div>
      ) : (
        linkTypes.map((linkType) => (
          <button
            key={linkType.id}
            type="button"
            className={`link-type-item ${selectedId === linkType.id ? 'is-selected' : ''}`}
            onClick={() => onSelect(linkType)}
          >
            <div className="link-type-item-visual">
              <span className="link-type-item-source">{linkType.sourceClassName}</span>
              <Icon icon="arrow-right" size={12} />
              <span className="link-type-item-target">{linkType.targetClassName}</span>
            </div>
            <div className="link-type-item-content">
              <span className="link-type-item-name">{linkType.name}</span>
              <span className="link-type-item-predicate">{linkType.predicate}</span>
            </div>
            {linkType.description && (
              <span className="link-type-item-desc">{linkType.description}</span>
            )}
          </button>
        ))
      )}
    </div>
  )
}
