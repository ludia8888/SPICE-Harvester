import { Icon, MenuItem, Button } from '@blueprintjs/core'
import { Select } from '@blueprintjs/select'

export type LineageTarget = {
  id: string
  name: string
  type: 'dataset' | 'pipeline' | 'object'
}

type LineageSelectorProps = {
  targets: LineageTarget[]
  selectedId: string | null
  onSelect: (target: LineageTarget) => void
  isLoading?: boolean
  placeholder?: string
}

const getTypeIcon = (type: LineageTarget['type']) => {
  switch (type) {
    case 'dataset':
      return 'database'
    case 'pipeline':
      return 'flow-branch'
    case 'object':
      return 'cube'
    default:
      return 'document'
  }
}

const getTypeLabel = (type: LineageTarget['type']) => {
  switch (type) {
    case 'dataset':
      return '데이터셋'
    case 'pipeline':
      return '파이프라인'
    case 'object':
      return '객체'
    default:
      return '기타'
  }
}

export const LineageSelector = ({
  targets,
  selectedId,
  onSelect,
  isLoading = false,
  placeholder = '추적할 항목 선택...',
}: LineageSelectorProps) => {
  const selectedTarget = targets.find((t) => t.id === selectedId)

  const renderTarget = (
    target: LineageTarget,
    { handleClick, modifiers }: { handleClick: React.MouseEventHandler; modifiers: { active: boolean; disabled: boolean } },
  ) => {
    if (modifiers.disabled) return null
    return (
      <MenuItem
        key={target.id}
        text={target.name}
        icon={getTypeIcon(target.type) as never}
        labelElement={<span className="lineage-selector-type">{getTypeLabel(target.type)}</span>}
        active={modifiers.active}
        onClick={handleClick}
        selected={target.id === selectedId}
      />
    )
  }

  return (
    <div className="lineage-selector">
      <label className="lineage-selector-label">추적할 항목</label>
      <Select<LineageTarget>
        items={targets}
        itemRenderer={renderTarget}
        onItemSelect={onSelect}
        filterable={targets.length > 10}
        itemPredicate={(query: string, target: LineageTarget) =>
          target.name.toLowerCase().includes(query.toLowerCase())
        }
        noResults={
          <MenuItem
            disabled
            text={isLoading ? '불러오는 중...' : '항목이 없습니다'}
            icon={isLoading ? 'refresh' : 'search'}
          />
        }
        popoverProps={{ minimal: true }}
      >
        <Button
          text={selectedTarget?.name || placeholder}
          rightIcon="caret-down"
          icon={selectedTarget ? getTypeIcon(selectedTarget.type) as never : 'search'}
          fill
          className="lineage-selector-button"
          loading={isLoading}
        />
      </Select>
    </div>
  )
}
