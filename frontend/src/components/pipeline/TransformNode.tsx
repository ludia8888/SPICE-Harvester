import { memo, useState, useCallback, useEffect, useRef } from 'react'
import { Handle, Position, type NodeProps } from 'reactflow'
import { Icon, EditableText, type IconName } from '@blueprintjs/core'

// Transform 타입 정의
export type TransformType =
  | 'join'
  | 'union'
  | 'filter'
  | 'dedupe'
  | 'select'
  | 'drop'
  | 'rename'
  | 'cast'
  | 'normalize'
  | 'regexReplace'
  | 'compute'
  | 'explode'
  | 'groupBy'
  | 'aggregate'
  | 'pivot'
  | 'window'
  | 'sort'
  | 'udf'
  | 'output'

export type TransformNodeData = {
  label: string
  transformType: TransformType
  description?: string
  config?: Record<string, unknown>
  onLabelChange?: (nodeId: string, newLabel: string) => void
}

// Transform 타입별 설정
const transformConfig: Record<TransformType, {
  icon: IconName
  color: string
  bgColor: string
  category: string
  defaultLabel: string
  inputs: number // 입력 핸들 수
}> = {
  // 데이터 조합 (보라색 계열)
  join: {
    icon: 'join-table',
    color: '#a855f7',
    bgColor: '#1e1b2e',
    category: 'Combine',
    defaultLabel: 'Join',
    inputs: 2,
  },
  union: {
    icon: 'merge-links',
    color: '#a855f7',
    bgColor: '#1e1b2e',
    category: 'Combine',
    defaultLabel: 'Union',
    inputs: 2,
  },

  // 행 필터링 (파란색 계열)
  filter: {
    icon: 'filter',
    color: '#3b82f6',
    bgColor: '#1a1f2e',
    category: 'Filter',
    defaultLabel: 'Filter',
    inputs: 1,
  },
  dedupe: {
    icon: 'duplicate',
    color: '#3b82f6',
    bgColor: '#1a1f2e',
    category: 'Filter',
    defaultLabel: 'Dedupe',
    inputs: 1,
  },

  // 컬럼 변환 (청록색 계열)
  select: {
    icon: 'column-layout',
    color: '#06b6d4',
    bgColor: '#1a2529',
    category: 'Column',
    defaultLabel: 'Select',
    inputs: 1,
  },
  drop: {
    icon: 'remove-column',
    color: '#06b6d4',
    bgColor: '#1a2529',
    category: 'Column',
    defaultLabel: 'Drop',
    inputs: 1,
  },
  rename: {
    icon: 'edit',
    color: '#06b6d4',
    bgColor: '#1a2529',
    category: 'Column',
    defaultLabel: 'Rename',
    inputs: 1,
  },
  cast: {
    icon: 'exchange',
    color: '#06b6d4',
    bgColor: '#1a2529',
    category: 'Column',
    defaultLabel: 'Cast',
    inputs: 1,
  },
  normalize: {
    icon: 'clean',
    color: '#06b6d4',
    bgColor: '#1a2529',
    category: 'Column',
    defaultLabel: 'Normalize',
    inputs: 1,
  },
  regexReplace: {
    icon: 'regex',
    color: '#06b6d4',
    bgColor: '#1a2529',
    category: 'Column',
    defaultLabel: 'Regex Replace',
    inputs: 1,
  },

  // 계산 & 생성 (초록색 계열)
  compute: {
    icon: 'function',
    color: '#22c55e',
    bgColor: '#1a2520',
    category: 'Compute',
    defaultLabel: 'Compute',
    inputs: 1,
  },
  explode: {
    icon: 'expand-all',
    color: '#22c55e',
    bgColor: '#1a2520',
    category: 'Compute',
    defaultLabel: 'Explode',
    inputs: 1,
  },

  // 집계 & 그룹화 (주황색 계열)
  groupBy: {
    icon: 'group-objects',
    color: '#f97316',
    bgColor: '#2a1f1a',
    category: 'Aggregate',
    defaultLabel: 'Group By',
    inputs: 1,
  },
  aggregate: {
    icon: 'grouped-bar-chart',
    color: '#f97316',
    bgColor: '#2a1f1a',
    category: 'Aggregate',
    defaultLabel: 'Aggregate',
    inputs: 1,
  },
  pivot: {
    icon: 'pivot-table',
    color: '#f97316',
    bgColor: '#2a1f1a',
    category: 'Aggregate',
    defaultLabel: 'Pivot',
    inputs: 1,
  },
  window: {
    icon: 'timeline-events',
    color: '#f97316',
    bgColor: '#2a1f1a',
    category: 'Aggregate',
    defaultLabel: 'Window',
    inputs: 1,
  },

  // 정렬 (노란색 계열)
  sort: {
    icon: 'sort',
    color: '#eab308',
    bgColor: '#262419',
    category: 'Order',
    defaultLabel: 'Sort',
    inputs: 1,
  },

  // 사용자 정의 (분홍색 계열)
  udf: {
    icon: 'code',
    color: '#ec4899',
    bgColor: '#2a1a24',
    category: 'Custom',
    defaultLabel: 'UDF',
    inputs: 1,
  },

  // 출력 (회색 계열)
  output: {
    icon: 'export',
    color: '#94a3b8',
    bgColor: '#1e2028',
    category: 'Output',
    defaultLabel: 'Output',
    inputs: 1,
  },
}

export const TransformNode = memo(({ id, data, selected }: NodeProps<TransformNodeData>) => {
  const { label, transformType, description, onLabelChange } = data
  const config = transformConfig[transformType] || transformConfig.compute
  const [isEditing, setIsEditing] = useState(false)
  const [editedLabel, setEditedLabel] = useState(label)
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    setEditedLabel(label)
  }, [label])

  const handleDoubleClick = useCallback((e: React.MouseEvent) => {
    e.stopPropagation()
    setIsEditing(true)
  }, [])

  const handleLabelChange = useCallback((value: string) => {
    setEditedLabel(value)
  }, [])

  const handleLabelConfirm = useCallback((value: string) => {
    setIsEditing(false)
    const newLabel = value.trim() || config.defaultLabel
    setEditedLabel(newLabel)
    if (onLabelChange && newLabel !== label) {
      onLabelChange(id, newLabel)
    }
  }, [id, label, config.defaultLabel, onLabelChange])

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleLabelConfirm(editedLabel)
    } else if (e.key === 'Escape') {
      setIsEditing(false)
      setEditedLabel(label)
    }
  }, [editedLabel, label, handleLabelConfirm])

  return (
    <div
      className={`transform-node ${selected ? 'selected' : ''}`}
      style={{
        background: '#1c2127',
        border: selected ? '2px solid #2d72d2' : '1px solid #404854',
        borderRadius: 6,
        padding: '10px 14px',
        minWidth: 160,
        fontSize: 12,
      }}
    >
      {/* 입력 핸들 */}
      {config.inputs >= 1 && (
        <Handle
          type="target"
          position={Position.Left}
          id="input-1"
          style={{
            width: 10,
            height: 10,
            background: '#404854',
            border: '2px solid #1c2127',
            top: config.inputs === 2 ? '30%' : '50%',
          }}
        />
      )}
      {config.inputs >= 2 && (
        <Handle
          type="target"
          position={Position.Left}
          id="input-2"
          style={{
            width: 10,
            height: 10,
            background: '#404854',
            border: '2px solid #1c2127',
            top: '70%',
          }}
        />
      )}

      {/* 헤더: 카테고리 뱃지 */}
      <div
        style={{
          fontSize: 9,
          color: config.color,
          textTransform: 'uppercase',
          letterSpacing: '0.5px',
          marginBottom: 6,
          opacity: 0.8,
        }}
      >
        {config.category}
      </div>

      {/* 메인: 아이콘 + 라벨 */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <div
          style={{
            width: 28,
            height: 28,
            borderRadius: 4,
            background: `${config.color}20`,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <Icon icon={config.icon} size={14} color={config.color} />
        </div>
        <div style={{ flex: 1, minWidth: 0 }}>
          {isEditing ? (
            <input
              ref={inputRef}
              type="text"
              value={editedLabel}
              onChange={(e) => handleLabelChange(e.target.value)}
              onBlur={() => handleLabelConfirm(editedLabel)}
              onKeyDown={handleKeyDown}
              autoFocus
              style={{
                background: 'transparent',
                border: 'none',
                borderBottom: `1px solid ${config.color}`,
                color: '#f5f8fa',
                fontSize: 13,
                fontWeight: 500,
                width: '100%',
                outline: 'none',
                padding: '2px 0',
              }}
            />
          ) : (
            <div
              onDoubleClick={handleDoubleClick}
              style={{
                color: '#f5f8fa',
                fontWeight: 500,
                fontSize: 13,
                cursor: 'text',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
              title="Double-click to edit"
            >
              {editedLabel}
            </div>
          )}
        </div>
      </div>

      {/* 설명 (옵션) */}
      {description && (
        <div
          style={{
            color: '#8a9ba8',
            fontSize: 10,
            marginTop: 6,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}
        >
          {description}
        </div>
      )}

      {/* 출력 핸들 */}
      <Handle
        type="source"
        position={Position.Right}
        style={{
          width: 10,
          height: 10,
          background: '#404854',
          border: '2px solid #1c2127',
        }}
      />
    </div>
  )
})

TransformNode.displayName = 'TransformNode'

// 노드 타입 내보내기 (ReactFlow nodeTypes에 등록용)
export const transformNodeTypes = {
  transform: TransformNode,
}
