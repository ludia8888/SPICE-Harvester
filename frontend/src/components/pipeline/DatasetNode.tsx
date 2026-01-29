import { memo, useState, useCallback, useEffect, useRef } from 'react'
import { Handle, Position, type NodeProps } from 'reactflow'
import { Icon, type IconName } from '@blueprintjs/core'

export type DatasetNodeData = {
  label: string
  columnCount?: number
  rowCount?: number
  sourceType?: string
  stage?: 'raw' | 'clean' | 'transform'
  isRaw?: boolean
  onLabelChange?: (nodeId: string, newLabel: string) => void
}

// 소스 타입별 아이콘 매핑
const getSourceIcon = (sourceType?: string): IconName => {
  switch (sourceType) {
    case 'csv':
      return 'th'
    case 'xlsx':
    case 'xls':
      return 'th'
    case 'parquet':
      return 'database'
    case 'json':
      return 'code-block'
    default:
      return 'document'
  }
}

// Dataset 노드 색상 (입력 데이터는 파란색 계열)
const datasetConfig = {
  icon: 'database' as IconName,
  color: '#8abbff',
  category: 'Input',
}

export const DatasetNode = memo(({ id, data, selected }: NodeProps<DatasetNodeData>) => {
  const { label, columnCount, sourceType, onLabelChange } = data
  const icon = getSourceIcon(sourceType)
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
    const newLabel = value.trim() || label
    setEditedLabel(newLabel)
    if (onLabelChange && newLabel !== label) {
      onLabelChange(id, newLabel)
    }
  }, [id, label, onLabelChange])

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
      className={`dataset-node ${selected ? 'selected' : ''}`}
      style={{
        background: '#1c2127',
        border: selected ? '2px solid #2d72d2' : '1px solid #404854',
        borderRadius: 6,
        padding: '10px 14px',
        minWidth: 160,
        fontSize: 12,
      }}
    >
      {/* 카테고리 뱃지 */}
      <div
        style={{
          fontSize: 9,
          color: datasetConfig.color,
          textTransform: 'uppercase',
          letterSpacing: '0.5px',
          marginBottom: 6,
          opacity: 0.8,
        }}
      >
        {datasetConfig.category}
      </div>

      {/* 메인: 아이콘 + 라벨 */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <div
          style={{
            width: 28,
            height: 28,
            borderRadius: 4,
            background: `${datasetConfig.color}20`,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <Icon icon={icon} size={14} color={datasetConfig.color} />
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
                borderBottom: `1px solid ${datasetConfig.color}`,
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

      {/* 컬럼 수 (옵션) */}
      {columnCount !== undefined && (
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
          {columnCount} columns
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

DatasetNode.displayName = 'DatasetNode'
