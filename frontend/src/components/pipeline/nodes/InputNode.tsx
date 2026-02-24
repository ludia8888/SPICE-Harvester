import { memo } from 'react'
import { Handle, Position } from 'reactflow'
import type { NodeProps } from 'reactflow'
import { Icon } from '@blueprintjs/core'
import { getCatalog } from './nodeStyles'

type InputNodeData = {
  label: string
  transformType: string
  config: Record<string, unknown>
  columnCount?: number
  status?: 'ok' | 'warning' | 'error'
  statusMessage?: string
  dimmed?: boolean
  legendColor?: string | null
  legendColorId?: string | null
}

export const InputNode = memo(({ data, selected }: NodeProps<InputNodeData>) => {
  const catalog = getCatalog(data.transformType ?? 'source')
  const statusClass = data.status === 'error' ? ' has-error' : data.status === 'warning' ? ' has-warning' : ''
  const dimClass = data.dimmed ? ' is-dimmed' : ''

  return (
    <div className={`pipeline-node pipeline-node--input${selected ? ' selected' : ''}${statusClass}${dimClass}`}>
      {/* Status indicator */}
      {data.status === 'error' && (
        <div className="pipeline-node-status-icon is-error" title={data.statusMessage}>
          <Icon icon="warning-sign" size={10} />
        </div>
      )}
      {data.status === 'warning' && (
        <div className="pipeline-node-status-icon is-warning" title={data.statusMessage}>
          <Icon icon="warning-sign" size={10} />
        </div>
      )}

      {/* Colored header bar */}
      <div className="pipeline-node-header" style={{ background: data.legendColor ?? catalog.color }}>
        <Icon icon={catalog.icon} size={14} color="#fff" />
        <div className="pipeline-node-title">
          {String(data.config?.name ?? data.config?.dataset_name ?? data.label ?? catalog.label)}
        </div>
      </div>

      {/* Meta row */}
      <div className="pipeline-node-meta">
        <Icon icon="th-list" size={10} />
        <span>{data.columnCount != null ? `${data.columnCount} columns` : 'Dataset'}</span>
      </div>

      <Handle type="source" position={Position.Right} id="out" className="pipeline-handle" />
    </div>
  )
})

InputNode.displayName = 'InputNode'
