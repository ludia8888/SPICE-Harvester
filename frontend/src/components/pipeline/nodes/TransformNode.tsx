import { memo, Fragment } from 'react'
import { Handle, Position } from 'reactflow'
import type { NodeProps } from 'reactflow'
import { Icon } from '@blueprintjs/core'
import { getCatalog } from './nodeStyles'

type TransformNodeData = {
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

export const TransformNode = memo(({ data, selected }: NodeProps<TransformNodeData>) => {
  const catalog = getCatalog(data.transformType)
  const inputs = catalog.inputs
  const hasMultiInputs = inputs.length > 1
  const statusClass = data.status === 'error' ? ' has-error' : data.status === 'warning' ? ' has-warning' : ''
  const dimClass = data.dimmed ? ' is-dimmed' : ''

  return (
    <div className={`pipeline-node pipeline-node--transform${selected ? ' selected' : ''}${statusClass}${dimClass}`}>
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

      {/* Input handles */}
      {inputs.map((input, i) => {
        const topPct = hasMultiInputs ? 30 + i * 40 : 50
        return (
          <Fragment key={input.id}>
            <Handle
              type="target"
              position={Position.Left}
              id={input.id}
              className="pipeline-handle"
              style={{ top: `${topPct}%` }}
            />
            {hasMultiInputs && (
              <div
                className="pipeline-node-handle-label pipeline-node-handle-label--left"
                style={{ top: `${topPct}%` }}
              >
                {input.label}
              </div>
            )}
          </Fragment>
        )
      })}

      {/* Colored header bar */}
      <div className="pipeline-node-header" style={{ background: data.legendColor ?? catalog.color }}>
        <Icon icon={catalog.icon} size={14} color="#fff" />
        <div className="pipeline-node-title">
          {String(data.config?.name ?? data.label ?? catalog.label)}
        </div>
      </div>

      {/* Meta row */}
      <div className="pipeline-node-meta">
        <Icon icon="th-list" size={10} />
        <span>{data.columnCount != null ? `${data.columnCount} columns` : catalog.label}</span>
      </div>

      {/* Output handle — no label */}
      {catalog.outputs.length > 0 && (
        <Handle
          type="source"
          position={Position.Right}
          id="out"
          className="pipeline-handle"
        />
      )}
    </div>
  )
})

TransformNode.displayName = 'TransformNode'
