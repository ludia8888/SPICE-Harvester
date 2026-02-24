import { memo } from 'react'
import { Handle, Position } from 'reactflow'
import type { NodeProps } from 'reactflow'
import { Icon } from '@blueprintjs/core'
import { getCatalog } from './nodeStyles'
import type { Expectation } from '../ExpectationEditor'

type OutputNodeData = {
  label: string
  transformType: string
  config: Record<string, unknown> & { expectations?: Expectation[] }
  columnCount?: number
  status?: 'ok' | 'warning' | 'error'
  statusMessage?: string
  dimmed?: boolean
  legendColor?: string | null
  legendColorId?: string | null
}

export const OutputNode = memo(({ data, selected }: NodeProps<OutputNodeData>) => {
  const catalog = getCatalog('output')
  const expectations = data.config?.expectations as Expectation[] | undefined
  const expCount = expectations?.length ?? 0

  return (
    <div
      className={`pipeline-node pipeline-node--output${selected ? ' selected' : ''}`}
      style={{ background: data.legendColor ?? catalog.color, borderColor: data.legendColor ?? catalog.color }}
    >
      <Handle type="target" position={Position.Left} id="in" className="pipeline-handle" />
      {/* Expectation count badge */}
      {expCount > 0 && (
        <div className="pipeline-node-badge">
          <Icon icon="shield" size={9} color="#fff" />
          {expCount} {expCount === 1 ? 'check' : 'checks'}
        </div>
      )}
      {/* Single solid-color row — no meta section */}
      <div className="pipeline-node-header" style={{ background: 'transparent' }}>
        <Icon icon={catalog.icon} size={14} color="#fff" />
        <div className="pipeline-node-title" style={{ color: '#fff' }}>
          {String(data.config?.name ?? data.config?.output_name ?? data.label ?? 'Output')}
        </div>
      </div>
    </div>
  )
})

OutputNode.displayName = 'OutputNode'
