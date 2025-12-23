import type { MouseEvent } from 'react'
import { Icon } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'
import type { PipelineNode as PipelineNodeType } from './pipelineTypes'

type NodeProps = {
    node: PipelineNodeType
    selected?: boolean
    onSelect?: (id: string) => void
    onDoubleClick?: (id: string) => void
}

export const PipelineNode = ({ node, selected, onSelect, onDoubleClick }: NodeProps) => {
    const columns = node.columns ?? (node.subtitle ? [node.subtitle] : [])
    const statusClass = node.status ? `status-${node.status}` : 'status-success'

    const handleClick = (event: MouseEvent<HTMLDivElement>) => {
        event.stopPropagation()
        onSelect?.(node.id)
    }

    return (
        <div
            className={`pipeline-node ${selected ? 'selected' : ''}`}
            style={{ left: node.x, top: node.y }}
            onClick={handleClick}
            onDoubleClick={() => onDoubleClick?.(node.id)}
        >
            <div className="pipeline-node-header">
                <Icon icon={node.icon as IconName} size={14} className="pipeline-node-icon" />
                <div className="pipeline-node-title">{node.title}</div>
                <span className={`pipeline-node-status ${statusClass}`}>●</span>
            </div>
            <div className="pipeline-node-body">
                {columns.length > 0 ? (
                    columns.map((col) => (
                        <div key={col} className="node-column">
                            <span>{col}</span>
                        </div>
                    ))
                ) : null}
            </div>
            <div className="pipeline-node-port pipeline-node-port-left" />
            <div className="pipeline-node-port pipeline-node-port-right" />
        </div>
    )
}
