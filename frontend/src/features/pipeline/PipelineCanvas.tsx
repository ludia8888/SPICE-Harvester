import { Icon } from '@blueprintjs/core'
import type { PipelineEdge, PipelineNode as PipelineNodeType } from './pipelineTypes'
import { PipelineNode } from './PipelineNode'

type ActionKind = 'join' | 'filter' | 'compute' | 'visualize' | 'edit'

type CanvasCopy = {
    join: string
    filter: string
    compute: string
    visualize: string
    edit: string
}

type CanvasProps = {
    nodes: PipelineNodeType[]
    edges: PipelineEdge[]
    selectedNodeId?: string | null
    zoom?: number
    copy: CanvasCopy
    onSelectNode?: (id: string | null) => void
    onNodeAction?: (action: ActionKind, nodeId: string) => void
}

const NODE_WIDTH = 240
const NODE_HEIGHT = 70

export const PipelineCanvas = ({
    nodes,
    edges,
    selectedNodeId,
    copy,
    zoom = 1,
    onSelectNode,
    onNodeAction,
}: CanvasProps) => {
    const renderBezier = (x1: number, y1: number, x2: number, y2: number) => {
        const controlPointX1 = x1 + (x2 - x1) / 2
        const controlPointX2 = x2 - (x2 - x1) / 2
        return `M ${x1} ${y1} C ${controlPointX1} ${y1}, ${controlPointX2} ${y2}, ${x2} ${y2}`
    }

    const nodeById = new Map(nodes.map((node) => [node.id, node]))
    const selectedNode = selectedNodeId ? nodeById.get(selectedNodeId) : undefined

    const edgePaths = edges
        .map((edge) => {
            const fromNode = nodeById.get(edge.from)
            const toNode = nodeById.get(edge.to)
            if (!fromNode || !toNode) {
                return null
            }
            const startX = fromNode.x + NODE_WIDTH
            const startY = fromNode.y + NODE_HEIGHT / 2
            const endX = toNode.x
            const endY = toNode.y + NODE_HEIGHT / 2
            return {
                id: edge.id,
                d: renderBezier(startX, startY, endX, endY),
            }
        })
        .filter((edge): edge is { id: string; d: string } => Boolean(edge))

    const actionBarPosition = selectedNode
        ? { left: selectedNode.x + NODE_WIDTH + 18, top: selectedNode.y + 10 }
        : null

    return (
        <div
            className="pipeline-canvas"
            style={{
                position: 'relative',
                width: '100%',
                height: '100%',
                transform: `scale(${zoom})`,
                transformOrigin: 'top left',
            }}
            onClick={() => onSelectNode?.(null)}
        >
            <svg className="pipeline-connections-layer">
                <defs>
                    <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
                        <polygon points="0 0, 10 3.5, 0 7" fill="var(--pipeline-soft)" />
                    </marker>
                </defs>
                {edgePaths.map((edge) => (
                    <path
                        key={edge.id}
                        d={edge.d}
                        fill="none"
                        className="pipeline-connection"
                        markerEnd="url(#arrowhead)"
                    />
                ))}
            </svg>

            {nodes.map((node) => (
                <PipelineNode
                    key={node.id}
                    node={node}
                    selected={node.id === selectedNodeId}
                    onSelect={onSelectNode}
                />
            ))}

            {selectedNode && actionBarPosition ? (
                <div
                    className="node-action-bar"
                    style={actionBarPosition}
                    onClick={(event) => {
                        event.stopPropagation()
                    }}
                >
                    <button
                        type="button"
                        className="node-action-btn"
                        aria-label={copy.join}
                        onClick={(event) => {
                            event.stopPropagation()
                            onNodeAction?.('join', selectedNode.id)
                        }}
                    >
                        <Icon icon="inner-join" size={12} />
                    </button>
                    <button
                        type="button"
                        className="node-action-btn"
                        aria-label={copy.filter}
                        onClick={(event) => {
                            event.stopPropagation()
                            onNodeAction?.('filter', selectedNode.id)
                        }}
                    >
                        <Icon icon="filter" size={12} />
                    </button>
                    <button
                        type="button"
                        className="node-action-btn"
                        aria-label={copy.compute}
                        onClick={(event) => {
                            event.stopPropagation()
                            onNodeAction?.('compute', selectedNode.id)
                        }}
                    >
                        <Icon icon="function" size={12} />
                    </button>
                    <button
                        type="button"
                        className="node-action-btn"
                        aria-label={copy.visualize}
                        onClick={(event) => {
                            event.stopPropagation()
                            onNodeAction?.('visualize', selectedNode.id)
                        }}
                    >
                        <Icon icon="timeline-events" size={12} />
                    </button>
                    <button
                        type="button"
                        className="node-action-btn"
                        aria-label={copy.edit}
                        onClick={(event) => {
                            event.stopPropagation()
                            onNodeAction?.('edit', selectedNode.id)
                        }}
                    >
                        <Icon icon="edit" size={12} />
                    </button>
                </div>
            ) : null}
        </div>
    )
}
