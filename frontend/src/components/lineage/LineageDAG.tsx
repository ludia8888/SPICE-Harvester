import { useCallback, useMemo } from 'react'
import ReactFlow, {
  type Node as FlowNode,
  type Edge,
  Position,
  MarkerType,
  Background,
  Controls,
} from 'reactflow'
import { Icon, Spinner } from '@blueprintjs/core'
import type { LineageNode, LineageEdge } from '../../api/bff'

type LineageDAGProps = {
  nodes: LineageNode[]
  edges: LineageEdge[]
  rootId: string
  isLoading?: boolean
  onNodeClick?: (node: LineageNode) => void
}

const NODE_TYPE_CONFIG: Record<string, { color: string; icon: string }> = {
  source: { color: '#9179f2', icon: 'cloud-upload' },
  dataset: { color: '#137cbd', icon: 'database' },
  pipeline: { color: '#0f9960', icon: 'flow-branch' },
  object: { color: '#d9822b', icon: 'cube' },
  report: { color: '#db3737', icon: 'chart' },
  default: { color: '#8a9ba8', icon: 'document' },
}

const getNodeConfig = (type: string) => {
  return NODE_TYPE_CONFIG[type] || NODE_TYPE_CONFIG.default
}

const buildLayoutPositions = (
  nodes: LineageNode[],
  edges: LineageEdge[],
  rootId: string,
) => {
  const positions = new Map<string, { x: number; y: number }>()
  const levels = new Map<string, number>()
  const adjacencyUp = new Map<string, string[]>()
  const adjacencyDown = new Map<string, string[]>()

  nodes.forEach((node) => {
    adjacencyUp.set(node.id, [])
    adjacencyDown.set(node.id, [])
  })

  edges.forEach((edge) => {
    adjacencyDown.get(edge.source)?.push(edge.target)
    adjacencyUp.get(edge.target)?.push(edge.source)
  })

  // BFS from root to assign levels
  const queue: Array<{ id: string; level: number }> = [{ id: rootId, level: 0 }]
  const visited = new Set<string>()

  while (queue.length > 0) {
    const { id, level } = queue.shift()!
    if (visited.has(id)) continue
    visited.add(id)
    levels.set(id, level)

    // Upstream nodes (negative levels)
    adjacencyUp.get(id)?.forEach((upId) => {
      if (!visited.has(upId)) {
        queue.push({ id: upId, level: level - 1 })
      }
    })

    // Downstream nodes (positive levels)
    adjacencyDown.get(id)?.forEach((downId) => {
      if (!visited.has(downId)) {
        queue.push({ id: downId, level: level + 1 })
      }
    })
  }

  // Group nodes by level
  const levelGroups = new Map<number, string[]>()
  nodes.forEach((node) => {
    const level = levels.get(node.id) ?? 0
    if (!levelGroups.has(level)) {
      levelGroups.set(level, [])
    }
    levelGroups.get(level)?.push(node.id)
  })

  // Assign positions
  const xGap = 250
  const yGap = 100
  const minLevel = Math.min(...Array.from(levels.values()))

  levelGroups.forEach((nodeIds, level) => {
    const x = (level - minLevel) * xGap
    nodeIds.forEach((nodeId, index) => {
      const y = index * yGap - ((nodeIds.length - 1) * yGap) / 2
      positions.set(nodeId, { x, y })
    })
  })

  return positions
}

export const LineageDAG = ({
  nodes,
  edges,
  rootId,
  isLoading = false,
  onNodeClick,
}: LineageDAGProps) => {
  const { flowNodes, flowEdges } = useMemo(() => {
    if (nodes.length === 0) {
      return { flowNodes: [], flowEdges: [] }
    }

    const positions = buildLayoutPositions(nodes, edges, rootId)

    const flowNodes: FlowNode[] = nodes.map((node) => {
      const position = positions.get(node.id) || { x: 0, y: 0 }
      const config = getNodeConfig(node.type)
      const isRoot = node.id === rootId

      return {
        id: node.id,
        position,
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
        data: {
          label: (
            <div className={`lineage-node ${isRoot ? 'is-root' : ''}`}>
              <div
                className="lineage-node-icon"
                style={{ backgroundColor: config.color }}
              >
                <Icon icon={config.icon as never} size={12} />
              </div>
              <div className="lineage-node-content">
                <span className="lineage-node-label">{node.label}</span>
                <span className="lineage-node-type">{node.type}</span>
              </div>
            </div>
          ),
          nodeData: node,
        },
        style: {
          border: isRoot ? '2px solid #48aff0' : '1px solid #394b59',
          borderRadius: '8px',
          background: '#182026',
          padding: 0,
        },
      }
    })

    const flowEdges: Edge[] = edges.map((edge) => ({
      id: edge.id,
      source: edge.source,
      target: edge.target,
      label: edge.label,
      animated: true,
      style: { stroke: '#5c7080' },
      labelStyle: { fontSize: 10, fill: '#8a9ba8' },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        width: 15,
        height: 15,
        color: '#5c7080',
      },
    }))

    return { flowNodes, flowEdges }
  }, [nodes, edges, rootId])

  const handleNodeClick = useCallback(
    (_: unknown, node: FlowNode) => {
      const nodeData = node.data?.nodeData as LineageNode | undefined
      if (nodeData && onNodeClick) {
        onNodeClick(nodeData)
      }
    },
    [onNodeClick],
  )

  if (isLoading) {
    return (
      <div className="lineage-dag-loading">
        <Spinner size={32} />
        <span>계보 정보를 불러오는 중...</span>
      </div>
    )
  }

  if (nodes.length === 0) {
    return (
      <div className="lineage-dag-empty">
        <Icon icon="data-lineage" size={48} />
        <span className="lineage-dag-empty-title">계보 정보가 없습니다</span>
        <span className="lineage-dag-empty-subtitle">
          추적할 항목을 선택하면 데이터 계보가 표시됩니다
        </span>
      </div>
    )
  }

  return (
    <div className="lineage-dag">
      <ReactFlow
        nodes={flowNodes}
        edges={flowEdges}
        onNodeClick={handleNodeClick}
        fitView
        fitViewOptions={{ padding: 0.2, minZoom: 0.5, maxZoom: 1.5 }}
        proOptions={{ hideAttribution: true }}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={true}
      >
        <Background color="#293742" gap={20} />
        <Controls />
      </ReactFlow>
    </div>
  )
}
