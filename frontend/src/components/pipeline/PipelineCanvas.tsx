import { useCallback } from 'react'
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  addEdge,
} from 'reactflow'
import type {
  Node,
  Edge,
  Connection,
  OnNodesChange,
  OnEdgesChange,
  DefaultEdgeOptions,
  NodeMouseHandler,
  OnConnectStart,
  OnConnectEnd,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { pipelineNodeTypes } from './nodes/nodeTypes'

const defaultEdgeOptions: DefaultEdgeOptions = {
  type: 'smoothstep',
  animated: false,
  style: { stroke: '#8a9ba8', strokeWidth: 1.5 },
}

type Props = {
  nodes: Node[]
  edges: Edge[]
  onNodesChange: OnNodesChange
  onEdgesChange: OnEdgesChange
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>
  onNodeClick?: NodeMouseHandler
  interactionMode?: 'pan' | 'select' | 'remove' | 'edit'
  onConnectStart?: OnConnectStart
  onConnectEnd?: OnConnectEnd
}

export const PipelineCanvas = ({
  nodes,
  edges,
  onNodesChange,
  onEdgesChange,
  setEdges,
  onNodeClick,
  interactionMode = 'pan',
  onConnectStart,
  onConnectEnd,
}: Props) => {
  const onConnect = useCallback(
    (params: Connection) => {
      setEdges((eds) => addEdge(params, eds))
    },
    [setEdges],
  )

  const isValidConnection = useCallback(
    (connection: Connection) => {
      // Prevent self-connections
      if (connection.source === connection.target) return false
      // Prevent duplicate connections
      const exists = edges.some(
        (e) =>
          e.source === connection.source &&
          e.target === connection.target &&
          e.sourceHandle === connection.sourceHandle &&
          e.targetHandle === connection.targetHandle,
      )
      if (exists) return false
      return true
    },
    [edges],
  )

  const modeClass = interactionMode === 'pan' ? 'is-pan' : interactionMode === 'select' ? 'is-select' : interactionMode === 'remove' ? 'is-remove' : interactionMode === 'edit' ? 'is-edit' : ''

  return (
    <div className={`pipeline-reactflow ${modeClass}`}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={onNodeClick}
        onConnectStart={onConnectStart}
        onConnectEnd={onConnectEnd}
        isValidConnection={isValidConnection}
        nodeTypes={pipelineNodeTypes}
        defaultEdgeOptions={defaultEdgeOptions}
        fitView
        panOnDrag={interactionMode === 'pan'}
        selectionOnDrag={interactionMode === 'select'}
        deleteKeyCode={['Backspace', 'Delete']}
        minZoom={0.2}
        maxZoom={2}
        snapToGrid
        snapGrid={[16, 16]}
      >
        <Background gap={16} size={1} />
        <Controls showInteractive={false} className="pipeline-viewport-controls" />
        <MiniMap
          className="pipeline-minimap"
          nodeStrokeWidth={3}
          zoomable
          pannable
        />
      </ReactFlow>
    </div>
  )
}
