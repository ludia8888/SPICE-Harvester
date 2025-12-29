import { useCallback, useMemo, useState } from 'react'
import { Button, Icon } from '@blueprintjs/core'
import ReactFlow, {
  addEdge,
  type Connection,
  type Edge,
  type Node,
  useEdgesState,
  useNodesState,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { useAppStore } from '../state/store'

type ToolMode = 'pan' | 'pointer' | 'select' | 'remove'

export const GraphPage = () => {
  const pipelineContext = useAppStore((state) => state.pipelineContext)
  const [activeTab, setActiveTab] = useState<'edit' | 'proposals' | 'history'>('edit')
  const [toolMode, setToolMode] = useState<ToolMode>('pointer')
  const [nodes, setNodes, onNodesChange] = useNodesState<Node[]>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge[]>([])
  const pipelineDisplayName = pipelineContext?.folderName || 'Pipeline Builder'
  const pipelineType = 'batch'
  const actionsDisabled = true
  const isPanMode = toolMode === 'pan'
  const isPointerMode = toolMode === 'pointer'
  const isSelectMode = toolMode === 'select'
  const isRemoveMode = toolMode === 'remove'
  const isSelectableMode = isPointerMode || isSelectMode

  const canvasClassName = useMemo(() => {
    if (isPanMode) {
      return 'pipeline-reactflow is-pan'
    }
    if (isSelectMode) {
      return 'pipeline-reactflow is-select'
    }
    if (isRemoveMode) {
      return 'pipeline-reactflow is-remove'
    }
    return 'pipeline-reactflow'
  }, [isPanMode, isSelectMode, isRemoveMode])

  const handleConnect = useCallback((connection: Connection) => {
    setEdges((current) => addEdge(connection, current))
  }, [setEdges])

  const handleNodeClick = useCallback((_: unknown, node: Node) => {
    if (!isRemoveMode) {
      return
    }
    setNodes((current) => current.filter((item) => item.id !== node.id))
    setEdges((current) => current.filter((item) => item.source !== node.id && item.target !== node.id))
  }, [isRemoveMode, setNodes, setEdges])

  const handleEdgeClick = useCallback((_: unknown, edge: Edge) => {
    if (!isRemoveMode) {
      return
    }
    setEdges((current) => current.filter((item) => item.id !== edge.id))
  }, [isRemoveMode, setEdges])

  const handleLayout = useCallback(() => {
    if (nodes.length === 0) {
      return
    }

    const nodeIds = nodes.map((node) => node.id)
    const incoming = new Map(nodeIds.map((id) => [id, 0]))
    const adjacency = new Map(nodeIds.map((id) => [id, [] as string[]]))

    edges.forEach((edge) => {
      if (!adjacency.has(edge.source)) {
        adjacency.set(edge.source, [])
      }
      adjacency.get(edge.source)?.push(edge.target)
      incoming.set(edge.target, (incoming.get(edge.target) ?? 0) + 1)
    })

    const queue: string[] = nodeIds.filter((id) => (incoming.get(id) ?? 0) === 0)
    const levels = new Map<string, number>()

    while (queue.length > 0) {
      const current = queue.shift()
      if (!current) {
        continue
      }
      const currentLevel = levels.get(current) ?? 0
      const neighbors = adjacency.get(current) ?? []
      neighbors.forEach((next) => {
        const nextLevel = Math.max(levels.get(next) ?? 0, currentLevel + 1)
        levels.set(next, nextLevel)
        incoming.set(next, (incoming.get(next) ?? 0) - 1)
        if ((incoming.get(next) ?? 0) <= 0) {
          queue.push(next)
        }
      })
    }

    const grouped = new Map<number, string[]>()
    nodeIds.forEach((id) => {
      const level = levels.get(id) ?? 0
      if (!grouped.has(level)) {
        grouped.set(level, [])
      }
      grouped.get(level)?.push(id)
    })

    const xGap = 260
    const yGap = 120

    setNodes((current) =>
      current.map((node) => {
        const level = levels.get(node.id) ?? 0
        const group = grouped.get(level) ?? []
        const index = group.indexOf(node.id)
        return {
          ...node,
          position: {
            x: level * xGap,
            y: index * yGap,
          },
        }
      }),
    )
  }, [nodes, edges, setNodes])

  const topbar = (
    <div className="pipeline-topbar">
      <div className="pipeline-topbar-left">
        <div className="pipeline-header-details">
          <div className="pipeline-title-row">
            <div className="pipeline-breadcrumb">
              <Icon icon="folder-close" size={14} className="pipeline-breadcrumb-icon" />
              <span className="pipeline-breadcrumb-text">Pipeline Builder</span>
            </div>
            <Icon icon="chevron-right" className="pipeline-breadcrumb-separator" size={14} />
            <div className="pipeline-name-wrapper">
              <span className="pipeline-name">{pipelineDisplayName}</span>
              <Button minimal small icon="star-empty" className="pipeline-star" disabled={actionsDisabled} />
            </div>
          </div>

          <div className="pipeline-menu-row">
            <div className="pipeline-menu">
              <Button minimal text="File" rightIcon="caret-down" small />
              <Button minimal text="Settings" rightIcon="caret-down" small />
              <Button minimal text="Help" rightIcon="caret-down" small />
            </div>
            <div className="pipeline-divider" />
            <div className="pipeline-info-item">
              <Icon icon="office" size={12} />
              <span>1</span>
            </div>
            <div className="pipeline-divider" />
            <div className="pipeline-batch-badge">
              <span>{pipelineType === 'batch' ? 'Batch' : 'Streaming'}</span>
            </div>
          </div>
        </div>
      </div>
      <div className="pipeline-topbar-center">
        <div className="pipeline-tabs">
          <Button
            minimal
            className={activeTab === 'edit' ? 'pipeline-tab-active' : ''}
            onClick={() => setActiveTab('edit')}
            text="Edit"
          />
          <Button
            minimal
            className={activeTab === 'proposals' ? 'pipeline-tab-active' : ''}
            onClick={() => setActiveTab('proposals')}
            text="Proposals"
          />
          <Button
            minimal
            className={activeTab === 'history' ? 'pipeline-tab-active' : ''}
            onClick={() => setActiveTab('history')}
            text="History"
          />
        </div>
      </div>
      <div className="pipeline-topbar-right">
        <div className="pipeline-history-controls">
          <Button minimal icon="undo" disabled={actionsDisabled} small />
          <Button minimal icon="redo" disabled={actionsDisabled} small />
        </div>

        <div className="pipeline-divider" />

        <div className="pipeline-branch-selector">
          <Icon icon="lock" size={12} className="pipeline-branch-icon" />
          <span className="pipeline-branch-name">Main</span>
          <Icon icon="caret-down" size={12} />
        </div>

        <Button
          className="pipeline-action-btn"
          intent="success"
          icon="floppy-disk"
          text="Save"
          small
          disabled={actionsDisabled}
        />
        <Button
          className="pipeline-action-btn"
          intent="primary"
          icon="git-pull"
          text="Propose"
          outlined
          small
          disabled={actionsDisabled}
        />
        <div className="pipeline-deploy-group">
          <Button
            className="pipeline-deploy-btn"
            intent="primary"
            text="Deploy"
            small
            disabled={actionsDisabled}
          />
          <Button
            className="pipeline-deploy-options"
            intent="primary"
            icon="settings"
            small
            disabled={actionsDisabled}
          />
        </div>

        <div className="pipeline-divider" />

        <Button minimal icon="share" text="Share" small disabled={actionsDisabled} />
        <Button minimal icon="menu" small />
      </div>
    </div>
  )

  return (
    <div className="page pipeline-page">
      {topbar}
      <div className="pipeline-canvas">
        <div className={canvasClassName}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={handleConnect}
            onNodeClick={handleNodeClick}
            onEdgeClick={handleEdgeClick}
            panOnDrag={isPanMode}
            nodesDraggable={isPointerMode}
            nodesConnectable={isPointerMode}
            elementsSelectable={isSelectableMode}
            selectionOnDrag={isSelectMode}
            selectNodesOnDrag={isSelectMode}
            selectionKeyCode={isSelectMode ? 'Shift' : null}
            multiSelectionKeyCode={isSelectMode ? ['Meta', 'Control'] : null}
            deleteKeyCode={['Backspace', 'Delete']}
            fitView
            proOptions={{ hideAttribution: true }}
          />
          <div className="pipeline-toolbox">
            <div className="pipeline-toolbox-group">
              <div className="pipeline-toolbox-button-group">
                <button
                  type="button"
                  className={`pipeline-tool-button ${isPanMode ? 'is-active' : ''}`}
                  onClick={() => setToolMode('pan')}
                  aria-pressed={isPanMode}
                >
                  <Icon icon="move" size={16} />
                </button>
                <button
                  type="button"
                  className={`pipeline-tool-button ${isPointerMode ? 'is-active' : ''}`}
                  onClick={() => setToolMode('pointer')}
                  aria-pressed={isPointerMode}
                >
                  <Icon icon="select" size={16} />
                </button>
              </div>
              <span className="pipeline-toolbox-label">Tools</span>
            </div>
            <div className="pipeline-toolbox-group">
              <button
                type="button"
                className={`pipeline-tool-button ${isSelectMode ? 'is-active' : ''}`}
                onClick={() => setToolMode('select')}
                aria-pressed={isSelectMode}
              >
                <Icon icon="locate" size={16} />
              </button>
              <span className="pipeline-toolbox-label">Select</span>
            </div>
            <div className="pipeline-toolbox-group">
              <button
                type="button"
                className={`pipeline-tool-button ${isRemoveMode ? 'is-active' : ''}`}
                onClick={() => setToolMode('remove')}
                aria-pressed={isRemoveMode}
              >
                <Icon icon="small-cross" size={16} />
              </button>
              <span className="pipeline-toolbox-label">Remove</span>
            </div>
            <div className="pipeline-toolbox-group">
              <button
                type="button"
                className="pipeline-tool-button"
                onClick={handleLayout}
              >
                <Icon icon="layout-hierarchy" size={16} />
              </button>
              <span className="pipeline-toolbox-label">Layout</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
