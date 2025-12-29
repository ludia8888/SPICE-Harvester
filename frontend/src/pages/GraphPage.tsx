import { useCallback, useMemo, useState } from 'react'
import { Button, Icon } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'
import ReactFlow, {
  addEdge,
  type Connection,
  type Edge,
  type Node,
  type ReactFlowInstance,
  useEdgesState,
  useNodesState,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { useAppStore } from '../state/store'

type ToolMode = 'pan' | 'pointer' | 'select' | 'remove'

const previewDatasetName = 'Clean Facility Data'

const previewColumns: Array<{ key: string; label: string; type: string; icon: IconName }> = [
  { key: 'id', label: 'id', type: 'String', icon: 'font' },
  { key: 'address', label: 'address', type: 'String', icon: 'font' },
  { key: 'city', label: 'city', type: 'String', icon: 'font' },
  { key: 'zip', label: 'zip', type: 'String', icon: 'font' },
  { key: 'latitude', label: 'latitude', type: 'Double', icon: 'numerical' },
  { key: 'longitude', label: 'longitude', type: 'Double', icon: 'numerical' },
]

const previewRows = [
  {
    id: '0022093277',
    address: '1100 SO. AKERS STREET',
    city: 'VISALIA',
    zip: '93277',
    latitude: '36.320843946',
    longitude: '-119.292350112',
  },
  {
    id: '0013177954',
    address: '1100 EAST LOOP 304',
    city: 'CROCKETT',
    zip: '77954',
    latitude: '31.33260364',
    longitude: '-94.78860542',
  },
  {
    id: '0015371373',
    address: '209 FRONT ST.',
    city: 'VIDALIA',
    zip: '71373',
    latitude: '31.561174863',
    longitude: '-91.421041997',
  },
  {
    id: '0072590723',
    address: '16453 SOUTH COLORADO AVE',
    city: 'PARAMOUNT',
    zip: '90723',
    latitude: '33.884631861',
    longitude: '-118.15974012',
  },
  {
    id: '0196500984',
    address: 'CALLE FERNANDEZ JUNCO',
    city: 'CAROLINA',
    zip: '00984',
    latitude: '18.380443',
    longitude: '-65.984141',
  },
]

export const GraphPage = () => {
  const pipelineContext = useAppStore((state) => state.pipelineContext)
  const [activeTab, setActiveTab] = useState<'edit' | 'proposals' | 'history'>('edit')
  const [toolMode, setToolMode] = useState<ToolMode>('pointer')
  const [isRightPanelOpen, setRightPanelOpen] = useState(false)
  const [isBottomPanelOpen, setBottomPanelOpen] = useState(false)
  const [columnSearch, setColumnSearch] = useState('')
  const [activeColumn, setActiveColumn] = useState(previewColumns[0]?.key ?? '')
  const [reactFlowInstance, setReactFlowInstance] = useState<ReactFlowInstance | null>(null)
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
  const isViewportReady = Boolean(reactFlowInstance)
  const filteredColumns = useMemo(() => {
    const query = columnSearch.trim().toLowerCase()
    if (!query) {
      return previewColumns
    }
    return previewColumns.filter((column) => column.label.toLowerCase().includes(query))
  }, [columnSearch])
  const tableGridTemplate = useMemo(() => {
    return `48px repeat(${previewColumns.length}, minmax(160px, 1fr))`
  }, [])

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
      if (!isSelectableMode) {
        return
      }
      setBottomPanelOpen(true)
      return
    }
    setNodes((current) => current.filter((item) => item.id !== node.id))
    setEdges((current) => current.filter((item) => item.source !== node.id && item.target !== node.id))
  }, [isRemoveMode, isSelectableMode, setNodes, setEdges])

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

  const handleZoomIn = useCallback(() => {
    reactFlowInstance?.zoomIn()
  }, [reactFlowInstance])

  const handleZoomOut = useCallback(() => {
    reactFlowInstance?.zoomOut()
  }, [reactFlowInstance])

  const handleFitView = useCallback(() => {
    reactFlowInstance?.fitView({ padding: 0.1 })
  }, [reactFlowInstance])

  const handleCloseBottomPanel = useCallback(() => {
    setBottomPanelOpen(false)
  }, [])

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
        <Button
          minimal
          icon="grid-view"
          small
          className={`pipeline-bottom-panel-toggle ${isBottomPanelOpen ? 'is-active' : ''}`}
          onClick={() => setBottomPanelOpen((open) => !open)}
          aria-pressed={isBottomPanelOpen}
        />
        <Button
          minimal
          icon="panel-table"
          small
          className={`pipeline-panel-toggle ${isRightPanelOpen ? 'is-active' : ''}`}
          onClick={() => setRightPanelOpen((open) => !open)}
          aria-pressed={isRightPanelOpen}
        />
      </div>
    </div>
  )

  return (
    <div className="page pipeline-page">
      {topbar}
      <div className="pipeline-canvas">
        <div className="pipeline-canvas-inner">
          <div className="pipeline-canvas-stage">
            <div className={canvasClassName}>
              <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onConnect={handleConnect}
                onNodeClick={handleNodeClick}
                onEdgeClick={handleEdgeClick}
                onInit={setReactFlowInstance}
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
              <div className="pipeline-viewport-controls">
                <button
                  type="button"
                  className="pipeline-viewport-button"
                  onClick={handleZoomIn}
                  disabled={!isViewportReady}
                  aria-label="Zoom in"
                >
                  <Icon icon="zoom-in" size={18} />
                </button>
                <button
                  type="button"
                  className="pipeline-viewport-button"
                  onClick={handleZoomOut}
                  disabled={!isViewportReady}
                  aria-label="Zoom out"
                >
                  <Icon icon="zoom-out" size={18} />
                </button>
                <button
                  type="button"
                  className="pipeline-viewport-button"
                  onClick={handleFitView}
                  disabled={!isViewportReady}
                  aria-label="Fit view"
                >
                  <Icon icon="zoom-to-fit" size={18} />
                </button>
              </div>
            </div>
            <div className={`pipeline-bottom-panel ${isBottomPanelOpen ? 'is-open' : ''}`}>
              <div className="pipeline-bottom-panel-header">
                <div className="pipeline-bottom-panel-title">
                  <Icon icon="grid-view" size={14} />
                  <span>Data preview</span>
                </div>
                <button
                  type="button"
                  className="pipeline-bottom-panel-collapse"
                  onClick={handleCloseBottomPanel}
                  aria-label="Close preview"
                >
                  <Icon icon="chevron-down" size={16} />
                </button>
              </div>
              <div className="pipeline-bottom-panel-body">
                <div className="pipeline-preview">
                  <div className="pipeline-preview-sidebar">
                    <button type="button" className="pipeline-preview-selector">
                      <div className="pipeline-preview-selector-left">
                        <Icon icon="database" size={14} />
                        <span>{previewDatasetName}</span>
                      </div>
                      <Icon icon="chevron-down" size={14} />
                    </button>
                    <label className="pipeline-preview-search">
                      <Icon icon="search" size={14} />
                      <input
                        className="pipeline-preview-input"
                        value={columnSearch}
                        onChange={(event) => setColumnSearch(event.target.value)}
                        placeholder="Search 27 columns..."
                      />
                    </label>
                    <div className="pipeline-preview-columns">
                      {filteredColumns.length === 0 ? (
                        <div className="pipeline-preview-empty">No matching columns.</div>
                      ) : (
                        filteredColumns.map((column) => (
                          <button
                            key={column.key}
                            type="button"
                            className={`pipeline-preview-column ${activeColumn === column.key ? 'is-active' : ''}`}
                            onClick={() => setActiveColumn(column.key)}
                          >
                            <div className="pipeline-preview-column-left">
                              <Icon icon={column.icon} size={14} className="pipeline-preview-column-icon" />
                              <span>{column.label}</span>
                            </div>
                            <Icon icon="drag-handle-vertical" size={14} className="pipeline-preview-column-menu" />
                          </button>
                        ))
                      )}
                    </div>
                  </div>
                  <div className="pipeline-preview-table">
                    <div className="pipeline-preview-table-scroll">
                      <div className="pipeline-preview-table-header" style={{ gridTemplateColumns: tableGridTemplate }}>
                        <div className="pipeline-preview-table-cell is-index">#</div>
                        {previewColumns.map((column) => (
                          <div
                            key={column.key}
                            className={`pipeline-preview-table-cell is-header ${activeColumn === column.key ? 'is-active' : ''}`}
                          >
                            <div className="pipeline-preview-header-main">
                              <span>{column.label}</span>
                              <Icon icon="caret-down" size={12} />
                            </div>
                            <span className="pipeline-preview-header-type">{column.type}</span>
                          </div>
                        ))}
                      </div>
                      {previewRows.map((row, index) => (
                        <div
                          key={row.id}
                          className="pipeline-preview-table-row"
                          style={{ gridTemplateColumns: tableGridTemplate }}
                        >
                          <div className="pipeline-preview-table-cell is-index">{index + 1}</div>
                          {previewColumns.map((column) => (
                            <div
                              key={column.key}
                              className={`pipeline-preview-table-cell ${activeColumn === column.key ? 'is-active' : ''}`}
                            >
                              {row[column.key as keyof typeof row]}
                            </div>
                          ))}
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <aside className={`pipeline-right-panel ${isRightPanelOpen ? 'is-open' : ''}`}>
            <div className="pipeline-right-panel-header">
              <span className="pipeline-right-panel-title">Panel</span>
              <button
                type="button"
                className="pipeline-right-panel-close"
                onClick={() => setRightPanelOpen(false)}
                aria-label="Close panel"
              >
                <Icon icon="cross" size={12} />
              </button>
            </div>
            <div className="pipeline-right-panel-body">
              <span className="pipeline-right-panel-empty">Select a node to view details.</span>
            </div>
          </aside>
        </div>
      </div>
    </div>
  )
}
