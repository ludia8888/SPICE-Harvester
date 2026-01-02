import { useCallback, useEffect, useMemo, useState } from 'react'
import { Button, Icon, Menu, MenuDivider, MenuItem, Popover } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import ReactFlow, {
  addEdge,
  type Connection,
  type Edge,
  type EdgeChange,
  type Node as FlowNode,
  type NodeChange,
  type ReactFlowInstance,
  useEdgesState,
  useNodesState,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { useAppStore } from '../state/store'
import {
  deployPipeline,
  getPipeline,
  getPipelineReadiness,
  listDatabases,
  listDatasets,
  listPipelineArtifacts,
  listPipelines,
  submitPipelineProposal,
  updatePipeline,
  type DatabaseRecord,
  type DatasetRecord,
  type PipelineArtifactRecord,
  type PipelineDetailRecord,
  type PipelineRecord,
  type PipelineReadiness,
} from '../api/bff'

type ToolMode = 'pan' | 'pointer' | 'select' | 'remove'

type PreviewColumn = {
  key: string
  label: string
  type: string
  icon: IconName
  width: number
}
type PreviewRow = Record<string, string>

type SchemaColumn = {
  name: string
  type?: string
}

type DefinitionNode = {
  id?: string
  type?: string
  metadata?: Record<string, unknown>
}

type DefinitionEdge = {
  id?: string
  from?: string
  to?: string
  source?: string
  target?: string
}

type FolderOption = {
  id: string
  name: string
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

const parseTimestamp = (value?: string) => {
  if (!value) {
    return 0
  }
  const parsed = Date.parse(value)
  return Number.isNaN(parsed) ? 0 : parsed
}

const formatCellValue = (value: unknown) => {
  if (value === null || value === undefined) {
    return ''
  }
  if (typeof value === 'string') {
    return value
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value)
  }
  if (value instanceof Date) {
    return value.toISOString()
  }
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

const extractSchemaColumns = (schemaJson?: Record<string, unknown>): SchemaColumn[] => {
  if (!schemaJson) {
    return []
  }
  const rawColumns = schemaJson.columns
  if (!Array.isArray(rawColumns)) {
    return []
  }
  return rawColumns
    .map((column) => {
      if (typeof column === 'string') {
        return { name: column }
      }
      if (isRecord(column)) {
        const name =
          typeof column.name === 'string'
            ? column.name
            : typeof column.column === 'string'
              ? column.column
              : ''
        const type = typeof column.type === 'string' ? column.type : undefined
        return { name, type }
      }
      return null
    })
    .filter((column): column is SchemaColumn => Boolean(column && column.name))
}

const extractSampleRows = (sampleJson?: Record<string, unknown>): Record<string, unknown>[] => {
  if (!sampleJson) {
    return []
  }
  const rows = sampleJson.rows
  if (Array.isArray(rows)) {
    return rows.filter(isRecord)
  }
  return []
}

const extractSampleColumns = (
  sampleJson: Record<string, unknown> | undefined,
  sampleRows: Record<string, unknown>[],
) => {
  if (sampleJson && Array.isArray(sampleJson.columns)) {
    return sampleJson.columns
      .map((column) => {
        if (typeof column === 'string') {
          return column
        }
        if (isRecord(column) && typeof column.name === 'string') {
          return column.name
        }
        return ''
      })
      .filter((column) => column)
  }

  if (sampleRows.length === 0) {
    return []
  }

  const keys = new Set<string>()
  sampleRows.forEach((row) => {
    Object.keys(row).forEach((key) => keys.add(key))
  })
  return Array.from(keys)
}

const normalizeTypeLabel = (value: string) => {
  const lower = value.toLowerCase()
  if (lower.includes('int')) {
    return 'Integer'
  }
  if (lower.includes('double') || lower.includes('float') || lower.includes('decimal') || lower.includes('number')) {
    return 'Number'
  }
  if (lower.includes('bool')) {
    return 'Boolean'
  }
  if (lower.includes('date') || lower.includes('time')) {
    return 'Datetime'
  }
  return value
}

const inferColumnType = (key: string, rows: Record<string, unknown>[]) => {
  for (const row of rows) {
    const value = row[key]
    if (value === null || value === undefined) {
      continue
    }
    if (typeof value === 'number') {
      return 'Number'
    }
    if (typeof value === 'boolean') {
      return 'Boolean'
    }
    if (Array.isArray(value)) {
      return 'Array'
    }
    if (typeof value === 'object') {
      return 'Object'
    }
    return 'String'
  }
  return 'String'
}

const selectColumnIcon = (type: string): IconName => {
  const lower = type.toLowerCase()
  if (lower.includes('int') || lower.includes('double') || lower.includes('float') || lower.includes('decimal')) {
    return 'numerical'
  }
  if (lower.includes('number')) {
    return 'numerical'
  }
  return 'font'
}

const estimateColumnWidth = (label: string) => {
  const width = Math.max(120, label.length * 12)
  return Math.min(320, width)
}

const buildPreviewFromDataset = (dataset: DatasetRecord | null) => {
  if (!dataset) {
    return { columns: [] as PreviewColumn[], rows: [] as PreviewRow[] }
  }
  const sampleJson = dataset.sample_json ?? {}
  const schemaJson = dataset.schema_json ?? {}
  const sampleRows = extractSampleRows(sampleJson)
  const schemaColumns = extractSchemaColumns(schemaJson)
  const sampleColumns = extractSampleColumns(sampleJson, sampleRows)
  const schemaNames = new Set(schemaColumns.map((column) => column.name))
  const mergedColumns = [...schemaColumns]
  sampleColumns.forEach((name) => {
    if (!schemaNames.has(name)) {
      mergedColumns.push({ name })
    }
  })
  const columns = mergedColumns.map((column) => {
    const typeLabel = column.type ? normalizeTypeLabel(column.type) : inferColumnType(column.name, sampleRows)
    return {
      key: column.name,
      label: column.name,
      type: typeLabel,
      icon: selectColumnIcon(typeLabel),
      width: estimateColumnWidth(column.name),
    }
  })
  const rows = sampleRows.slice(0, 25).map((row) => {
    const formatted: PreviewRow = {}
    columns.forEach((column) => {
      formatted[column.key] = formatCellValue(row[column.key])
    })
    return formatted
  })
  return { columns, rows }
}

const buildNodeLabel = (node: DefinitionNode) => {
  const metadata = node.metadata
  if (metadata && typeof metadata === 'object' && !Array.isArray(metadata)) {
    if (typeof metadata.datasetName === 'string') {
      return metadata.datasetName
    }
    if (typeof metadata.dataset_name === 'string') {
      return metadata.dataset_name
    }
    if (typeof metadata.outputName === 'string') {
      return metadata.outputName
    }
    if (typeof metadata.output_dataset_name === 'string') {
      return metadata.output_dataset_name
    }
    if (typeof metadata.name === 'string') {
      return metadata.name
    }
  }
  if (node.type) {
    return node.type
  }
  return node.id ?? 'node'
}

const buildLayoutPositions = (nodeIds: string[], edges: Edge[]) => {
  const incoming = new Map(nodeIds.map((id) => [id, 0]))
  const adjacency = new Map(nodeIds.map((id) => [id, [] as string[]]))

  edges.forEach((edge) => {
    if (!adjacency.has(edge.source)) {
      adjacency.set(edge.source, [])
    }
    adjacency.get(edge.source)?.push(edge.target)
    incoming.set(edge.target, (incoming.get(edge.target) ?? 0) + 1)
  })

  const queue = nodeIds.filter((id) => (incoming.get(id) ?? 0) === 0)
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
  const positions = new Map<string, { x: number; y: number }>()

  nodeIds.forEach((id) => {
    const level = levels.get(id) ?? 0
    const group = grouped.get(level) ?? []
    const index = group.indexOf(id)
    positions.set(id, { x: level * xGap, y: index * yGap })
  })

  return positions
}

const layoutFlowNodes = (nodes: FlowNode[], edges: Edge[]) => {
  if (nodes.length === 0) {
    return nodes
  }
  const nodeIds = nodes.map((node) => node.id)
  const positions = buildLayoutPositions(nodeIds, edges)
  return nodes.map((node) => ({
    ...node,
    position: positions.get(node.id) ?? node.position,
  }))
}

const buildFlowFromDefinition = (definition: PipelineDetailRecord['definition_json']) => {
  if (!definition || typeof definition !== 'object') {
    return { nodes: [] as FlowNode[], edges: [] as Edge[] }
  }
  const rawNodes = Array.isArray(definition.nodes) ? definition.nodes : []
  const rawEdges = Array.isArray(definition.edges) ? definition.edges : []

  const nodes = rawNodes
    .map((node, index): FlowNode | null => {
      if (!isRecord(node) || typeof node.id !== 'string') {
        return null
      }
      const typedNode = node as DefinitionNode
      const flowNode: FlowNode = {
        id: node.id,
        data: { label: buildNodeLabel(typedNode) },
        position: { x: 0, y: index * 120 },
      }
      if (typedNode.type === 'input' || typedNode.type === 'output') {
        flowNode.type = typedNode.type
      }
      return flowNode
    })
    .filter((node): node is FlowNode => node !== null)

  const nodeIds = new Set(nodes.map((node) => node.id))
  const edges = rawEdges
    .map((edge, index) => {
      if (!isRecord(edge)) {
        return null
      }
      const typedEdge = edge as DefinitionEdge
      const source = typedEdge.from ?? typedEdge.source
      const target = typedEdge.to ?? typedEdge.target
      if (!source || !target || !nodeIds.has(source) || !nodeIds.has(target)) {
        return null
      }
      return {
        id: typedEdge.id ?? `${source}-${target}-${index}`,
        source,
        target,
      } satisfies Edge
    })
    .filter((edge): edge is Edge => Boolean(edge))

  return { nodes, edges }
}

const normalizeFolder = (record: DatabaseRecord | string): FolderOption | null => {
  if (typeof record === 'string') {
    return { id: record, name: record }
  }
  const id = record.name || record.db_name || record.id
  if (!id) {
    return null
  }
  return {
    id,
    name: record.display_name || record.label || id,
  }
}

const extractDefinitionNodes = (definition: PipelineDetailRecord['definition_json']) => {
  if (!definition || typeof definition !== 'object') {
    return [] as Array<Record<string, unknown>>
  }
  const nodes = (definition as Record<string, unknown>).nodes
  if (!Array.isArray(nodes)) {
    return [] as Array<Record<string, unknown>>
  }
  return nodes.filter((node): node is Record<string, unknown> => isRecord(node))
}

const extractDefinitionEdges = (definition: PipelineDetailRecord['definition_json']) => {
  if (!definition || typeof definition !== 'object') {
    return [] as Array<Record<string, unknown>>
  }
  const edges = (definition as Record<string, unknown>).edges
  if (!Array.isArray(edges)) {
    return [] as Array<Record<string, unknown>>
  }
  return edges.filter((edge): edge is Record<string, unknown> => isRecord(edge))
}

const getDefinitionEdgeKey = (from: string, to: string) => `${from}::${to}`

const resolveEdgeEndpoints = (edge: Record<string, unknown>) => {
  const from =
    typeof edge.from === 'string'
      ? edge.from
      : typeof edge.source === 'string'
        ? edge.source
        : ''
  const to =
    typeof edge.to === 'string'
      ? edge.to
      : typeof edge.target === 'string'
        ? edge.target
        : ''
  return { from, to }
}

const buildDefinitionDraft = (
  definition: PipelineDetailRecord['definition_json'],
  nodes: FlowNode[],
  edges: Edge[],
) => {
  const base = isRecord(definition) ? { ...definition } : {}
  const baseNodes = extractDefinitionNodes(definition)
  const baseEdges = extractDefinitionEdges(definition)
  const baseNodesById = new Map<string, Record<string, unknown>>()
  baseNodes.forEach((node) => {
    const id = typeof node.id === 'string' ? node.id : ''
    if (id) {
      baseNodesById.set(id, node)
    }
  })
  const baseEdgesByKey = new Map<string, Record<string, unknown>>()
  baseEdges.forEach((edge) => {
    const { from, to } = resolveEdgeEndpoints(edge)
    if (from && to) {
      baseEdgesByKey.set(getDefinitionEdgeKey(from, to), edge)
    }
  })

  const nextNodes = nodes.map((node) => {
    const existing = baseNodesById.get(node.id)
    if (existing) {
      return existing
    }
    const fresh: Record<string, unknown> = { id: node.id }
    if (node.type) {
      fresh.type = node.type
    }
    return fresh
  })

  const nextEdges = edges.map((edge) => {
    const key = getDefinitionEdgeKey(edge.source, edge.target)
    const existing = baseEdgesByKey.get(key)
    if (existing) {
      return existing
    }
    return { from: edge.source, to: edge.target }
  })

  return { ...base, nodes: nextNodes, edges: nextEdges }
}

const extractOutputNodeIds = (definition: PipelineDetailRecord['definition_json']) => {
  const nodes = extractDefinitionNodes(definition)
  return nodes
    .filter((node) => String(node.type || '').toLowerCase() === 'output')
    .map((node) => String(node.id || '').trim())
    .filter((id) => id)
}

export const GraphPage = () => {
  const queryClient = useQueryClient()
  const pipelineContext = useAppStore((state) => state.pipelineContext)
  const setPipelineContext = useAppStore((state) => state.setPipelineContext)
  const [activeTab, setActiveTab] = useState<'edit' | 'proposals' | 'history'>('edit')
  const [toolMode, setToolMode] = useState<ToolMode>('pointer')
  const [isRightPanelOpen, setRightPanelOpen] = useState(true)
  const [isBottomPanelOpen, setBottomPanelOpen] = useState(true)
  const [columnSearch, setColumnSearch] = useState('')
  const [previewColumns, setPreviewColumns] = useState<PreviewColumn[]>([])
  const [previewRows, setPreviewRows] = useState<PreviewRow[]>([])
  const [activeColumn, setActiveColumn] = useState<string>('')
  const [draggedColumnKey, setDraggedColumnKey] = useState<string | null>(null)
  const [dragOverColumnKey, setDragOverColumnKey] = useState<string | null>(null)
  const [activeOutputTab, setActiveOutputTab] = useState<'datasets' | 'objectTypes' | 'linkTypes'>('datasets')
  const [reactFlowInstance, setReactFlowInstance] = useState<ReactFlowInstance | null>(null)
  const [nodes, setNodes, onNodesChange] = useNodesState<FlowNode[]>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge[]>([])
  const [definitionDirty, setDefinitionDirty] = useState(false)
  const [isSaving, setIsSaving] = useState(false)
  const [isProposing, setIsProposing] = useState(false)
  const [isDeploying, setIsDeploying] = useState(false)
  const [isCheckingReadiness, setIsCheckingReadiness] = useState(false)
  const activeDbName = pipelineContext?.folderId ?? ''
  const { data: databases = [] } = useQuery({
    queryKey: ['databases'],
    queryFn: listDatabases,
  })
  const { data: datasets = [] } = useQuery({
    queryKey: ['datasets', activeDbName],
    queryFn: () => listDatasets(activeDbName),
    enabled: Boolean(activeDbName),
  })
  const { data: pipelines = [] } = useQuery({
    queryKey: ['pipelines', activeDbName],
    queryFn: () => listPipelines(activeDbName),
    enabled: Boolean(activeDbName),
  })
  const primaryPipeline = useMemo<PipelineRecord | null>(() => {
    if (pipelines.length === 0) {
      return null
    }
    const sorted = [...pipelines].sort((left, right) => parseTimestamp(right.updated_at) - parseTimestamp(left.updated_at))
    return sorted[0] ?? null
  }, [pipelines])
  const { data: pipelineDetail = null } = useQuery({
    queryKey: ['pipeline-detail', primaryPipeline?.pipeline_id],
    queryFn: () => getPipeline(primaryPipeline?.pipeline_id ?? '', { dbName: activeDbName }),
    enabled: Boolean(primaryPipeline?.pipeline_id),
  })
  const { data: pipelineArtifacts = [] } = useQuery({
    queryKey: ['pipeline-artifacts', primaryPipeline?.pipeline_id, 'build'],
    queryFn: () =>
      listPipelineArtifacts(primaryPipeline?.pipeline_id ?? '', { mode: 'build', limit: 50, dbName: activeDbName }),
    enabled: Boolean(primaryPipeline?.pipeline_id),
  })
  const pipelineFolderLabel =
    pipelineContext?.folderName || primaryPipeline?.db_name || pipelineDetail?.db_name || 'Pipeline Builder'
  const pipelineDisplayName = primaryPipeline?.name || pipelineContext?.folderName || 'Pipeline Builder'
  const folderOptions = useMemo(
    () => {
      const options = (databases ?? []).map(normalizeFolder).filter(Boolean) as FolderOption[]
      return options.sort((left, right) => left.name.localeCompare(right.name))
    },
    [databases],
  )
  const pipelineType = (primaryPipeline?.pipeline_type || 'batch').toLowerCase()
  const pipelineTypeLabel = pipelineType.includes('stream') ? 'Streaming' : 'Batch'
  const pipelineBranchName = pipelineDetail?.branch || primaryPipeline?.branch || 'main'
  const pipelineBranchLabel = pipelineBranchName === 'main' ? 'Main' : pipelineBranchName
  const isStreamingPipeline = pipelineType.includes('stream')
  const latestBuildArtifact = useMemo<PipelineArtifactRecord | null>(() => {
    const successful = pipelineArtifacts.filter(
      (artifact) => String(artifact.status || '').toUpperCase() === 'SUCCESS',
    )
    if (successful.length === 0) {
      return null
    }
    const sorted = [...successful].sort(
      (left, right) => parseTimestamp(right.created_at) - parseTimestamp(left.created_at),
    )
    return sorted[0] ?? null
  }, [pipelineArtifacts])
  const outputNodeIds = useMemo(
    () => extractOutputNodeIds(pipelineDetail?.definition_json),
    [pipelineDetail?.definition_json],
  )
  const defaultOutputNodeId = outputNodeIds[0] ?? null
  const canSave = Boolean(primaryPipeline && pipelineDetail && definitionDirty) && !isSaving
  const canPropose = Boolean(primaryPipeline) && !isProposing
  const canDeploy =
    Boolean(primaryPipeline && latestBuildArtifact) &&
    !definitionDirty &&
    (isStreamingPipeline || Boolean(defaultOutputNodeId)) &&
    !isDeploying
  const pipelineCount = primaryPipeline ? nodes.length : pipelines.length
  const previewDataset = useMemo(() => {
    if (datasets.length === 0) {
      return null
    }
    const withSample = datasets.find((dataset) => extractSampleRows(dataset.sample_json ?? {}).length > 0)
    if (withSample) {
      return withSample
    }
    const withSchema = datasets.find((dataset) => extractSchemaColumns(dataset.schema_json).length > 0)
    return withSchema ?? datasets[0]
  }, [datasets])
  const previewDatasetName = previewDataset?.name ?? 'No datasets available'
  const previewPayload = useMemo(() => buildPreviewFromDataset(previewDataset), [previewDataset])
  const datasetOutputs = useMemo(() => {
    return datasets.map((dataset) => {
      const sampleRows = extractSampleRows(dataset.sample_json ?? {})
      const schemaColumns = extractSchemaColumns(dataset.schema_json)
      const sampleColumns = extractSampleColumns(dataset.sample_json ?? {}, sampleRows)
      const schemaNames = new Set(schemaColumns.map((column) => column.name))
      const extraColumns = schemaNames.size > 0 ? sampleColumns.filter((name) => !schemaNames.has(name)) : []
      const columnCount = schemaColumns.length || sampleColumns.length
      const rowCount = dataset.row_count ?? sampleRows.length
      return {
        dataset,
        columnCount,
        rowCount,
        extraColumnsCount: extraColumns.length,
      }
    })
  }, [datasets])
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
  }, [columnSearch, previewColumns])
  const tableGridTemplate = useMemo(() => {
    return ['48px', ...previewColumns.map((column) => `minmax(${column.width}px, 1fr)`)].join(' ')
  }, [previewColumns])
  const tableMinWidth = useMemo(() => {
    const baseWidth = previewColumns.reduce((sum, column) => sum + column.width, 48)
    const gaps = previewColumns.length * 8
    return baseWidth + gaps
  }, [previewColumns])

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
    setDefinitionDirty(true)
  }, [setEdges])

  const handleNodesChange = useCallback((changes: NodeChange[]) => {
    onNodesChange(changes)
    if (changes.some((change) => change.type === 'remove')) {
      setDefinitionDirty(true)
    }
  }, [onNodesChange])

  const handleEdgesChange = useCallback((changes: EdgeChange[]) => {
    onEdgesChange(changes)
    if (changes.some((change) => change.type === 'remove' || change.type === 'add')) {
      setDefinitionDirty(true)
    }
  }, [onEdgesChange])

  const handleNodeClick = useCallback((_: unknown, node: FlowNode) => {
    if (!isRemoveMode) {
      if (!isSelectableMode) {
        return
      }
      setBottomPanelOpen(true)
      return
    }
    setNodes((current) => current.filter((item) => item.id !== node.id))
    setEdges((current) => current.filter((item) => item.source !== node.id && item.target !== node.id))
    setDefinitionDirty(true)
  }, [isRemoveMode, isSelectableMode, setNodes, setEdges])

  const handleEdgeClick = useCallback((_: unknown, edge: Edge) => {
    if (!isRemoveMode) {
      return
    }
    setEdges((current) => current.filter((item) => item.id !== edge.id))
    setDefinitionDirty(true)
  }, [isRemoveMode, setEdges])

  const handleLayout = useCallback(() => {
    if (nodes.length === 0) {
      return
    }
    setNodes((current) => layoutFlowNodes(current, edges))
  }, [nodes, edges, setNodes])

  const reorderColumns = useCallback((sourceKey: string, targetKey: string) => {
    if (sourceKey === targetKey) {
      return
    }
    setPreviewColumns((current) => {
      const sourceIndex = current.findIndex((column) => column.key === sourceKey)
      const targetIndex = current.findIndex((column) => column.key === targetKey)
      if (sourceIndex < 0 || targetIndex < 0) {
        return current
      }
      const next = [...current]
      const [moved] = next.splice(sourceIndex, 1)
      next.splice(targetIndex, 0, moved)
      return next
    })
  }, [])

  const handleColumnDragStart = useCallback((event: React.DragEvent<HTMLButtonElement>, key: string) => {
    event.dataTransfer.effectAllowed = 'move'
    event.dataTransfer.setData('text/plain', key)
    setDraggedColumnKey(key)
  }, [])

  const handleColumnDragEnd = useCallback(() => {
    setDraggedColumnKey(null)
    setDragOverColumnKey(null)
  }, [])

  const handleColumnDragOver = useCallback((event: React.DragEvent<HTMLButtonElement>) => {
    event.preventDefault()
    event.dataTransfer.dropEffect = 'move'
  }, [])

  const handleColumnDragEnter = useCallback((key: string) => {
    if (draggedColumnKey === key) {
      return
    }
    setDragOverColumnKey(key)
  }, [draggedColumnKey])

  const handleColumnDragLeave = useCallback((event: React.DragEvent<HTMLButtonElement>) => {
    const nextTarget = event.relatedTarget as Element | null
    if (nextTarget && event.currentTarget.contains(nextTarget)) {
      return
    }
    setDragOverColumnKey(null)
  }, [])

  const handleColumnDrop = useCallback((event: React.DragEvent<HTMLButtonElement>, targetKey: string) => {
    event.preventDefault()
    const sourceKey = event.dataTransfer.getData('text/plain')
    if (!sourceKey) {
      return
    }
    reorderColumns(sourceKey, targetKey)
    setDraggedColumnKey(null)
    setDragOverColumnKey(null)
  }, [reorderColumns])

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

  const savePipelineDefinition = useCallback(async () => {
    if (!primaryPipeline || !pipelineDetail) {
      return false
    }
    const draft = buildDefinitionDraft(pipelineDetail.definition_json, nodes, edges)
    setIsSaving(true)
    try {
      await updatePipeline(primaryPipeline.pipeline_id, {
        definition_json: draft,
        dbName: activeDbName,
      })
      await queryClient.invalidateQueries({ queryKey: ['pipeline-detail', primaryPipeline.pipeline_id] })
      await queryClient.invalidateQueries({ queryKey: ['pipelines', activeDbName] })
      setDefinitionDirty(false)
      return true
    } catch (error) {
      console.error('Failed to save pipeline definition', error)
      return false
    } finally {
      setIsSaving(false)
    }
  }, [primaryPipeline, pipelineDetail, nodes, edges, activeDbName, queryClient])

  const handleSave = useCallback(() => {
    void savePipelineDefinition()
  }, [savePipelineDefinition])

  const handlePropose = useCallback(async () => {
    if (!primaryPipeline) {
      return
    }
    setIsProposing(true)
    try {
      if (definitionDirty) {
        const saved = await savePipelineDefinition()
        if (!saved) {
          return
        }
      }
      const timestamp = new Date().toISOString().slice(0, 19)
      const title = `${pipelineDisplayName} proposal ${timestamp}`
      await submitPipelineProposal(primaryPipeline.pipeline_id, {
        title,
        description: 'Submitted from Pipeline Builder',
        buildJobId: latestBuildArtifact?.job_id,
        dbName: activeDbName,
      })
      await queryClient.invalidateQueries({ queryKey: ['pipelines', activeDbName] })
    } catch (error) {
      console.error('Failed to submit proposal', error)
    } finally {
      setIsProposing(false)
    }
  }, [
    primaryPipeline,
    definitionDirty,
    savePipelineDefinition,
    pipelineDisplayName,
    latestBuildArtifact,
    activeDbName,
    queryClient,
  ])

  const handleDeploy = useCallback(async () => {
    if (!primaryPipeline || !latestBuildArtifact) {
      return
    }
    if (!isStreamingPipeline && !defaultOutputNodeId) {
      return
    }
    setIsDeploying(true)
    try {
      await deployPipeline(primaryPipeline.pipeline_id, {
        promoteBuild: true,
        buildJobId: latestBuildArtifact.job_id,
        artifactId: latestBuildArtifact.artifact_id,
        nodeId: isStreamingPipeline ? undefined : defaultOutputNodeId ?? undefined,
        replayOnDeploy: false,
        dbName: activeDbName,
      })
      await queryClient.invalidateQueries({ queryKey: ['datasets', activeDbName] })
      await queryClient.invalidateQueries({ queryKey: ['pipeline-artifacts', primaryPipeline.pipeline_id, 'build'] })
    } catch (error) {
      console.error('Failed to deploy pipeline', error)
    } finally {
      setIsDeploying(false)
    }
  }, [
    primaryPipeline,
    latestBuildArtifact,
    isStreamingPipeline,
    defaultOutputNodeId,
    activeDbName,
    queryClient,
  ])

  const handleSelectFolder = useCallback(
    (folder: FolderOption) => {
      setPipelineContext({ folderId: folder.id, folderName: folder.name })
    },
    [setPipelineContext],
  )

  const handleRefresh = useCallback(() => {
    if (activeDbName) {
      void queryClient.invalidateQueries({ queryKey: ['datasets', activeDbName] })
      void queryClient.invalidateQueries({ queryKey: ['pipelines', activeDbName] })
    }
    if (primaryPipeline?.pipeline_id) {
      void queryClient.invalidateQueries({ queryKey: ['pipeline-detail', primaryPipeline.pipeline_id] })
      void queryClient.invalidateQueries({ queryKey: ['pipeline-artifacts', primaryPipeline.pipeline_id, 'build'] })
    }
  }, [activeDbName, primaryPipeline, queryClient])

  const summarizeReadiness = useCallback((readiness: PipelineReadiness) => {
    const status = readiness.status ?? 'UNKNOWN'
    const inputs = readiness.inputs ?? []
    const blocked = inputs.filter((input) => String(input.status || '').toUpperCase() !== 'READY')
    if (inputs.length === 0) {
      return `Readiness: ${status}\nNo inputs configured yet.`
    }
    return `Readiness: ${status}\nInputs: ${inputs.length}\nNot ready: ${blocked.length}`
  }, [])

  const handleSettings = useCallback(async () => {
    if (!primaryPipeline) {
      return
    }
    setIsCheckingReadiness(true)
    try {
      const readiness = await getPipelineReadiness(primaryPipeline.pipeline_id, { dbName: activeDbName })
      window.alert(summarizeReadiness(readiness))
      handleRefresh()
    } catch (error) {
      console.error('Failed to load pipeline readiness', error)
      window.alert('Failed to load pipeline readiness.')
    } finally {
      setIsCheckingReadiness(false)
    }
  }, [primaryPipeline, activeDbName, summarizeReadiness, handleRefresh])

  const handleHelp = useCallback(() => {
    const helpUrl =
      (import.meta.env.VITE_PIPELINE_HELP_URL as string | undefined) ??
      '/docs/PipelineBuilder_checklist.md'
    window.open(helpUrl, '_blank', 'noopener,noreferrer')
  }, [])

  const fileMenu = useMemo(() => {
    const hasFolders = folderOptions.length > 0
    return (
      <Menu>
        <MenuItem icon="folder-close" text={`Current: ${pipelineFolderLabel}`} disabled />
        <MenuDivider />
        {hasFolders
          ? folderOptions.map((folder) => {
              const isActive = folder.id === activeDbName
              return (
                <MenuItem
                  key={folder.id}
                  icon={isActive ? 'small-tick' : 'folder-close'}
                  text={folder.name}
                  disabled={isActive}
                  onClick={() => handleSelectFolder(folder)}
                />
              )
            })
          : <MenuItem text="No folders found" disabled />}
      </Menu>
    )
  }, [
    folderOptions,
    pipelineFolderLabel,
    activeDbName,
    handleSelectFolder,
  ])

  useEffect(() => {
    setPreviewColumns(previewPayload.columns)
    setPreviewRows(previewPayload.rows)
    setActiveColumn((current) => {
      if (previewPayload.columns.some((column) => column.key === current)) {
        return current
      }
      return previewPayload.columns[0]?.key ?? ''
    })
  }, [previewPayload])

  useEffect(() => {
    if (!pipelineDetail?.definition_json) {
      setNodes([])
      setEdges([])
      setDefinitionDirty(false)
      return
    }
    const { nodes: flowNodes, edges: flowEdges } = buildFlowFromDefinition(pipelineDetail.definition_json)
    const laidOutNodes = layoutFlowNodes(flowNodes, flowEdges)
    setNodes(laidOutNodes)
    setEdges(flowEdges)
    setDefinitionDirty(false)
  }, [pipelineDetail?.definition_json, setNodes, setEdges])

  const topbar = (
    <div className="pipeline-topbar">
      <div className="pipeline-topbar-left">
        <div className="pipeline-header-details">
          <div className="pipeline-title-row">
            <div className="pipeline-breadcrumb">
              <Icon icon="folder-close" size={14} className="pipeline-breadcrumb-icon" />
              <span className="pipeline-breadcrumb-text">{pipelineFolderLabel}</span>
            </div>
            <Icon icon="chevron-right" className="pipeline-breadcrumb-separator" size={14} />
            <div className="pipeline-name-wrapper">
              <span className="pipeline-name">{pipelineDisplayName}</span>
              <Button minimal small icon="star-empty" className="pipeline-star" disabled />
            </div>
          </div>

          <div className="pipeline-menu-row">
            <div className="pipeline-menu">
              <Popover content={fileMenu} position="bottom-left" usePortal={false}>
                <Button minimal text="File" rightIcon="caret-down" small />
              </Popover>
              <Button
                minimal
                text="Settings"
                rightIcon="caret-down"
                small
                onClick={handleSettings}
                loading={isCheckingReadiness}
                disabled={!primaryPipeline}
              />
              <Button
                minimal
                text="Help"
                rightIcon="caret-down"
                small
                onClick={handleHelp}
              />
            </div>
            <div className="pipeline-divider" />
            <div className="pipeline-info-item">
              <Icon icon="office" size={12} />
              <span>{pipelineCount}</span>
            </div>
            <div className="pipeline-divider" />
            <div className="pipeline-batch-badge">
              <span>{pipelineTypeLabel}</span>
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
          <Button minimal icon="undo" disabled small />
          <Button minimal icon="redo" disabled small />
        </div>

        <div className="pipeline-divider" />

        <div className="pipeline-branch-selector">
          <Icon icon="lock" size={12} className="pipeline-branch-icon" />
          <span className="pipeline-branch-name">{pipelineBranchLabel}</span>
          <Icon icon="caret-down" size={12} />
        </div>

        <Button
          className="pipeline-action-btn"
          intent="success"
          icon="floppy-disk"
          text="Save"
          small
          disabled={!canSave}
          loading={isSaving}
          onClick={handleSave}
        />
        <Button
          className="pipeline-action-btn"
          intent="primary"
          icon="git-pull"
          text="Propose"
          outlined
          small
          disabled={!canPropose}
          loading={isProposing}
          onClick={handlePropose}
        />
        <div className="pipeline-deploy-group">
          <Button
            className="pipeline-deploy-btn"
            intent="primary"
            text="Deploy"
            small
            disabled={!canDeploy}
            loading={isDeploying}
            onClick={handleDeploy}
          />
          <Button
            className="pipeline-deploy-options"
            intent="primary"
            icon="settings"
            small
            disabled={!canDeploy}
          />
        </div>

        <div className="pipeline-divider" />

        <Button minimal icon="share" text="Share" small disabled />
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
                onNodesChange={handleNodesChange}
                onEdgesChange={handleEdgesChange}
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
                  <Icon icon="th" size={14} />
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
                        placeholder={`Search ${previewColumns.length} columns...`}
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
                            className={`pipeline-preview-column${activeColumn === column.key ? ' is-active' : ''}${
                              draggedColumnKey === column.key ? ' is-dragging' : ''
                            }${dragOverColumnKey === column.key ? ' is-dragover' : ''}`}
                            onClick={() => setActiveColumn(column.key)}
                            draggable
                            onDragStart={(event) => handleColumnDragStart(event, column.key)}
                            onDragEnd={handleColumnDragEnd}
                            onDragOver={handleColumnDragOver}
                            onDragEnter={() => handleColumnDragEnter(column.key)}
                            onDragLeave={handleColumnDragLeave}
                            onDrop={(event) => handleColumnDrop(event, column.key)}
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
                      <div
                        className="pipeline-preview-table-header"
                        style={{ gridTemplateColumns: tableGridTemplate, minWidth: tableMinWidth }}
                      >
                        <div className="pipeline-preview-table-cell is-index">#</div>
                        {previewColumns.map((column) => (
                          <div
                            key={column.key}
                            className={`pipeline-preview-table-cell is-header${
                              activeColumn === column.key ? ' is-active' : ''
                            }`}
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
                          key={`${index + 1}`}
                          className="pipeline-preview-table-row"
                          style={{ gridTemplateColumns: tableGridTemplate, minWidth: tableMinWidth }}
                        >
                          <div className="pipeline-preview-table-cell is-index">{index + 1}</div>
                          {previewColumns.map((column) => (
                            <div
                              key={column.key}
                              className={`pipeline-preview-table-cell${
                                activeColumn === column.key ? ' is-active' : ''
                              }`}
                            >
                              {row[column.key] ?? ''}
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
              <div className="pipeline-right-panel-tabs">
                <button
                  type="button"
                  className={`pipeline-right-panel-tab ${activeOutputTab === 'datasets' ? 'is-active' : ''}`}
                  onClick={() => setActiveOutputTab('datasets')}
                >
                  <Icon icon="database" size={12} />
                  <span>Datasets</span>
                </button>
                <button
                  type="button"
                  className={`pipeline-right-panel-tab ${activeOutputTab === 'objectTypes' ? 'is-active' : ''}`}
                  onClick={() => setActiveOutputTab('objectTypes')}
                >
                  <Icon icon="cube" size={12} />
                  <span>Object types</span>
                </button>
                <button
                  type="button"
                  className={`pipeline-right-panel-tab ${activeOutputTab === 'linkTypes' ? 'is-active' : ''}`}
                  onClick={() => setActiveOutputTab('linkTypes')}
                >
                  <Icon icon="link" size={12} />
                  <span>Link types</span>
                </button>
              </div>
              <button
                type="button"
                className="pipeline-right-panel-close"
                onClick={() => setRightPanelOpen(false)}
                aria-label="Close panel"
              >
                <Icon icon="chevron-right" size={12} />
              </button>
            </div>
            <div className="pipeline-right-panel-body">
              {activeOutputTab === 'datasets' ? (
                <div className="pipeline-output-panel">
                  <div className="pipeline-output-list">
                    {datasetOutputs.length === 0 ? (
                      <div className="pipeline-output-empty">
                        <Icon icon="info-sign" size={14} />
                        <span>No datasets available yet.</span>
                      </div>
                    ) : (
                      datasetOutputs.map(({ dataset, columnCount, rowCount, extraColumnsCount }) => {
                        const statusLabel = columnCount > 0 ? `${columnCount} columns` : 'No schema yet'
                        const rowLabel = rowCount > 0 ? `${rowCount} rows` : ''
                        const statusText = rowLabel ? `${statusLabel} · ${rowLabel}` : statusLabel
                        return (
                          <div className="pipeline-output-card" key={dataset.dataset_id}>
                            <div className="pipeline-output-header">
                              <div className="pipeline-output-title">
                                <Icon icon="th" size={14} />
                                <span>{dataset.name}</span>
                              </div>
                              <button type="button" className="pipeline-output-menu" aria-label="Output actions">
                                <Icon icon="more" size={12} />
                              </button>
                            </div>
                            <div className="pipeline-output-path">
                              <Icon icon="folder-close" size={12} />
                              <span>/{dataset.db_name || pipelineDisplayName}</span>
                            </div>
                            <div className="pipeline-output-row">
                              <div className="pipeline-output-status is-success">
                                <Icon icon="tick-circle" size={12} />
                                <span>{statusText}</span>
                              </div>
                              <button type="button" className="pipeline-output-action">
                                <Icon icon="edit" size={12} />
                                <span>Edit schema</span>
                              </button>
                            </div>
                            {extraColumnsCount > 0 ? (
                              <div className="pipeline-output-alert">
                                <Icon icon="warning-sign" size={12} />
                                <span>{extraColumnsCount} column(s) not in schema</span>
                              </div>
                            ) : null}
                          </div>
                        )
                      })
                    )}
                  </div>
                  <div className="pipeline-output-footer">
                    <button type="button" className="pipeline-output-add">
                      <Icon icon="add" size={12} />
                      <span>Add output</span>
                    </button>
                  </div>
                </div>
              ) : (
                <div className="pipeline-output-empty">
                  <Icon icon="info-sign" size={14} />
                  <span>No outputs configured yet.</span>
                </div>
              )}
            </div>
          </aside>
        </div>
      </div>
    </div>
  )
}
