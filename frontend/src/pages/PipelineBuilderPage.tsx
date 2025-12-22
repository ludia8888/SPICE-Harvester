import { useEffect, useMemo, useRef, useState } from 'react'
import {
  Button,
  Callout,
  Card,
  Dialog,
  Divider,
  FormGroup,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
  Tab,
  Tabs,
  Tag,
  Text,
  TextArea,
} from '@blueprintjs/core'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  createDataset,
  createDatasetVersion,
  createPipeline,
  deployPipeline,
  getPipeline,
  listRegisteredSheets,
  previewRegisteredSheet,
  startPipeliningSheet,
  listBranches,
  listDatasets,
  listPipelines,
  previewPipeline,
  updatePipeline,
} from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { showAppToast } from '../app/AppToaster'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { navigate } from '../state/pathname'
import { useAppStore } from '../store/useAppStore'
import { PipelineCanvas } from '../features/pipeline/PipelineCanvas'
import { PipelineHeader } from '../features/pipeline/PipelineHeader'
import { PipelinePreview } from '../features/pipeline/PipelinePreview'
import { PipelineToolbar } from '../features/pipeline/PipelineToolbar'
import {
  createDefaultDefinition,
  createId,
  type PipelineDefinition,
  type PipelineMode,
  type PipelineNode,
  type PipelineParameter,
  type PipelineTool,
  type PreviewColumn,
  type PreviewRow,
} from '../features/pipeline/pipelineTypes'
import '../features/pipeline/pipeline.css'

type PipelineRecord = Record<string, unknown>
type DatasetRecord = Record<string, unknown>
type ConnectorPreview = {
  sheetId?: string
  sheetTitle?: string
  worksheetTitle?: string
  columns: string[]
  rows: Array<Array<string | number | boolean | null>>
  totalRows?: number
  totalColumns?: number
}

const extractList = <T,>(payload: unknown, key: string): T[] => {
  if (!payload || typeof payload !== 'object') return []
  const data = payload as { data?: Record<string, unknown> }
  const nested = data.data?.[key]
  if (Array.isArray(nested)) return nested as T[]
  const direct = (payload as Record<string, unknown>)[key]
  if (Array.isArray(direct)) return direct as T[]
  return []
}

const extractPipeline = (payload: unknown): PipelineRecord | null => {
  if (!payload || typeof payload !== 'object') return null
  const data = payload as { data?: { pipeline?: PipelineRecord } }
  return data.data?.pipeline ?? (payload as { pipeline?: PipelineRecord }).pipeline ?? null
}

const ensureDefinition = (raw: unknown): PipelineDefinition => {
  const fallback = createDefaultDefinition()
  if (!raw || typeof raw !== 'object') return fallback
  const value = raw as Partial<PipelineDefinition>
  const nodes = Array.isArray(value.nodes) ? value.nodes : fallback.nodes
  const normalizedNodes = nodes.map((node, index) => {
    const safeNode = node as PipelineNode
    return {
      id: typeof safeNode.id === 'string' ? safeNode.id : createId('node'),
      title: safeNode.title ?? `Node ${index + 1}`,
      type: safeNode.type ?? 'transform',
      icon: safeNode.icon ?? (safeNode.type === 'output' ? 'export' : 'th'),
      x: Number.isFinite(safeNode.x) ? Number(safeNode.x) : 80,
      y: Number.isFinite(safeNode.y) ? Number(safeNode.y) : 80 + index * 140,
      subtitle: safeNode.subtitle,
      columns: Array.isArray(safeNode.columns) ? safeNode.columns : undefined,
      status: safeNode.status ?? 'success',
      metadata: safeNode.metadata ?? {},
    }
  })
  const nodeIds = new Set(normalizedNodes.map((node) => node.id))
  const edges = (Array.isArray(value.edges) ? value.edges : fallback.edges)
    .map((edge) => ({
      id: typeof edge.id === 'string' ? edge.id : createId('edge'),
      from: edge.from ?? '',
      to: edge.to ?? '',
    }))
    .filter((edge) => nodeIds.has(edge.from) && nodeIds.has(edge.to))
  const parameters = Array.isArray(value.parameters) ? value.parameters : fallback.parameters
  const outputs = Array.isArray(value.outputs) ? value.outputs : fallback.outputs
  return {
    nodes: normalizedNodes,
    edges,
    parameters: parameters.map((param) => ({
      id: typeof param.id === 'string' ? param.id : createId('param'),
      name: param.name ?? 'param',
      value: param.value ?? '',
    })),
    outputs: outputs.map((output) => ({
      id: typeof output.id === 'string' ? output.id : createId('output'),
      name: output.name ?? 'Output',
      datasetName: output.datasetName ?? 'output_dataset',
      description: output.description,
    })),
    settings: value.settings ?? fallback.settings,
  }
}

const extractColumns = (schema: unknown, fallbackType = 'String'): PreviewColumn[] => {
  if (!schema || typeof schema !== 'object') return []
  const raw = schema as Record<string, unknown>
  if (Array.isArray(raw.columns)) {
    return raw.columns
      .map((column) => {
        const col = column as Record<string, unknown>
        const key = String(col.name ?? col.key ?? '')
        if (!key) return null
        return { key, type: String(col.type ?? fallbackType) }
      })
      .filter((col): col is PreviewColumn => Boolean(col))
  }
  if (Array.isArray(raw.fields)) {
    return raw.fields
      .map((column) => {
        const col = column as Record<string, unknown>
        const key = String(col.name ?? col.key ?? '')
        if (!key) return null
        return { key, type: String(col.type ?? fallbackType) }
      })
      .filter((col): col is PreviewColumn => Boolean(col))
  }
  if (raw.properties && typeof raw.properties === 'object') {
    return Object.entries(raw.properties as Record<string, unknown>).map(([key, value]) => ({
      key,
      type: typeof value === 'object' && value ? String((value as Record<string, unknown>).type ?? fallbackType) : fallbackType,
    }))
  }
  return []
}

const extractRows = (sample: unknown): PreviewRow[] => {
  if (!sample || typeof sample !== 'object') return []
  const raw = sample as Record<string, unknown>
  if (Array.isArray(raw.rows)) {
    return raw.rows.filter((row) => row && typeof row === 'object') as PreviewRow[]
  }
  if (Array.isArray(raw.data)) {
    return raw.data.filter((row) => row && typeof row === 'object') as PreviewRow[]
  }
  return []
}

const extractConnectorPreview = (payload: unknown): ConnectorPreview | null => {
  if (!payload || typeof payload !== 'object') return null
  const data = (payload as { data?: Record<string, unknown> }).data ?? (payload as Record<string, unknown>)
  if (!data || typeof data !== 'object') return null
  const raw = data as Record<string, unknown>
  const columns = Array.isArray(raw.columns) ? raw.columns.map((col) => String(col)) : []
  const rows = Array.isArray(raw.sample_rows)
    ? (raw.sample_rows as Array<Array<string | number | boolean | null>>)
    : Array.isArray(raw.preview_data)
      ? (raw.preview_data as Array<Array<string | number | boolean | null>>)
      : []
  return {
    sheetId: raw.sheet_id ? String(raw.sheet_id) : undefined,
    sheetTitle: raw.sheet_title ? String(raw.sheet_title) : undefined,
    worksheetTitle: raw.worksheet_title ? String(raw.worksheet_title) : raw.worksheet_name ? String(raw.worksheet_name) : undefined,
    columns,
    rows,
    totalRows: raw.total_rows ? Number(raw.total_rows) : rows.length,
    totalColumns: raw.total_columns ? Number(raw.total_columns) : columns.length,
  }
}

const buildRowsFromMatrix = (columns: string[], rows: unknown): PreviewRow[] => {
  if (!Array.isArray(rows)) return []
  return rows.map((row) => {
    const cells = Array.isArray(row) ? row : []
    const record: PreviewRow = {}
    columns.forEach((key, index) => {
      record[key] = cells[index] ?? ''
    })
    return record
  })
}

const buildColumnsFromManual = (raw: string): Array<{ name: string; type: string }> =>
  raw
    .split(',')
    .map((col) => col.trim())
    .filter(Boolean)
    .map((name) => ({ name, type: 'String' }))

const buildManualRows = (raw: string, columns: string[] = []): PreviewRow[] => {
  if (!raw.trim()) return []
  const lines = raw
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
  if (lines.length === 0) return []
  const delimiter = lines[0].includes('\t') ? '\t' : ','
  const rows: PreviewRow[] = []
  if (columns.length > 0) {
    lines.forEach((line) => {
      const cells = line.split(delimiter)
      const row: PreviewRow = {}
      columns.forEach((key, index) => {
        row[key] = cells[index]?.trim() ?? ''
      })
      rows.push(row)
    })
    return rows
  }
  const header = lines[0].split(delimiter).map((cell) => cell.trim())
  lines.slice(1).forEach((line) => {
    const cells = line.split(delimiter)
    const row: PreviewRow = {}
    header.forEach((key, index) => {
      row[key] = cells[index]?.trim() ?? ''
    })
    rows.push(row)
  })
  return rows
}

export const PipelineBuilderPage = ({ dbName }: { dbName: string }) => {
  const requestContext = useRequestContext()
  const queryClient = useQueryClient()
  const language = useAppStore((state) => state.context.language)
  const branch = useAppStore((state) => state.context.branch)
  const setBranch = useAppStore((state) => state.setBranch)
  const setCommandDrawerOpen = useAppStore((state) => state.setCommandDrawerOpen)
  const commands = useAppStore((state) => state.commands)

  const [mode, setMode] = useState<PipelineMode>('edit')
  const [activeTool, setActiveTool] = useState<PipelineTool>('tools')
  const [definition, setDefinition] = useState<PipelineDefinition>(() => createDefaultDefinition())
  const [pipelineId, setPipelineId] = useState<string | null>(null)
  const [pipelineName, setPipelineName] = useState('')
  const [pipelineLocation, setPipelineLocation] = useState(`/projects/${dbName}/pipelines`)
  const [history, setHistory] = useState<PipelineDefinition[]>([])
  const [future, setFuture] = useState<PipelineDefinition[]>([])
  const [isDirty, setIsDirty] = useState(false)
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const [previewNodeId, setPreviewNodeId] = useState<string | null>(null)
  const [canvasZoom, setCanvasZoom] = useState(1)
  const [inspectorOpen, setInspectorOpen] = useState(false)
  const [previewCollapsed, setPreviewCollapsed] = useState(false)
  const [previewFullscreen, setPreviewFullscreen] = useState(false)
  const [previewSample, setPreviewSample] = useState<{ columns: PreviewColumn[]; rows: PreviewRow[] } | null>(null)

  const [pipelineDialogOpen, setPipelineDialogOpen] = useState(false)
  const [datasetDialogOpen, setDatasetDialogOpen] = useState(false)
  const [parametersOpen, setParametersOpen] = useState(false)
  const [transformOpen, setTransformOpen] = useState(false)
  const [joinOpen, setJoinOpen] = useState(false)
  const [visualizeOpen, setVisualizeOpen] = useState(false)
  const [deployOpen, setDeployOpen] = useState(false)
  const [deploySettingsOpen, setDeploySettingsOpen] = useState(false)
  const [helpOpen, setHelpOpen] = useState(false)
  const [outputOpen, setOutputOpen] = useState(false)

  const [newPipelineName, setNewPipelineName] = useState('')
  const [newPipelineLocation, setNewPipelineLocation] = useState('')
  const [newPipelineDescription, setNewPipelineDescription] = useState('')

  const [datasetSearch, setDatasetSearch] = useState('')
  const [selectedDatasetId, setSelectedDatasetId] = useState<string | null>(null)
  const [datasetTab, setDatasetTab] = useState('connectors')
  const [connectorSearch, setConnectorSearch] = useState('')
  const [connectorPreview, setConnectorPreview] = useState<ConnectorPreview | null>(null)
  const [manualDatasetName, setManualDatasetName] = useState('')
  const [manualColumns, setManualColumns] = useState('')
  const [manualRows, setManualRows] = useState('')

  const [parameterDrafts, setParameterDrafts] = useState<PipelineParameter[]>([])
  const [transformDraft, setTransformDraft] = useState({ title: '', operation: '', expression: '' })
  const [joinLeft, setJoinLeft] = useState('')
  const [joinRight, setJoinRight] = useState('')
  const [joinType, setJoinType] = useState('inner')
  const [outputDraft, setOutputDraft] = useState({ name: '', datasetName: '', description: '' })
  const [deployDraft, setDeployDraft] = useState({ datasetName: '', rowCount: '500', artifactKey: '' })
  const [deploySettings, setDeploySettings] = useState({ compute: 'Medium', memory: '4 GB', schedule: 'Manual', engine: 'Batch' })

  const autoPrompted = useRef(false)
  const loadedPipeline = useRef<string | null>(null)

  const pipelinesQuery = useQuery({
    queryKey: qk.pipelines(dbName, requestContext.language),
    queryFn: () => listPipelines(requestContext, dbName),
    enabled: Boolean(dbName),
  })

  const branchesQuery = useQuery({
    queryKey: qk.branches(dbName, requestContext.language),
    queryFn: () => listBranches(requestContext, dbName),
    enabled: Boolean(dbName),
  })

  const datasetsQuery = useQuery({
    queryKey: qk.datasets(dbName, requestContext.language),
    queryFn: () => listDatasets(requestContext, dbName),
    enabled: datasetDialogOpen,
  })

  const connectorsQuery = useQuery({
    queryKey: qk.registeredSheets(dbName, requestContext.language),
    queryFn: () => listRegisteredSheets(requestContext, dbName),
    enabled: datasetDialogOpen,
  })

  const pipelineQuery = useQuery({
    queryKey: qk.pipeline(pipelineId ?? 'pending', requestContext.language),
    queryFn: () => getPipeline(requestContext, pipelineId ?? ''),
    enabled: Boolean(pipelineId),
  })

  const createPipelineMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => createPipeline(requestContext, payload),
    onSuccess: (payload) => {
      const pipeline = extractPipeline(payload)
      if (!pipeline) return
      setPipelineId(String(pipeline.pipeline_id ?? ''))
      setPipelineName(String(pipeline.name ?? ''))
      setPipelineLocation(String(pipeline.location ?? pipelineLocation))
      setDefinition(ensureDefinition(pipeline.definition_json))
      setHistory([])
      setFuture([])
      setIsDirty(false)
      setPipelineDialogOpen(false)
      void queryClient.invalidateQueries({ queryKey: qk.pipelines(dbName, requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.pipelineCreated })
    },
    onError: (error) => toastApiError(error, language),
  })

  const updatePipelineMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => updatePipeline(requestContext, pipelineId ?? '', payload),
    onSuccess: () => {
      setIsDirty(false)
      void queryClient.invalidateQueries({ queryKey: qk.pipeline(pipelineId ?? 'pending', requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.pipelineSaved })
    },
    onError: (error) => toastApiError(error, language),
  })

  const previewMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => previewPipeline(requestContext, pipelineId ?? '', payload),
    onError: (error) => toastApiError(error, language),
  })

  const deployMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => deployPipeline(requestContext, pipelineId ?? '', payload),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.datasets(dbName, requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.pipelineDeployed })
    },
    onError: (error) => toastApiError(error, language),
  })

  const createDatasetMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => createDataset(requestContext, payload),
    onSuccess: (payload) => {
      void queryClient.invalidateQueries({ queryKey: qk.datasets(dbName, requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.datasetCreated })
      const dataset = (payload as { data?: { dataset?: Record<string, unknown> } })?.data?.dataset
      const datasetId = typeof dataset?.dataset_id === 'string' ? dataset.dataset_id : ''
      if (datasetId) {
        const columns = buildColumnsFromManual(manualColumns).map((col) => col.name)
        const sampleRows = buildManualRows(manualRows, columns)
        if (sampleRows.length > 0) {
          void createDatasetVersion(requestContext, datasetId, {
            sample_json: { rows: sampleRows },
            schema_json: { columns: buildColumnsFromManual(manualColumns) },
            row_count: sampleRows.length,
          })
        }
      }
      setManualDatasetName('')
      setManualColumns('')
      setManualRows('')
    },
    onError: (error) => toastApiError(error, language),
  })

  const startPipeliningMutation = useMutation({
    mutationFn: (payload: { sheetId: string; dbName: string; worksheetName?: string; apiKey?: string }) =>
      startPipeliningSheet(requestContext, payload.sheetId, {
        db_name: payload.dbName,
        worksheet_name: payload.worksheetName,
        api_key: payload.apiKey,
      }),
    onSuccess: (payload) => {
      void queryClient.invalidateQueries({ queryKey: qk.datasets(dbName, requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.connectorReady })
      const dataset = (payload as { data?: { dataset?: Record<string, unknown> } })?.data?.dataset
      if (dataset) {
        handleAddDatasetNode(dataset)
      }
    },
    onError: (error) => toastApiError(error, language),
  })

  const previewRegisteredSheetMutation = useMutation({
    mutationFn: (payload: { sheetId: string; worksheetName?: string }) =>
      previewRegisteredSheet(requestContext, payload.sheetId, { worksheet_name: payload.worksheetName, limit: 25 }),
    onSuccess: (payload) => {
      const preview = extractConnectorPreview(payload)
      if (preview) {
        setConnectorPreview(preview)
      }
    },
    onError: (error) => toastApiError(error, language),
  })

  const pipelines = useMemo(() => extractList<PipelineRecord>(pipelinesQuery.data, 'pipelines'), [pipelinesQuery.data])
  const pipelineOptions = useMemo(
    () =>
      pipelines.map((pipeline) => ({
        id: String(pipeline.pipeline_id ?? ''),
        name: String(pipeline.name ?? 'Pipeline'),
      })).filter((pipeline) => pipeline.id),
    [pipelines],
  )
  const branches = useMemo(() => {
    const raw = extractList<Record<string, unknown>>(branchesQuery.data, 'branches')
    return raw
      .map((branchItem) => String(branchItem.name ?? branchItem.branch_name ?? branchItem.id ?? ''))
      .filter(Boolean)
  }, [branchesQuery.data])
  const datasets = useMemo(() => extractList<DatasetRecord>(datasetsQuery.data, 'datasets'), [datasetsQuery.data])
  const connectorSheets = useMemo(
    () => extractList<Record<string, unknown>>(connectorsQuery.data, 'sheets'),
    [connectorsQuery.data],
  )

  useEffect(() => {
    if (!pipelinesQuery.data || pipelineId) return
    if (pipelines.length > 0) {
      const first = pipelines[0]
      setPipelineId(String(first.pipeline_id ?? ''))
      setPipelineName(String(first.name ?? ''))
      setPipelineLocation(String(first.location ?? pipelineLocation))
      return
    }
    if (autoPrompted.current) return
    autoPrompted.current = true
    setPipelineDialogOpen(true)
  }, [pipelines, pipelinesQuery.data, pipelineId, pipelineLocation])

  useEffect(() => {
    if (!pipelineQuery.data || !pipelineId) return
    if (loadedPipeline.current === pipelineId && isDirty) return
    const pipeline = extractPipeline(pipelineQuery.data)
    if (!pipeline) return
    loadedPipeline.current = pipelineId
    setPipelineName(String(pipeline.name ?? ''))
    setPipelineLocation(String(pipeline.location ?? pipelineLocation))
    setDefinition(ensureDefinition(pipeline.definition_json))
    setHistory([])
    setFuture([])
    setIsDirty(false)
  }, [pipelineQuery.data, pipelineId, pipelineLocation, isDirty])

  useEffect(() => {
    if (!datasetDialogOpen) return
    setDatasetTab('connectors')
    setSelectedDatasetId(null)
    setDatasetSearch('')
    setConnectorSearch('')
    setConnectorPreview(null)
  }, [datasetDialogOpen])

  useEffect(() => {
    setNewPipelineLocation(pipelineLocation)
  }, [pipelineLocation])

  useEffect(() => {
    if (!parametersOpen) return
    setParameterDrafts(definition.parameters)
  }, [parametersOpen, definition.parameters])

  useEffect(() => {
    if (!transformOpen) return
    const node = definition.nodes.find((item) => item.id === selectedNodeId)
    if (!node) return
    setTransformDraft({
      title: node.title,
      operation: node.metadata?.operation ?? '',
      expression: node.metadata?.expression ?? '',
    })
  }, [transformOpen, definition.nodes, selectedNodeId])

  useEffect(() => {
    if (!joinOpen) return
    setJoinLeft(selectedNodeId ?? '')
    setJoinRight('')
    setJoinType('inner')
  }, [joinOpen, selectedNodeId])

  useEffect(() => {
    if (!outputOpen) return
    setOutputDraft({ name: '', datasetName: '', description: '' })
  }, [outputOpen])

  useEffect(() => {
    if (!deploySettingsOpen) return
    setDeploySettings((current) => ({
      compute: definition.settings?.compute ?? current.compute,
      memory: definition.settings?.memory ?? current.memory,
      schedule: definition.settings?.schedule ?? current.schedule,
      engine: definition.settings?.engine ?? current.engine,
    }))
  }, [deploySettingsOpen, definition.settings])

  const selectedNode = useMemo(
    () => definition.nodes.find((node) => node.id === selectedNodeId) ?? null,
    [definition.nodes, selectedNodeId],
  )

  const previewNode = useMemo(() => {
    const candidate = definition.nodes.find((node) => node.id === previewNodeId)
    return candidate ?? definition.nodes[0] ?? null
  }, [definition.nodes, previewNodeId])

  useEffect(() => {
    setPreviewSample(null)
  }, [previewNode?.id])

  const previewSchema = useMemo(() => {
    if (previewSample?.columns?.length) return previewSample.columns
    if (!previewNode) return [] as PreviewColumn[]
    const datasetId = previewNode.metadata?.datasetId
    if (datasetId) {
      const dataset = datasets.find((item) => String(item.dataset_id ?? '') === datasetId)
      if (dataset) {
        const columns = extractColumns(dataset.schema_json, language === 'ko' ? '문자열' : 'String')
        if (columns.length) return columns
        const sampleColumns = extractColumns(dataset.sample_json, language === 'ko' ? '문자열' : 'String')
        if (sampleColumns.length) return sampleColumns
        const latestColumns = extractColumns(dataset.latest_sample_json, language === 'ko' ? '문자열' : 'String')
        if (latestColumns.length) return latestColumns
      }
    }
    const fallbackNames = previewNode.columns ?? []
    if (fallbackNames.length) {
      return fallbackNames.map((name, index) => ({
        key: name.replace(/\s+/g, '_').toLowerCase() || `col_${index + 1}`,
        type: language === 'ko' ? '문자열' : 'String',
      }))
    }
    return []
  }, [previewNode, datasets, previewSample, language])

  const previewRows = useMemo(() => {
    if (previewSample?.rows?.length) return previewSample.rows
    if (!previewNode) return []
    const datasetId = previewNode.metadata?.datasetId
    if (datasetId) {
      const dataset = datasets.find((item) => String(item.dataset_id ?? '') === datasetId)
      const rows = extractRows(dataset?.sample_json)
      if (rows.length) return rows
      const latestRows = extractRows(dataset?.latest_sample_json)
      if (latestRows.length) return latestRows
    }
    return []
  }, [previewNode, datasets, previewSchema, previewSample])

  useEffect(() => {
    if (!pipelineId || !previewNode) return
    if (previewMutation.isPending) return
    previewMutation.mutate({
      db_name: dbName,
      definition_json: definition,
      node_id: previewNode.id,
      limit: 200,
    })
  }, [pipelineId, previewNode?.id, definition, dbName, previewMutation.isPending])

  useEffect(() => {
    if (!previewMutation.data) return
    const sample = (previewMutation.data as { data?: { sample?: Record<string, unknown> } })?.data?.sample
    if (!sample || typeof sample !== 'object') return
    setPreviewSample({
      columns: extractColumns(sample),
      rows: extractRows(sample),
    })
  }, [previewMutation.data])

  useEffect(() => {
    if (!previewFullscreen) return
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setPreviewFullscreen(false)
      }
    }
    window.addEventListener('keydown', handleKey)
    return () => window.removeEventListener('keydown', handleKey)
  }, [previewFullscreen])

  const updateDefinition = (updater: (current: PipelineDefinition) => PipelineDefinition) => {
    setDefinition((current) => {
      const next = updater(current)
      setHistory((prev) => [...prev, current])
      setFuture([])
      setIsDirty(true)
      return next
    })
  }

  const removeNode = (nodeId: string) => {
    updateDefinition((current) => ({
      ...current,
      nodes: current.nodes.filter((node) => node.id !== nodeId),
      edges: current.edges.filter((edge) => edge.from !== nodeId && edge.to !== nodeId),
    }))
    setSelectedNodeId(null)
  }

  const handleSelectNode = (nodeId: string | null) => {
    if (!nodeId) {
      setSelectedNodeId(null)
      setInspectorOpen(false)
      return
    }
    if (activeTool === 'remove') {
      removeNode(nodeId)
      return
    }
    setSelectedNodeId(nodeId)
    setPreviewNodeId(nodeId)
    setInspectorOpen(true)
    setPreviewCollapsed(false)
  }

  const handleLayout = () => {
    updateDefinition((current) => {
      const grouped = {
        input: current.nodes.filter((node) => node.type === 'input'),
        transform: current.nodes.filter((node) => node.type === 'transform'),
        output: current.nodes.filter((node) => node.type === 'output'),
      }
      const spaced = (nodes: PipelineNode[], x: number) =>
        nodes
          .slice()
          .sort((a, b) => a.y - b.y)
          .map((node, index) => ({ ...node, x, y: 80 + index * 140 }))
      return {
        ...current,
        nodes: [...spaced(grouped.input, 80), ...spaced(grouped.transform, 400), ...spaced(grouped.output, 1300)],
      }
    })
  }

  const handleSave = () => {
    if (!pipelineId) return
    updatePipelineMutation.mutate({
      name: pipelineName,
      location: pipelineLocation,
      pipeline_type: 'batch',
      definition_json: definition,
    })
  }

  const handleUndo = () => {
    setHistory((prev) => {
      if (prev.length === 0) return prev
      const last = prev[prev.length - 1]
      setFuture((next) => [definition, ...next])
      setDefinition(last)
      setIsDirty(true)
      return prev.slice(0, -1)
    })
  }

  const handleRedo = () => {
    setFuture((prev) => {
      if (prev.length === 0) return prev
      const [next, ...rest] = prev
      setHistory((historyPrev) => [...historyPrev, definition])
      setDefinition(next)
      setIsDirty(true)
      return rest
    })
  }

  const handleAddDatasetNode = (dataset: DatasetRecord) => {
    const datasetId = String(dataset.dataset_id ?? '')
    const columnCount = extractColumns(dataset.schema_json, language === 'ko' ? '문자열' : 'String').length
    updateDefinition((current) => {
      const inputs = current.nodes.filter((node) => node.type === 'input')
      const nextY = inputs.length > 0 ? Math.max(...inputs.map((node) => node.y)) + 140 : 80
      const node: PipelineNode = {
        id: createId('node'),
        title: String(dataset.name ?? 'dataset'),
        type: 'input',
        icon: 'th',
        x: 80,
        y: nextY,
        subtitle: columnCount ? `${columnCount} ${uiCopy.labels.columnsSuffix}` : undefined,
        status: 'success',
        metadata: { datasetId },
      }
      return { ...current, nodes: [...current.nodes, node] }
    })
    setDatasetDialogOpen(false)
  }

  const handleStartPipelining = (sheet: Record<string, unknown>) => {
    const sheetId = String(sheet.sheet_id ?? '')
    if (!sheetId) return
    const worksheetName = String(sheet.worksheet_name ?? '') || undefined
    startPipeliningMutation.mutate({ sheetId, dbName, worksheetName })
  }

  const handlePreviewConnector = (sheet: Record<string, unknown>) => {
    const sheetId = String(sheet.sheet_id ?? '')
    if (!sheetId) return
    setConnectorPreview(null)
    previewRegisteredSheetMutation.mutate({ sheetId, worksheetName: String(sheet.worksheet_name ?? '') || undefined })
  }

  const handleSimpleTransform = (operation: string, icon: PipelineNode['icon']) => {
    if (!selectedNode) return
    updateDefinition((current) => {
      const source = current.nodes.find((node) => node.id === selectedNode.id)
      if (!source) return current
      const node: PipelineNode = {
        id: createId('node'),
        title: `${source.title} · ${operation}`,
        type: 'transform',
        icon,
        x: source.x + 320,
        y: source.y,
        subtitle: source.subtitle,
        status: 'success',
        metadata: { operation },
      }
      return {
        ...current,
        nodes: [...current.nodes, node],
        edges: [...current.edges, { id: createId('edge'), from: source.id, to: node.id }],
      }
    })
  }

  const handleJoin = () => {
    if (!joinLeft || !joinRight) return
    updateDefinition((current) => {
      const leftNode = current.nodes.find((node) => node.id === joinLeft)
      const rightNode = current.nodes.find((node) => node.id === joinRight)
      if (!leftNode || !rightNode) return current
      const node: PipelineNode = {
        id: createId('node'),
        title: uiCopy.labels.join,
        type: 'transform',
        icon: 'inner-join',
        x: Math.max(leftNode.x, rightNode.x) + 320,
        y: Math.round((leftNode.y + rightNode.y) / 2),
        status: 'success',
        metadata: { operation: 'join', joinType },
      }
      return {
        ...current,
        nodes: [...current.nodes, node],
        edges: [
          ...current.edges,
          { id: createId('edge'), from: leftNode.id, to: node.id },
          { id: createId('edge'), from: rightNode.id, to: node.id },
        ],
      }
    })
    setJoinOpen(false)
  }

  const handleTransformSave = () => {
    if (!selectedNode) return
    updateDefinition((current) => ({
      ...current,
      nodes: current.nodes.map((node) =>
        node.id === selectedNode.id
          ? {
              ...node,
              title: transformDraft.title || node.title,
              metadata: { ...node.metadata, operation: transformDraft.operation, expression: transformDraft.expression },
            }
          : node,
      ),
    }))
    setTransformOpen(false)
  }

  const handleDeploy = () => {
    if (!pipelineId) return
      const outputDatasetName = deployDraft.datasetName || definition.outputs[0]?.datasetName || uiCopy.labels.outputDatasetFallback
    const outputNodeId = definition.nodes.find((node) => node.type === 'output')?.id
    deployMutation.mutate({
      db_name: dbName,
      definition_json: definition,
      node_id: outputNodeId,
      output: {
        db_name: dbName,
        dataset_name: outputDatasetName,
      },
    })
    setDeployOpen(false)
  }

  const handleSaveParameters = () => {
    updateDefinition((current) => ({ ...current, parameters: parameterDrafts }))
    setParametersOpen(false)
  }

  const handleSaveSettings = () => {
    updateDefinition((current) => ({ ...current, settings: { ...current.settings, ...deploySettings } }))
    setDeploySettingsOpen(false)
  }

  const handleAddOutput = () => {
    updateDefinition((current) => {
      const outputId = createId('output')
      const outputName = outputDraft.name || uiCopy.labels.output
      const outputDataset = outputDraft.datasetName || uiCopy.labels.outputDatasetFallback
      const outputs = [
        ...current.outputs,
        {
          id: outputId,
          name: outputName,
          datasetName: outputDataset,
          description: outputDraft.description || undefined,
        },
      ]
      const outputNodes = current.nodes.filter((node) => node.type === 'output')
      const nextY = outputNodes.length > 0 ? Math.max(...outputNodes.map((node) => node.y)) + 140 : 80
      const outputNode: PipelineNode = {
        id: createId('node'),
        title: outputName,
        type: 'output',
        icon: 'export',
        x: 1300,
        y: nextY,
        subtitle: outputDataset,
        status: 'success',
      }
      const edges = selectedNode
        ? [...current.edges, { id: createId('edge'), from: selectedNode.id, to: outputNode.id }]
        : current.edges
      return { ...current, outputs, nodes: [...current.nodes, outputNode], edges }
    })
    setOutputOpen(false)
  }

  const handleCreateManualDataset = () => {
    if (!manualDatasetName.trim()) return
    const columns = buildColumnsFromManual(manualColumns)
    createDatasetMutation.mutate({
      db_name: dbName,
      name: manualDatasetName.trim(),
      source_type: 'manual',
      schema_json: { columns },
    })
  }

  const handleNodeAction = (action: 'join' | 'filter' | 'compute' | 'visualize' | 'edit') => {
    if (!selectedNode) return
    if (action === 'join') {
      setJoinOpen(true)
      return
    }
    if (action === 'filter') {
      handleSimpleTransform('filter', 'filter')
      return
    }
    if (action === 'compute') {
      handleSimpleTransform('compute', 'function')
      return
    }
    if (action === 'visualize') {
      setVisualizeOpen(true)
      return
    }
    setTransformOpen(true)
  }

  const datasetOptions = useMemo(() => {
    const normalized = datasetSearch.trim().toLowerCase()
    return datasets.filter((dataset) =>
      String(dataset.name ?? '').toLowerCase().includes(normalized),
    )
  }, [datasets, datasetSearch])

  const connectorOptions = useMemo(() => {
    const normalized = connectorSearch.trim().toLowerCase()
    return connectorSheets.filter((sheet) => {
      const sheetId = String(sheet.sheet_id ?? '').toLowerCase()
      const worksheet = String(sheet.worksheet_name ?? '').toLowerCase()
      return sheetId.includes(normalized) || worksheet.includes(normalized)
    })
  }, [connectorSheets, connectorSearch])

  const connectorPreviewColumns = useMemo(() => connectorPreview?.columns ?? [], [connectorPreview])
  const connectorPreviewRows = useMemo(
    () => buildRowsFromMatrix(connectorPreview?.columns ?? [], connectorPreview?.rows ?? []),
    [connectorPreview],
  )

  const nodeOptions = definition.nodes.map((node) => ({ id: node.id, title: node.title }))
  const canUndo = history.length > 0
  const canRedo = future.length > 0
  const activeCommandCount = useMemo(
    () =>
      Object.values(commands).filter((cmd) => {
        if (cmd.expired) return false
        if (cmd.writePhase === 'FAILED' || cmd.writePhase === 'CANCELLED') return false
        if (cmd.writePhase === 'WRITE_DONE' && cmd.indexPhase === 'VISIBLE_IN_SEARCH') return false
        return true
      }).length,
    [commands],
  )

  const uiCopy = useMemo(
    () =>
      language === 'ko'
        ? {
            header: {
              file: '파일',
              help: '도움말',
              batch: '배치',
              undo: '되돌리기',
              redo: '다시 실행',
              deploySettings: '배포 설정',
              tabs: { edit: '편집', proposals: '제안', history: '기록' },
              commands: '커맨드',
              save: '저장',
              deploy: '배포',
              projectFallback: '프로젝트',
              pipelineFallback: '파이프라인',
              newPipeline: '새 파이프라인',
              openPipeline: '파이프라인 열기',
              saveMenu: '저장',
              noPipelines: '파이프라인 없음',
              noBranches: '브랜치 없음',
            },
            toolbar: {
              tools: '도구',
              select: '선택',
              remove: '삭제',
              layout: '정렬',
              addDatasets: '데이터 추가',
              parameters: '파라미터',
              transform: '변환',
              edit: '편집',
              zoomIn: '확대',
              zoomOut: '축소',
            },
            sidebar: {
              folders: '폴더',
              inputs: '입력',
              transforms: '변환',
              outputs: '출력',
              addData: '데이터 추가',
              details: '상세 정보',
              nameLabel: '이름',
              typeLabel: '유형',
              operationLabel: '작업',
              expressionLabel: '수식',
              outputsTitle: '출력',
              noOutputs: '출력 없음',
              editNode: '노드 편집',
              selectNode: '노드를 선택하세요.',
            },
            preview: {
              title: '미리보기',
              dataPreview: '데이터 미리보기',
              noNodes: '노드 없음',
              searchPlaceholder: (count: number) => `${count}개 컬럼 검색...`,
              formatType: (type: string) => {
                const normalized = type.toLowerCase()
                if (normalized.includes('date') || normalized.includes('time')) return '날짜'
                if (normalized.includes('bool')) return '불리언'
                if (normalized.includes('int') || normalized.includes('float') || normalized.includes('double') || normalized.includes('decimal') || normalized.includes('number')) return '숫자'
                return '문자열'
              },
              showPreview: '미리보기 열기',
            },
            dialogs: {
              warnings: {
                selectTransform: '변환할 노드를 선택하세요.',
                selectEdit: '편집할 노드를 선택하세요.',
              },
              pipeline: {
                title: '새 파이프라인',
                name: '파이프라인 이름',
                location: '저장 위치',
                description: '설명',
                create: '생성',
                cancel: '취소',
              },
              dataset: {
                title: '데이터 추가',
                tabs: {
                  connectors: '커넥터',
                  datasets: '데이터셋',
                  manual: '수동 입력',
                },
                connectorTitle: 'Google Sheets 연결',
                connectorPreview: '미리보기',
                connectorSearch: '커넥터 검색',
                connectorEmpty: '등록된 커넥터가 없습니다.',
                connectorPreviewEmpty: '미리보기 데이터를 불러오세요.',
                connectorPreviewTitle: '시트 미리보기',
                connectorAddToGraph: '그래프에 추가',
                callout: '추가 커넥터 관리는 데이터 연결에서 진행할 수 있습니다.',
                openConnections: '데이터 연결 열기',
                startPipelining: '파이프라인 시작 (등록된 커넥터)',
                noConnectors: '등록된 커넥터가 없습니다.',
                search: '데이터셋 검색',
                noDatasets: '데이터셋이 없습니다.',
                columns: '컬럼',
                manual: '수동 데이터셋 생성',
                datasetName: '데이터셋 이름',
                columnsPlaceholder: '컬럼 (쉼표로 구분)',
                sampleRows: '샘플 행 (선택, CSV 라인)',
                createDataset: '데이터셋 생성',
                addToGraph: '그래프에 추가',
                cancel: '취소',
              },
              parameters: {
                title: '파라미터',
                add: '파라미터 추가',
                namePlaceholder: '이름',
                valuePlaceholder: '값',
                cancel: '취소',
                save: '저장',
              },
              transform: {
                title: '변환 편집',
                nodeName: '노드 이름',
                operation: '작업',
                expression: '수식',
                cancel: '취소',
                save: '저장',
              },
              join: {
                title: '조인 생성',
                leftDataset: '왼쪽 데이터셋',
                rightDataset: '오른쪽 데이터셋',
                joinType: '조인 유형',
                selectNode: '노드 선택',
                inner: '내부',
                left: '왼쪽',
                right: '오른쪽',
                full: '전체',
                cancel: '취소',
                create: '조인 생성',
              },
              visualize: {
                title: '시각화',
                body: '미리보기 데이터가 로드되면 시각화를 사용할 수 있습니다.',
                close: '닫기',
              },
              deploy: {
                title: '파이프라인 배포',
                outputDataset: '출력 데이터셋 이름',
                rowCount: '행 수',
                artifactKey: '아티팩트 키',
                cancel: '취소',
                deploy: '배포',
              },
              deploySettings: {
                title: '배포 설정',
                compute: '컴퓨트 티어',
                memory: '메모리',
                schedule: '스케줄',
                engine: '엔진',
                cancel: '취소',
                save: '저장',
              },
              output: {
                title: '출력 추가',
                outputName: '출력 이름',
                datasetName: '데이터셋 이름',
                description: '설명',
                cancel: '취소',
                add: '출력 추가',
              },
              help: {
                title: '파이프라인 빌더 도움말',
                body: '데이터셋을 추가하고 그래프에서 변환을 만든 뒤 배포해 정제된 데이터셋을 생성합니다.',
                close: '닫기',
              },
            },
            toast: {
              pipelineCreated: '파이프라인이 생성되었습니다.',
              pipelineSaved: '파이프라인이 저장되었습니다.',
              pipelineDeployed: '파이프라인이 배포되었습니다.',
              datasetCreated: '데이터셋이 생성되었습니다.',
              connectorReady: '커넥터 데이터셋이 준비되었습니다.',
            },
            labels: {
              columnsSuffix: '컬럼',
              join: '조인',
              output: '출력',
              outputDatasetFallback: 'output_dataset',
              pipelineFallback: '파이프라인',
            },
            operations: {
              filter: '필터',
              compute: '계산',
              join: '조인',
            },
            nodeTypes: {
              input: '입력',
              transform: '변환',
              output: '출력',
            },
            canvas: {
              join: '조인',
              filter: '필터',
              compute: '계산',
              visualize: '시각화',
              edit: '편집',
            },
          }
        : {
            header: {
              file: 'File',
              help: 'Help',
              batch: 'Batch',
              undo: 'Undo',
              redo: 'Redo',
              deploySettings: 'Deployment settings',
              tabs: { edit: 'Edit', proposals: 'Proposals', history: 'History' },
              commands: 'Commands',
              save: 'Save',
              deploy: 'Deploy',
              projectFallback: 'Project',
              pipelineFallback: 'Pipeline',
              newPipeline: 'New pipeline',
              openPipeline: 'Open pipeline',
              saveMenu: 'Save',
              noPipelines: 'No pipelines',
              noBranches: 'No branches',
            },
            toolbar: {
              tools: 'Tools',
              select: 'Select',
              remove: 'Remove',
              layout: 'Layout',
              addDatasets: 'Add datasets',
              parameters: 'Parameters',
              transform: 'Transform',
              edit: 'Edit',
              zoomIn: 'Zoom in',
              zoomOut: 'Zoom out',
            },
            sidebar: {
              folders: 'Folders',
              inputs: 'Inputs',
              transforms: 'Transforms',
              outputs: 'Outputs',
              addData: 'Add data',
              details: 'Details',
              nameLabel: 'Name',
              typeLabel: 'Type',
              operationLabel: 'Operation',
              expressionLabel: 'Expression',
              outputsTitle: 'Outputs',
              noOutputs: 'No outputs defined yet.',
              editNode: 'Edit node',
              selectNode: 'Select a node to see details.',
            },
            preview: {
              title: 'Preview',
              dataPreview: 'Data preview',
              noNodes: 'No nodes',
              searchPlaceholder: (count: number) => `Search ${count} columns...`,
              formatType: (type: string) => type,
              showPreview: 'Show preview',
            },
            dialogs: {
              warnings: {
                selectTransform: 'Select a node to edit a transform.',
                selectEdit: 'Select a node to edit.',
              },
              pipeline: {
                title: 'New pipeline',
                name: 'Pipeline name',
                location: 'Location',
                description: 'Description',
                create: 'Create',
                cancel: 'Cancel',
              },
              dataset: {
                title: 'Add dataset',
                tabs: {
                  connectors: 'Connectors',
                  datasets: 'Datasets',
                  manual: 'Manual',
                },
                connectorTitle: 'Connect Google Sheets',
                connectorPreview: 'Preview',
                connectorSearch: 'Search connectors',
                connectorEmpty: 'No registered connectors yet.',
                connectorPreviewEmpty: 'Run a preview to see data here.',
                connectorPreviewTitle: 'Sheet preview',
                connectorAddToGraph: 'Add to graph',
                callout: 'Manage additional connectors in Data Connections.',
                openConnections: 'Open data connections',
                startPipelining: 'Start pipelining (registered connectors)',
                noConnectors: 'No registered connectors yet.',
                search: 'Search datasets',
                noDatasets: 'No datasets yet.',
                columns: 'columns',
                manual: 'Create manual dataset',
                datasetName: 'Dataset name',
                columnsPlaceholder: 'Columns (comma separated)',
                sampleRows: 'Sample rows (optional, CSV lines)',
                createDataset: 'Create dataset',
                addToGraph: 'Add to graph',
                cancel: 'Cancel',
              },
              parameters: {
                title: 'Parameters',
                add: 'Add parameter',
                namePlaceholder: 'Name',
                valuePlaceholder: 'Value',
                cancel: 'Cancel',
                save: 'Save',
              },
              transform: {
                title: 'Edit transform',
                nodeName: 'Node name',
                operation: 'Operation',
                expression: 'Expression',
                cancel: 'Cancel',
                save: 'Save',
              },
              join: {
                title: 'Create join',
                leftDataset: 'Left dataset',
                rightDataset: 'Right dataset',
                joinType: 'Join type',
                selectNode: 'Select node',
                inner: 'Inner',
                left: 'Left',
                right: 'Right',
                full: 'Full',
                cancel: 'Cancel',
                create: 'Create join',
              },
              visualize: {
                title: 'Visualize',
                body: 'Visualization will be available once preview data is loaded.',
                close: 'Close',
              },
              deploy: {
                title: 'Deploy pipeline',
                outputDataset: 'Output dataset name',
                rowCount: 'Row count',
                artifactKey: 'Artifact key',
                cancel: 'Cancel',
                deploy: 'Deploy',
              },
              deploySettings: {
                title: 'Deployment settings',
                compute: 'Compute tier',
                memory: 'Memory',
                schedule: 'Schedule',
                engine: 'Engine',
                cancel: 'Cancel',
                save: 'Save',
              },
              output: {
                title: 'Add output',
                outputName: 'Output name',
                datasetName: 'Dataset name',
                description: 'Description',
                cancel: 'Cancel',
                add: 'Add output',
              },
              help: {
                title: 'Pipeline Builder help',
                body: 'Add datasets, build transforms on the graph, and deploy outputs to create canonical datasets.',
                close: 'Close',
              },
            },
            toast: {
              pipelineCreated: 'Pipeline created.',
              pipelineSaved: 'Pipeline saved.',
              pipelineDeployed: 'Pipeline deployed.',
              datasetCreated: 'Dataset created.',
              connectorReady: 'Connector dataset ready.',
            },
            labels: {
              columnsSuffix: 'columns',
              join: 'Join',
              output: 'Output',
              outputDatasetFallback: 'output_dataset',
              pipelineFallback: 'Pipeline',
            },
            operations: {
              filter: 'Filter',
              compute: 'Compute',
              join: 'Join',
            },
            nodeTypes: {
              input: 'Input',
              transform: 'Transform',
              output: 'Output',
            },
            canvas: {
              join: 'Join',
              filter: 'Filter',
              compute: 'Compute',
              visualize: 'Visualize',
              edit: 'Edit',
            },
          },
    [language],
  )

  const connectorPanel = (
    <div className="dataset-tab">
      <div className="dataset-connector-panel">
        <FormGroup label={uiCopy.dialogs.dataset.connectorSearch}>
          <InputGroup value={connectorSearch} onChange={(event) => setConnectorSearch(event.currentTarget.value)} />
        </FormGroup>
        <div className="dialog-scroll">
          {connectorOptions.length === 0 ? (
            <Text className="muted">{uiCopy.dialogs.dataset.connectorEmpty}</Text>
          ) : (
            connectorOptions.map((sheet) => (
              <Card key={String(sheet.sheet_id ?? '')} className="dataset-card">
                <div className="dataset-card-header">
                  <Text>{String(sheet.sheet_id ?? '')}</Text>
                  <Tag minimal>{String(sheet.worksheet_name ?? uiCopy.dialogs.dataset.connectorTitle)}</Tag>
                </div>
                <Text className="muted">{String(sheet.sheet_url ?? '')}</Text>
                <div className="dataset-card-actions">
                  <Button minimal icon="eye-open" onClick={() => handlePreviewConnector(sheet)}>
                    {uiCopy.dialogs.dataset.connectorPreview}
                  </Button>
                  <Button minimal icon="add" onClick={() => handleStartPipelining(sheet)}>
                    {uiCopy.dialogs.dataset.connectorAddToGraph}
                  </Button>
                </div>
              </Card>
            ))
          )}
        </div>
      </div>
      <div className="dataset-preview-panel">
        <div className="dataset-preview-header">
          <Text>{uiCopy.dialogs.dataset.connectorPreviewTitle}</Text>
          {connectorPreview ? (
            <Text className="muted">
              {connectorPreview.sheetTitle ?? connectorPreview.sheetId} · {connectorPreview.worksheetTitle ?? ''}
            </Text>
          ) : null}
        </div>
        {connectorPreviewColumns.length === 0 ? (
          <Text className="muted">{uiCopy.dialogs.dataset.connectorPreviewEmpty}</Text>
        ) : (
          <HTMLTable compact striped className="dataset-preview-table">
            <thead>
              <tr>
                {connectorPreviewColumns.map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {connectorPreviewRows.map((row, index) => (
                <tr key={`${index}-${connectorPreview?.sheetId ?? 'preview'}`}>
                  {connectorPreviewColumns.map((column) => (
                    <td key={`${index}-${column}`}>{row[column] ?? ''}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </HTMLTable>
        )}
      </div>
    </div>
  )

  const datasetsPanel = (
    <div className="dataset-tab">
      <FormGroup label={uiCopy.dialogs.dataset.search}>
        <InputGroup value={datasetSearch} onChange={(event) => setDatasetSearch(event.currentTarget.value)} />
      </FormGroup>
      <div className="dialog-scroll">
        {datasetOptions.length === 0 ? (
          <Text className="muted">{uiCopy.dialogs.dataset.noDatasets}</Text>
        ) : (
          datasetOptions.map((dataset) => {
            const datasetId = String(dataset.dataset_id ?? '')
            const columnCount = extractColumns(dataset.schema_json).length
            return (
              <Card
                key={datasetId}
                className={`dataset-card ${datasetId === selectedDatasetId ? 'is-selected' : ''}`}
                onClick={() => setSelectedDatasetId(datasetId)}
              >
                <Text>{String(dataset.name ?? datasetId)}</Text>
                <Text className="muted">{columnCount} {uiCopy.dialogs.dataset.columns}</Text>
              </Card>
            )
          })
        )}
      </div>
    </div>
  )

  const manualPanel = (
    <div className="dataset-tab">
      <FormGroup label={uiCopy.dialogs.dataset.manual}>
        <InputGroup
          placeholder={uiCopy.dialogs.dataset.datasetName}
          value={manualDatasetName}
          onChange={(event) => setManualDatasetName(event.currentTarget.value)}
        />
        <TextArea
          placeholder={uiCopy.dialogs.dataset.columnsPlaceholder}
          value={manualColumns}
          onChange={(event) => setManualColumns(event.currentTarget.value)}
        />
        <TextArea
          placeholder={uiCopy.dialogs.dataset.sampleRows}
          value={manualRows}
          onChange={(event) => setManualRows(event.currentTarget.value)}
        />
        <Button
          icon="add"
          minimal
          onClick={handleCreateManualDataset}
          disabled={!manualDatasetName.trim() || createDatasetMutation.isPending}
          loading={createDatasetMutation.isPending}
        >
          {uiCopy.dialogs.dataset.createDataset}
        </Button>
      </FormGroup>
    </div>
  )

  return (
    <div className="pipeline-builder-container">
      <PipelineHeader
        dbName={dbName}
        pipelineName={pipelineName || uiCopy.labels.pipelineFallback}
        pipelines={pipelineOptions}
        mode={mode}
        branch={branch}
        branches={branches}
        activeCommandCount={activeCommandCount}
        copy={uiCopy.header}
        canUndo={canUndo}
        canRedo={canRedo}
        isDirty={isDirty}
        onModeChange={setMode}
        onUndo={handleUndo}
        onRedo={handleRedo}
        onBranchSelect={setBranch}
        onPipelineSelect={(id) => {
          setPipelineId(id)
          setPipelineName('')
          setPipelineLocation(pipelineLocation)
        }}
        onSave={handleSave}
        onDeploy={() => {
          setDeployDraft({
            datasetName: definition.outputs[0]?.datasetName ?? '',
            rowCount: '500',
            artifactKey: '',
          })
          setDeployOpen(true)
        }}
        onOpenDeploySettings={() => setDeploySettingsOpen(true)}
        onOpenHelp={() => setHelpOpen(true)}
        onCreatePipeline={() => setPipelineDialogOpen(true)}
        onOpenCommandDrawer={() => setCommandDrawerOpen(true)}
      />

      <PipelineToolbar
        activeTool={activeTool}
        copy={uiCopy.toolbar}
        onToolChange={setActiveTool}
        onLayout={handleLayout}
        onRemove={() => {
          if (selectedNodeId) {
            removeNode(selectedNodeId)
            return
          }
          setActiveTool('remove')
        }}
        onAddDatasets={() => setDatasetDialogOpen(true)}
        onParameters={() => setParametersOpen(true)}
        onTransform={() => {
          if (!selectedNodeId) {
            void showAppToast({ intent: Intent.WARNING, message: uiCopy.dialogs.warnings.selectTransform })
            return
          }
          setTransformOpen(true)
        }}
        onEdit={() => {
          if (!selectedNodeId) {
            void showAppToast({ intent: Intent.WARNING, message: uiCopy.dialogs.warnings.selectEdit })
            return
          }
          setTransformOpen(true)
        }}
        onZoomIn={() => setCanvasZoom((current) => Math.min(1.4, current + 0.1))}
        onZoomOut={() => setCanvasZoom((current) => Math.max(0.6, current - 0.1))}
      />

      <div className={`pipeline-body ${inspectorOpen ? 'has-inspector' : 'no-inspector'}`}>
        <aside className="pipeline-sidebar pipeline-sidebar-left">
          <div className="pipeline-sidebar-section">
            <div className="pipeline-sidebar-title">{uiCopy.sidebar.folders}</div>
            <div className="pipeline-sidebar-item">
              <Tag minimal icon="folder-close">{pipelineLocation}</Tag>
            </div>
          </div>
          <Divider />
          <div className="pipeline-sidebar-section">
            <div className="pipeline-sidebar-title">{uiCopy.sidebar.inputs}</div>
            {definition.nodes.filter((node) => node.type === 'input').map((node) => (
              <button
                key={node.id}
                type="button"
                className={`pipeline-sidebar-item ${node.id === selectedNodeId ? 'is-active' : ''}`}
                onClick={() => handleSelectNode(node.id)}
              >
                {node.title}
              </button>
            ))}
          </div>
          <Divider />
          <div className="pipeline-sidebar-section">
            <div className="pipeline-sidebar-title">{uiCopy.sidebar.transforms}</div>
            {definition.nodes.filter((node) => node.type === 'transform').map((node) => (
              <button
                key={node.id}
                type="button"
                className={`pipeline-sidebar-item ${node.id === selectedNodeId ? 'is-active' : ''}`}
                onClick={() => handleSelectNode(node.id)}
              >
                {node.title}
              </button>
            ))}
          </div>
          <Divider />
          <div className="pipeline-sidebar-section">
            <div className="pipeline-sidebar-title">{uiCopy.sidebar.outputs}</div>
            {definition.nodes.filter((node) => node.type === 'output').map((node) => (
              <button
                key={node.id}
                type="button"
                className={`pipeline-sidebar-item ${node.id === selectedNodeId ? 'is-active' : ''}`}
                onClick={() => handleSelectNode(node.id)}
              >
                {node.title}
              </button>
            ))}
          </div>
        </aside>

        <div className="pipeline-canvas-area">
          <PipelineCanvas
            nodes={definition.nodes}
            edges={definition.edges}
            selectedNodeId={selectedNodeId}
            copy={uiCopy.canvas}
            zoom={canvasZoom}
            onSelectNode={handleSelectNode}
            onNodeAction={(action) => handleNodeAction(action)}
          />
        </div>

        <aside className={`pipeline-sidebar pipeline-sidebar-right ${inspectorOpen ? 'is-open' : 'is-collapsed'}`}>
          <div className="pipeline-sidebar-section">
            <div className="pipeline-sidebar-header">
              <div className="pipeline-sidebar-title">{uiCopy.sidebar.details}</div>
              <Button
                icon="chevron-right"
                minimal
                small
                aria-label={language === 'ko' ? '인스펙터 접기' : 'Collapse inspector'}
                onClick={() => setInspectorOpen(false)}
              />
            </div>
            {selectedNode ? (
              <Card className="pipeline-detail-card">
                <Text className="pipeline-detail-label">{uiCopy.sidebar.nameLabel}</Text>
                <Text>{selectedNode.title}</Text>
                <Text className="pipeline-detail-label">{uiCopy.sidebar.typeLabel}</Text>
                <Text>{uiCopy.nodeTypes[selectedNode.type] ?? selectedNode.type}</Text>
                <Text className="pipeline-detail-label">{uiCopy.sidebar.operationLabel}</Text>
                <Text>{selectedNode.metadata?.operation ? uiCopy.operations[selectedNode.metadata.operation as keyof typeof uiCopy.operations] ?? selectedNode.metadata.operation : '—'}</Text>
                <Text className="pipeline-detail-label">{uiCopy.sidebar.expressionLabel}</Text>
                <Text>{selectedNode.metadata?.expression ?? '—'}</Text>
                <Button icon="edit" minimal small onClick={() => setTransformOpen(true)}>{uiCopy.sidebar.editNode}</Button>
              </Card>
            ) : (
              <Text className="muted">{uiCopy.sidebar.selectNode}</Text>
            )}
          </div>
          <Divider />
          <div className="pipeline-sidebar-section">
            <div className="pipeline-sidebar-title">{uiCopy.sidebar.outputsTitle}</div>
            {definition.outputs.length === 0 ? (
              <Text className="muted">{uiCopy.sidebar.noOutputs}</Text>
            ) : (
              definition.outputs.map((output) => (
                <Card key={output.id} className="pipeline-output-card">
                  <Text>{output.name}</Text>
                  <Text className="muted">{output.datasetName}</Text>
                </Card>
              ))
            )}
            <Button icon="add" minimal small onClick={() => setOutputOpen(true)}>{uiCopy.dialogs.output.add}</Button>
          </div>
        </aside>
      </div>

      {previewCollapsed ? (
        <div className="preview-collapsed">
          <Button
            icon="chevron-up"
            minimal
            small
            aria-label={uiCopy.preview.showPreview}
            onClick={() => setPreviewCollapsed(false)}
          >
            {uiCopy.preview.showPreview}
          </Button>
        </div>
      ) : (
        <PipelinePreview
          nodes={nodeOptions}
          selectedNodeId={previewNode?.id ?? null}
          columns={previewSchema}
          rows={previewRows}
          copy={uiCopy.preview}
          onSelectNode={(nodeId) => setPreviewNodeId(nodeId)}
          onToggleFullscreen={() => {
            setPreviewFullscreen((current) => !current)
          }}
          onCollapse={() => {
            setPreviewCollapsed(true)
            setPreviewFullscreen(false)
          }}
          fullscreen={previewFullscreen}
        />
      )}

      <Dialog isOpen={pipelineDialogOpen} onClose={() => setPipelineDialogOpen(false)} title={uiCopy.dialogs.pipeline.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.pipeline.name}>
            <InputGroup value={newPipelineName} onChange={(event) => setNewPipelineName(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.pipeline.location}>
            <InputGroup value={newPipelineLocation} onChange={(event) => setNewPipelineLocation(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.pipeline.description}>
            <TextArea value={newPipelineDescription} onChange={(event) => setNewPipelineDescription(event.currentTarget.value)} />
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setPipelineDialogOpen(false)}>{uiCopy.dialogs.pipeline.cancel}</Button>
          <Button
            intent={Intent.PRIMARY}
            onClick={() =>
              createPipelineMutation.mutate({
                db_name: dbName,
                name: newPipelineName.trim(),
                pipeline_type: 'batch',
                location: newPipelineLocation.trim(),
                description: newPipelineDescription.trim() || undefined,
                definition_json: definition,
              })
            }
            disabled={!newPipelineName.trim() || !newPipelineLocation.trim()}
            loading={createPipelineMutation.isPending}
          >
            {uiCopy.dialogs.pipeline.create}
          </Button>
        </div>
      </Dialog>

      <Dialog isOpen={datasetDialogOpen} onClose={() => setDatasetDialogOpen(false)} title={uiCopy.dialogs.dataset.title}>
        <div className="dialog-body">
          <Callout intent={Intent.PRIMARY} icon="database">
            {uiCopy.dialogs.dataset.callout}
            <Button minimal icon="link" onClick={() => navigate(`/db/${encodeURIComponent(dbName)}/data/sheets`)}>
              {uiCopy.dialogs.dataset.openConnections}
            </Button>
          </Callout>
          <Tabs id="dataset-tabs" selectedTabId={datasetTab} onChange={(tabId) => setDatasetTab(String(tabId))}>
            <Tab id="connectors" title={uiCopy.dialogs.dataset.tabs.connectors} panel={connectorPanel} />
            <Tab id="datasets" title={uiCopy.dialogs.dataset.tabs.datasets} panel={datasetsPanel} />
            <Tab id="manual" title={uiCopy.dialogs.dataset.tabs.manual} panel={manualPanel} />
          </Tabs>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setDatasetDialogOpen(false)}>{uiCopy.dialogs.dataset.cancel}</Button>
          {datasetTab === 'datasets' ? (
            <Button
              intent={Intent.PRIMARY}
              onClick={() => {
                const dataset = datasets.find((item) => String(item.dataset_id ?? '') === selectedDatasetId)
                if (dataset) {
                  handleAddDatasetNode(dataset)
                }
              }}
              disabled={!selectedDatasetId}
            >
              {uiCopy.dialogs.dataset.addToGraph}
            </Button>
          ) : null}
        </div>
      </Dialog>

      <Dialog isOpen={parametersOpen} onClose={() => setParametersOpen(false)} title={uiCopy.dialogs.parameters.title}>
        <div className="dialog-body">
          {parameterDrafts.map((param, index) => (
            <div key={param.id} className="parameter-row">
              <InputGroup
                placeholder={uiCopy.dialogs.parameters.namePlaceholder}
                value={param.name}
                onChange={(event) => {
                  const value = event.currentTarget.value
                  setParameterDrafts((current) => current.map((item, idx) => (idx === index ? { ...item, name: value } : item)))
                }}
              />
              <InputGroup
                placeholder={uiCopy.dialogs.parameters.valuePlaceholder}
                value={param.value}
                onChange={(event) => {
                  const value = event.currentTarget.value
                  setParameterDrafts((current) => current.map((item, idx) => (idx === index ? { ...item, value } : item)))
                }}
              />
              <Button
                icon="trash"
                minimal
                intent={Intent.DANGER}
                onClick={() => setParameterDrafts((current) => current.filter((item) => item.id !== param.id))}
              />
            </div>
          ))}
          <Button
            icon="add"
            minimal
            onClick={() => setParameterDrafts((current) => [...current, { id: createId('param'), name: '', value: '' }])}
          >
            {uiCopy.dialogs.parameters.add}
          </Button>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setParametersOpen(false)}>{uiCopy.dialogs.parameters.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleSaveParameters}>{uiCopy.dialogs.parameters.save}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={transformOpen} onClose={() => setTransformOpen(false)} title={uiCopy.dialogs.transform.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.transform.nodeName}>
            <InputGroup value={transformDraft.title} onChange={(event) => setTransformDraft((prev) => ({ ...prev, title: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.transform.operation}>
            <InputGroup value={transformDraft.operation} onChange={(event) => setTransformDraft((prev) => ({ ...prev, operation: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.transform.expression}>
            <TextArea value={transformDraft.expression} onChange={(event) => setTransformDraft((prev) => ({ ...prev, expression: event.currentTarget.value }))} />
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setTransformOpen(false)}>{uiCopy.dialogs.transform.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleTransformSave} disabled={!selectedNode}>{uiCopy.dialogs.transform.save}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={joinOpen} onClose={() => setJoinOpen(false)} title={uiCopy.dialogs.join.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.join.leftDataset}>
            <HTMLSelect value={joinLeft} onChange={(event) => setJoinLeft(event.currentTarget.value)}>
              <option value="">{uiCopy.dialogs.join.selectNode}</option>
              {definition.nodes.map((node) => (
                <option key={node.id} value={node.id}>{node.title}</option>
              ))}
            </HTMLSelect>
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.join.rightDataset}>
            <HTMLSelect value={joinRight} onChange={(event) => setJoinRight(event.currentTarget.value)}>
              <option value="">{uiCopy.dialogs.join.selectNode}</option>
              {definition.nodes.map((node) => (
                <option key={node.id} value={node.id}>{node.title}</option>
              ))}
            </HTMLSelect>
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.join.joinType}>
            <HTMLSelect value={joinType} onChange={(event) => setJoinType(event.currentTarget.value)}>
              <option value="inner">{uiCopy.dialogs.join.inner}</option>
              <option value="left">{uiCopy.dialogs.join.left}</option>
              <option value="right">{uiCopy.dialogs.join.right}</option>
              <option value="full">{uiCopy.dialogs.join.full}</option>
            </HTMLSelect>
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setJoinOpen(false)}>{uiCopy.dialogs.join.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleJoin} disabled={!joinLeft || !joinRight}>{uiCopy.dialogs.join.create}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={visualizeOpen} onClose={() => setVisualizeOpen(false)} title={uiCopy.dialogs.visualize.title}>
        <div className="dialog-body">
          <Text className="muted">{uiCopy.dialogs.visualize.body}</Text>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setVisualizeOpen(false)}>{uiCopy.dialogs.visualize.close}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={deployOpen} onClose={() => setDeployOpen(false)} title={uiCopy.dialogs.deploy.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.deploy.outputDataset}>
            <InputGroup value={deployDraft.datasetName} onChange={(event) => setDeployDraft((prev) => ({ ...prev, datasetName: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploy.rowCount}>
            <InputGroup value={deployDraft.rowCount} onChange={(event) => setDeployDraft((prev) => ({ ...prev, rowCount: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploy.artifactKey}>
            <InputGroup value={deployDraft.artifactKey} onChange={(event) => setDeployDraft((prev) => ({ ...prev, artifactKey: event.currentTarget.value }))} />
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setDeployOpen(false)}>{uiCopy.dialogs.deploy.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleDeploy} loading={deployMutation.isPending}>{uiCopy.dialogs.deploy.deploy}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={deploySettingsOpen} onClose={() => setDeploySettingsOpen(false)} title={uiCopy.dialogs.deploySettings.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.deploySettings.compute}>
            <InputGroup value={deploySettings.compute} onChange={(event) => setDeploySettings((prev) => ({ ...prev, compute: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.memory}>
            <InputGroup value={deploySettings.memory} onChange={(event) => setDeploySettings((prev) => ({ ...prev, memory: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.schedule}>
            <InputGroup value={deploySettings.schedule} onChange={(event) => setDeploySettings((prev) => ({ ...prev, schedule: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.engine}>
            <InputGroup value={deploySettings.engine} onChange={(event) => setDeploySettings((prev) => ({ ...prev, engine: event.currentTarget.value }))} />
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setDeploySettingsOpen(false)}>{uiCopy.dialogs.deploySettings.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleSaveSettings}>{uiCopy.dialogs.deploySettings.save}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={outputOpen} onClose={() => setOutputOpen(false)} title={uiCopy.dialogs.output.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.output.outputName}>
            <InputGroup value={outputDraft.name} onChange={(event) => setOutputDraft((prev) => ({ ...prev, name: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.output.datasetName}>
            <InputGroup value={outputDraft.datasetName} onChange={(event) => setOutputDraft((prev) => ({ ...prev, datasetName: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.output.description}>
            <TextArea value={outputDraft.description} onChange={(event) => setOutputDraft((prev) => ({ ...prev, description: event.currentTarget.value }))} />
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setOutputOpen(false)}>{uiCopy.dialogs.output.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleAddOutput} disabled={!outputDraft.datasetName.trim()}>{uiCopy.dialogs.output.add}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={helpOpen} onClose={() => setHelpOpen(false)} title={uiCopy.dialogs.help.title}>
        <div className="dialog-body">
          <Text className="muted">
            {uiCopy.dialogs.help.body}
          </Text>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setHelpOpen(false)}>{uiCopy.dialogs.help.close}</Button>
        </div>
      </Dialog>


    </div>
  )
}
