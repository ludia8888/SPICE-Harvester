import { useState, useCallback, useEffect, useMemo, useRef } from 'react'
import {
  Button,
  Callout,
  Intent,
  Spinner,
} from '@blueprintjs/core'
import {
  useNodesState,
  useEdgesState,
  addEdge,
  type Node,
  type Edge,
  type OnConnectStart,
  type OnConnectEnd,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useRequestContext } from '../api/useRequestContext'
import { useAppStore } from '../store/useAppStore'
import { navigate } from '../state/pathname'
import type { RequestContext, PipelineRecord, PipelineDetailRecord, DatasetRecord, PipelineDefinition, PipelineRunRecord } from '../api/bff'
import {
  listPipelines,
  createPipeline,
  getPipeline,
  updatePipeline,
  deletePipeline,
  previewPipeline,
  buildPipeline,
  deployPipeline,
  listPipelineDatasets,
  listPipelineBranches,
  createPipelineBranch,
  listPipelineRuns,
  getDatasetSchema,
} from '../api/bff'

/* New visual pipeline components */
import { PipelineCanvas } from '../components/pipeline/PipelineCanvas'
import { PipelineToolbar } from '../components/pipeline/PipelineToolbar'
import { PipelineToolbox } from '../components/pipeline/PipelineToolbox'
import { PipelinePreviewPanel } from '../components/pipeline/PipelinePreviewPanel'
import { PipelineRightPanel } from '../components/pipeline/PipelineRightPanel'
import { DatasetPickerDialog } from '../components/pipeline/DatasetPickerDialog'
import { QuickAddMenu } from '../components/pipeline/QuickAddMenu'
import { NodeContextToolbar } from '../components/pipeline/NodeContextToolbar'
import { ScheduleDialog } from '../components/pipeline/ScheduleDialog'
import { PipelineManagerDialog } from '../components/pipeline/PipelineManagerDialog'
import { PipelineProposalsView } from '../components/pipeline/PipelineProposalsView'
import { PipelineLegend, type LegendColor } from '../components/pipeline/PipelineLegend'
import { PipelineHistoryView } from '../components/pipeline/PipelineHistoryView'
import { nodeToBackend, backendNodeToConfig } from '../components/pipeline/pipelineSerializer'
import { resolveRFNodeType, getCatalog } from '../components/pipeline/nodes/nodeStyles'
import { PageHeader } from '../components/layout/PageHeader'

/* ── query keys ─────────────────────────────────────── */
const plKeys = {
  list: (db: string, branch: string) => ['pipelines', db, branch] as const,
  detail: (id: string) => ['pipelines', 'detail', id] as const,
  runs: (id: string) => ['pipelines', 'runs', id] as const,
  readiness: (id: string) => ['pipelines', 'readiness', id] as const,
}

/* ── node id generator ──────────────────────────────── */
let nodeIdCounter = 0
const nextNodeId = () => `node_${++nodeIdCounter}`

type PipelineNodeData = {
  label: string
  transformType: string
  config: Record<string, unknown>
  dimmed?: boolean
  legendColorId?: string | null
  legendColor?: string | null
}

type PipelineNode = Node<PipelineNodeData>

/* ── page ────────────────────────────────────────────── */
export const PipelineBuilderPage = ({
  dbName,
  pipelineId,
}: {
  dbName: string
  pipelineId: string | null
}) => {
  const ctx = useRequestContext()
  const branch = useAppStore((s) => s.context.branch)

  /* ── No pipelineId → auto-redirect to latest (or create one) */
  if (!pipelineId) {
    return <PipelineAutoRedirect dbName={dbName} branch={branch} ctx={ctx} />
  }
  return <PipelineEditorView dbName={dbName} pipelineId={pipelineId} branch={branch} ctx={ctx} />
}

/* ════════════════════════════════════════════════════════
   Auto-redirect: navigate to latest pipeline or create one
   ════════════════════════════════════════════════════════ */
const PipelineAutoRedirect = ({
  dbName,
  branch,
  ctx,
}: {
  dbName: string
  branch: string
  ctx: RequestContext
}) => {
  const [status, setStatus] = useState<'loading' | 'creating' | 'error'>('loading')
  const [error, setError] = useState<string | null>(null)
  const didRun = useRef(false)

  const base = `/db/${encodeURIComponent(dbName)}/pipelines`

  useEffect(() => {
    if (didRun.current) return
    didRun.current = true

    ;(async () => {
      try {
        const pipelines = await listPipelines(ctx, { db_name: dbName, branch })
        const list: PipelineRecord[] = Array.isArray(pipelines) ? pipelines : []

        if (list.length > 0) {
          // Navigate to the most recently updated pipeline
          const sorted = [...list].sort((a, b) => {
            const da = a.updated_at ? new Date(a.updated_at).getTime() : 0
            const db = b.updated_at ? new Date(b.updated_at).getTime() : 0
            return db - da
          })
          navigate(`${base}/${sorted[0].pipeline_id}`, 'replace')
        } else {
          // No pipelines — auto-create one
          setStatus('creating')
          const created = await createPipeline(ctx, {
            db_name: dbName,
            name: 'Untitled Pipeline',
            branch,
            location: dbName,
            pipeline_type: 'transform',
          })
          if (created?.pipeline_id) {
            navigate(`${base}/${created.pipeline_id}`, 'replace')
          } else {
            setStatus('error')
            setError('Failed to create pipeline: no ID returned.')
          }
        }
      } catch (err) {
        setStatus('error')
        setError(err instanceof Error ? err.message : String(err))
      }
    })()
  }, [ctx, dbName, branch, base])

  if (status === 'error') {
    return (
      <div style={{ padding: 40, textAlign: 'center' }}>
        <Callout intent={Intent.DANGER}>{error ?? 'Something went wrong.'}</Callout>
      </div>
    )
  }

  return (
    <div style={{ padding: 40, textAlign: 'center' }}>
      <Spinner size={30} />
      <p style={{ marginTop: 12, color: 'var(--foundry-text-muted)' }}>
        {status === 'creating' ? 'Creating new pipeline…' : 'Loading pipelines…'}
      </p>
    </div>
  )
}

/* ════════════════════════════════════════════════════════
   Pipeline Editor View (Visual DAG Builder)
   ════════════════════════════════════════════════════════ */
const PipelineEditorView = ({
  dbName,
  pipelineId,
  branch,
  ctx,
}: {
  dbName: string
  pipelineId: string
  branch: string
  ctx: RequestContext
}) => {
  const queryClient = useQueryClient()
  const setSettingsOpen = useAppStore((s) => s.setSettingsOpen)

  /* ── UI state ───────────────────────────────────── */
  const [activeTab, setActiveTab] = useState<'edit' | 'proposals' | 'history'>('edit')
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const [interactionMode, setInteractionMode] = useState<'pan' | 'select' | 'remove' | 'edit'>('pan')
  const [previewOpen, setPreviewOpen] = useState(false)
  const [datasetPickerOpen, setDatasetPickerOpen] = useState(false)
  const [scheduleDialogOpen, setScheduleDialogOpen] = useState(false)
  const [pipelineManagerOpen, setPipelineManagerOpen] = useState(false)
  const [legendColors, setLegendColors] = useState<LegendColor[]>([])
  const [lastBuildJobId, setLastBuildJobId] = useState<string | null>(null)
  const [nodeContextToolbar, setNodeContextToolbar] = useState<{
    x: number; y: number; sourceNodeId: string
  } | null>(null)

  /* ── Preview polling state ────────────────────── */
  const [previewData, setPreviewData] = useState<Record<string, unknown> | null>(null)
  const [previewError, setPreviewError] = useState<string | null>(null)
  const [previewPolling, setPreviewPolling] = useState(false)
  const previewPollRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const previewJobIdRef = useRef<string | null>(null)

  /* ── Node-click auto-preview state ─────────────── */
  const [nodePreviewLoading, setNodePreviewLoading] = useState(false)
  const lastPreviewedNodeRef = useRef<string | null>(null)

  /* ── Quick-add menu state (drag-from-handle) ──── */
  const connectingFromRef = useRef<{ nodeId: string; handleId: string } | null>(null)
  const [quickAddMenu, setQuickAddMenu] = useState<{
    x: number; y: number; sourceNodeId: string; sourceHandleId: string
  } | null>(null)

  /* ── Undo/Redo stacks ──────────────────────────── */
  const undoStack = useRef<Array<{ nodes: PipelineNode[]; edges: Edge[] }>>([])
  const redoStack = useRef<Array<{ nodes: PipelineNode[]; edges: Edge[] }>>([])
  const pushUndo = (ns: PipelineNode[], es: Edge[]) => {
    undoStack.current.push({ nodes: ns.map((n) => ({ ...n })), edges: es.map((e) => ({ ...e })) })
    if (undoStack.current.length > 50) undoStack.current.shift()
    redoStack.current = []
  }

  /* ── Preview polling cleanup ───────────────────── */
  useEffect(() => {
    return () => {
      if (previewPollRef.current) clearInterval(previewPollRef.current)
    }
  }, [])

  const pollPreviewResult = useCallback((jobId: string) => {
    previewJobIdRef.current = jobId
    setPreviewPolling(true)
    setPreviewError(null)

    if (previewPollRef.current) clearInterval(previewPollRef.current)

    const poll = async () => {
      try {
        const runs = await listPipelineRuns(ctx, pipelineId)
        const matchingRun = runs.find((r: PipelineRunRecord) => r.job_id === jobId)
        if (!matchingRun) return // not found yet, keep polling

        const runStatus = (matchingRun.status ?? '').toUpperCase()
        if (runStatus === 'SUCCESS' || runStatus === 'FAILED') {
          if (previewPollRef.current) clearInterval(previewPollRef.current)
          previewPollRef.current = null
          setPreviewPolling(false)
          previewJobIdRef.current = null

          if (runStatus === 'SUCCESS' && matchingRun.sample_json) {
            setPreviewData(matchingRun.sample_json)
            setPreviewError(null)
          } else if (runStatus === 'FAILED') {
            const errDetail = matchingRun.sample_json as Record<string, unknown> | null
            const errMsg = errDetail?.message ?? errDetail?.error ?? 'Preview execution failed'
            setPreviewError(String(errMsg))
            setPreviewData(null)
          }
        }
      } catch (err) {
        console.warn('Preview poll error:', err)
      }
    }

    poll()
    previewPollRef.current = setInterval(poll, 2000)

    // Safety: stop polling after 5 minutes
    setTimeout(() => {
      if (previewPollRef.current && previewJobIdRef.current === jobId) {
        clearInterval(previewPollRef.current)
        previewPollRef.current = null
        setPreviewPolling(false)
        setPreviewError('Preview timed out. Try again.')
      }
    }, 5 * 60 * 1000)
  }, [ctx, pipelineId])

  /* ── Load pipeline detail ──────────────────────── */
  const detailQ = useQuery({
    queryKey: plKeys.detail(pipelineId),
    queryFn: () => getPipeline(ctx, pipelineId),
  })

  const pipeline = detailQ.data as PipelineDetailRecord | undefined
  const definition = pipeline?.definition_json ?? { nodes: [], edges: [] }
  const pipelineBranch = useMemo(
    () => String(pipeline?.branch ?? branch).trim() || 'main',
    [pipeline?.branch, branch],
  )

  useEffect(() => {
    setLastBuildJobId(null)
  }, [pipelineId])

  /* ── Load available datasets (for right panel + picker) ── */
  const datasetsQ = useQuery({
    queryKey: ['pipeline-datasets', dbName, pipelineBranch],
    queryFn: () => listPipelineDatasets(ctx, { db_name: dbName, branch: pipelineBranch }),
    enabled: !!dbName,
  })
  const availableDatasets: DatasetRecord[] = Array.isArray(datasetsQ.data) ? datasetsQ.data : []

  /* ── Load available branches ─────────────────────── */
  const branchesQ = useQuery({
    queryKey: ['pipeline-branches', dbName],
    queryFn: () => listPipelineBranches(ctx, { db_name: dbName }),
    enabled: !!dbName,
    retry: 1,
    staleTime: 30_000,
  })
  const branches: string[] = Array.isArray(branchesQ.data) ? branchesQ.data : ['main']

  const setBranch = useAppStore((s) => s.setBranch)
  const handleBranchChange = useCallback((newBranch: string) => {
    setBranch(newBranch)
    queryClient.invalidateQueries({ queryKey: plKeys.detail(pipelineId) })
    queryClient.invalidateQueries({ queryKey: ['pipeline-datasets', dbName] })
  }, [setBranch, queryClient, pipelineId, dbName])

  const createBranchMut = useMutation({
    mutationFn: (name: string) =>
      createPipelineBranch(ctx, { pipeline_id: pipelineId, branch: name }),
    onSuccess: (_data, name) => {
      queryClient.invalidateQueries({ queryKey: ['pipeline-branches', dbName] })
      handleBranchChange(name)
    },
  })
  const handleBranchCreate = useCallback((name: string) => {
    createBranchMut.mutate(name)
  }, [createBranchMut])

  /* ── Dataset schema cache ─────────────────────────── */
  type ColSchema = { name: string; type: string }
  const [schemaCache, setSchemaCache] = useState<Map<string, ColSchema[]>>(new Map())

  // Extract schemas from already-loaded dataset records (schema_json field)
  useEffect(() => {
    const newEntries: Array<[string, ColSchema[]]> = []
    for (const ds of availableDatasets) {
      if (schemaCache.has(ds.dataset_id)) continue
      const sj = ds.schema_json as Record<string, unknown> | undefined
      const cols = (sj?.columns ?? []) as Array<Record<string, unknown>>
      if (Array.isArray(cols) && cols.length > 0) {
        newEntries.push([
          ds.dataset_id,
          cols.map((c) => ({ name: String(c.name ?? ''), type: String(c.type ?? 'unknown') })).filter((c) => c.name),
        ])
      }
    }
    if (newEntries.length > 0) {
      setSchemaCache((prev) => {
        const next = new Map(prev)
        for (const [k, v] of newEntries) next.set(k, v)
        return next
      })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [availableDatasets])

  /* ── Auto-fetch preview when a node is clicked ──── */
  const fetchNodePreview = useCallback(
    async (nodeId: string, nodesList: PipelineNode[]) => {
      const node = nodesList.find((n) => n.id === nodeId)
      if (!node) return

      // Skip if already previewing this exact node
      if (lastPreviewedNodeRef.current === nodeId) return
      lastPreviewedNodeRef.current = nodeId

      const transformType = String(node.data?.transformType ?? '')
      setNodePreviewLoading(true)
      setPreviewError(null)
      setPreviewData(null)

      try {
        if (transformType === 'source') {
          /* ── Source node: use sample_json from already-loaded DatasetRecord ── */
          const dsId = String(node.data?.config?.dataset_id ?? '')
          if (!dsId) {
            setNodePreviewLoading(false)
            return
          }

          const dsRecord = availableDatasets.find((d) => d.dataset_id === dsId)
          const sampleJson = dsRecord?.sample_json as Record<string, unknown> | undefined
          const schemaJson = dsRecord?.schema_json as Record<string, unknown> | undefined

          if (sampleJson) {
            // sample_json has {columns, rows} or {columns, data}
            setPreviewData(sampleJson)
            setNodePreviewLoading(false)
          } else if (schemaJson) {
            // At least show column structure from schema_json
            const cols = (schemaJson.columns ?? []) as Array<Record<string, unknown>>
            const columns = cols.map((c) => ({
              name: String(c.name ?? ''),
              type: String(c.type ?? 'unknown'),
            })).filter((c) => c.name)
            setPreviewData({ columns, rows: [] })
            setNodePreviewLoading(false)
          } else {
            // Fallback: show schema from cache
            const cachedSchema = schemaCache.get(dsId)
            if (cachedSchema && cachedSchema.length > 0) {
              setPreviewData({ columns: cachedSchema, rows: [] })
            }
            setNodePreviewLoading(false)
          }
        } else {
          /* ── Transform/Output node: previewPipeline with node_id ── */
          const result = await previewPipeline(ctx, pipelineId, {
            limit: 50,
            node_id: nodeId,
          })
          const sample = (result as Record<string, unknown>)?.sample as
            | Record<string, unknown>
            | undefined
          if (sample && sample.queued === true) {
            const jobId = String(
              sample.job_id ?? (result as Record<string, unknown>)?.job_id ?? '',
            )
            if (jobId) {
              pollPreviewResult(jobId)
            }
            setNodePreviewLoading(false)
          } else if (sample && (sample.columns || sample.rows)) {
            setPreviewData(sample)
            setNodePreviewLoading(false)
          } else {
            setPreviewData(result as Record<string, unknown>)
            setNodePreviewLoading(false)
          }
        }
      } catch (err) {
        setPreviewError(err instanceof Error ? err.message : String(err))
        setPreviewData(null)
        setNodePreviewLoading(false)
      }
    },
    [ctx, pipelineId, availableDatasets, schemaCache, pollPreviewResult],
  )

  /* ── ReactFlow state (initialized from definition) ── */
  const initialNodes = useMemo<PipelineNode[]>(() => {
    const rawNodes = (definition.nodes ?? []) as Array<Record<string, unknown>>
    const rawEdges = (definition.edges ?? []) as Array<Record<string, unknown>>

    // First pass: create node objects, marking which ones lack explicit positions
    const nodeList: Array<PipelineNode & { _hasPosition: boolean }> = rawNodes.map((n, i) => {
      const { transformType, config } = backendNodeToConfig(n)
      const hasPosition = n.x != null && n.y != null
      return {
        id: String(n.id ?? `node_${i}`),
        type: resolveRFNodeType(transformType),
        position: { x: Number(n.x ?? -9999), y: Number(n.y ?? -9999) },
        data: {
          label: String(n.name ?? config.name ?? getCatalog(transformType).label),
          transformType,
          config,
        },
        _hasPosition: hasPosition,
      }
    })

    // Second pass: position nodes that lack explicit x/y between their neighbors
    const missingPos = nodeList.filter((n) => !n._hasPosition)
    if (missingPos.length > 0) {
      const nodeMap = new Map(nodeList.map((n) => [n.id, n]))
      for (const node of missingPos) {
        // Find source and target nodes from edges
        const sourceEdge = rawEdges.find(
          (e) => String(e.target ?? e.to ?? '') === node.id,
        )
        const targetEdge = rawEdges.find(
          (e) => String(e.source ?? e.from ?? '') === node.id,
        )
        const srcNode = sourceEdge
          ? nodeMap.get(String(sourceEdge.source ?? sourceEdge.from ?? ''))
          : undefined
        const tgtNode = targetEdge
          ? nodeMap.get(String(targetEdge.target ?? targetEdge.to ?? ''))
          : undefined

        if (srcNode && srcNode._hasPosition && tgtNode && tgtNode._hasPosition) {
          // Position between source and target
          node.position = {
            x: (srcNode.position.x + tgtNode.position.x) / 2,
            y: (srcNode.position.y + tgtNode.position.y) / 2,
          }
        } else if (srcNode && srcNode._hasPosition) {
          // Position to the right of source
          node.position = { x: srcNode.position.x + 280, y: srcNode.position.y }
        } else if (tgtNode && tgtNode._hasPosition) {
          // Position to the left of target
          node.position = { x: tgtNode.position.x - 280, y: tgtNode.position.y }
        } else {
          // Fallback: place in a reasonable position
          node.position = { x: 60 + missingPos.indexOf(node) * 280, y: 100 }
        }
      }
    }

    // Strip internal _hasPosition flag
    return nodeList.map(({ _hasPosition, ...rest }) => rest)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pipelineId, detailQ.dataUpdatedAt])

  const initialEdges = useMemo(() => {
    const rawEdges = (definition.edges ?? []) as Array<Record<string, unknown>>
    const rawNodes = (definition.nodes ?? []) as Array<Record<string, unknown>>

    // Build nodeId → transformType map for handle inference
    const nodeTypeMap = new Map<string, string>()
    for (const n of rawNodes) {
      const { transformType } = backendNodeToConfig(n)
      nodeTypeMap.set(String(n.id ?? ''), transformType)
    }

    // Track how many edges connect to each target (for join/union left/right inference)
    const targetHandleCounts = new Map<string, number>()

    return rawEdges
      .map((e, i) => {
        // Backend uses {from, to} while frontend uses {source, target}
        const source = String(e.source ?? e.from ?? '')
        const target = String(e.target ?? e.to ?? '')
        if (!source || !target) return null // skip broken edges

        // Infer sourceHandle: always 'out' for all node types
        let sourceHandle = e.sourceHandle != null ? String(e.sourceHandle) : undefined
        if (!sourceHandle) sourceHandle = 'out'

        // Infer targetHandle based on target node type
        let targetHandle = e.targetHandle != null ? String(e.targetHandle) : undefined
        if (!targetHandle) {
          const targetType = nodeTypeMap.get(target)
          if (targetType === 'join' || targetType === 'union') {
            const count = targetHandleCounts.get(target) ?? 0
            targetHandle = count === 0 ? 'left' : 'right'
            targetHandleCounts.set(target, count + 1)
          } else {
            targetHandle = 'in'
          }
        }

        return {
          id: String(e.id ?? `edge_${i}`),
          source,
          target,
          sourceHandle,
          targetHandle,
        }
      })
      .filter((e): e is NonNullable<typeof e> => e !== null)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pipelineId, detailQ.dataUpdatedAt])

  const [nodes, setNodes, onNodesChange] = useNodesState<PipelineNodeData>(initialNodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges)

  // Sync state when data loads/changes (useNodesState only uses initial value on first render)
  useEffect(() => {
    setNodes(initialNodes)
    setEdges(initialEdges)

    // Bump nodeIdCounter past any existing node IDs to avoid collisions.
    let nextCounter = nodeIdCounter
    for (const n of initialNodes) {
      const match = n.id.match(/node_(\d+)/)
      if (!match) continue
      const num = parseInt(match[1], 10)
      if (Number.isFinite(num)) {
        nextCounter = Math.max(nextCounter, num + 1)
      }
    }
    nodeIdCounter = nextCounter
  }, [initialNodes, initialEdges, setNodes, setEdges])

  /* ── Selected node ─────────────────────────────── */
  const selectedNode = useMemo(
    () => nodes.find((n) => n.id === selectedNodeId) ?? null,
    [nodes, selectedNodeId],
  )

  /* ── Lazy schema fetch for source nodes without cached schema ── */
  useEffect(() => {
    // Collect all source dataset IDs that need schema
    const neededIds: string[] = []
    for (const n of nodes) {
      if (n.data?.transformType !== 'source') continue
      const dsId = String(n.data?.config?.dataset_id ?? '')
      if (dsId && !schemaCache.has(dsId)) neededIds.push(dsId)
    }
    if (neededIds.length === 0) return

    // Fetch schemas in parallel
    for (const dsId of neededIds) {
      getDatasetSchema(ctx, dsId, { branch: pipelineBranch }).then((cols) => {
        if (cols.length > 0) {
          setSchemaCache((prev) => new Map(prev).set(dsId, cols))
        }
      })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodes, ctx, pipelineBranch])

  /* ── Compute upstream columns for selected node ─── */
  const upstreamColumns = useMemo((): ColSchema[] => {
    if (!selectedNodeId) return []
    // BFS backward through edges to find source nodes
    const visited = new Set<string>()
    const queue = [selectedNodeId]
    const cols = new Map<string, string>()

    while (queue.length > 0) {
      const current = queue.shift()!
      if (visited.has(current)) continue
      visited.add(current)

      const node = nodes.find((n) => n.id === current)
      if (node?.data?.transformType === 'source') {
        const dsId = String(node.data?.config?.dataset_id ?? '')
        const schema = schemaCache.get(dsId)
        if (schema) {
          for (const c of schema) {
            if (!cols.has(c.name)) cols.set(c.name, c.type)
          }
        }
        continue
      }
      // Find edges targeting this node and traverse backward
      for (const e of edges) {
        if (e.target === current && !visited.has(e.source)) {
          queue.push(e.source)
        }
      }
    }

    return Array.from(cols.entries())
      .map(([name, type]) => ({ name, type }))
      .sort((a, b) => a.name.localeCompare(b.name))
  }, [selectedNodeId, nodes, edges, schemaCache])

  /* ── Output schema for selected node ─────────────── */
  const selectedNodeOutputSchema = useMemo(() => {
    if (!selectedNode) return undefined
    if (selectedNode.data?.transformType === 'source') {
      const dsId = String(selectedNode.data?.config?.dataset_id ?? '')
      return schemaCache.get(dsId)
    }
    // For transform nodes, upstream columns = approximate output
    return upstreamColumns.length > 0 ? upstreamColumns : undefined
  }, [selectedNode, schemaCache, upstreamColumns])

  /* ── Build path pruning: compute reachable nodes from outputs ── */
  const reachableNodeIds = useMemo(() => {
    // Find all output nodes
    const outputNodeIds = nodes
      .filter((n) => n.data?.transformType === 'output')
      .map((n) => n.id)

    if (outputNodeIds.length === 0) return null // No outputs → no dimming

    // Build reverse adjacency (target → sources)
    const reverseAdj = new Map<string, string[]>()
    for (const n of nodes) reverseAdj.set(n.id, [])
    for (const e of edges) {
      const arr = reverseAdj.get(e.target)
      if (arr) arr.push(e.source)
    }

    // BFS from output nodes backward
    const reachable = new Set<string>()
    const queue = [...outputNodeIds]
    while (queue.length > 0) {
      const current = queue.shift()!
      if (reachable.has(current)) continue
      reachable.add(current)
      for (const src of reverseAdj.get(current) ?? []) {
        if (!reachable.has(src)) queue.push(src)
      }
    }
    return reachable
  }, [nodes, edges])

  /* ── Apply dimming to nodes based on reachability ── */
  useEffect(() => {
    if (reachableNodeIds === null) {
      const hasDimmedNodes = nodes.some((n) => Boolean(n.data?.dimmed))
      if (hasDimmedNodes) {
        setNodes((prev) =>
          prev.map((n) =>
            n.data?.dimmed
              ? { ...n, data: { ...n.data, dimmed: false } }
              : n,
          ),
        )
      }
      return
    }

    let changed = false
    const updated = nodes.map((n) => {
      const shouldDim = !reachableNodeIds.has(n.id)
      if (n.data?.dimmed !== shouldDim) {
        changed = true
        return { ...n, data: { ...n.data, dimmed: shouldDim } }
      }
      return n
    })
    if (changed) setNodes(updated)
  }, [reachableNodeIds, nodes, setNodes])

  /* ── Previewable nodes list (for preview panel selector) ── */
  /* ── Add a transform/output node ───────────────── */
  const addTransformNode = useCallback(
    (transformType: string) => {
      pushUndo(nodes, edges)
      const catalog = getCatalog(transformType)
      const id = nextNodeId()
      const newNode: PipelineNode = {
        id,
        type: resolveRFNodeType(transformType),
        position: { x: 200 + nodes.length * 60, y: 150 + nodes.length * 40 },
        data: {
          label: catalog.label,
          transformType,
          config: { type: transformType, name: catalog.label },
        },
      }
      setNodes((prev) => [...prev, newNode])
    },
    [nodes, edges, setNodes],
  )

  /* ── Add dataset input nodes ───────────────────── */
  const addDatasetNodes = useCallback(
    (datasets: DatasetRecord[]) => {
      pushUndo(nodes, edges)
      const cols = datasets.length > 4 ? 2 : 1
      const newNodes: PipelineNode[] = datasets.map((ds, i) => {
        const id = nextNodeId()
        const col = i % cols
        const row = Math.floor(i / cols)
        return {
          id,
          type: resolveRFNodeType('source'),
          position: { x: 60 + col * 280, y: 80 + row * 120 },
          data: {
            label: ds.name,
            transformType: 'source',
            config: {
              type: 'source',
              name: ds.name,
              dataset_id: ds.dataset_id,
              dataset_name: ds.name,
            },
          },
        }
      })
      setNodes((prev) => [...prev, ...newNodes])
    },
    [nodes, edges, setNodes],
  )

  /* ── Auto layout (simple left-to-right topological) ── */
  const autoLayout = useCallback(() => {
    pushUndo(nodes, edges)
    // Build adjacency & in-degree
    const adj = new Map<string, string[]>()
    const indeg = new Map<string, number>()
    for (const n of nodes) {
      adj.set(n.id, [])
      indeg.set(n.id, 0)
    }
    for (const e of edges) {
      adj.get(e.source)?.push(e.target)
      indeg.set(e.target, (indeg.get(e.target) ?? 0) + 1)
    }

    // Topological sort → layers
    const queue = nodes.filter((n) => (indeg.get(n.id) ?? 0) === 0).map((n) => n.id)
    const layers: string[][] = []
    const visited = new Set<string>()
    let currentLayer = [...queue]

    while (currentLayer.length > 0) {
      layers.push(currentLayer)
      const nextLayer: string[] = []
      for (const nid of currentLayer) {
        visited.add(nid)
        for (const t of adj.get(nid) ?? []) {
          indeg.set(t, (indeg.get(t) ?? 0) - 1)
          if ((indeg.get(t) ?? 0) <= 0 && !visited.has(t)) {
            nextLayer.push(t)
            visited.add(t)
          }
        }
      }
      currentLayer = nextLayer
    }

    // Separate orphans (no edges at all) from connected layer-0 nodes
    const connectedNodes = new Set<string>()
    for (const e of edges) { connectedNodes.add(e.source); connectedNodes.add(e.target) }
    const orphans: string[] = []
    for (const n of nodes) {
      if (!visited.has(n.id)) {
        if (connectedNodes.has(n.id)) {
          layers[0] = layers[0] ?? []
          layers[0].push(n.id)
        } else {
          orphans.push(n.id)
        }
      }
    }

    // Assign positions
    const X_OFFSET = 60
    const X_GAP = 280
    const Y_OFFSET = 80
    const Y_GAP = 120
    const posMap = new Map<string, { x: number; y: number }>()
    for (let col = 0; col < layers.length; col++) {
      for (let row = 0; row < layers[col].length; row++) {
        posMap.set(layers[col][row], { x: X_OFFSET + col * X_GAP, y: Y_OFFSET + row * Y_GAP })
      }
    }
    // Place orphan nodes in a compact grid below the main graph
    if (orphans.length > 0) {
      const mainMaxY = Math.max(...Array.from(posMap.values()).map(p => p.y), 0)
      const orphanStartY = mainMaxY + Y_GAP + 40
      const orphanCols = orphans.length > 4 ? 2 : 1
      for (let i = 0; i < orphans.length; i++) {
        const col = i % orphanCols
        const row = Math.floor(i / orphanCols)
        posMap.set(orphans[i], { x: X_OFFSET + col * X_GAP, y: orphanStartY + row * Y_GAP })
      }
    }

    setNodes((prev) =>
      prev.map((n) => {
        const pos = posMap.get(n.id)
        return pos ? { ...n, position: pos } : n
      }),
    )
  }, [nodes, edges, setNodes])

  /* ── Undo / Redo ──────────────────────────────── */
  const undo = useCallback(() => {
    const prev = undoStack.current.pop()
    if (!prev) return
    redoStack.current.push({ nodes: nodes.map((n) => ({ ...n })), edges: edges.map((e) => ({ ...e })) })
    setNodes(prev.nodes)
    setEdges(prev.edges)
  }, [nodes, edges, setNodes, setEdges])

  const redo = useCallback(() => {
    const next = redoStack.current.pop()
    if (!next) return
    undoStack.current.push({ nodes: nodes.map((n) => ({ ...n })), edges: edges.map((e) => ({ ...e })) })
    setNodes(next.nodes)
    setEdges(next.edges)
  }, [nodes, edges, setNodes, setEdges])

  /* ── Node config update ────────────────────────── */
  const updateNodeConfig = useCallback(
    (nodeId: string, config: Record<string, unknown>) => {
      pushUndo(nodes, edges)
      setNodes((prev) =>
        prev.map((n) =>
          n.id === nodeId
            ? {
                ...n,
                data: {
                  ...n.data,
                  config,
                  label: String(config.name ?? config.output_name ?? n.data.label),
                },
              }
            : n,
        ),
      )
    },
    [nodes, edges, setNodes],
  )

  /* ── Set node legend color ─────────────────────── */
  const setNodeLegendColor = useCallback(
    (nodeId: string, colorId: string | null) => {
      pushUndo(nodes, edges)
      setNodes((prev) =>
        prev.map((n) =>
          n.id === nodeId
            ? {
                ...n,
                data: {
                  ...n.data,
                  legendColorId: colorId,
                  legendColor: colorId
                    ? legendColors.find((c) => c.id === colorId)?.color ?? null
                    : null,
                },
              }
            : n,
        ),
      )
    },
    [nodes, edges, setNodes, legendColors],
  )

  /* ── Delete node ───────────────────────────────── */
  const deleteNode = useCallback(
    (nodeId: string) => {
      pushUndo(nodes, edges)
      setNodes((prev) => prev.filter((n) => n.id !== nodeId))
      setEdges((prev) => prev.filter((e) => e.source !== nodeId && e.target !== nodeId))
      if (selectedNodeId === nodeId) setSelectedNodeId(null)
    },
    [nodes, edges, setNodes, setEdges, selectedNodeId],
  )

  /* ── Quick-add: connection start/end ────────────── */
  const onConnectStart = useCallback<OnConnectStart>((_, params) => {
    if (params.handleType === 'source') {
      connectingFromRef.current = { nodeId: params.nodeId ?? '', handleId: params.handleId ?? '' }
    }
  }, [])

  const onConnectEnd = useCallback<OnConnectEnd>(
    (event) => {
      const from = connectingFromRef.current
      connectingFromRef.current = null
      if (!from) return
      // Check if we dropped on the pane (not on a node/handle)
      const target = event.target as Element | null
      if (target?.closest('.react-flow__handle') || target?.closest('.react-flow__node')) return
      const touch = 'changedTouches' in event ? event.changedTouches?.[0] : undefined
      const clientX = touch ? touch.clientX : (event as MouseEvent).clientX
      const clientY = touch ? touch.clientY : (event as MouseEvent).clientY
      setQuickAddMenu({ x: clientX, y: clientY, sourceNodeId: from.nodeId, sourceHandleId: from.handleId })
    },
    [],
  )

  const onQuickAddSelect = useCallback(
    (transformType: string) => {
      if (!quickAddMenu) return
      pushUndo(nodes, edges)
      const catalog = getCatalog(transformType)
      const id = nextNodeId()
      const sourceNode = nodes.find((n) => n.id === quickAddMenu.sourceNodeId)
      const newNode: PipelineNode = {
        id,
        type: resolveRFNodeType(transformType),
        position: { x: (sourceNode?.position.x ?? 200) + 280, y: sourceNode?.position.y ?? 150 },
        data: {
          label: catalog.label,
          transformType,
          config: { type: transformType, name: catalog.label },
        },
      }
      setNodes((prev) => [...prev, newNode])
      // Auto-connect
      const targetHandle = catalog.inputs.length > 0 ? catalog.inputs[0].id : 'in'
      const newEdge: Edge = {
        id: `edge_${quickAddMenu.sourceNodeId}_${id}`,
        source: quickAddMenu.sourceNodeId,
        target: id,
        sourceHandle: quickAddMenu.sourceHandleId,
        targetHandle,
      }
      setEdges((prev) => addEdge(newEdge, prev))
      setQuickAddMenu(null)
      setSelectedNodeId(id)
    },
    [quickAddMenu, nodes, edges, setNodes, setEdges],
  )

  /* ── Node context toolbar select ──────────────── */
  const onContextToolbarSelect = useCallback(
    (transformType: string) => {
      if (!nodeContextToolbar) return
      pushUndo(nodes, edges)
      const catalog = getCatalog(transformType)
      const id = nextNodeId()
      const sourceNode = nodes.find((n) => n.id === nodeContextToolbar.sourceNodeId)
      const newNode: PipelineNode = {
        id,
        type: resolveRFNodeType(transformType),
        position: { x: (sourceNode?.position.x ?? 200) + 280, y: sourceNode?.position.y ?? 150 },
        data: {
          label: catalog.label,
          transformType,
          config: { type: transformType, name: catalog.label },
        },
      }
      setNodes((prev) => [...prev, newNode])
      const targetHandle = catalog.inputs.length > 0 ? catalog.inputs[0].id : 'in'
      const newEdge: Edge = {
        id: `edge_${nodeContextToolbar.sourceNodeId}_${id}`,
        source: nodeContextToolbar.sourceNodeId,
        target: id,
        sourceHandle: 'out',
        targetHandle,
      }
      setEdges((prev) => addEdge(newEdge, prev))
      setNodeContextToolbar(null)
      setSelectedNodeId(id)
    },
    [nodeContextToolbar, nodes, edges, setNodes, setEdges],
  )

  /* ── Collect all expectations from output nodes ── */
  const collectExpectations = useCallback(() => {
    return nodes
      .filter((n) => n.data?.transformType === 'output')
      .flatMap((n) => {
        const exps = n.data?.config?.expectations
        return Array.isArray(exps) ? exps : []
      })
  }, [nodes])

  /* ── Save pipeline ─────────────────────────────── */
  const saveMut = useMutation({
    mutationFn: () => {
      const allExpectations = collectExpectations()
      const defJson: PipelineDefinition = {
        nodes: nodes.map((n) => nodeToBackend(n)),
        edges: edges.map((e) => ({
          id: e.id,
          source: e.source,
          target: e.target,
          from: e.source,
          to: e.target,
          sourceHandle: e.sourceHandle,
          targetHandle: e.targetHandle,
        })),
        ...(allExpectations.length > 0 ? { expectations: allExpectations } : {}),
      }
      return updatePipeline(ctx, pipelineId, { definition_json: defJson })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: plKeys.detail(pipelineId) })
    },
  })

  /* ── Preview ───────────────────────────────────── */
  const previewMut = useMutation({
    mutationFn: () => previewPipeline(ctx, pipelineId, { limit: 100 }),
    onSuccess: (result) => {
      setPreviewOpen(true)
      const sample = (result as Record<string, unknown>)?.sample as Record<string, unknown> | undefined
      if (sample && sample.queued === true) {
        const jobId = String(sample.job_id ?? (result as Record<string, unknown>)?.job_id ?? '')
        if (jobId) {
          setPreviewData(null)
          setPreviewError(null)
          pollPreviewResult(jobId)
        } else {
          setPreviewError('Preview was queued but no job_id returned.')
        }
      } else if (sample && (sample.columns || sample.rows)) {
        setPreviewData(sample)
        setPreviewError(null)
        setPreviewPolling(false)
      } else {
        setPreviewData(result as Record<string, unknown>)
        setPreviewError(null)
        setPreviewPolling(false)
      }
    },
    onError: (err) => {
      setPreviewOpen(true)
      setPreviewError(err instanceof Error ? err.message : String(err))
      setPreviewData(null)
      setPreviewPolling(false)
    },
  })

  /* ── Toast state for build/deploy notifications ── */
  const [toast, setToast] = useState<{ intent: Intent; message: string } | null>(null)
  const toastTimer = useRef<ReturnType<typeof setTimeout>>()
  const showToast = useCallback((intent: Intent, message: string) => {
    setToast({ intent, message })
    if (toastTimer.current) clearTimeout(toastTimer.current)
    toastTimer.current = setTimeout(() => setToast(null), 5000)
  }, [])

  const resolveLatestSuccessfulBuildJobId = useCallback(async (): Promise<string | null> => {
    const runs = await listPipelineRuns(ctx, pipelineId)
    const successStatuses = new Set(['SUCCESS', 'SUCCEEDED', 'COMPLETED', 'DONE'])
    const successfulBuildRuns = runs
      .filter((run) => {
        const mode = String(run.mode ?? '').toLowerCase()
        const status = String(run.status ?? '').toUpperCase()
        return mode === 'build' && successStatuses.has(status) && String(run.job_id ?? '').trim().length > 0
      })
      .sort((a, b) => {
        const ta = new Date(String(a.finished_at ?? a.started_at ?? '')).getTime()
        const tb = new Date(String(b.finished_at ?? b.started_at ?? '')).getTime()
        return (Number.isFinite(tb) ? tb : 0) - (Number.isFinite(ta) ? ta : 0)
      })
    if (lastBuildJobId) {
      const cached = successfulBuildRuns.find((run) => String(run.job_id ?? '').trim() === lastBuildJobId)
      if (cached) {
        return lastBuildJobId
      }
    }
    const latest = successfulBuildRuns[0]
    return latest ? String(latest.job_id) : null
  }, [ctx, pipelineId, lastBuildJobId])

  /* ── Build ─────────────────────────────────────── */
  const buildMut = useMutation({
    mutationFn: () => buildPipeline(ctx, pipelineId, { branch: pipelineBranch }),
    onSuccess: (data) => {
      const jobId = String(data?.job_id ?? data?.task_id ?? '').trim()
      if (jobId) {
        setLastBuildJobId(jobId)
      }
      showToast(Intent.SUCCESS, `Build started${jobId ? ` (job: ${jobId})` : ''}`)
    },
    onError: (err) => {
      showToast(Intent.DANGER, `Build failed: ${err instanceof Error ? err.message : String(err)}`)
    },
  })

  /* ── Deploy ────────────────────────────────────── */
  const deployMut = useMutation({
    mutationFn: async (schedule?: { interval_seconds?: number; cron?: string }) => {
      const buildJobId = await resolveLatestSuccessfulBuildJobId()
      if (!buildJobId) {
        throw new Error('No successful build found. Run Build first.')
      }
      const allExpectations = collectExpectations()
      const payload: Record<string, unknown> = {
        promote_build: true,
        build_job_id: buildJobId,
        branch: pipelineBranch,
      }
      if (allExpectations.length > 0) payload.expectations = allExpectations
      if (schedule) payload.schedule = schedule
      return deployPipeline(ctx, pipelineId, payload)
    },
    onSuccess: (data) => {
      setScheduleDialogOpen(false)
      const jobId = String(data?.job_id ?? data?.task_id ?? '').trim()
      showToast(Intent.SUCCESS, `Deploy started${jobId ? ` (job: ${jobId})` : ''}`)
    },
    onError: (err) => {
      showToast(Intent.DANGER, `Deploy failed: ${err instanceof Error ? err.message : String(err)}`)
    },
  })

  /* ── Keyboard shortcuts ────────────────────────── */
  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      const isCtrl = e.ctrlKey || e.metaKey
      if (isCtrl && e.key === 'z' && !e.shiftKey) {
        e.preventDefault()
        undo()
      } else if (isCtrl && e.key === 'z' && e.shiftKey) {
        e.preventDefault()
        redo()
      } else if (isCtrl && e.key === 's') {
        e.preventDefault()
        saveMut.mutate()
      }
    },
    [undo, redo, saveMut],
  )

  /* ── Navigation ────────────────────────────────── */
  const base = `/db/${encodeURIComponent(dbName)}/pipelines`
  const goBack = () => setPipelineManagerOpen(true)

  /* ── Loading / Error states ────────────────────── */
  if (detailQ.isLoading) {
    return (
      <div style={{ padding: 40, textAlign: 'center' }}>
        <Spinner size={30} />
        <p>Loading pipeline...</p>
      </div>
    )
  }

  if (detailQ.error) {
    return (
      <div>
        <PageHeader title="Pipeline Error" />
        <Callout intent={Intent.DANGER}>Failed to load pipeline {pipelineId}.</Callout>
      </div>
    )
  }

  /* ── Render ────────────────────────────────────── */
  return (
    <div className="pipeline-page" onKeyDown={handleKeyDown} tabIndex={0}>
      {/* Top toolbar */}
      <PipelineToolbar
        pipelineName={pipeline?.name ?? pipelineId}
        pipelineType={pipeline?.pipeline_type}
        pipelineStatus={pipeline?.status}
        branch={pipelineBranch}
        saveMut={saveMut}
        buildMut={buildMut}
        deployMut={deployMut}
        onUndo={undo}
        onRedo={redo}
        canUndo={undoStack.current.length > 0}
        canRedo={redoStack.current.length > 0}
        onBack={goBack}
        branches={branches}
        branchesLoading={branchesQ.isLoading && !branchesQ.isError}
        onBranchChange={handleBranchChange}
        onBranchCreate={handleBranchCreate}
        branchCreating={createBranchMut.isPending}
        onDeployClick={() => setScheduleDialogOpen(true)}
        activeTab={activeTab}
        onTabChange={setActiveTab}
      />

      {/* ─── Tab content ─── */}
      {activeTab === 'proposals' && (
        <PipelineProposalsView pipelineId={pipelineId} branch={pipelineBranch} />
      )}

      {activeTab === 'history' && (
        <PipelineHistoryView pipelineId={pipelineId} branch={pipelineBranch} />
      )}

      {/* Canvas area — only visible in Edit tab */}
      {activeTab === 'edit' && <div className="pipeline-canvas">
        <div className="pipeline-canvas-inner">
          <div className="pipeline-canvas-stage">
            {/* Floating toolbox */}
            <PipelineToolbox
              interactionMode={interactionMode}
              onSetMode={setInteractionMode}
              onAddTransform={addTransformNode}
              onAddDatasets={() => setDatasetPickerOpen(true)}
              onAutoLayout={autoLayout}
              onSelectAll={() => {
                setNodes((prev) => prev.map((n) => ({ ...n, selected: true })))
              }}
            />

            {/* ReactFlow canvas */}
            <PipelineCanvas
              nodes={nodes}
              edges={edges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              setEdges={setEdges}
              onNodeClick={(_, node) => {
                if (interactionMode === 'edit') {
                  const currentLabel = String(node.data?.label ?? node.id)
                  const newLabel = window.prompt('Rename node', currentLabel)
                  if (newLabel != null && newLabel.trim() !== '' && newLabel !== currentLabel) {
                    setNodes((prev) =>
                      prev.map((n) =>
                        n.id === node.id ? { ...n, data: { ...n.data, label: newLabel.trim() } } : n,
                      ),
                    )
                  }
                  return
                }
                // Show context toolbar next to node
                const nodeEl = document.querySelector(`.react-flow__node[data-id="${node.id}"]`)
                if (nodeEl) {
                  const rect = nodeEl.getBoundingClientRect()
                  setNodeContextToolbar({
                    x: rect.right + 8,
                    y: rect.top,
                    sourceNodeId: node.id,
                  })
                }
                setSelectedNodeId(node.id)
                setPreviewOpen(true)
                fetchNodePreview(node.id, nodes)
              }}
              interactionMode={interactionMode}
              onConnectStart={onConnectStart}
              onConnectEnd={onConnectEnd}
            />

            {/* Empty canvas onboarding overlay */}
            {nodes.length === 0 && (
              <div className="pipeline-empty-overlay">
                <div className="pipeline-empty-card">
                  <div className="pipeline-empty-icon">✦</div>
                  <h3>Start building your pipeline</h3>
                  <p>Add a data source to begin, then chain transforms to shape your data.</p>
                  <div className="pipeline-empty-actions">
                    <Button
                      intent={Intent.PRIMARY}
                      icon="database"
                      onClick={() => setDatasetPickerOpen(true)}
                    >
                      Add Data Source
                    </Button>
                  </div>
                </div>
              </div>
            )}

            {/* Legend panel — floating top-right of canvas */}
            <PipelineLegend
              colors={legendColors}
              onAddColor={(label, color) => {
                setLegendColors((prev) => [
                  ...prev,
                  { id: `color_${Date.now()}`, label, color },
                ])
              }}
              onRemoveColor={(id) => {
                setLegendColors((prev) => prev.filter((c) => c.id !== id))
                // Clear color from nodes using this legend color
                setNodes((prev) =>
                  prev.map((n) =>
                    n.data?.legendColorId === id
                      ? { ...n, data: { ...n.data, legendColorId: null, legendColor: null } }
                      : n,
                  ),
                )
              }}
              colorCounts={legendColors.reduce((acc, c) => {
                acc[c.id] = nodes.filter((n) => n.data?.legendColorId === c.id).length
                return acc
              }, {} as Record<string, number>)}
            />

          </div>

          {/* Right config panel */}
          <PipelineRightPanel
            isOpen={!!selectedNodeId}
            node={selectedNode}
            onClose={() => setSelectedNodeId(null)}
            onUpdateConfig={updateNodeConfig}
            onDeleteNode={deleteNode}
            datasets={availableDatasets.map((d) => ({
              dataset_id: d.dataset_id,
              name: d.name,
            }))}
            outputSchema={selectedNodeOutputSchema}
            upstreamColumns={upstreamColumns}
            legendColors={legendColors}
            onSetNodeColor={setNodeLegendColor}
          />
        </div>

        {/* Bottom preview panel — full width below canvas+right panel */}
        <PipelinePreviewPanel
          isOpen={previewOpen}
          onToggle={() => setPreviewOpen((o) => !o)}
          loading={previewMut.isPending || previewPolling || nodePreviewLoading}
          error={previewMut.error ? String(previewMut.error) : previewError}
          data={previewData}
          selectedNodeLabel={selectedNode?.data?.label}
        />
      </div>}

      {/* Save status toast */}
      {saveMut.isSuccess && (
        <div className="pipeline-save-toast">
          <Callout intent={Intent.SUCCESS} icon="tick" compact>Pipeline saved.</Callout>
        </div>
      )}

      {/* Build / Deploy toast */}
      {toast && (
        <div className="pipeline-save-toast">
          <Callout intent={toast.intent} icon={toast.intent === Intent.SUCCESS ? 'tick' : 'warning-sign'} compact>
            {toast.message}
          </Callout>
        </div>
      )}

      {/* Quick-add menu (drag from handle to empty canvas) */}
      {quickAddMenu && (
        <QuickAddMenu
          position={{ x: quickAddMenu.x, y: quickAddMenu.y }}
          onSelect={onQuickAddSelect}
          onClose={() => setQuickAddMenu(null)}
        />
      )}

      {/* Node context toolbar (click a node → quick-add next to it) */}
      {nodeContextToolbar && (
        <NodeContextToolbar
          position={{ x: nodeContextToolbar.x, y: nodeContextToolbar.y }}
          onSelect={onContextToolbarSelect}
          onClose={() => setNodeContextToolbar(null)}
        />
      )}

      {/* Dataset picker dialog */}
      <DatasetPickerDialog
        isOpen={datasetPickerOpen}
        onClose={() => setDatasetPickerOpen(false)}
        onAdd={addDatasetNodes}
        ctx={ctx}
        dbName={dbName}
        branch={pipelineBranch}
      />

      {/* Schedule + Deploy dialog */}
      <ScheduleDialog
        isOpen={scheduleDialogOpen}
        onClose={() => setScheduleDialogOpen(false)}
        onDeploy={(schedule) => deployMut.mutate(schedule)}
        loading={deployMut.isPending}
      />

      {/* Pipeline manager dialog (switch / create / delete) */}
      <PipelineManagerDialog
        isOpen={pipelineManagerOpen}
        onClose={() => setPipelineManagerOpen(false)}
        ctx={ctx}
        dbName={dbName}
        branch={branch}
        currentPipelineId={pipelineId}
        onSelect={(id) => {
          setPipelineManagerOpen(false)
          navigate(`${base}/${id}`, 'push')
        }}
      />
    </div>
  )
}
