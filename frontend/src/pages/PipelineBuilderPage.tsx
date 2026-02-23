import { useState, useCallback, useMemo, useRef } from 'react'
import {
  Button,
  Card,
  Callout,
  Dialog,
  DialogBody,
  DialogFooter,
  FormGroup,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
  Spinner,
  Tab,
  Tabs,
  Tag,
  Tooltip,
} from '@blueprintjs/core'
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  addEdge,
  useNodesState,
  useEdgesState,
  type Connection,
  type Edge,
  type Node,
  MarkerType,
  Panel,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { useRequestContext } from '../api/useRequestContext'
import { useAppStore } from '../store/useAppStore'
import type { RequestContext, PipelineRecord, PipelineDetailRecord, PipelineArtifactRecord, PipelineDefinition } from '../api/bff'
import {
  listPipelines,
  createPipeline,
  getPipeline,
  updatePipeline,
  deletePipeline,
  previewPipeline,
  buildPipeline,
  deployPipeline,
  listPipelineRuns,
  getPipelineReadiness,
} from '../api/bff'

/* ── query keys ─────────────────────────────────────── */
const plKeys = {
  list: (db: string, branch: string) => ['pipelines', db, branch] as const,
  detail: (id: string) => ['pipelines', 'detail', id] as const,
  runs: (id: string) => ['pipelines', 'runs', id] as const,
  readiness: (id: string) => ['pipelines', 'readiness', id] as const,
}

/* ── transform catalog ──────────────────────────────── */
const TRANSFORM_CATALOG = [
  { type: 'source', label: 'Source Dataset', icon: '📥', color: '#15B371' },
  { type: 'filter', label: 'Filter', icon: '🔍', color: '#2B95D6' },
  { type: 'map', label: 'Map / Rename', icon: '🗺️', color: '#2B95D6' },
  { type: 'select', label: 'Select Columns', icon: '📌', color: '#2B95D6' },
  { type: 'drop', label: 'Drop Columns', icon: '🗑️', color: '#2B95D6' },
  { type: 'cast', label: 'Type Cast', icon: '📐', color: '#2B95D6' },
  { type: 'compute', label: 'Compute / Derive', icon: '🧮', color: '#7157D9' },
  { type: 'join', label: 'Join', icon: '🔗', color: '#D9822B' },
  { type: 'union', label: 'Union', icon: '🔄', color: '#D9822B' },
  { type: 'aggregate', label: 'Aggregate', icon: '📊', color: '#D9822B' },
  { type: 'window', label: 'Window Function', icon: '✨', color: '#D9822B' },
  { type: 'sort', label: 'Sort / Order', icon: '↕️', color: '#2B95D6' },
  { type: 'limit', label: 'Limit / Sample', icon: '✂️', color: '#2B95D6' },
  { type: 'dedupe', label: 'Deduplicate', icon: '🎯', color: '#2B95D6' },
  { type: 'pivot', label: 'Pivot', icon: '📋', color: '#7157D9' },
  { type: 'unpivot', label: 'Unpivot / Melt', icon: '📋', color: '#7157D9' },
  { type: 'flatten', label: 'Flatten JSON', icon: '📎', color: '#7157D9' },
  { type: 'split', label: 'Split Column', icon: '✂️', color: '#7157D9' },
  { type: 'fill', label: 'Fill Null', icon: '🖊️', color: '#2B95D6' },
  { type: 'udf', label: 'Custom UDF', icon: '⚡', color: '#F5498B' },
  { type: 'lookup', label: 'Lookup', icon: '🔎', color: '#D9822B' },
  { type: 'output', label: 'Output', icon: '📤', color: '#0D8050' },
] as const

/* ── node id generator ──────────────────────────────── */
let nodeIdCounter = 0
const nextNodeId = () => `node_${++nodeIdCounter}`

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
  const queryClient = useQueryClient()

  /* ── list mode vs editor mode */
  if (!pipelineId) {
    return <PipelineListView dbName={dbName} branch={branch} ctx={ctx} />
  }
  return <PipelineEditorView dbName={dbName} pipelineId={pipelineId} branch={branch} ctx={ctx} />
}

/* ════════════════════════════════════════════════════════
   Pipeline List View
   ════════════════════════════════════════════════════════ */
const PipelineListView = ({
  dbName,
  branch,
  ctx,
}: {
  dbName: string
  branch: string
  ctx: RequestContext
}) => {
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newDesc, setNewDesc] = useState('')

  const listQ = useQuery({
    queryKey: plKeys.list(dbName, branch),
    queryFn: () => listPipelines(ctx, { db_name: dbName, branch }),
  })

  const createMut = useMutation({
    mutationFn: () =>
      createPipeline(ctx, {
        db_name: dbName,
        name: newName,
        description: newDesc || undefined,
        branch,
        pipeline_type: 'transform',
        definition_json: { nodes: [], edges: [] },
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: plKeys.list(dbName, branch) })
      setCreateOpen(false)
      setNewName('')
      setNewDesc('')
    },
  })

  const deleteMut = useMutation({
    mutationFn: (id: string) => deletePipeline(ctx, id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: plKeys.list(dbName, branch) }),
  })

  const pipelines: PipelineRecord[] = listQ.data ?? []
  const base = `/db/${encodeURIComponent(dbName)}/pipelines`

  return (
    <div>
      <PageHeader
        title="Pipelines"
        subtitle={`${pipelines.length} pipelines in ${dbName}`}
        actions={
          <div className="form-row">
            <Button icon="plus" intent={Intent.PRIMARY} onClick={() => setCreateOpen(true)}>
              New Pipeline
            </Button>
            <Button icon="refresh" minimal loading={listQ.isFetching} onClick={() => listQ.refetch()}>
              Refresh
            </Button>
          </div>
        }
      />

      <Card>
        {listQ.isLoading && <Spinner size={20} />}
        {listQ.error && <Callout intent={Intent.DANGER}>Failed to load pipelines.</Callout>}
        {pipelines.length === 0 && !listQ.isLoading && (
          <Callout intent={Intent.NONE}>
            No pipelines yet. Create one to start building data transformations.
          </Callout>
        )}
        {pipelines.length > 0 && (
          <HTMLTable striped interactive compact style={{ width: '100%' }}>
            <thead>
              <tr>
                <th>Name</th>
                <th>Type</th>
                <th>Status</th>
                <th>Updated</th>
                <th style={{ width: 100 }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {pipelines.map((pl) => (
                <tr
                  key={pl.pipeline_id}
                  onClick={() => window.history.pushState({}, '', `${base}/${pl.pipeline_id}`)}
                  style={{ cursor: 'pointer' }}
                >
                  <td style={{ fontWeight: 500 }}>{pl.name}</td>
                  <td><Tag minimal>{pl.pipeline_type}</Tag></td>
                  <td>
                    <Tag
                      minimal
                      intent={
                        pl.status === 'deployed'
                          ? Intent.SUCCESS
                          : pl.status === 'failed'
                            ? Intent.DANGER
                            : Intent.NONE
                      }
                    >
                      {pl.status ?? 'draft'}
                    </Tag>
                  </td>
                  <td style={{ fontSize: 12 }}>
                    {pl.updated_at ? new Date(pl.updated_at).toLocaleDateString() : '—'}
                  </td>
                  <td>
                    <Button
                      small
                      minimal
                      icon="trash"
                      intent={Intent.DANGER}
                      loading={deleteMut.isPending}
                      onClick={(e) => {
                        e.stopPropagation()
                        if (confirm(`Delete pipeline "${pl.name}"?`)) deleteMut.mutate(pl.pipeline_id)
                      }}
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </HTMLTable>
        )}
      </Card>

      {/* Create Dialog */}
      <Dialog isOpen={createOpen} onClose={() => setCreateOpen(false)} title="New Pipeline" icon="data-lineage">
        <DialogBody>
          <FormGroup label="Pipeline Name" labelFor="pl-name">
            <InputGroup
              id="pl-name"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              placeholder="e.g. orders_etl"
            />
          </FormGroup>
          <FormGroup label="Description" labelFor="pl-desc">
            <InputGroup
              id="pl-desc"
              value={newDesc}
              onChange={(e) => setNewDesc(e.target.value)}
              placeholder="What does this pipeline do?"
            />
          </FormGroup>
          {createMut.error && <Callout intent={Intent.DANGER}>Failed to create pipeline.</Callout>}
        </DialogBody>
        <DialogFooter
          actions={
            <>
              <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
              <Button
                intent={Intent.PRIMARY}
                icon="plus"
                loading={createMut.isPending}
                disabled={!newName}
                onClick={() => createMut.mutate()}
              >
                Create
              </Button>
            </>
          }
        />
      </Dialog>
    </div>
  )
}

/* ════════════════════════════════════════════════════════
   Pipeline Editor View (ReactFlow DAG)
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
  const [selectedNode, setSelectedNode] = useState<Node | null>(null)
  const [bottomTab, setBottomTab] = useState<string>('preview')
  const reactFlowWrapper = useRef<HTMLDivElement>(null)

  /* load pipeline detail */
  const detailQ = useQuery({
    queryKey: plKeys.detail(pipelineId),
    queryFn: () => getPipeline(ctx, pipelineId),
  })

  const pipeline = detailQ.data as PipelineDetailRecord | undefined
  const definition = pipeline?.definition_json ?? { nodes: [], edges: [] }

  /* ReactFlow state */
  const initialNodes = useMemo(
    () =>
      (definition.nodes ?? []).map((n: Record<string, unknown>, i: number) => ({
        id: String(n.id ?? `node_${i}`),
        type: 'default',
        position: { x: Number(n.x ?? i * 200), y: Number(n.y ?? 100) },
        data: {
          label: buildNodeLabel(n),
          transformType: n.type ?? n.transform_type ?? 'source',
          config: n,
        },
        style: nodeStyle(String(n.type ?? n.transform_type ?? 'source')),
      })),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [pipelineId, detailQ.dataUpdatedAt],
  )

  const initialEdges = useMemo(
    () =>
      (definition.edges ?? []).map((e: Record<string, unknown>, i: number) => ({
        id: String(e.id ?? `edge_${i}`),
        source: String(e.source ?? ''),
        target: String(e.target ?? ''),
        animated: true,
        markerEnd: { type: MarkerType.ArrowClosed },
      })),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [pipelineId, detailQ.dataUpdatedAt],
  )

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges)

  const onConnect = useCallback(
    (connection: Connection) =>
      setEdges((eds) =>
        addEdge({ ...connection, animated: true, markerEnd: { type: MarkerType.ArrowClosed } }, eds),
      ),
    [setEdges],
  )

  /* add transform node */
  const addTransformNode = (transformType: string) => {
    const catalogEntry = TRANSFORM_CATALOG.find((c) => c.type === transformType)
    const id = nextNodeId()
    const newNode: Node = {
      id,
      type: 'default',
      position: { x: 100 + nodes.length * 50, y: 100 + nodes.length * 60 },
      data: {
        label: `${catalogEntry?.icon ?? '🔧'} ${catalogEntry?.label ?? transformType}`,
        transformType,
        config: { type: transformType },
      },
      style: nodeStyle(transformType),
    }
    setNodes((prev) => [...prev, newNode])
  }

  /* save pipeline */
  const saveMut = useMutation({
    mutationFn: () => {
      const defJson: PipelineDefinition = {
        nodes: nodes.map((n) => ({
          id: n.id,
          type: n.data.transformType,
          x: n.position.x,
          y: n.position.y,
          ...(n.data.config ?? {}),
        })),
        edges: edges.map((e) => ({
          id: e.id,
          source: e.source,
          target: e.target,
        })),
      }
      return updatePipeline(ctx, pipelineId, { definition_json: defJson })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: plKeys.detail(pipelineId) })
    },
  })

  /* preview */
  const previewMut = useMutation({
    mutationFn: () => previewPipeline(ctx, pipelineId, { limit: 100 }),
  })

  /* build */
  const buildMut = useMutation({
    mutationFn: () => buildPipeline(ctx, pipelineId),
  })

  /* deploy */
  const deployMut = useMutation({
    mutationFn: () => deployPipeline(ctx, pipelineId),
  })

  /* readiness */
  const readinessQ = useQuery({
    queryKey: plKeys.readiness(pipelineId),
    queryFn: () => getPipelineReadiness(ctx, pipelineId),
    enabled: !!pipelineId,
  })

  /* runs */
  const runsQ = useQuery({
    queryKey: plKeys.runs(pipelineId),
    queryFn: () => listPipelineRuns(ctx, pipelineId),
    enabled: !!pipelineId,
  })

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

  const base = `/db/${encodeURIComponent(dbName)}/pipelines`

  return (
    <div>
      <PageHeader
        title={pipeline?.name ?? pipelineId}
        subtitle={pipeline?.description ?? 'Data transformation pipeline'}
        actions={
          <div className="form-row">
            <Button icon="arrow-left" minimal onClick={() => {
              window.history.pushState({}, '', base)
              window.dispatchEvent(new CustomEvent('spice:urlchange'))
            }}>
              Back
            </Button>
            <Button
              icon="floppy-disk"
              intent={Intent.PRIMARY}
              loading={saveMut.isPending}
              onClick={() => saveMut.mutate()}
            >
              Save
            </Button>
            <Button
              icon="eye-open"
              loading={previewMut.isPending}
              onClick={() => { previewMut.mutate(); setBottomTab('preview') }}
            >
              Preview
            </Button>
            <Button
              icon="build"
              intent={Intent.WARNING}
              loading={buildMut.isPending}
              onClick={() => { buildMut.mutate(); setBottomTab('runs') }}
            >
              Build
            </Button>
            <Button
              icon="rocket-slant"
              intent={Intent.SUCCESS}
              loading={deployMut.isPending}
              onClick={() => deployMut.mutate()}
            >
              Deploy
            </Button>
          </div>
        }
      />

      {saveMut.isSuccess && <Callout intent={Intent.SUCCESS} icon="tick" style={{ marginBottom: 8 }}>Pipeline saved.</Callout>}
      {saveMut.error && <Callout intent={Intent.DANGER} style={{ marginBottom: 8 }}>Save failed.</Callout>}

      <div style={{ display: 'flex', gap: 12, height: 'calc(100vh - 260px)', minHeight: 400 }}>
        {/* Left: Transform Palette */}
        <Card style={{ width: 200, overflow: 'auto', flexShrink: 0, padding: 8 }}>
          <div className="card-title" style={{ fontSize: 13, marginBottom: 8 }}>Transforms</div>
          {TRANSFORM_CATALOG.map((t) => (
            <Button
              key={t.type}
              fill
              alignText="left"
              minimal
              small
              style={{ marginBottom: 2, fontSize: 12 }}
              onClick={() => addTransformNode(t.type)}
            >
              {t.icon} {t.label}
            </Button>
          ))}
        </Card>

        {/* Center: ReactFlow Canvas */}
        <div ref={reactFlowWrapper} style={{ flex: 1, border: '1px solid #d8e1e8', borderRadius: 4 }}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onNodeClick={(_, node) => setSelectedNode(node)}
            onPaneClick={() => setSelectedNode(null)}
            fitView
            snapToGrid
            snapGrid={[15, 15]}
            deleteKeyCode="Backspace"
          >
            <Background gap={15} size={1} />
            <Controls />
            <MiniMap
              nodeStrokeWidth={3}
              style={{ height: 80, width: 120 }}
            />
            <Panel position="top-right">
              <Tag minimal>
                {nodes.length} nodes · {edges.length} edges
              </Tag>
            </Panel>
          </ReactFlow>
        </div>

        {/* Right: Node Config Panel */}
        <Card style={{ width: 280, overflow: 'auto', flexShrink: 0 }}>
          <div className="card-title" style={{ fontSize: 13, marginBottom: 8 }}>
            {selectedNode ? 'Node Configuration' : 'Select a Node'}
          </div>
          {selectedNode ? (
            <NodeConfigPanel
              node={selectedNode}
              onUpdate={(data) => {
                setNodes((prev) =>
                  prev.map((n) =>
                    n.id === selectedNode.id ? { ...n, data: { ...n.data, ...data } } : n,
                  ),
                )
              }}
              onDelete={() => {
                setNodes((prev) => prev.filter((n) => n.id !== selectedNode.id))
                setEdges((prev) =>
                  prev.filter((e) => e.source !== selectedNode.id && e.target !== selectedNode.id),
                )
                setSelectedNode(null)
              }}
            />
          ) : (
            <Callout icon="info-sign" intent={Intent.NONE}>
              Click a node on the canvas to configure it, or add a transform from the palette.
            </Callout>
          )}

          {/* Readiness */}
          {readinessQ.data && (
            <div style={{ marginTop: 16 }}>
              <div className="card-title" style={{ fontSize: 13, marginBottom: 4 }}>Readiness</div>
              <Tag
                intent={
                  readinessQ.data.status === 'ready'
                    ? Intent.SUCCESS
                    : readinessQ.data.status === 'error'
                      ? Intent.DANGER
                      : Intent.WARNING
                }
              >
                {readinessQ.data.status ?? 'checking...'}
              </Tag>
              {readinessQ.data.inputs && readinessQ.data.inputs.length > 0 && (
                <div style={{ marginTop: 4 }}>
                  {readinessQ.data.inputs.map((inp, i) => (
                    <div key={i} style={{ fontSize: 11, marginTop: 2 }}>
                      <Tag minimal>{inp.dataset_name ?? inp.node_id ?? `input_${i}`}</Tag>
                      {' '}
                      <Tag
                        minimal
                        intent={inp.status === 'ready' ? Intent.SUCCESS : Intent.WARNING}
                      >
                        {inp.status ?? '?'}
                      </Tag>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </Card>
      </div>

      {/* Bottom: Preview / Runs / JSON */}
      <Card style={{ marginTop: 12, maxHeight: 300, overflow: 'auto' }}>
        <Tabs selectedTabId={bottomTab} onChange={(id) => setBottomTab(id as string)}>
          <Tab
            id="preview"
            title="Preview Results"
            panel={
              <div>
                {previewMut.isPending && <Spinner size={20} />}
                {previewMut.error && <Callout intent={Intent.DANGER}>Preview failed.</Callout>}
                {previewMut.data && <PreviewTable data={previewMut.data} />}
                {!previewMut.data && !previewMut.isPending && (
                  <Callout>Click &quot;Preview&quot; to see transformation results.</Callout>
                )}
              </div>
            }
          />
          <Tab
            id="runs"
            title="Run History"
            panel={
              <div>
                {buildMut.data && (
                  <Callout intent={Intent.SUCCESS} icon="tick" style={{ marginBottom: 8 }}>
                    Build started. Task ID: {buildMut.data.task_id}
                  </Callout>
                )}
                {runsQ.isLoading && <Spinner size={20} />}
                {(runsQ.data ?? []).length === 0 && !runsQ.isLoading && (
                  <Callout>No runs recorded yet.</Callout>
                )}
                {(runsQ.data ?? []).length > 0 && (
                  <HTMLTable compact striped style={{ width: '100%' }}>
                    <thead>
                      <tr><th>Run ID</th><th>Mode</th><th>Status</th><th>Created</th></tr>
                    </thead>
                    <tbody>
                      {(runsQ.data as PipelineArtifactRecord[]).map((run) => (
                        <tr key={run.artifact_id}>
                          <td style={{ fontFamily: 'monospace', fontSize: 11 }}>{run.artifact_id}</td>
                          <td><Tag minimal>{run.mode}</Tag></td>
                          <td>
                            <Tag
                              minimal
                              intent={
                                run.status === 'completed' ? Intent.SUCCESS
                                : run.status === 'failed' ? Intent.DANGER
                                : Intent.WARNING
                              }
                            >
                              {run.status}
                            </Tag>
                          </td>
                          <td style={{ fontSize: 12 }}>
                            {run.created_at ? new Date(run.created_at).toLocaleString() : '—'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </HTMLTable>
                )}
              </div>
            }
          />
          <Tab id="json" title="Pipeline JSON" panel={<JsonViewer value={pipeline} />} />
        </Tabs>
      </Card>
    </div>
  )
}

/* ── Node Config Panel ───────────────────────────────── */
const NodeConfigPanel = ({
  node,
  onUpdate,
  onDelete,
}: {
  node: Node
  onUpdate: (data: Record<string, unknown>) => void
  onDelete: () => void
}) => {
  const data = node.data as Record<string, unknown>
  const transformType = String(data.transformType ?? 'source')
  const config = (data.config ?? {}) as Record<string, unknown>

  const catalogEntry = TRANSFORM_CATALOG.find((c) => c.type === transformType)

  return (
    <div>
      <div className="form-row" style={{ marginBottom: 8 }}>
        <Tag intent={Intent.PRIMARY}>{catalogEntry?.icon} {catalogEntry?.label ?? transformType}</Tag>
        <Button small icon="trash" intent={Intent.DANGER} minimal onClick={onDelete} />
      </div>

      <FormGroup label="Node ID">
        <InputGroup value={node.id} disabled small />
      </FormGroup>

      <FormGroup label="Label">
        <InputGroup
          value={String(data.label ?? '')}
          onChange={(e) => onUpdate({ label: e.target.value })}
          small
        />
      </FormGroup>

      {/* Transform type-specific config */}
      {transformType === 'source' && (
        <FormGroup label="Dataset ID">
          <InputGroup
            value={String(config.dataset_id ?? '')}
            onChange={(e) =>
              onUpdate({ config: { ...config, dataset_id: e.target.value } })
            }
            placeholder="dataset_id"
            small
          />
        </FormGroup>
      )}

      {transformType === 'filter' && (
        <>
          <FormGroup label="Filter Column">
            <InputGroup
              value={String(config.filter_column ?? '')}
              onChange={(e) =>
                onUpdate({ config: { ...config, filter_column: e.target.value } })
              }
              placeholder="e.g. amount"
              small
            />
          </FormGroup>
          <FormGroup label="Operator">
            <HTMLSelect
              value={String(config.filter_op ?? 'gt')}
              onChange={(e) =>
                onUpdate({ config: { ...config, filter_op: e.target.value } })
              }
              fill
              options={[
                { value: 'eq', label: '= equals' },
                { value: 'neq', label: '≠ not equals' },
                { value: 'gt', label: '> greater than' },
                { value: 'gte', label: '≥ greater or equal' },
                { value: 'lt', label: '< less than' },
                { value: 'lte', label: '≤ less or equal' },
                { value: 'contains', label: 'contains' },
                { value: 'not_null', label: 'is not null' },
                { value: 'is_null', label: 'is null' },
              ]}
            />
          </FormGroup>
          {!['not_null', 'is_null'].includes(String(config.filter_op ?? '')) && (
            <FormGroup label="Value">
              <InputGroup
                value={String(config.filter_value ?? '')}
                onChange={(e) =>
                  onUpdate({ config: { ...config, filter_value: e.target.value } })
                }
                placeholder="e.g. 1000"
                small
              />
            </FormGroup>
          )}
        </>
      )}

      {transformType === 'join' && (
        <>
          <FormGroup label="Join Type">
            <HTMLSelect
              value={String(config.join_type ?? 'left')}
              onChange={(e) =>
                onUpdate({ config: { ...config, join_type: e.target.value } })
              }
              options={['inner', 'left', 'right', 'outer', 'cross']}
              fill
            />
          </FormGroup>
          <FormGroup label="Left Key">
            <InputGroup
              value={String(config.left_key ?? '')}
              onChange={(e) =>
                onUpdate({ config: { ...config, left_key: e.target.value } })
              }
              placeholder="column_name"
              small
            />
          </FormGroup>
          <FormGroup label="Right Key">
            <InputGroup
              value={String(config.right_key ?? '')}
              onChange={(e) =>
                onUpdate({ config: { ...config, right_key: e.target.value } })
              }
              placeholder="column_name"
              small
            />
          </FormGroup>
        </>
      )}

      {transformType === 'aggregate' && (
        <>
          <FormGroup label="Group By">
            <InputGroup
              value={String(config.group_by ?? '')}
              onChange={(e) =>
                onUpdate({ config: { ...config, group_by: e.target.value } })
              }
              placeholder="col1, col2"
              small
            />
          </FormGroup>
          <FormGroup label="Aggregations">
            {(() => {
              const aggs = Array.isArray(config.aggregations) ? config.aggregations as Array<Record<string, string>> : []
              return (
                <div>
                  {aggs.map((agg, i) => (
                    <div key={i} className="kv-editor-row">
                      <InputGroup
                        value={agg.column ?? ''}
                        placeholder="Column"
                        onChange={(e) => {
                          const next = [...aggs]
                          next[i] = { ...next[i], column: e.target.value }
                          onUpdate({ config: { ...config, aggregations: next } })
                        }}
                        small
                      />
                      <HTMLSelect
                        value={agg.fn ?? 'sum'}
                        options={['sum', 'avg', 'min', 'max', 'count', 'first', 'last']}
                        onChange={(e) => {
                          const next = [...aggs]
                          next[i] = { ...next[i], fn: e.target.value }
                          onUpdate({ config: { ...config, aggregations: next } })
                        }}
                      />
                      <InputGroup
                        value={agg.alias ?? ''}
                        placeholder="Alias"
                        onChange={(e) => {
                          const next = [...aggs]
                          next[i] = { ...next[i], alias: e.target.value }
                          onUpdate({ config: { ...config, aggregations: next } })
                        }}
                        small
                      />
                      <Button minimal icon="cross" intent={Intent.DANGER} onClick={() => {
                        onUpdate({ config: { ...config, aggregations: aggs.filter((_, j) => j !== i) } })
                      }} />
                    </div>
                  ))}
                  <Button minimal small icon="plus" intent={Intent.PRIMARY} onClick={() => {
                    onUpdate({ config: { ...config, aggregations: [...aggs, { column: '', fn: 'sum', alias: '' }] } })
                  }}>
                    Add Aggregation
                  </Button>
                </div>
              )
            })()}
          </FormGroup>
        </>
      )}

      {transformType === 'select' && (
        <FormGroup label="Columns (comma-separated)">
          <InputGroup
            value={String(config.columns ?? '')}
            onChange={(e) =>
              onUpdate({ config: { ...config, columns: e.target.value } })
            }
            placeholder="col1, col2, col3"
            small
          />
        </FormGroup>
      )}

      {transformType === 'compute' && (
        <>
          <FormGroup label="Source Column">
            <InputGroup
              value={String(config.source_column ?? '')}
              onChange={(e) =>
                onUpdate({ config: { ...config, source_column: e.target.value } })
              }
              placeholder="e.g. amount"
              small
            />
          </FormGroup>
          <FormGroup label="Operation">
            <HTMLSelect
              value={String(config.compute_op ?? 'multiply')}
              onChange={(e) =>
                onUpdate({ config: { ...config, compute_op: e.target.value } })
              }
              fill
              options={[
                { value: 'multiply', label: '× multiply' },
                { value: 'divide', label: '÷ divide' },
                { value: 'add', label: '+ add' },
                { value: 'subtract', label: '− subtract' },
                { value: 'round', label: 'round' },
                { value: 'ceil', label: 'ceil' },
                { value: 'floor', label: 'floor' },
                { value: 'abs', label: 'abs' },
                { value: 'upper', label: 'UPPER (text)' },
                { value: 'lower', label: 'lower (text)' },
                { value: 'trim', label: 'trim (text)' },
                { value: 'custom', label: 'Custom expression' },
              ]}
            />
          </FormGroup>
          {!['round', 'ceil', 'floor', 'abs', 'upper', 'lower', 'trim'].includes(String(config.compute_op ?? '')) && String(config.compute_op ?? '') !== 'custom' && (
            <FormGroup label="Value">
              <InputGroup
                value={String(config.compute_value ?? '')}
                onChange={(e) =>
                  onUpdate({ config: { ...config, compute_value: e.target.value } })
                }
                placeholder="e.g. 1.1"
                small
              />
            </FormGroup>
          )}
          {String(config.compute_op ?? '') === 'custom' && (
            <FormGroup label="Custom Expression">
              <InputGroup
                value={String(config.expression ?? '')}
                onChange={(e) =>
                  onUpdate({ config: { ...config, expression: e.target.value } })
                }
                placeholder="e.g. amount * 1.1"
                small
                style={{ fontFamily: 'monospace' }}
              />
            </FormGroup>
          )}
          <FormGroup label="Output Column Name (AS)">
            <InputGroup
              value={String(config.output_alias ?? '')}
              onChange={(e) =>
                onUpdate({ config: { ...config, output_alias: e.target.value } })
              }
              placeholder="e.g. amount_with_tax"
              small
            />
          </FormGroup>
        </>
      )}

      {transformType === 'output' && (
        <FormGroup label="Target Object Type">
          <InputGroup
            value={String(config.target_type ?? '')}
            onChange={(e) =>
              onUpdate({ config: { ...config, target_type: e.target.value } })
            }
            placeholder="e.g. Order"
            small
          />
        </FormGroup>
      )}

      {/* Advanced: raw config viewer */}
      <details style={{ marginTop: 12 }}>
        <summary style={{ fontSize: 12, cursor: 'pointer', opacity: 0.6 }}>Advanced: Raw Config JSON</summary>
        <div style={{ marginTop: 8 }}>
          <JsonViewer value={config} />
        </div>
      </details>
    </div>
  )
}

/* ── Preview Table ───────────────────────────────────── */
const PreviewTable = ({ data }: { data: Record<string, unknown> }) => {
  const columns = (data.columns ?? []) as Array<{ name?: string } | string>
  const rows = (data.rows ?? data.data ?? []) as Array<Record<string, unknown> | unknown[]>

  if (rows.length === 0) {
    return <Callout>Preview returned 0 rows.</Callout>
  }

  const colNames = columns.length > 0
    ? columns.map((c) => (typeof c === 'string' ? c : c.name ?? ''))
    : rows.length > 0 && typeof rows[0] === 'object' && !Array.isArray(rows[0])
      ? Object.keys(rows[0] as Record<string, unknown>)
      : []

  return (
    <div>
      <Tag minimal style={{ marginBottom: 8 }}>
        {rows.length} rows × {colNames.length} columns
      </Tag>
      <div style={{ overflow: 'auto', maxHeight: 200 }}>
        <HTMLTable compact striped style={{ width: '100%', fontSize: 12 }}>
          <thead>
            <tr>
              {colNames.map((name, i) => (
                <th key={i}>{name}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.slice(0, 50).map((row, ri) => (
              <tr key={ri}>
                {colNames.map((colName, ci) => {
                  const val = Array.isArray(row)
                    ? row[ci]
                    : (row as Record<string, unknown>)[colName]
                  return <td key={ci}>{val == null ? '—' : String(val)}</td>
                })}
              </tr>
            ))}
          </tbody>
        </HTMLTable>
      </div>
    </div>
  )
}

/* ── helpers ─────────────────────────────────────────── */
function buildNodeLabel(n: Record<string, unknown>): string {
  const transformType = String(n.type ?? n.transform_type ?? 'source')
  const catalogEntry = TRANSFORM_CATALOG.find((c) => c.type === transformType)
  const name = n.name ?? n.label ?? catalogEntry?.label ?? transformType
  return `${catalogEntry?.icon ?? '🔧'} ${name}`
}

function nodeStyle(transformType: string): React.CSSProperties {
  const catalogEntry = TRANSFORM_CATALOG.find((c) => c.type === transformType)
  return {
    background: '#fff',
    border: `2px solid ${catalogEntry?.color ?? '#aaa'}`,
    borderRadius: 8,
    padding: '8px 12px',
    fontSize: 13,
    fontWeight: 500,
    minWidth: 120,
  }
}
