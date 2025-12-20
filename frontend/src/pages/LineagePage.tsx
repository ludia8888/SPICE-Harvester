import { useCallback, useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { Button, Card, Callout, FormGroup, Icon, InputGroup, Intent, Text } from '@blueprintjs/core'
import { useAppStore as useAppStoreNew } from '../store/useAppStore'
import { useAppStore as useAppStoreLegacy } from '../state/store'
import {
  listDatasets,
  listPipelines,
  getLineageGraph,
  getLineageImpact,
  getLineageMetrics,
  type LineageNode,
  type LineageEdge,
  type LineageImpactResponse,
} from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { GraphCanvas } from '../components/GraphCanvas'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { toastApiError } from '../errors/toastApiError'
import {
  LineageSelector,
  DirectionButtons,
  LineageDAG,
  ImpactSummary,
  type LineageTarget,
  type LineageDirection,
} from '../components/lineage'

/* ─── BFF-aligned Lineage page (new) ──────────────────────────────────── */

export const LineagePage = ({ dbName }: { dbName: string }) => {
  const requestContext = useRequestContext()
  const language = useAppStoreNew((state) => state.context.language)

  const searchParams = useMemo(() => {
    if (typeof window === 'undefined') {
      return new URLSearchParams()
    }
    return new URL(window.location.href).searchParams
  }, [])
  const [root, setRoot] = useState(() => searchParams.get('root') ?? '')

  const graphMutation = useMutation({
    mutationFn: () => getLineageGraph(requestContext, { root, db_name: dbName }),
    onError: (error) => toastApiError(error, language),
  })

  const impactMutation = useMutation({
    mutationFn: () => getLineageImpact(requestContext, { root, db_name: dbName }),
    onError: (error) => toastApiError(error, language),
  })

  const metricsMutation = useMutation({
    mutationFn: () => getLineageMetrics(requestContext, { db_name: dbName, window_minutes: 60 }),
    onError: (error) => toastApiError(error, language),
  })

  const graphPayload = graphMutation.data as { data?: { graph?: { nodes?: Array<Record<string, unknown>>; edges?: Array<Record<string, unknown>>; warnings?: string[] } } } | undefined
  const graphData = graphPayload?.data?.graph
  const graphWarnings = graphData?.warnings ?? []
  const graphNodes = useMemo(
    () =>
      (graphData?.nodes ?? [])
        .map((node) => {
          const id = String(node.node_id ?? '')
          if (!id) return null
          const label = String(node.label ?? node.node_type ?? node.node_id ?? '')
          return { id, label, raw: node }
        })
        .filter((item): item is { id: string; label: string; raw: Record<string, unknown> } => Boolean(item)),
    [graphData?.nodes],
  )

  const graphEdges = useMemo(
    () =>
      (graphData?.edges ?? [])
        .map((edge, index) => {
          const source = String(edge.from_node_id ?? '')
          const target = String(edge.to_node_id ?? '')
          if (!source || !target) return null
          const label = String(edge.edge_type ?? '')
          return {
            id: `${source}-${label || 'edge'}-${target}-${index}`,
            source,
            target,
            label,
            raw: edge,
          }
        })
        .filter((item): item is { id: string; source: string; target: string; label: string; raw: Record<string, unknown> } => Boolean(item)),
    [graphData?.edges],
  )

  return (
    <div>
      <PageHeader title="Lineage" subtitle="Provenance graph, impact, and metrics." />

      <div className="page-grid two-col">
        <Card className="card-stack">
          <FormGroup label="Root (event id or node id)">
            <InputGroup value={root} onChange={(event) => setRoot(event.currentTarget.value)} />
          </FormGroup>
          <div className="form-row">
            <Button intent={Intent.PRIMARY} onClick={() => graphMutation.mutate()} disabled={!root} loading={graphMutation.isPending}>
              Load graph
            </Button>
            <Button onClick={() => impactMutation.mutate()} disabled={!root} loading={impactMutation.isPending}>
              Impact
            </Button>
            <Button onClick={() => metricsMutation.mutate()} loading={metricsMutation.isPending}>
              Metrics
            </Button>
          </div>
          {graphWarnings.length ? (
            <Callout intent={Intent.WARNING}>
              {graphWarnings.map((warning, index) => (
                <div key={`${warning}-${index}`}>{warning}</div>
              ))}
            </Callout>
          ) : null}
          {graphNodes.length === 0 ? (
            <Text className="muted">Graph will appear here.</Text>
          ) : (
            <GraphCanvas
              nodes={graphNodes}
              edges={graphEdges}
              height={360}
            />
          )}
          <JsonViewer value={graphMutation.data} empty="Graph payload will appear here." />
        </Card>

        <Card className="card-stack">
          <div className="card-title">Impact</div>
          <JsonViewer value={impactMutation.data} empty="Impact results will appear here." />
          <div className="card-title">Metrics</div>
          <JsonViewer value={metricsMutation.data} empty="Metrics will appear here." />
        </Card>
      </div>
    </div>
  )
}

/* ─── Legacy Lineage Explorer (pipeline-context aware) ────────────────── */

export const LineageExplorerPage = () => {
  const pipelineContext = useAppStoreLegacy((state) => state.pipelineContext)
  const lineageTargetId = useAppStoreLegacy((state) => state.lineageTargetId)
  const setLineageTargetId = useAppStoreLegacy((state) => state.setLineageTargetId)
  const lineageDirection = useAppStoreLegacy((state) => state.lineageDirection)
  const setLineageDirection = useAppStoreLegacy((state) => state.setLineageDirection)

  const [lineageNodes, setLineageNodes] = useState<LineageNode[]>([])
  const [lineageEdges, setLineageEdges] = useState<LineageEdge[]>([])
  const [impact, setImpact] = useState<LineageImpactResponse | null>(null)
  const [isLoadingLineage, setIsLoadingLineage] = useState(false)
  const [isLoadingImpact, setIsLoadingImpact] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const activeDbName = pipelineContext?.folderId ?? ''

  const { data: datasets = [], isLoading: isLoadingDatasets } = useQuery({
    queryKey: ['datasets', activeDbName],
    queryFn: () => listDatasets(activeDbName),
    enabled: Boolean(activeDbName),
  })

  const { data: pipelines = [], isLoading: isLoadingPipelines } = useQuery({
    queryKey: ['pipelines', activeDbName],
    queryFn: () => listPipelines(activeDbName),
    enabled: Boolean(activeDbName),
  })

  // Build targets list from datasets and pipelines
  const targets = useMemo<LineageTarget[]>(() => {
    const datasetTargets: LineageTarget[] = datasets.map((d) => ({
      id: d.dataset_id,
      name: d.name,
      type: 'dataset' as const,
    }))

    const pipelineTargets: LineageTarget[] = pipelines.map((p) => ({
      id: p.pipeline_id,
      name: p.name,
      type: 'pipeline' as const,
    }))

    return [...datasetTargets, ...pipelineTargets]
  }, [datasets, pipelines])

  const selectedTarget = useMemo(
    () => targets.find((t) => t.id === lineageTargetId) || null,
    [targets, lineageTargetId],
  )

  const fetchLineage = useCallback(async () => {
    if (!activeDbName || !lineageTargetId) {
      setLineageNodes([])
      setLineageEdges([])
      return
    }

    setIsLoadingLineage(true)
    setError(null)

    try {
      const response = await getLineageGraph({
        dbName: activeDbName,
        rootId: lineageTargetId,
        direction: lineageDirection,
        maxDepth: 5,
        maxNodes: 50,
      })

      setLineageNodes(response.nodes)
      setLineageEdges(response.edges)
    } catch (err) {
      setError(err instanceof Error ? err.message : '계보 정보를 불러오는데 실패했습니다')
      setLineageNodes([])
      setLineageEdges([])
    } finally {
      setIsLoadingLineage(false)
    }
  }, [activeDbName, lineageTargetId, lineageDirection])

  const fetchImpact = useCallback(async () => {
    if (!activeDbName || !lineageTargetId) {
      setImpact(null)
      return
    }

    setIsLoadingImpact(true)

    try {
      const response = await getLineageImpact(activeDbName, lineageTargetId)
      setImpact(response)
    } catch {
      setImpact(null)
    } finally {
      setIsLoadingImpact(false)
    }
  }, [activeDbName, lineageTargetId])

  useEffect(() => {
    fetchLineage()
    fetchImpact()
  }, [fetchLineage, fetchImpact])

  const handleTargetSelect = useCallback(
    (target: LineageTarget) => {
      setLineageTargetId(target.id)
    },
    [setLineageTargetId],
  )

  const handleDirectionChange = useCallback(
    (direction: LineageDirection) => {
      setLineageDirection(direction)
    },
    [setLineageDirection],
  )

  const handleNodeClick = useCallback(
    (node: LineageNode) => {
      setLineageTargetId(node.id)
    },
    [setLineageTargetId],
  )

  if (!activeDbName) {
    return (
      <div className="page lineage-page">
        <div className="lineage-empty-state">
          <Icon icon="data-lineage" size={48} />
          <h2>프로젝트를 선택해주세요</h2>
          <p>Pipeline Builder에서 프로젝트를 선택하면 데이터 계보를 확인할 수 있습니다</p>
        </div>
      </div>
    )
  }

  return (
    <div className="page lineage-page">
      <div className="lineage-topbar">
        <span className="lineage-home-label">데이터 계보 추적</span>
        <span className="lineage-project-label">
          <Icon icon="folder-close" size={12} />
          {pipelineContext?.folderName || activeDbName}
        </span>
      </div>

      <div className="lineage-controls">
        <LineageSelector
          targets={targets}
          selectedId={lineageTargetId}
          onSelect={handleTargetSelect}
          isLoading={isLoadingDatasets || isLoadingPipelines}
          placeholder="추적할 데이터셋 또는 파이프라인 선택..."
        />
        <DirectionButtons
          direction={lineageDirection}
          onDirectionChange={handleDirectionChange}
          disabled={!lineageTargetId}
        />
      </div>

      {error && (
        <div className="lineage-error">
          <Icon icon="error" size={14} />
          <span>{error}</span>
        </div>
      )}

      <div className="lineage-layout">
        <Card className="card lineage-graph-card">
          <div className="lineage-graph-header">
            <Icon icon="data-lineage" size={14} />
            <span>계보 그래프</span>
            {selectedTarget && (
              <span className="lineage-graph-selected">
                선택됨: {selectedTarget.name}
              </span>
            )}
          </div>
          <div className="lineage-graph-body">
            <LineageDAG
              nodes={lineageNodes}
              edges={lineageEdges}
              rootId={lineageTargetId || ''}
              isLoading={isLoadingLineage}
              onNodeClick={handleNodeClick}
            />
          </div>
        </Card>

        <aside className="lineage-sidebar">
          <ImpactSummary impact={impact} isLoading={isLoadingImpact} />

          {selectedTarget && (
            <Card className="card lineage-detail-card">
              <div className="lineage-detail-header">
                <Icon
                  icon={selectedTarget.type === 'dataset' ? 'database' : 'flow-branch'}
                  size={16}
                />
                <span>{selectedTarget.name}</span>
              </div>
              <div className="lineage-detail-body">
                <div className="lineage-detail-row">
                  <span className="lineage-detail-label">유형</span>
                  <span className="lineage-detail-value">
                    {selectedTarget.type === 'dataset' ? '데이터셋' : '파이프라인'}
                  </span>
                </div>
                <div className="lineage-detail-row">
                  <span className="lineage-detail-label">ID</span>
                  <span className="lineage-detail-value lineage-detail-id">
                    {selectedTarget.id}
                  </span>
                </div>
              </div>
            </Card>
          )}
        </aside>
      </div>
    </div>
  )
}
