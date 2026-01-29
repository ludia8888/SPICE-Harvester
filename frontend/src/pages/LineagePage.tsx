import { useCallback, useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Card, Icon } from '@blueprintjs/core'
import { useAppStore } from '../state/store'
import {
  listDatasets,
  listPipelines,
  getLineageGraph,
  getLineageImpact,
  type LineageNode,
  type LineageEdge,
  type LineageImpactResponse,
} from '../api/bff'
import {
  LineageSelector,
  DirectionButtons,
  LineageDAG,
  ImpactSummary,
  type LineageTarget,
  type LineageDirection,
} from '../components/lineage'

export const LineagePage = () => {
  const pipelineContext = useAppStore((state) => state.pipelineContext)
  const lineageTargetId = useAppStore((state) => state.lineageTargetId)
  const setLineageTargetId = useAppStore((state) => state.setLineageTargetId)
  const lineageDirection = useAppStore((state) => state.lineageDirection)
  const setLineageDirection = useAppStore((state) => state.setLineageDirection)

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
