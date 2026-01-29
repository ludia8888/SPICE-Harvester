import { useCallback, useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Card, Icon } from '@blueprintjs/core'
import { useAppStore } from '../state/store'
import {
  listOntologyClasses,
  naturalLanguageQuery,
  getInstance,
  getInstanceRelationships,
  type Instance,
  type Relationship,
  type OntologyClass,
} from '../api/bff'
import {
  NaturalLanguageSearch,
  ClassFilter,
  ResultsTable,
  InstanceDetailPanel,
  ForceDirectedGraph,
  type GraphNode,
  type GraphEdge,
} from '../components/explorer'
import type { ExplorerResult } from '../state/store'

const SEARCH_SUGGESTIONS = [
  '김철수와 관련된 모든 거래 보여줘',
  'A계좌에서 B계좌로 송금 내역',
  '최근 일주일간 거래 목록',
  '1000만원 이상 거래 건수',
]

export const ExplorerPage = () => {
  const pipelineContext = useAppStore((state) => state.pipelineContext)
  const explorerSearchQuery = useAppStore((state) => state.explorerSearchQuery)
  const setExplorerSearchQuery = useAppStore((state) => state.setExplorerSearchQuery)
  const explorerResults = useAppStore((state) => state.explorerResults)
  const setExplorerResults = useAppStore((state) => state.setExplorerResults)
  const explorerSelectedClasses = useAppStore((state) => state.explorerSelectedClasses)
  const setExplorerSelectedClasses = useAppStore((state) => state.setExplorerSelectedClasses)
  const selectedInstanceId = useAppStore((state) => state.selectedInstanceId)
  const setSelectedInstanceId = useAppStore((state) => state.setSelectedInstanceId)

  const [isSearching, setIsSearching] = useState(false)
  const [searchError, setSearchError] = useState<string | null>(null)
  const [resultColumns, setResultColumns] = useState<string[]>([])
  const [totalCount, setTotalCount] = useState<number | undefined>()
  const [selectedInstance, setSelectedInstance] = useState<Instance | null>(null)
  const [instanceRelationships, setInstanceRelationships] = useState<Relationship[]>([])
  const [isLoadingInstance, setIsLoadingInstance] = useState(false)

  const activeDbName = pipelineContext?.folderId ?? ''

  const { data: ontologyClasses = [], isLoading: isLoadingClasses } = useQuery({
    queryKey: ['ontology-classes', activeDbName],
    queryFn: () => listOntologyClasses(activeDbName),
    enabled: Boolean(activeDbName),
  })

  // Initialize selected classes when classes load
  useEffect(() => {
    if (ontologyClasses.length > 0 && explorerSelectedClasses.length === 0) {
      setExplorerSelectedClasses(ontologyClasses.map((c) => c.id))
    }
  }, [ontologyClasses, explorerSelectedClasses.length, setExplorerSelectedClasses])

  const handleSearch = useCallback(
    async (query: string) => {
      if (!activeDbName || !query.trim()) return

      setIsSearching(true)
      setSearchError(null)

      try {
        const response = await naturalLanguageQuery(activeDbName, query)

        // Transform results
        const results: ExplorerResult[] = (response.results || []).map((row, index) => {
          const id = String(row.id || row._id || `result-${index}`)
          const classId = String(row.classId || row.class_id || row.type || 'Unknown')
          const className = String(row.className || row.class_name || classId)
          const label = String(row.label || row.name || row.title || id)

          return {
            id,
            classId,
            className,
            label,
            properties: row,
          }
        })

        // Filter by selected classes
        const filteredResults = results.filter((r) =>
          explorerSelectedClasses.length === 0 || explorerSelectedClasses.includes(r.classId),
        )

        setExplorerResults(filteredResults)
        setResultColumns(response.columns || [])
        setTotalCount(response.totalCount)
      } catch (error) {
        setSearchError(
          error instanceof Error ? error.message : '검색 중 오류가 발생했습니다',
        )
        setExplorerResults([])
      } finally {
        setIsSearching(false)
      }
    },
    [activeDbName, explorerSelectedClasses, setExplorerResults],
  )

  const handleSelectResult = useCallback(
    async (result: ExplorerResult) => {
      setSelectedInstanceId(result.id)
      setIsLoadingInstance(true)

      try {
        const [instance, relationships] = await Promise.all([
          getInstance(activeDbName, result.classId, result.id),
          getInstanceRelationships(activeDbName, result.classId, result.id),
        ])
        setSelectedInstance(instance)
        setInstanceRelationships(relationships)
      } catch {
        // Use result data as fallback
        setSelectedInstance({
          id: result.id,
          classId: result.classId,
          label: result.label,
          properties: result.properties,
        })
        setInstanceRelationships([])
      } finally {
        setIsLoadingInstance(false)
      }
    },
    [activeDbName, setSelectedInstanceId],
  )

  const handleCloseDetail = useCallback(() => {
    setSelectedInstanceId(null)
    setSelectedInstance(null)
    setInstanceRelationships([])
  }, [setSelectedInstanceId])

  const handleRelationshipClick = useCallback(
    (relationship: Relationship) => {
      // Navigate to the target instance
      const targetResult: ExplorerResult = {
        id: relationship.targetId,
        classId: relationship.targetClass,
        className: relationship.targetClass,
        label: relationship.targetLabel,
        properties: {},
      }
      handleSelectResult(targetResult)
    },
    [handleSelectResult],
  )

  // Build graph data from selected instance and relationships
  const graphData = useMemo(() => {
    if (!selectedInstance) {
      return { nodes: [], edges: [] }
    }

    const nodes: GraphNode[] = [
      {
        id: selectedInstance.id,
        label: selectedInstance.label,
        classType: selectedInstance.classId,
        degree: instanceRelationships.length,
      },
    ]

    const edges: GraphEdge[] = []
    const addedNodeIds = new Set([selectedInstance.id])

    instanceRelationships.forEach((rel, index) => {
      if (!addedNodeIds.has(rel.targetId)) {
        nodes.push({
          id: rel.targetId,
          label: rel.targetLabel,
          classType: rel.targetClass,
        })
        addedNodeIds.add(rel.targetId)
      }

      edges.push({
        id: rel.id || `edge-${index}`,
        source: rel.sourceId,
        target: rel.targetId,
        predicate: rel.predicateLabel || rel.predicate,
        style: rel.predicate.includes('owns') || rel.predicate.includes('belongs')
          ? 'solid'
          : 'dashed',
      })
    })

    return { nodes, edges }
  }, [selectedInstance, instanceRelationships])

  const handleNodeClick = useCallback(
    (node: GraphNode) => {
      const result: ExplorerResult = {
        id: node.id,
        classId: node.classType,
        className: node.classType,
        label: node.label,
        properties: {},
      }
      handleSelectResult(result)
    },
    [handleSelectResult],
  )

  const filteredResults = useMemo(() => {
    if (explorerSelectedClasses.length === 0) {
      return explorerResults
    }
    return explorerResults.filter((r) => explorerSelectedClasses.includes(r.classId))
  }, [explorerResults, explorerSelectedClasses])

  if (!activeDbName) {
    return (
      <div className="page explorer-page">
        <div className="explorer-empty-state">
          <Icon icon="search" size={48} />
          <h2>프로젝트를 선택해주세요</h2>
          <p>Pipeline Builder에서 프로젝트를 선택하면 탐색을 시작할 수 있습니다</p>
        </div>
      </div>
    )
  }

  return (
    <div className="page explorer-page">
      <div className="explorer-topbar">
        <span className="explorer-home-label">Explorer</span>
        <span className="explorer-project-label">
          <Icon icon="folder-close" size={12} />
          {pipelineContext?.folderName || activeDbName}
        </span>
      </div>

      <div className="explorer-search-bar">
        <NaturalLanguageSearch
          value={explorerSearchQuery}
          onChange={setExplorerSearchQuery}
          onSearch={handleSearch}
          isLoading={isSearching}
          placeholder="자연어로 검색하세요... (예: 김철수와 관련된 거래)"
          suggestions={SEARCH_SUGGESTIONS}
        />
        {searchError && (
          <div className="explorer-search-error">
            <Icon icon="error" size={14} />
            <span>{searchError}</span>
          </div>
        )}
      </div>

      <div className="explorer-content">
        <aside className="explorer-sidebar">
          <ClassFilter
            classes={ontologyClasses}
            selectedClasses={explorerSelectedClasses}
            onSelectionChange={setExplorerSelectedClasses}
            isLoading={isLoadingClasses}
          />
        </aside>

        <div className="explorer-main">
          <Card className="card explorer-results">
            <ResultsTable
              results={filteredResults}
              columns={resultColumns}
              selectedId={selectedInstanceId}
              onSelect={handleSelectResult}
              isLoading={isSearching}
              totalCount={totalCount}
            />
          </Card>

          <Card className="card explorer-graph">
            <div className="explorer-graph-header">
              <Icon icon="graph" size={14} />
              <span>관계 그래프</span>
            </div>
            <ForceDirectedGraph
              nodes={graphData.nodes}
              edges={graphData.edges}
              selectedNodeId={selectedInstanceId}
              onNodeClick={handleNodeClick}
              onNodeDoubleClick={handleNodeClick}
              width={600}
              height={350}
            />
          </Card>
        </div>

        {selectedInstanceId && (
          <InstanceDetailPanel
            instance={selectedInstance}
            relationships={instanceRelationships}
            isLoading={isLoadingInstance}
            onClose={handleCloseDetail}
            onRelationshipClick={handleRelationshipClick}
          />
        )}
      </div>
    </div>
  )
}
