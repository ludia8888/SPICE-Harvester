import { useCallback, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { useMutation } from '@tanstack/react-query'
import {
  Button,
  Card,
  Callout,
  Checkbox,
  FormGroup,
  H5,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
  NumericInput,
  Tag,
  TextArea,
} from '@blueprintjs/core'
import { aiQuery, aiTranslatePlan, graphPaths, graphQuery } from '../api/bff'
import { HttpError } from '../api/bff'
import { GraphCanvas, type GraphCanvasSelect } from '../components/GraphCanvas'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { useOntologyRegistry } from '../hooks/useOntologyRegistry'
import { useCooldown } from '../hooks/useCooldown'
import { toastApiError } from '../errors/toastApiError'
import { useAppStore } from '../store/useAppStore'
import { formatLabel } from '../utils/labels'
import { asArray, asRecord, getBoolean, getNumber, getString, type UnknownRecord } from '../utils/typed'

type Hop = { predicate: string; target_class: string }

type GraphNode = {
  id: string
  type?: string
  data_status?: string
  display?: UnknownRecord
  data?: UnknownRecord
  [key: string]: unknown
}

type GraphEdge = {
  from_node: string
  to_node: string
  predicate: string
  [key: string]: unknown
}

const downloadJson = (filename: string, data: unknown) => {
  const blob = new Blob([JSON.stringify(data ?? {}, null, 2)], { type: 'application/json' })
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  anchor.click()
  URL.revokeObjectURL(url)
}

const buildElements = (
  nodes: GraphNode[],
  edges: GraphEdge[],
  labelForClass: (id: string) => string,
  labelForPredicate: (id: string) => string,
) => {
  const nodeElements = nodes.map((node) => ({
    data: {
      id: node.id,
      label: getString(asRecord(node.display).label) ?? labelForClass(node.type ?? node.id) ?? node.id,
      raw: node,
    },
    classes:
      node.data_status === 'FULL'
        ? 'status-full'
        : node.data_status === 'PARTIAL'
          ? 'status-partial'
          : node.data_status === 'MISSING'
            ? 'status-missing'
            : undefined,
  }))

  const edgeElements = edges.map((edge, index) => ({
    data: {
      id: `edge-${index}-${edge.from_node}-${edge.to_node}`,
      source: edge.from_node,
      target: edge.to_node,
      label: labelForPredicate(edge.predicate) || edge.predicate,
      raw: edge,
    },
  }))

  return [...nodeElements, ...edgeElements]
}

export const GraphExplorerPage = () => {
  const { db } = useParams()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)
  const setInspector = useAppStore((state) => state.setInspector)

  const registry = useOntologyRegistry(db, context.branch)

  const [startClassId, setStartClassId] = useState('')
  const [hops, setHops] = useState<Hop[]>([])
  const [filtersJson, setFiltersJson] = useState('')
  const [limit, setLimit] = useState(10)
  const [maxNodes, setMaxNodes] = useState(200)
  const [maxEdges, setMaxEdges] = useState(500)
  const [noCycles, setNoCycles] = useState(true)
  const [includeDocuments, setIncludeDocuments] = useState(true)
  const [includePaths, setIncludePaths] = useState(false)
  const [includeProvenance, setIncludeProvenance] = useState(false)
  const [includeAudit, setIncludeAudit] = useState(false)

  const [graphResult, setGraphResult] = useState<unknown>(null)
  const [pathsResult, setPathsResult] = useState<unknown>(null)
  const [selectedElement, setSelectedElement] = useState<GraphCanvasSelect | null>(null)
  const [showResults, setShowResults] = useState(false)

  const [aiQuestion, setAiQuestion] = useState('')
  const [aiPlan, setAiPlan] = useState<unknown>(null)
  const [aiAnswer, setAiAnswer] = useState<unknown>(null)
  const aiCooldown = useCooldown()

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const graphMutation = useMutation({
    mutationFn: () => {
      const filters = filtersJson ? JSON.parse(filtersJson) : undefined
      return graphQuery(requestContext, db ?? '', context.branch, {
        start_class: startClassId,
        hops,
        filters,
        limit,
        max_nodes: maxNodes,
        max_edges: maxEdges,
        no_cycles: noCycles,
        include_documents: includeDocuments,
        include_paths: includePaths,
        include_provenance: includeProvenance,
        include_audit: includeAudit,
      })
    },
    onSuccess: (result) => setGraphResult(result),
    onError: (error) => toastApiError(error, context.language),
  })

  const pathsMutation = useMutation({
    mutationFn: () =>
      graphPaths(requestContext, db ?? '', {
        source_class: startClassId,
        target_class: hops[hops.length - 1]?.target_class ?? startClassId,
        max_depth: hops.length || 2,
        branch: context.branch,
      }),
    onSuccess: (result) => setPathsResult(result),
    onError: (error) => toastApiError(error, context.language),
  })

  const aiPlanMutation = useMutation({
    mutationFn: () =>
      aiTranslatePlan(requestContext, db ?? '', {
        question: aiQuestion,
        branch: context.branch,
        mode: 'auto',
        limit,
        include_provenance: includeProvenance,
        include_documents: includeDocuments,
      }),
    onSuccess: (result) => setAiPlan(result),
    onError: (error) => {
      if (error instanceof HttpError) {
        aiCooldown.startCooldown(error.retryAfterSeconds)
      }
      toastApiError(error, context.language)
    },
  }) 

  const aiQueryMutation = useMutation({
    mutationFn: () =>
      aiQuery(requestContext, db ?? '', {
        question: aiQuestion,
        branch: context.branch,
        mode: 'auto',
        limit,
        include_provenance: includeProvenance,
        include_documents: includeDocuments,
      }),
    onSuccess: (result) => setAiAnswer(result),
    onError: (error) => {
      if (error instanceof HttpError) {
        aiCooldown.startCooldown(error.retryAfterSeconds)
      }
      toastApiError(error, context.language)
    },
  })

  const labelForClass = useCallback(
    (id: string) => formatLabel(registry.classMap.get(id)?.label, context.language, id),
    [context.language, registry.classMap],
  )
  const labelForPredicate = useCallback(
    (id: string) => registry.predicateMap.get(id) ?? id,
    [registry.predicateMap],
  )

  const classOptions = useMemo(
    () => [{ label: 'Select class', value: '' }, ...registry.classOptions.map((item) => ({
      label: `${item.label} (${item.value})`,
      value: item.value,
    }))],
    [registry.classOptions],
  )

  const predicateOptions = useMemo(() => {
    const entries = Array.from(registry.predicateMap.entries())
    return [
      { label: 'Select predicate', value: '' },
      ...entries.map(([id, label]) => ({ label: `${label} (${id})`, value: id })),
    ]
  }, [registry.predicateMap])

  const applyAiPlan = () => {
    const planRoot = asRecord(aiPlan)
    const planContainer = asRecord(planRoot.plan)
    const plan = (planContainer.graph_query ?? planContainer.graphQuery) as UnknownRecord | undefined
    if (!plan) {
      return
    }
    setStartClassId(getString(plan.start_class) ?? getString(plan.startClass) ?? '')
    setHops(asArray<Hop>(plan.hops))
    setLimit(getNumber(plan.limit) ?? limit)
    setMaxNodes(getNumber(plan.max_nodes) ?? maxNodes)
    setMaxEdges(getNumber(plan.max_edges) ?? maxEdges)
    setIncludePaths(getBoolean(plan.include_paths) ?? includePaths)
    setIncludeProvenance(getBoolean(plan.include_provenance) ?? includeProvenance)
    setIncludeAudit(getBoolean(plan.include_audit) ?? includeAudit)
    setIncludeDocuments(getBoolean(plan.include_documents) ?? includeDocuments)
    setNoCycles(getBoolean(plan.no_cycles) ?? noCycles)
  }

  const handleSelect = (selection: GraphCanvasSelect) => {
    setSelectedElement(selection)
    const dataRecord = asRecord(selection.data)
    const rawRecord = asRecord(dataRecord.raw ?? dataRecord)
    const label = getString(dataRecord.label)
    const title =
      selection.kind === 'edge'
        ? label ?? getString(rawRecord.predicate) ?? 'Edge'
        : label ?? getString(rawRecord.id) ?? getString(rawRecord.node_id) ?? 'Node'
    const subtitle =
      selection.kind === 'edge'
        ? `${getString(rawRecord.from_node) ?? getString(rawRecord.from_node_id) ?? ''} → ${getString(rawRecord.to_node) ?? getString(rawRecord.to_node_id) ?? ''}`.trim()
        : getString(rawRecord.type) ?? getString(rawRecord.node_type)
    const metadata = asRecord(rawRecord.metadata)
    const commandId =
      getString(rawRecord.command_id) ?? getString(rawRecord.commandId) ?? getString(metadata.command_id)
    const nodeId = getString(rawRecord.id) ?? getString(rawRecord.node_id) ?? getString(rawRecord.nodeId)
    setInspector({
      title,
      subtitle: subtitle || undefined,
      data: rawRecord,
      kind: selection?.kind === 'edge' ? 'GraphEdge' : 'GraphNode',
      auditCommandId: commandId,
      lineageRootId: selection?.kind === 'node' && nodeId ? String(nodeId) : undefined,
    })
  }

  const graphNodes = useMemo(
    () => asArray<GraphNode>(asRecord(graphResult).nodes),
    [graphResult],
  )
  const graphEdges = useMemo(
    () => asArray<GraphEdge>(asRecord(graphResult).edges),
    [graphResult],
  )

  const elements = useMemo(
    () => buildElements(graphNodes, graphEdges, labelForClass, labelForPredicate),
    [graphEdges, graphNodes, labelForClass, labelForPredicate],
  )

  const selectedData = asRecord(selectedElement?.data)
  const selectedRaw = asRecord(selectedData.raw ?? selectedData)
  const selectedStatus = getString(selectedRaw.data_status)
  const selectedNodeId =
    getString(selectedRaw.id) ?? getString(selectedRaw.node_id) ?? getString(selectedData.id)
  const selectedCommandId =
    getString(selectedRaw.command_id) ?? getString(selectedRaw.commandId) ?? getString(asRecord(selectedRaw.metadata).command_id) ?? null

  return (
    <div>
      <PageHeader title="Graph Explorer" subtitle="Graph traversal + ES 문서 조합" />

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>AI Assist</H5>
        </div>
        <Callout intent={Intent.PRIMARY}>AI는 실행 전 Plan 검토를 권장합니다.</Callout>
        <div className="form-row">
          <InputGroup value={aiQuestion} onChange={(event) => setAiQuestion(event.currentTarget.value)} placeholder="질문을 입력하세요" />
          <Button
            intent={Intent.PRIMARY}
            onClick={() => aiPlanMutation.mutate()}
            disabled={!aiQuestion || aiCooldown.active}
          >
            Generate Plan {aiCooldown.active ? `(${aiCooldown.remainingSeconds}s)` : ''}
          </Button>
          <Button onClick={applyAiPlan} disabled={!aiPlan?.plan?.graph_query && !aiPlan?.plan?.graphQuery}>
            Apply Plan
          </Button>
          <Button onClick={() => aiQueryMutation.mutate()} disabled={!aiQuestion || aiCooldown.active}>
            Ask & Run
          </Button>
        </div>
        <JsonView value={aiPlan} />
        <JsonView value={aiAnswer} />
      </Card>

      <div className="graph-layout">
        <Card elevation={1} className="section-card">
          <div className="card-title">
            <H5>Builder</H5>
            <Tag minimal>branch: {context.branch}</Tag>
          </div>
          <FormGroup label="Start class">
            <HTMLSelect
              value={startClassId}
              onChange={(event) => setStartClassId(event.currentTarget.value)}
              options={classOptions}
            />
          </FormGroup>
          <Button
            minimal
            icon="add"
            onClick={() => setHops((prev) => [...prev, { predicate: '', target_class: '' }])}
          >
            Add hop
          </Button>
          {hops.map((hop, index) => (
            <div key={`${hop.predicate}-${index}`} className="row-grid">
              <HTMLSelect
                value={hop.predicate}
                onChange={(event) => {
                  const value = event.currentTarget.value
                  setHops((prev) => {
                    const next = [...prev]
                    next[index] = { ...next[index], predicate: value }
                    return next
                  })
                }}
                options={predicateOptions}
              />
              <HTMLSelect
                value={hop.target_class}
                onChange={(event) => {
                  const value = event.currentTarget.value
                  setHops((prev) => {
                    const next = [...prev]
                    next[index] = { ...next[index], target_class: value }
                    return next
                  })
                }}
                options={classOptions}
              />
              <Button
                minimal
                icon="trash"
                onClick={() => setHops((prev) => prev.filter((_, i) => i !== index))}
              />
            </div>
          ))}
          <FormGroup label="Filters (JSON)">
            <TextArea
              rows={5}
              value={filtersJson}
              onChange={(event) => setFiltersJson(event.currentTarget.value)}
              placeholder='{"field":"property_id","operator":"eq","value":"..."}'
            />
          </FormGroup>
          <div className="form-row">
            <NumericInput value={limit} min={1} max={500} onValueChange={(value) => setLimit(value)} />
            <NumericInput value={maxNodes} min={10} max={2000} onValueChange={(value) => setMaxNodes(value)} />
            <NumericInput value={maxEdges} min={10} max={5000} onValueChange={(value) => setMaxEdges(value)} />
          </div>
          <Checkbox checked={noCycles} onChange={(event) => setNoCycles(event.currentTarget.checked)}>
            no cycles
          </Checkbox>
          <Checkbox
            checked={includeDocuments}
            onChange={(event) => setIncludeDocuments(event.currentTarget.checked)}
          >
            include documents
          </Checkbox>
          <Checkbox checked={includePaths} onChange={(event) => setIncludePaths(event.currentTarget.checked)}>
            include paths
          </Checkbox>
          <Checkbox
            checked={includeProvenance}
            onChange={(event) => setIncludeProvenance(event.currentTarget.checked)}
          >
            include provenance
          </Checkbox>
          <Checkbox checked={includeAudit} onChange={(event) => setIncludeAudit(event.currentTarget.checked)}>
            include audit
          </Checkbox>
          <div className="button-row">
            <Button intent={Intent.PRIMARY} onClick={() => graphMutation.mutate()} disabled={!startClassId}>
              Run
            </Button>
            <Button onClick={() => pathsMutation.mutate()} disabled={!startClassId}>
              Suggest Paths
            </Button>
          </div>
          <JsonView value={pathsResult} />
        </Card>

        <Card elevation={1} className="section-card graph-panel">
          <div className="card-title">
            <H5>Graph Canvas</H5>
          </div>
          {graphNodes.length ? (
            <GraphCanvas elements={elements} onSelect={handleSelect} />
          ) : (
            <Callout intent={Intent.PRIMARY}>Run a query to render the graph.</Callout>
          )}
        </Card>

        <Card elevation={1} className="section-card">
          <div className="card-title">
            <H5>Inspector</H5>
          </div>
          {selectedElement ? (
            <>
              {selectedStatus ? <Tag intent={Intent.PRIMARY}>data_status: {selectedStatus}</Tag> : null}
              {selectedStatus === 'PARTIAL' || selectedStatus === 'MISSING' ? (
                <Callout intent={Intent.WARNING}>프로젝션 지연으로 일부 데이터가 누락될 수 있습니다.</Callout>
              ) : null}
              <div className="button-row">
                {selectedNodeId && db ? (
                  <Link to={`/db/${encodeURIComponent(db)}/lineage?root=${encodeURIComponent(String(selectedNodeId))}`}>
                    <Button minimal>Set as Lineage root</Button>
                  </Link>
                ) : null}
                {selectedCommandId && db ? (
                  <Link to={`/db/${encodeURIComponent(db)}/audit?command_id=${encodeURIComponent(String(selectedCommandId))}`}>
                    <Button minimal>Open Audit</Button>
                  </Link>
                ) : null}
              </div>
              <JsonView value={selectedRaw} />
            </>
          ) : (
            <Callout intent={Intent.PRIMARY}>Select a node or edge.</Callout>
          )}
        </Card>
      </div>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Results</H5>
          <div className="button-row">
            <Button
              minimal
              icon="refresh"
              onClick={() => graphMutation.mutate()}
              disabled={!startClassId || graphMutation.isPending}
            >
              Re-run
            </Button>
            <Button
              minimal
              icon="export"
              onClick={() => downloadJson(`graph-nodes-${db ?? 'db'}.json`, graphNodes)}
              disabled={!graphNodes.length}
            >
              Export Nodes
            </Button>
            <Button
              minimal
              icon="export"
              onClick={() => downloadJson(`graph-edges-${db ?? 'db'}.json`, graphEdges)}
              disabled={!graphEdges.length}
            >
              Export Edges
            </Button>
            <Button
              minimal
              icon={showResults ? 'chevron-up' : 'chevron-down'}
              onClick={() => setShowResults((prev) => !prev)}
            >
              {showResults ? 'Collapse' : 'Expand'}
            </Button>
          </div>
        </div>
        {showResults ? (
          <>
            <H5>Nodes</H5>
            <HTMLTable striped className="full-width">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Type</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {graphNodes.map((node) => (
                  <tr key={node.id}>
                    <td>{node.id}</td>
                    <td>{node.type ?? '-'}</td>
                    <td>{node.data_status ?? '-'}</td>
                  </tr>
                ))}
              </tbody>
            </HTMLTable>
            <H5>Edges</H5>
            <HTMLTable striped className="full-width">
              <thead>
                <tr>
                  <th>From</th>
                  <th>To</th>
                  <th>Predicate</th>
                </tr>
              </thead>
              <tbody>
                {graphEdges.map((edge, index) => (
                  <tr key={`${edge.from_node}-${edge.to_node}-${index}`}>
                    <td>{edge.from_node}</td>
                    <td>{edge.to_node}</td>
                    <td>{edge.predicate}</td>
                  </tr>
                ))}
              </tbody>
            </HTMLTable>
          </>
        ) : (
          <Callout intent={Intent.PRIMARY}>결과 테이블을 열어 노드/엣지 목록을 확인하세요.</Callout>
        )}
      </Card>
    </div>
  )
}
