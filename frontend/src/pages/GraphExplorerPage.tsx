import { useMemo, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import {
  Button,
  Card,
  Callout,
  FormGroup,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
  Switch,
  Tag,
  Text,
  TextArea,
} from '@blueprintjs/core'
import { getGraphHealth, getGraphPaths, runAiQuery, runGraphQuery, translateQueryPlan } from '../api/bff'
import { useRateLimitRetry } from '../api/useRateLimitRetry'
import { useRequestContext } from '../api/useRequestContext'
import { GraphCanvas } from '../components/GraphCanvas'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { toastApiError } from '../errors/toastApiError'
import { useOntologyRegistry } from '../query/useOntologyRegistry'
import { useAppStore } from '../store/useAppStore'

type Hop = { predicate: string; target_class: string }

const parseJson = <T,>(raw: string, fallback: T): T => {
  try {
    return JSON.parse(raw) as T
  } catch {
    return fallback
  }
}

export const GraphExplorerPage = ({ dbName }: { dbName: string }) => {
  const requestContext = useRequestContext()
  const language = useAppStore((state) => state.context.language)
  const branch = useAppStore((state) => state.context.branch)
  const setInspector = useAppStore((state) => state.setInspector)
  const { cooldown: aiCooldown, withRateLimitRetry } = useRateLimitRetry(1)
  const registry = useOntologyRegistry(dbName, branch)

  const [startClass, setStartClass] = useState('')
  const [targetClass, setTargetClass] = useState('')
  const [hops, setHops] = useState<Hop[]>([])
  const [filtersJson, setFiltersJson] = useState('{}')
  const [limit, setLimit] = useState('10')
  const [maxNodes, setMaxNodes] = useState('200')
  const [maxEdges, setMaxEdges] = useState('500')
  const [includeDocs, setIncludeDocs] = useState(true)
  const [includePaths, setIncludePaths] = useState(false)
  const [includeProvenance, setIncludeProvenance] = useState(false)
  const [includeAudit, setIncludeAudit] = useState(false)
  const [noCycles, setNoCycles] = useState(true)
  const [showResults, setShowResults] = useState(true)

  const [question, setQuestion] = useState('')

  const classOptions = useMemo(() => {
    const list = registry.classes.map((item) => ({
      label: item.label ? `${item.label} (${item.id})` : item.id,
      value: item.id,
    }))
    return [{ label: 'Select class', value: '' }, ...list]
  }, [registry.classes])

  const relationshipOptions = useMemo(() => {
    const seen = new Set<string>()
    const options = [] as Array<{ label: string; value: string }>
    registry.classes.forEach((item) => {
      item.relationships.forEach((rel) => {
        if (!rel.predicate || seen.has(rel.predicate)) {
          return
        }
        seen.add(rel.predicate)
        options.push({
          label: rel.label ? `${rel.label} (${rel.predicate})` : rel.predicate,
          value: rel.predicate,
        })
      })
    })
    return [{ label: 'Select predicate', value: '' }, ...options]
  }, [registry.classes])

  const runMutation = useMutation({
    mutationFn: () => {
      const filters = parseJson<Record<string, unknown>>(filtersJson, {})
      const normalizedHops = hops.filter((hop) => hop.predicate && hop.target_class)
      return runGraphQuery(requestContext, dbName, branch, {
        start_class: startClass,
        hops: normalizedHops,
        filters,
        limit: Number(limit) || 10,
        max_nodes: Number(maxNodes) || 200,
        max_edges: Number(maxEdges) || 500,
        include_documents: includeDocs,
        include_paths: includePaths,
        include_provenance: includeProvenance,
        include_audit: includeAudit,
        no_cycles: noCycles,
      })
    },
    onError: (error) => toastApiError(error, language),
  })

  const pathsMutation = useMutation({
    mutationFn: () =>
      getGraphPaths(requestContext, dbName, {
        source_class: startClass,
        target_class: targetClass || undefined,
        max_depth: 3,
        branch,
      }),
    onError: (error) => toastApiError(error, language),
  })

  const healthMutation = useMutation({
    mutationFn: () => getGraphHealth(requestContext),
    onError: (error) => toastApiError(error, language),
  })

  const planMutation = useMutation({
    mutationFn: () =>
      withRateLimitRetry(() =>
        translateQueryPlan(requestContext, dbName, { question, branch, mode: 'graph_query', limit: 20 }),
      ),
    onError: (error) => toastApiError(error, language),
  })

  const aiMutation = useMutation({
    mutationFn: () =>
      withRateLimitRetry(() => runAiQuery(requestContext, dbName, { question, branch, mode: 'auto', limit: 20 })),
    onError: (error) => toastApiError(error, language),
  })

  const applyPlan = () => {
    const planPayload = planMutation.data as { plan?: { graph_query?: Record<string, unknown> } } | undefined
    const graph = planPayload?.plan?.graph_query
    if (!graph) return
    setStartClass(String(graph.start_class ?? ''))
    setHops(Array.isArray(graph.hops) ? (graph.hops as Hop[]) : [])
    setFiltersJson(JSON.stringify(graph.filters ?? {}, null, 2))
    if (graph.limit) setLimit(String(graph.limit))
    if (graph.max_nodes) setMaxNodes(String(graph.max_nodes))
    if (graph.max_edges) setMaxEdges(String(graph.max_edges))
    if (typeof graph.include_documents === 'boolean') setIncludeDocs(graph.include_documents)
    if (typeof graph.include_paths === 'boolean') setIncludePaths(graph.include_paths)
    if (typeof graph.include_provenance === 'boolean') setIncludeProvenance(graph.include_provenance)
    if (typeof graph.include_audit === 'boolean') setIncludeAudit(graph.include_audit)
    if (typeof graph.no_cycles === 'boolean') setNoCycles(graph.no_cycles)
  }

  const resultPayload = runMutation.data as
    | { nodes?: Array<Record<string, unknown>>; edges?: Array<Record<string, unknown>>; warnings?: string[] }
    | undefined
  const nodes = resultPayload?.nodes ?? []
  const edges = resultPayload?.edges ?? []
  const warnings = resultPayload?.warnings ?? []
  const hasPartial = nodes.some((node) => {
    const status = node.data_status
    return status === 'PARTIAL' || status === 'MISSING'
  })
  const statusIntent = (status?: string) => {
    if (status === 'FULL') return Intent.SUCCESS
    if (status === 'PARTIAL') return Intent.WARNING
    if (status === 'MISSING') return Intent.DANGER
    return Intent.NONE
  }

  const graphNodes = useMemo(
    () =>
      nodes
        .map((node) => {
          const id = String(node.id ?? '')
          if (!id) return null
          const display = node.display as { label?: string; name?: string } | undefined
          const label =
            display?.label ||
            display?.name ||
            String(node.label ?? node.type ?? node.id ?? '')
          return {
            id,
            label,
            status: String(node.data_status ?? 'UNKNOWN'),
            raw: node,
          }
        })
        .filter((item): item is { id: string; label: string; status: string; raw: Record<string, unknown> } => Boolean(item)),
    [nodes],
  )

  const graphEdges = useMemo(
    () =>
      edges
        .map((edge, index) => {
          const source = String(edge.from_node ?? '')
          const target = String(edge.to_node ?? '')
          if (!source || !target) return null
          const label = String(edge.predicate ?? '')
          return {
            id: `${source}-${label || 'edge'}-${target}-${index}`,
            source,
            target,
            label,
            raw: edge,
          }
        })
        .filter((item): item is { id: string; source: string; target: string; label: string; raw: Record<string, unknown> } => Boolean(item)),
    [edges],
  )

  return (
    <div>
      <PageHeader title="Graph Explorer" subtitle={`Branch: ${branch}`} />

      <div className="page-grid two-col">
        <Card className="card-stack">
          <div className="card-title">Builder</div>
          <FormGroup label="Start class id">
            <HTMLSelect
              options={classOptions}
              value={startClass}
              onChange={(event) => setStartClass(event.currentTarget.value)}
            />
          </FormGroup>
          <FormGroup label="Hops">
            {hops.length === 0 ? (
              <Text className="muted small">No hops defined yet.</Text>
            ) : (
              hops.map((hop, index) => (
                <div className="form-row" key={`${hop.predicate}-${hop.target_class}-${index}`}>
                  <HTMLSelect
                    options={relationshipOptions}
                    value={hop.predicate}
                    onChange={(event) => {
                      const next = [...hops]
                      next[index] = { ...hop, predicate: event.currentTarget.value }
                      setHops(next)
                    }}
                  />
                  <HTMLSelect
                    options={classOptions}
                    value={hop.target_class}
                    onChange={(event) => {
                      const next = [...hops]
                      next[index] = { ...hop, target_class: event.currentTarget.value }
                      setHops(next)
                    }}
                  />
                  <Button
                    icon="cross"
                    minimal
                    onClick={() => setHops(hops.filter((_item, hopIndex) => hopIndex !== index))}
                  />
                </div>
              ))
            )}
            <Button
              icon="plus"
              small
              onClick={() => setHops([...hops, { predicate: '', target_class: '' }])}
            >
              Add hop
            </Button>
          </FormGroup>
          <FormGroup label="Filters (JSON object)">
            <TextArea value={filtersJson} onChange={(event) => setFiltersJson(event.currentTarget.value)} rows={3} />
          </FormGroup>
          <FormGroup label="Limit / Max nodes / Max edges">
            <div className="form-row">
              <InputGroup value={limit} onChange={(event) => setLimit(event.currentTarget.value)} placeholder="limit" />
              <InputGroup value={maxNodes} onChange={(event) => setMaxNodes(event.currentTarget.value)} placeholder="max nodes" />
              <InputGroup value={maxEdges} onChange={(event) => setMaxEdges(event.currentTarget.value)} placeholder="max edges" />
            </div>
          </FormGroup>
          <div className="form-row">
            <Switch checked={includeDocs} label="Include documents" onChange={(event) => setIncludeDocs(event.currentTarget.checked)} />
            <Switch checked={includePaths} label="Include paths" onChange={(event) => setIncludePaths(event.currentTarget.checked)} />
            <Switch checked={includeProvenance} label="Include provenance" onChange={(event) => setIncludeProvenance(event.currentTarget.checked)} />
            <Switch checked={includeAudit} label="Include audit" onChange={(event) => setIncludeAudit(event.currentTarget.checked)} />
            <Switch checked={noCycles} label="No cycles" onChange={(event) => setNoCycles(event.currentTarget.checked)} />
          </div>
          <div className="form-row">
            <Button intent={Intent.PRIMARY} onClick={() => runMutation.mutate()} disabled={!startClass} loading={runMutation.isPending}>
              Run
            </Button>
            <HTMLSelect
              options={classOptions}
              value={targetClass}
              onChange={(event) => setTargetClass(event.currentTarget.value)}
            />
            <Button onClick={() => pathsMutation.mutate()} disabled={!startClass} loading={pathsMutation.isPending}>
              Suggest paths
            </Button>
            <Button onClick={() => healthMutation.mutate()} loading={healthMutation.isPending}>
              Health
            </Button>
          </div>
          <JsonViewer value={pathsMutation.data} empty="Path suggestions will appear here." />
          <JsonViewer value={healthMutation.data} empty="Graph service health will appear here." />
        </Card>

        <Card className="card-stack">
          <div className="card-title">Graph canvas</div>
          {graphNodes.length === 0 ? (
            <Text className="muted">Run a query to render the graph.</Text>
          ) : (
            <GraphCanvas
              nodes={graphNodes}
              edges={graphEdges}
              height={380}
              onNodeSelect={(node) =>
                setInspector({
                  title: node.label,
                  kind: 'Graph node',
                  data: node.raw,
                })
              }
              onEdgeSelect={(edge) =>
                setInspector({
                  title: edge.label || 'Edge',
                  kind: 'Graph edge',
                  data: edge.raw,
                })
              }
            />
          )}

          <div className="card-title">Results</div>
          {warnings.length ? (
            <Callout intent={Intent.WARNING}>
              {warnings.map((warning, index) => (
                <div key={`${warning}-${index}`}>{warning}</div>
              ))}
            </Callout>
          ) : null}
          {hasPartial ? (
            <Callout intent={Intent.WARNING}>
              data_status PARTIAL/MISSING may be a projection delay. Re-run after indexing.
            </Callout>
          ) : null}
          <div className="form-row">
            <Switch
              checked={showResults}
              label="Show results table"
              onChange={(event) => setShowResults(event.currentTarget.checked)}
            />
          </div>
          {showResults ? (
            <>
              {nodes.length === 0 ? (
                <Text className="muted">No nodes returned yet.</Text>
              ) : (
                <HTMLTable striped interactive className="command-table">
                  <thead>
                    <tr>
                      <th>Node ID</th>
                      <th>Type</th>
                      <th>Status</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {nodes.map((node) => {
                      const id = String(node.id ?? '')
                      const status = String(node.data_status ?? '')
                      return (
                        <tr key={id}>
                          <td>{id}</td>
                          <td>{String(node.type ?? '')}</td>
                          <td>
                            <Tag minimal intent={statusIntent(status)}>{status || 'UNKNOWN'}</Tag>
                          </td>
                          <td>
                            <Button
                              small
                              onClick={() =>
                                setInspector({
                                  title: id,
                                  kind: 'Graph node',
                                  data: node,
                                })
                              }
                            >
                              Inspect
                            </Button>
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </HTMLTable>
              )}
              {edges.length === 0 ? null : (
                <HTMLTable striped interactive className="command-table">
                  <thead>
                    <tr>
                      <th>From</th>
                      <th>Predicate</th>
                      <th>To</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {edges.map((edge, index) => {
                      const fromNode = String(edge.from_node ?? '')
                      const predicate = String(edge.predicate ?? '')
                      const toNode = String(edge.to_node ?? '')
                      return (
                        <tr key={`${fromNode}-${predicate}-${toNode}-${index}`}>
                          <td>{fromNode}</td>
                          <td>{predicate}</td>
                          <td>{toNode}</td>
                          <td>
                            <Button
                              small
                              onClick={() =>
                                setInspector({
                                  title: predicate || 'Edge',
                                  kind: 'Graph edge',
                                  data: edge,
                                })
                              }
                            >
                              Inspect
                            </Button>
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </HTMLTable>
              )}
            </>
          ) : null}
          <JsonViewer value={runMutation.data} empty="Run a query to see results." />
        </Card>
      </div>

      <Card className="card-stack" style={{ marginTop: 16 }}>
        <div className="card-title">AI Assist</div>
        <FormGroup label="Question">
          <InputGroup value={question} onChange={(event) => setQuestion(event.currentTarget.value)} />
        </FormGroup>
        <div className="form-row">
          <Button
            onClick={() => planMutation.mutate()}
            disabled={!question || aiCooldown > 0}
            loading={planMutation.isPending}
          >
            {aiCooldown > 0 ? `Retry in ${aiCooldown}s` : 'Generate plan'}
          </Button>
          <Button onClick={applyPlan} disabled={!planMutation.data}>
            Apply plan
          </Button>
          <Button
            intent={Intent.PRIMARY}
            onClick={() => aiMutation.mutate()}
            disabled={!question || aiCooldown > 0}
            loading={aiMutation.isPending}
          >
            {aiCooldown > 0 ? `Retry in ${aiCooldown}s` : 'Ask & run'}
          </Button>
        </div>
        <JsonViewer value={planMutation.data} empty="Plan will appear here." />
        <JsonViewer value={aiMutation.data} empty="AI answer will appear here." />
      </Card>
    </div>
  )
}
