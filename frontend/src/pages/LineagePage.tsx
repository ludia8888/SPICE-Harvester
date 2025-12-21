import { useMemo, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { Button, Card, Callout, FormGroup, InputGroup, Intent, Text } from '@blueprintjs/core'
import { getLineageGraph, getLineageImpact, getLineageMetrics } from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { GraphCanvas } from '../components/GraphCanvas'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { toastApiError } from '../errors/toastApiError'
import { useAppStore } from '../store/useAppStore'

export const LineagePage = ({ dbName }: { dbName: string }) => {
  const requestContext = useRequestContext()
  const language = useAppStore((state) => state.context.language)

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
