import { useMemo, useState } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
import { useMutation } from '@tanstack/react-query'
import {
  Button,
  Card,
  Callout,
  H5,
  InputGroup,
  Tab,
  Tabs,
} from '@blueprintjs/core'
import { getLineageGraph, getLineageImpact, getLineageMetrics } from '../api/bff'
import { GraphCanvas } from '../components/GraphCanvas'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { toastApiError } from '../errors/toastApiError'
import { useAppStore } from '../store/useAppStore'

type LineageNode = { node_id: string; label?: string; node_type?: string }

type LineageEdge = { from_node_id: string; to_node_id: string; edge_type?: string }

const buildLineageElements = (nodes: LineageNode[], edges: LineageEdge[]) => {
  const nodeElements = nodes.map((node) => ({
    data: {
      id: node.node_id,
      label: node.label ?? node.node_id,
      raw: node,
    },
  }))
  const edgeElements = edges.map((edge, index) => ({
    data: {
      id: `edge-${index}-${edge.from_node_id}-${edge.to_node_id}`,
      source: edge.from_node_id,
      target: edge.to_node_id,
      label: edge.edge_type ?? '',
      raw: edge,
    },
  }))
  return [...nodeElements, ...edgeElements]
}

export const LineagePage = () => {
  const { db } = useParams()
  const [searchParams] = useSearchParams()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)

  const [root, setRoot] = useState(searchParams.get('root') ?? '')
  const [tab, setTab] = useState('graph')
  const [graphResult, setGraphResult] = useState<any>(null)
  const [impactResult, setImpactResult] = useState<any>(null)
  const [metricsResult, setMetricsResult] = useState<any>(null)
  const [selectedNodeId, setSelectedNodeId] = useState('')

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const graphMutation = useMutation({
    mutationFn: () => getLineageGraph(requestContext, { root, db_name: db ?? '' }),
    onSuccess: (payload) => setGraphResult(payload),
    onError: (error) => toastApiError(error, context.language),
  })

  const impactMutation = useMutation({
    mutationFn: () => getLineageImpact(requestContext, { root, db_name: db ?? '' }),
    onSuccess: (payload) => setImpactResult(payload),
    onError: (error) => toastApiError(error, context.language),
  })

  const metricsMutation = useMutation({
    mutationFn: () => getLineageMetrics(requestContext, { db_name: db ?? '' }),
    onSuccess: (payload) => setMetricsResult(payload),
    onError: (error) => toastApiError(error, context.language),
  })

  const nodes = (graphResult as any)?.nodes ?? []
  const edges = (graphResult as any)?.edges ?? []
  const elements = buildLineageElements(nodes, edges)

  return (
    <div>
      <PageHeader title="Lineage" subtitle="Root 기반 라인리지 그래프/영향 분석" />

      <Card elevation={1} className="section-card">
        <div className="form-row">
          <InputGroup value={root} onChange={(event) => setRoot(event.currentTarget.value)} placeholder="root id" />
          <Button onClick={() => graphMutation.mutate()} disabled={!root}>
            Load
          </Button>
          <Button
            onClick={() => {
              if (!selectedNodeId) {
                return
              }
              setRoot(selectedNodeId)
              graphMutation.mutate()
            }}
            disabled={!selectedNodeId}
          >
            Use Selected Graph Node
          </Button>
          <Button onClick={() => impactMutation.mutate()} disabled={!root}>
            Impact
          </Button>
          <Button onClick={() => metricsMutation.mutate()}>
            Metrics
          </Button>
        </div>
      </Card>

      {!root ? <Callout intent="primary">root를 입력하세요.</Callout> : null}

      <Tabs id="lineage-tabs" selectedTabId={tab} onChange={(value) => setTab(value as string)}>
        <Tab id="graph" title="Graph" />
        <Tab id="impact" title="Impact" />
        <Tab id="metrics" title="Metrics" />
      </Tabs>

      {tab === 'graph' ? (
        <Card elevation={1} className="section-card">
          {elements.length ? (
            <GraphCanvas
              elements={elements}
              onSelect={(selection) => {
                if (selection.kind === 'node') {
                  const raw = selection.data?.raw ?? selection.data
                  const nodeId =
                    (raw as any)?.node_id ?? (raw as any)?.id ?? (selection.data as any)?.id
                  if (nodeId) {
                    setSelectedNodeId(String(nodeId))
                  }
                }
              }}
            />
          ) : (
            <JsonView value={graphResult} />
          )}
        </Card>
      ) : null}
      {tab === 'impact' ? (
        <Card elevation={1} className="section-card">
          <H5>Impact</H5>
          <JsonView value={impactResult} />
        </Card>
      ) : null}
      {tab === 'metrics' ? (
        <Card elevation={1} className="section-card">
          <H5>Metrics</H5>
          <JsonView value={metricsResult} />
        </Card>
      ) : null}
    </div>
  )
}
