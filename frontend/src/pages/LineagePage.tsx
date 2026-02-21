import { useEffect, useMemo, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { Button, Card, Callout, FormGroup, InputGroup, Intent, Text } from '@blueprintjs/core'
import { useAppStore as useAppStoreNew } from '../store/useAppStore'
import {
  getLineageColumnLineageCtx,
  getLineageDiffCtx,
  getLineageGraphCtx,
  getLineageImpactCtx,
  getLineageMetrics,
  getLineageOutOfDateCtx,
  getLineagePathCtx,
  getLineageRunImpactCtx,
  getLineageRunsCtx,
  getLineageTimelineCtx,
} from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { GraphCanvas } from '../components/GraphCanvas'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { toastApiError } from '../errors/toastApiError'

/* ─── BFF-aligned Lineage page (new) ──────────────────────────────────── */

const isoMinusMinutes = (minutes: number) => new Date(Date.now() - minutes * 60_000).toISOString()

const asRecord = (value: unknown) =>
  value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : null

const asNumber = (value: string, fallback: number) => {
  const n = Number(value)
  if (!Number.isFinite(n)) {
    return fallback
  }
  return n
}

type LineageTimelineEvent = {
  from_node_id?: string
  to_node_id?: string
}

export const LineagePage = ({ dbName }: { dbName: string }) => {
  const requestContext = useRequestContext()
  const language = useAppStoreNew((state) => state.context.language)
  const branch = useAppStoreNew((state) => state.context.branch)

  const searchParams = useMemo(() => {
    if (typeof window === 'undefined') {
      return new URLSearchParams()
    }
    return new URL(window.location.href).searchParams
  }, [])
  const [root, setRoot] = useState(() => searchParams.get('root') ?? '')
  const [source, setSource] = useState(() => searchParams.get('source') ?? '')
  const [target, setTarget] = useState(() => searchParams.get('target') ?? '')
  const [runId, setRunId] = useState(() => searchParams.get('run_id') ?? '')
  const [windowMinutes, setWindowMinutes] = useState('60')
  const [freshnessSloMinutes, setFreshnessSloMinutes] = useState('120')
  const [since, setSince] = useState(() => isoMinusMinutes(120))
  const [until, setUntil] = useState(() => new Date().toISOString())
  const [fromAsOf, setFromAsOf] = useState(() => isoMinusMinutes(180))
  const [toAsOf, setToAsOf] = useState(() => new Date().toISOString())
  const [bucketMinutes, setBucketMinutes] = useState('15')
  const [eventLimit, setEventLimit] = useState('500')
  const [pairLimit, setPairLimit] = useState('200')

  const graphMutation = useMutation({
    mutationFn: () =>
      getLineageGraphCtx(requestContext, {
        root,
        db_name: dbName,
        branch,
        direction: 'both',
        max_depth: 5,
        max_nodes: 2000,
        max_edges: 5000,
      }),
    onError: (error) => toastApiError(error, language),
  })

  const impactMutation = useMutation({
    mutationFn: () =>
      getLineageImpactCtx(requestContext, {
        root,
        db_name: dbName,
        branch,
        direction: 'downstream',
        max_depth: 10,
        max_nodes: 2000,
        max_edges: 5000,
      }),
    onError: (error) => toastApiError(error, language),
  })

  const metricsMutation = useMutation({
    mutationFn: () =>
      getLineageMetrics(requestContext, {
        db_name: dbName,
        branch,
        window_minutes: asNumber(windowMinutes, 60),
      }),
    onError: (error) => toastApiError(error, language),
  })

  const runsMutation = useMutation({
    mutationFn: () =>
      getLineageRunsCtx(requestContext, {
        db_name: dbName,
        branch,
        since,
        until,
        run_limit: 200,
        freshness_slo_minutes: asNumber(freshnessSloMinutes, 120),
        include_impact_preview: true,
        impact_preview_runs_limit: 20,
        impact_preview_artifacts_limit: 20,
        impact_preview_edge_limit: 2000,
      }),
    onError: (error) => toastApiError(error, language),
  })

  const timelineMutation = useMutation({
    mutationFn: () =>
      getLineageTimelineCtx(requestContext, {
        db_name: dbName,
        branch,
        since,
        until,
        bucket_minutes: asNumber(bucketMinutes, 15),
        event_limit: asNumber(eventLimit, 500),
        event_preview_limit: Math.min(2000, asNumber(eventLimit, 500)),
      }),
    onError: (error) => toastApiError(error, language),
  })

  const pathMutation = useMutation({
    mutationFn: () =>
      getLineagePathCtx(requestContext, {
        source,
        target,
        db_name: dbName,
        branch,
        direction: 'downstream',
        max_depth: 20,
        max_nodes: 5000,
        max_edges: 15000,
      }),
    onError: (error) => toastApiError(error, language),
  })

  const diffMutation = useMutation({
    mutationFn: () =>
      getLineageDiffCtx(requestContext, {
        root,
        from_as_of: fromAsOf,
        to_as_of: toAsOf,
        db_name: dbName,
        branch,
        direction: 'downstream',
        max_depth: 10,
        max_nodes: 5000,
        max_edges: 15000,
      }),
    onError: (error) => toastApiError(error, language),
  })

  const runImpactMutation = useMutation({
    mutationFn: () =>
      getLineageRunImpactCtx(requestContext, {
        run_id: runId,
        db_name: dbName,
        branch,
        since,
        until,
        event_limit: asNumber(eventLimit, 500),
        artifact_preview_limit: 200,
      }),
    onError: (error) => toastApiError(error, language),
  })

  const columnLineageMutation = useMutation({
    mutationFn: () =>
      getLineageColumnLineageCtx(requestContext, {
        db_name: dbName,
        branch,
        run_id: runId || undefined,
        since,
        until,
        edge_limit: asNumber(eventLimit, 500),
        pair_limit: asNumber(pairLimit, 200),
      }),
    onError: (error) => toastApiError(error, language),
  })

  const outOfDateMutation = useMutation({
    mutationFn: () =>
      getLineageOutOfDateCtx(requestContext, {
        db_name: dbName,
        branch,
        as_of: toAsOf,
        freshness_slo_minutes: asNumber(freshnessSloMinutes, 120),
        artifact_limit: 5000,
        stale_preview_limit: 200,
        projection_limit: 1000,
        projection_preview_limit: 200,
      }),
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

  const timelinePayload = asRecord(timelineMutation.data)
  const timelineData = asRecord(timelinePayload?.data)
  const timelineEvents = (Array.isArray(timelineData?.events) ? timelineData.events : [])
    .map((item) => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))
  const timelineFirst = (timelineEvents[0] ?? null) as LineageTimelineEvent | null
  const timelineEventCount = Number(timelineData?.event_count_loaded ?? timelineEvents.length ?? 0)

  const runsPayload = asRecord(runsMutation.data)
  const runsData = asRecord(runsPayload?.data)
  const runRows = (Array.isArray(runsData?.runs) ? runsData.runs : [])
    .map((item) => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))

  useEffect(() => {
    if (!timelineFirst) {
      return
    }
    const nextSource = String(timelineFirst.from_node_id ?? '').trim()
    const nextTarget = String(timelineFirst.to_node_id ?? '').trim()
    if (!source && nextSource) {
      setSource(nextSource)
    }
    if (!target && nextTarget) {
      setTarget(nextTarget)
    }
    if (!root && nextSource) {
      setRoot(nextSource)
    }
  }, [root, source, target, timelineFirst])

  useEffect(() => {
    if (runId || runRows.length === 0) {
      return
    }
    const candidate = String(runRows[0].run_id ?? '')
    if (candidate) {
      setRunId(candidate)
    }
  }, [runId, runRows])

  const seedFromTimeline = () => {
    if (!timelineFirst) {
      return
    }
    const nextSource = String(timelineFirst.from_node_id ?? '').trim()
    const nextTarget = String(timelineFirst.to_node_id ?? '').trim()
    if (nextSource) {
      setRoot(nextSource)
      setSource(nextSource)
    }
    if (nextTarget) {
      setTarget(nextTarget)
    }
  }

  return (
    <div>
      <PageHeader title="Lineage" subtitle={`Foundry-style lineage diagnostics via BFF (${branch})`} />

      <div className="page-grid two-col">
        <Card className="card-stack">
          <FormGroup label="Root node / event id">
            <InputGroup value={root} onChange={(event) => setRoot(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Path source / target">
            <div className="form-row">
              <InputGroup value={source} onChange={(event) => setSource(event.currentTarget.value)} placeholder="source" />
              <InputGroup value={target} onChange={(event) => setTarget(event.currentTarget.value)} placeholder="target" />
            </div>
          </FormGroup>
          <FormGroup label="Run id (for run-impact / column lineage filter)">
            <InputGroup value={runId} onChange={(event) => setRunId(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Window controls">
            <div className="form-row">
              <InputGroup value={since} onChange={(event) => setSince(event.currentTarget.value)} placeholder="since (ISO-8601)" />
              <InputGroup value={until} onChange={(event) => setUntil(event.currentTarget.value)} placeholder="until (ISO-8601)" />
            </div>
          </FormGroup>
          <FormGroup label="Diff controls (from / to)">
            <div className="form-row">
              <InputGroup value={fromAsOf} onChange={(event) => setFromAsOf(event.currentTarget.value)} placeholder="from_as_of" />
              <InputGroup value={toAsOf} onChange={(event) => setToAsOf(event.currentTarget.value)} placeholder="to_as_of" />
            </div>
          </FormGroup>
          <FormGroup label="Numeric controls">
            <div className="form-row">
              <InputGroup value={windowMinutes} onChange={(event) => setWindowMinutes(event.currentTarget.value)} placeholder="window_minutes" />
              <InputGroup value={freshnessSloMinutes} onChange={(event) => setFreshnessSloMinutes(event.currentTarget.value)} placeholder="freshness_slo_minutes" />
              <InputGroup value={bucketMinutes} onChange={(event) => setBucketMinutes(event.currentTarget.value)} placeholder="bucket_minutes" />
              <InputGroup value={eventLimit} onChange={(event) => setEventLimit(event.currentTarget.value)} placeholder="event_limit" />
              <InputGroup value={pairLimit} onChange={(event) => setPairLimit(event.currentTarget.value)} placeholder="pair_limit" />
            </div>
          </FormGroup>
          <div className="form-row">
            <Button intent={Intent.PRIMARY} onClick={() => graphMutation.mutate()} disabled={!root} loading={graphMutation.isPending}>
              Graph
            </Button>
            <Button onClick={() => pathMutation.mutate()} disabled={!source || !target} loading={pathMutation.isPending}>
              Path
            </Button>
            <Button onClick={() => impactMutation.mutate()} disabled={!root} loading={impactMutation.isPending}>
              Impact
            </Button>
            <Button onClick={() => diffMutation.mutate()} disabled={!root || !fromAsOf} loading={diffMutation.isPending}>
              Diff
            </Button>
            <Button onClick={() => metricsMutation.mutate()} loading={metricsMutation.isPending}>
              Metrics
            </Button>
          </div>
          <div className="form-row">
            <Button onClick={() => runsMutation.mutate()} loading={runsMutation.isPending}>
              Runs
            </Button>
            <Button onClick={() => timelineMutation.mutate()} loading={timelineMutation.isPending}>
              Timeline
            </Button>
            <Button onClick={seedFromTimeline} disabled={!timelineFirst}>
              Seed from timeline
            </Button>
            <Button onClick={() => runImpactMutation.mutate()} disabled={!runId} loading={runImpactMutation.isPending}>
              Run impact
            </Button>
            <Button onClick={() => columnLineageMutation.mutate()} loading={columnLineageMutation.isPending}>
              Column lineage
            </Button>
            <Button onClick={() => outOfDateMutation.mutate()} loading={outOfDateMutation.isPending}>
              Out-of-date
            </Button>
          </div>
          {timelineMutation.isSuccess && timelineEventCount === 0 ? (
            <Callout intent={Intent.WARNING}>
              Timeline returned no events. Execute ingest/build/action flow first, then rerun lineage diagnostics.
            </Callout>
          ) : null}
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
          <div className="card-title">Runs</div>
          <JsonViewer value={runsMutation.data} empty="Run timeline will appear here." />
          <div className="card-title">Timeline</div>
          <JsonViewer value={timelineMutation.data} empty="Timeline payload will appear here." />
          <div className="card-title">Path</div>
          <JsonViewer value={pathMutation.data} empty="Path payload will appear here." />
          <div className="card-title">Diff</div>
          <JsonViewer value={diffMutation.data} empty="Diff payload will appear here." />
          <div className="card-title">Run impact</div>
          <JsonViewer value={runImpactMutation.data} empty="Run impact payload will appear here." />
          <div className="card-title">Column lineage</div>
          <JsonViewer value={columnLineageMutation.data} empty="Column lineage payload will appear here." />
          <div className="card-title">Out-of-date</div>
          <JsonViewer value={outOfDateMutation.data} empty="Out-of-date diagnostics will appear here." />
        </Card>
      </div>
    </div>
  )
}
