import { useMemo } from 'react'
import { Button, Card, Callout, HTMLTable, Intent, Spinner, Tag, Tooltip } from '@blueprintjs/core'
import { useQuery } from '@tanstack/react-query'
import { PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis, ResponsiveContainer, Tooltip as RTooltip } from 'recharts'
import {
  getSummary,
  listObjectTypesV2,
  listPipelineDatasets,
  listConnectionsV2,
  listAuditLogs,
  getSystemHealth,
} from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { PageHeader } from '../components/layout/PageHeader'
import { StatCard } from '../components/ux/StatCard'
import { StatusBadge } from '../components/ux/StatusBadge'
import { qk } from '../query/queryKeys'
import { navigate } from '../state/pathname'
import { useAppStore } from '../store/useAppStore'

const CHART_COLORS = ['#137cbd', '#0f9960', '#d9822b', '#db3737', '#8f398f', '#1f4b99', '#29a634', '#c23030']

function healthLevel(status?: string): 'success' | 'warning' | 'danger' | 'unknown' {
  if (!status) return 'unknown'
  const s = status.toLowerCase()
  if (s === 'healthy' || s === 'ok' || s === 'green' || s === 'connected') return 'success'
  if (s === 'degraded' || s === 'yellow' || s === 'warning') return 'warning'
  if (s === 'unhealthy' || s === 'red' || s === 'error' || s === 'down') return 'danger'
  return 'unknown'
}

export const OverviewPage = ({ dbName }: { dbName: string }) => {
  const ctx = useRequestContext()
  const branch = useAppStore((state) => state.context.branch)
  const base = `/db/${encodeURIComponent(dbName)}`

  const summaryQ = useQuery({
    queryKey: qk.summary({ dbName, branch, language: ctx.language }),
    queryFn: () => getSummary(ctx, { dbName, branch }),
  })
  const summary = (summaryQ.data ?? {}) as Record<string, unknown>

  const typesQ = useQuery({
    queryKey: ['overview', 'objectTypes', dbName, branch],
    queryFn: () => listObjectTypesV2(ctx, dbName, { branch }),
  })
  const objectTypes = Array.isArray(typesQ.data)
    ? typesQ.data
    : (typesQ.data as { data?: unknown[] })?.data ?? []

  const datasetsQ = useQuery({
    queryKey: ['overview', 'datasets', dbName, branch],
    queryFn: () => listPipelineDatasets(ctx, { db_name: dbName, branch }),
  })
  const datasets = Array.isArray(datasetsQ.data)
    ? datasetsQ.data
    : (datasetsQ.data as { data?: unknown[] })?.data ?? []

  const connectionsQ = useQuery({
    queryKey: ['overview', 'connections'],
    queryFn: () => listConnectionsV2(ctx),
  })
  const connections = Array.isArray(connectionsQ.data)
    ? connectionsQ.data
    : (connectionsQ.data as { data?: unknown[] })?.data ?? []

  const auditQ = useQuery({
    queryKey: ['overview', 'audit', dbName],
    queryFn: () => listAuditLogs(ctx, { db: dbName, limit: 10 }),
  })
  const auditLogs = useMemo(() => {
    const d = auditQ.data as Record<string, unknown> | unknown[] | null
    if (Array.isArray(d)) return d
    if (d && typeof d === 'object' && 'data' in d) {
      const inner = (d as { data?: unknown }).data
      if (Array.isArray(inner)) return inner
    }
    if (d && typeof d === 'object' && 'logs' in d) {
      const inner = (d as { logs?: unknown }).logs
      if (Array.isArray(inner)) return inner
    }
    return []
  }, [auditQ.data])

  const healthQ = useQuery({
    queryKey: ['overview', 'health'],
    queryFn: () => getSystemHealth(ctx),
    refetchInterval: 30_000,
  })
  const healthData = (healthQ.data ?? {}) as Record<string, unknown>
  const healthChecks = (healthData.checks ?? healthData.services ?? healthData) as Record<string, unknown>

  const instanceCount =
    (summary.totalInstances as number) ??
    (summary.total_instances as number) ??
    (summary.instance_count as number) ??
    '—'

  /* chart data: object types with property count */
  const typeChartData = objectTypes.slice(0, 8).map((ot) => {
    const entry = ot as Record<string, unknown>
    const name = String(entry.apiName ?? entry.name ?? entry.displayName ?? entry.class_id ?? 'type')
    const props = Array.isArray(entry.properties)
      ? entry.properties.length
      : typeof entry.properties === 'object' && entry.properties
        ? Object.keys(entry.properties).length
        : 1
    return { name: name.length > 12 ? name.slice(0, 12) + '..' : name, value: props }
  })

  /* audit bar chart data: events per type */
  const auditBarData = useMemo(() => {
    const counts: Record<string, number> = {}
    for (const log of auditLogs.slice(0, 50)) {
      const entry = log as Record<string, unknown>
      const event = String(entry.event_type ?? entry.action ?? entry.type ?? 'other')
      counts[event] = (counts[event] ?? 0) + 1
    }
    return Object.entries(counts).slice(0, 6).map(([name, count]) => ({ name, count }))
  }, [auditLogs])

  return (
    <div>
      <PageHeader
        title={dbName}
        subtitle={`Branch: ${branch}`}
        actions={
          <Button icon="refresh" minimal onClick={() => {
            summaryQ.refetch()
            typesQ.refetch()
            datasetsQ.refetch()
            connectionsQ.refetch()
            auditQ.refetch()
            healthQ.refetch()
          }}>
            Refresh All
          </Button>
        }
      />

      {/* KPI Row */}
      <div className="stat-card-grid">
        <div onClick={() => navigate(`${base}/ontology`)} style={{ cursor: 'pointer' }}>
          <StatCard
            label="Object Types"
            value={typesQ.isLoading ? '...' : objectTypes.length}
            tooltip="Ontology object type definitions"
            color="#137cbd"
          />
        </div>
        <div onClick={() => navigate(`${base}/instances`)} style={{ cursor: 'pointer' }}>
          <StatCard
            label="Instances"
            value={summaryQ.isLoading ? '...' : instanceCount}
            tooltip="Total object instances in Elasticsearch"
            color="#0f9960"
          />
        </div>
        <div onClick={() => navigate(`${base}/datasets`)} style={{ cursor: 'pointer' }}>
          <StatCard
            label="Datasets"
            value={datasetsQ.isLoading ? '...' : datasets.length}
            tooltip="Uploaded data files (CSV, Excel, etc.)"
            color="#d9822b"
          />
        </div>
        <div onClick={() => navigate('/connections')} style={{ cursor: 'pointer' }}>
          <StatCard
            label="Connections"
            value={connectionsQ.isLoading ? '...' : connections.length}
            tooltip="External data source connections"
            color="#8f398f"
          />
        </div>
      </div>

      <div className="two-col-grid">
        {/* Left column */}
        <div className="card-stack">
          {/* Charts */}
          {(typeChartData.length > 0 || auditBarData.length > 0) && (
            <Card>
              <div className="card-title">
                <Tooltip content="Visual summary of object types and recent activity" placement="top">
                  <span className="tooltip-label">Analytics Overview</span>
                </Tooltip>
              </div>
              <div className="two-col-grid">
                {typeChartData.length > 0 && (
                  <div>
                    <div style={{ fontSize: 12, opacity: 0.6, marginBottom: 8 }}>Properties per Type</div>
                    <ResponsiveContainer width="100%" height={180}>
                      <PieChart>
                        <Pie
                          data={typeChartData}
                          dataKey="value"
                          nameKey="name"
                          cx="50%"
                          cy="50%"
                          outerRadius={70}
                          innerRadius={35}
                          paddingAngle={2}
                          isAnimationActive={false}
                        >
                          {typeChartData.map((_, i) => (
                            <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
                          ))}
                        </Pie>
                        <RTooltip />
                      </PieChart>
                    </ResponsiveContainer>
                  </div>
                )}
                {auditBarData.length > 0 && (
                  <div>
                    <div style={{ fontSize: 12, opacity: 0.6, marginBottom: 8 }}>Event Types</div>
                    <ResponsiveContainer width="100%" height={180}>
                      <BarChart data={auditBarData}>
                        <XAxis dataKey="name" tick={{ fontSize: 10 }} />
                        <YAxis tick={{ fontSize: 10 }} />
                        <RTooltip />
                        <Bar dataKey="count" fill="#137cbd" radius={[4, 4, 0, 0]} isAnimationActive={false} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                )}
              </div>
            </Card>
          )}

          {/* Recent Activity */}
          <Card>
            <div className="card-title">
              <Tooltip content="Chronological log of changes in this database" placement="top">
                <span className="tooltip-label">Recent Activity</span>
              </Tooltip>
            </div>
            {auditQ.isLoading && <Spinner size={20} />}
            {auditQ.error && <Callout intent={Intent.DANGER}>Failed to load audit logs.</Callout>}
            {auditLogs.length === 0 && !auditQ.isLoading && (
              <Callout intent={Intent.NONE}>No recent activity recorded.</Callout>
            )}
            {auditLogs.length > 0 && (
              <HTMLTable compact striped style={{ width: '100%' }}>
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Event</th>
                    <th>Detail</th>
                  </tr>
                </thead>
                <tbody>
                  {auditLogs.slice(0, 10).map((log, i) => {
                    const entry = log as Record<string, unknown>
                    const ts = entry.timestamp ?? entry.created_at ?? entry.time
                    const event = entry.event_type ?? entry.action ?? entry.type ?? '—'
                    const detail = entry.summary ?? entry.message ?? entry.entity_id ?? entry.target ?? '—'
                    return (
                      <tr key={i}>
                        <td style={{ whiteSpace: 'nowrap', fontSize: 12, opacity: 0.7 }}>
                          {ts ? new Date(String(ts)).toLocaleString() : '—'}
                        </td>
                        <td><Tag minimal>{String(event)}</Tag></td>
                        <td style={{ fontSize: 12 }}>{String(detail).slice(0, 80)}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </HTMLTable>
            )}
            <Button small minimal icon="arrow-right" style={{ marginTop: 8 }} onClick={() => navigate(`${base}/audit`)}>
              View all audit logs
            </Button>
          </Card>

          {/* Quick Navigation */}
          <Card>
            <div className="card-title">Quick Navigation</div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
              <Button icon="cube" alignText="left" onClick={() => navigate(`${base}/ontology`)}>Ontology</Button>
              <Button icon="database" alignText="left" onClick={() => navigate(`${base}/datasets`)}>Datasets</Button>
              <Button icon="data-lineage" alignText="left" onClick={() => navigate(`${base}/pipelines`)}>Pipelines</Button>
              <Button icon="exchange" alignText="left" onClick={() => navigate(`${base}/objectify`)}>Objectify</Button>
              <Button icon="search-template" alignText="left" onClick={() => navigate(`${base}/explore/objects`)}>Object Explorer</Button>
              <Button icon="graph" alignText="left" onClick={() => navigate(`${base}/explore/graph`)}>Graph Explorer</Button>
              <Button icon="code-block" alignText="left" onClick={() => navigate(`${base}/explore/query`)}>Query Builder</Button>
              <Button icon="flow-branch" alignText="left" onClick={() => navigate(`${base}/lineage`)}>Data Lineage</Button>
            </div>
          </Card>
        </div>

        {/* Right column */}
        <div className="card-stack">
          {/* System Health */}
          <Card>
            <div className="card-title">
              <Tooltip content="Real-time health status of backend services" placement="top">
                <span className="tooltip-label">System Health</span>
              </Tooltip>
            </div>
            {healthQ.isLoading && <Spinner size={20} />}
            {healthQ.error && <Callout intent={Intent.WARNING}>Unable to fetch health status.</Callout>}
            {!healthQ.isLoading && !healthQ.error && (
              <HTMLTable compact style={{ width: '100%' }}>
                <thead>
                  <tr>
                    <th>Service</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(healthChecks).map(([service, value]) => {
                    const status =
                      typeof value === 'string'
                        ? value
                        : typeof value === 'object' && value !== null
                          ? ((value as Record<string, unknown>).status as string) ?? 'unknown'
                          : 'unknown'
                    return (
                      <tr key={service}>
                        <td style={{ textTransform: 'capitalize' }}>{service.replace(/_/g, ' ')}</td>
                        <td>
                          <StatusBadge status={healthLevel(status)} label={status} />
                        </td>
                      </tr>
                    )
                  })}
                  {Object.keys(healthChecks).length === 0 && (
                    <tr><td colSpan={2} style={{ opacity: 0.5 }}>No health data available</td></tr>
                  )}
                </tbody>
              </HTMLTable>
            )}
          </Card>

          {/* Object Types Summary */}
          <Card>
            <div className="card-title">
              <Tooltip content="Data model definitions in the ontology" placement="top">
                <span className="tooltip-label">Object Types</span>
              </Tooltip>
            </div>
            {typesQ.isLoading && <Spinner size={20} />}
            {objectTypes.length === 0 && !typesQ.isLoading && (
              <Callout intent={Intent.NONE}>
                No object types defined yet.
                <Button small minimal icon="plus" onClick={() => navigate(`${base}/ontology`)}>Create one</Button>
              </Callout>
            )}
            {objectTypes.length > 0 && (
              <HTMLTable compact striped style={{ width: '100%' }}>
                <thead>
                  <tr><th>Type</th><th>Properties</th></tr>
                </thead>
                <tbody>
                  {objectTypes.slice(0, 10).map((ot, i) => {
                    const entry = ot as Record<string, unknown>
                    const name = entry.apiName ?? entry.name ?? entry.displayName ?? entry.class_id ?? `type_${i}`
                    const props = Array.isArray(entry.properties)
                      ? entry.properties.length
                      : typeof entry.properties === 'object' && entry.properties
                        ? Object.keys(entry.properties).length
                        : '—'
                    return (
                      <tr key={i}>
                        <td><Tag minimal>{String(name)}</Tag></td>
                        <td>{props}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </HTMLTable>
            )}
            {objectTypes.length > 10 && (
              <div style={{ fontSize: 12, opacity: 0.6, marginTop: 4 }}>Showing 10 of {objectTypes.length} types.</div>
            )}
            <Button small minimal icon="arrow-right" style={{ marginTop: 8 }} onClick={() => navigate(`${base}/ontology`)}>
              Manage ontology
            </Button>
          </Card>

          {/* Datasets Summary */}
          <Card>
            <div className="card-title">
              <Tooltip content="Uploaded data files available for pipelines and objectification" placement="top">
                <span className="tooltip-label">Datasets</span>
              </Tooltip>
            </div>
            {datasetsQ.isLoading && <Spinner size={20} />}
            {datasets.length === 0 && !datasetsQ.isLoading && (
              <Callout intent={Intent.NONE}>
                No datasets uploaded yet.
                <Button small minimal icon="upload" onClick={() => navigate(`${base}/datasets`)}>Upload one</Button>
              </Callout>
            )}
            {datasets.length > 0 && (
              <HTMLTable compact striped style={{ width: '100%' }}>
                <thead>
                  <tr><th>Name</th><th>Source</th><th>Rows</th></tr>
                </thead>
                <tbody>
                  {datasets.slice(0, 8).map((ds, i) => {
                    const entry = ds as Record<string, unknown>
                    return (
                      <tr key={i}>
                        <td>{String(entry.name ?? entry.displayName ?? `dataset_${i}`)}</td>
                        <td><Tag minimal>{String(entry.source_type ?? '—')}</Tag></td>
                        <td>{entry.row_count != null ? String(entry.row_count) : '—'}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </HTMLTable>
            )}
            <Button small minimal icon="arrow-right" style={{ marginTop: 8 }} onClick={() => navigate(`${base}/datasets`)}>
              View all datasets
            </Button>
          </Card>
        </div>
      </div>
    </div>
  )
}
