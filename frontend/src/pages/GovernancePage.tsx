import { useState, useMemo } from 'react'
import {
  Button,
  Card,
  Callout,
  Dialog,
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
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { KeyValueEditor } from '../components/ux/KeyValueEditor'
import { StatusBadge } from '../components/ux/StatusBadge'
import { StatCard } from '../components/ux/StatCard'
import { useRequestContext } from '../api/useRequestContext'
import { useAppStore } from '../store/useAppStore'
import {
  listAccessPoliciesV1,
  upsertAccessPolicyV1,
  listGatePolicies,
  createGatePolicy,
  listGateResults,
  listKeySpecs,
  createKeySpec,
  listSchemaChangeHistory,
  acknowledgeSchemaChangeDrift,
  getSchemaChangeStats,
} from '../api/bff'

/* ── query keys ─────────────────────────────────────── */
const govKeys = {
  policies: (db: string) => ['governance', 'policies', db] as const,
  gates: (db: string) => ['governance', 'gates', db] as const,
  gateResults: (db: string) => ['governance', 'gateResults', db] as const,
  keySpecs: (db: string) => ['governance', 'keySpecs', db] as const,
  schemaHistory: (db: string) => ['governance', 'schemaHistory', db] as const,
  schemaStats: (db: string) => ['governance', 'schemaStats', db] as const,
}

/* ── page ────────────────────────────────────────────── */
export const GovernancePage = ({ dbName }: { dbName: string }) => {
  const ctx = useRequestContext()
  const queryClient = useQueryClient()
  const [activeTab, setActiveTab] = useState('access')

  return (
    <div>
      <PageHeader
        title="Governance"
        subtitle={`Access control, quality gates & schema management · ${dbName}`}
      />

      <Tabs
        selectedTabId={activeTab}
        onChange={(id) => setActiveTab(id as string)}
        large
      >
        <Tab id="access" title="Access Policies" panel={
          <AccessPoliciesTab dbName={dbName} ctx={ctx} queryClient={queryClient} />
        } />
        <Tab id="gates" title="Quality Gates" panel={
          <QualityGatesTab dbName={dbName} ctx={ctx} queryClient={queryClient} />
        } />
        <Tab id="keys" title="Key Specs" panel={
          <KeySpecsTab dbName={dbName} ctx={ctx} queryClient={queryClient} />
        } />
        <Tab id="schema" title="Schema Changes" panel={
          <SchemaChangesTab dbName={dbName} ctx={ctx} queryClient={queryClient} />
        } />
      </Tabs>
    </div>
  )
}

/* ── Access Policies Tab ────────────────────────────── */
const AccessPoliciesTab = ({
  dbName,
  ctx,
  queryClient,
}: {
  dbName: string
  ctx: ReturnType<typeof useRequestContext>
  queryClient: ReturnType<typeof useQueryClient>
}) => {
  const [dialogOpen, setDialogOpen] = useState(false)
  const [subjectType, setSubjectType] = useState('user')
  const [subjectId, setSubjectId] = useState('')
  const [scope, setScope] = useState('database')
  const [policyKv, setPolicyKv] = useState<Record<string, string>>({ permission: 'read' })

  const policiesQ = useQuery({
    queryKey: govKeys.policies(dbName),
    queryFn: () => listAccessPoliciesV1(ctx, { db_name: dbName }),
  })

  const createMut = useMutation({
    mutationFn: () => {
      let parsed: Record<string, unknown> = {}
      parsed = policyKv as Record<string, unknown>
      return upsertAccessPolicyV1(ctx, {
        db_name: dbName,
        scope,
        subject_type: subjectType,
        subject_id: subjectId,
        policy: parsed,
      })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: govKeys.policies(dbName) })
      setDialogOpen(false)
      setSubjectId('')
      setPolicyKv({ permission: 'read' })
    },
  })

  const policies = policiesQ.data ?? []

  return (
    <div style={{ marginTop: 12 }}>
      <div className="form-row" style={{ justifyContent: 'space-between', marginBottom: 12 }}>
        <Tag minimal>{policies.length} policies</Tag>
        <Button icon="plus" intent={Intent.PRIMARY} onClick={() => setDialogOpen(true)}>
          New Policy
        </Button>
      </div>

      {policiesQ.isLoading && <Spinner size={20} />}
      {policiesQ.error && <Callout intent={Intent.DANGER}>Failed to load access policies.</Callout>}

      {policies.length === 0 && !policiesQ.isLoading && (
        <Card>
          <Callout icon="info-sign">
            No access policies defined. Create one to control data access.
          </Callout>
        </Card>
      )}

      {policies.length > 0 && (
        <Card>
          <HTMLTable compact striped style={{ width: '100%' }}>
            <thead>
              <tr>
                <th>Subject</th>
                <th>Type</th>
                <th>Scope</th>
                <th>Status</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {policies.map((p, i) => (
                <tr key={i}>
                  <td style={{ fontFamily: 'monospace', fontSize: 12 }}>
                    {String(p.subject_id ?? '—')}
                  </td>
                  <td><Tag minimal>{String(p.subject_type ?? '—')}</Tag></td>
                  <td>{String(p.scope ?? 'database')}</td>
                  <td>
                    <Tag
                      minimal
                      intent={String(p.status) === 'active' ? Intent.SUCCESS : Intent.NONE}
                    >
                      {String(p.status ?? 'active')}
                    </Tag>
                  </td>
                  <td style={{ fontSize: 12 }}>
                    {p.created_at ? new Date(String(p.created_at)).toLocaleDateString() : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </HTMLTable>
        </Card>
      )}

      <Dialog
        isOpen={dialogOpen}
        onClose={() => setDialogOpen(false)}
        title="Create Access Policy"
        style={{ width: 500 }}
      >
        <div style={{ padding: 20 }}>
          <FormGroup label="Subject Type">
            <HTMLSelect
              value={subjectType}
              onChange={(e) => setSubjectType(e.target.value)}
              fill
              options={[
                { value: 'user', label: 'User' },
                { value: 'group', label: 'Group' },
                { value: 'role', label: 'Role' },
                { value: 'service', label: 'Service Account' },
              ]}
            />
          </FormGroup>
          <FormGroup label="Subject ID">
            <InputGroup
              value={subjectId}
              onChange={(e) => setSubjectId(e.target.value)}
              placeholder="e.g. user@example.com"
            />
          </FormGroup>
          <FormGroup label="Scope">
            <HTMLSelect
              value={scope}
              onChange={(e) => setScope(e.target.value)}
              fill
              options={[
                { value: 'database', label: 'Database' },
                { value: 'dataset', label: 'Dataset' },
                { value: 'ontology', label: 'Ontology' },
                { value: 'pipeline', label: 'Pipeline' },
              ]}
            />
          </FormGroup>
          <FormGroup label={
            <Tooltip content="Define access rules as key-value pairs (e.g. permission: read, resource: datasets/*)" placement="top">
              <span className="tooltip-label">Policy Rules</span>
            </Tooltip>
          }>
            <KeyValueEditor
              value={policyKv}
              onChange={setPolicyKv}
              keyPlaceholder="Rule key (e.g. permission)"
              valuePlaceholder="Rule value (e.g. read)"
              addLabel="Add rule"
            />
          </FormGroup>
          <div className="form-row" style={{ justifyContent: 'flex-end', gap: 8 }}>
            <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
            <Button
              intent={Intent.PRIMARY}
              loading={createMut.isPending}
              disabled={!subjectId}
              onClick={() => createMut.mutate()}
            >
              Create Policy
            </Button>
          </div>
          {createMut.error && (
            <Callout intent={Intent.DANGER} style={{ marginTop: 8 }}>Failed to create policy.</Callout>
          )}
        </div>
      </Dialog>
    </div>
  )
}

/* ── Quality Gates Tab ──────────────────────────────── */
const QualityGatesTab = ({
  dbName,
  ctx,
  queryClient,
}: {
  dbName: string
  ctx: ReturnType<typeof useRequestContext>
  queryClient: ReturnType<typeof useQueryClient>
}) => {
  const [dialogOpen, setDialogOpen] = useState(false)
  const [gateName, setGateName] = useState('')
  const [gateType, setGateType] = useState('completeness')
  const [threshold, setThreshold] = useState('0.95')
  const [gateConfigKv, setGateConfigKv] = useState<Record<string, string>>({})

  const gatesQ = useQuery({
    queryKey: govKeys.gates(dbName),
    queryFn: () => listGatePolicies(ctx, { db_name: dbName }),
  })

  const resultsQ = useQuery({
    queryKey: govKeys.gateResults(dbName),
    queryFn: () => listGateResults(ctx, { db_name: dbName }),
  })

  const createMut = useMutation({
    mutationFn: () => {
      let config: Record<string, unknown> = {}
      config = gateConfigKv as Record<string, unknown>
      return createGatePolicy(ctx, {
        db_name: dbName,
        name: gateName,
        gate_type: gateType,
        threshold: Number(threshold) || 0.95,
        ...config,
      })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: govKeys.gates(dbName) })
      setDialogOpen(false)
      setGateName('')
    },
  })

  const gates = gatesQ.data ?? []
  const results = resultsQ.data ?? []

  return (
    <div style={{ marginTop: 12 }}>
      <div className="form-row" style={{ justifyContent: 'space-between', marginBottom: 12 }}>
        <div className="form-row" style={{ gap: 8 }}>
          <Tag minimal>{gates.length} gates</Tag>
          <Tag minimal intent={Intent.SUCCESS}>
            {results.filter((r) => String(r.status) === 'passed').length} passed
          </Tag>
          <Tag minimal intent={Intent.DANGER}>
            {results.filter((r) => String(r.status) === 'failed').length} failed
          </Tag>
        </div>
        <Button icon="plus" intent={Intent.PRIMARY} onClick={() => setDialogOpen(true)}>
          New Gate
        </Button>
      </div>

      {gatesQ.isLoading && <Spinner size={20} />}

      <div className="two-col-grid">
        {/* Gate Definitions */}
        <Card>
          <div className="card-title">Gate Definitions</div>
          {gates.length === 0 && !gatesQ.isLoading && (
            <Callout icon="info-sign">No quality gates defined.</Callout>
          )}
          {gates.length > 0 && (
            <HTMLTable compact striped style={{ width: '100%' }}>
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Type</th>
                  <th>Threshold</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {gates.map((g, i) => (
                  <tr key={i}>
                    <td style={{ fontFamily: 'monospace', fontSize: 12 }}>{String(g.name ?? g.gate_id ?? `gate_${i}`)}</td>
                    <td><Tag minimal>{String(g.gate_type ?? g.type ?? '—')}</Tag></td>
                    <td style={{ fontSize: 12 }}>{String(g.threshold ?? '—')}</td>
                    <td>
                      <Tag
                        minimal
                        intent={String(g.status) === 'active' ? Intent.SUCCESS : Intent.NONE}
                      >
                        {String(g.status ?? 'active')}
                      </Tag>
                    </td>
                  </tr>
                ))}
              </tbody>
            </HTMLTable>
          )}
        </Card>

        {/* Gate Results */}
        <Card>
          <div className="card-title">Recent Results</div>
          {resultsQ.isLoading && <Spinner size={20} />}
          {results.length === 0 && !resultsQ.isLoading && (
            <Callout icon="info-sign">No gate results yet.</Callout>
          )}
          {results.length > 0 && (
            <HTMLTable compact striped style={{ width: '100%' }}>
              <thead>
                <tr>
                  <th>Gate</th>
                  <th>Result</th>
                  <th>Score</th>
                  <th>Checked</th>
                </tr>
              </thead>
              <tbody>
                {results.slice(0, 20).map((r, i) => (
                  <tr key={i}>
                    <td style={{ fontSize: 12 }}>{String(r.gate_name ?? r.gate_id ?? '—')}</td>
                    <td>
                      <Tag
                        intent={String(r.status) === 'passed' ? Intent.SUCCESS : Intent.DANGER}
                        minimal
                      >
                        {String(r.status ?? '—')}
                      </Tag>
                    </td>
                    <td style={{ fontFamily: 'monospace', fontSize: 12 }}>
                      {r.score != null ? String(Number(r.score).toFixed(4)) : '—'}
                    </td>
                    <td style={{ fontSize: 12 }}>
                      {r.checked_at ? new Date(String(r.checked_at)).toLocaleString() : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </HTMLTable>
          )}
        </Card>
      </div>

      <Dialog
        isOpen={dialogOpen}
        onClose={() => setDialogOpen(false)}
        title="Create Quality Gate"
        style={{ width: 500 }}
      >
        <div style={{ padding: 20 }}>
          <FormGroup label="Gate Name">
            <InputGroup
              value={gateName}
              onChange={(e) => setGateName(e.target.value)}
              placeholder="e.g. orders_completeness_check"
            />
          </FormGroup>
          <FormGroup label="Gate Type">
            <HTMLSelect
              value={gateType}
              onChange={(e) => setGateType(e.target.value)}
              fill
              options={[
                { value: 'completeness', label: 'Completeness (non-null ratio)' },
                { value: 'uniqueness', label: 'Uniqueness (unique value ratio)' },
                { value: 'freshness', label: 'Freshness (data age limit)' },
                { value: 'schema', label: 'Schema Conformance' },
                { value: 'row_count', label: 'Row Count Range' },
                { value: 'custom', label: 'Custom SQL Assertion' },
              ]}
            />
          </FormGroup>
          <FormGroup label="Threshold (0.0 - 1.0)">
            <InputGroup
              value={threshold}
              onChange={(e) => setThreshold(e.target.value)}
              placeholder="0.95"
            />
          </FormGroup>
          <FormGroup label={
            <Tooltip content="Additional config (e.g. columns to check, SQL assertion)" placement="top">
              <span className="tooltip-label">Gate Configuration</span>
            </Tooltip>
          }>
            <KeyValueEditor
              value={gateConfigKv}
              onChange={setGateConfigKv}
              keyPlaceholder="Config key"
              valuePlaceholder="Config value"
              addLabel="Add config field"
            />
          </FormGroup>
          <div className="form-row" style={{ justifyContent: 'flex-end', gap: 8 }}>
            <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
            <Button
              intent={Intent.PRIMARY}
              loading={createMut.isPending}
              disabled={!gateName}
              onClick={() => createMut.mutate()}
            >
              Create Gate
            </Button>
          </div>
          {createMut.error && (
            <Callout intent={Intent.DANGER} style={{ marginTop: 8 }}>Failed to create gate.</Callout>
          )}
        </div>
      </Dialog>
    </div>
  )
}

/* ── Key Specs Tab ──────────────────────────────────── */
const KeySpecsTab = ({
  dbName,
  ctx,
  queryClient,
}: {
  dbName: string
  ctx: ReturnType<typeof useRequestContext>
  queryClient: ReturnType<typeof useQueryClient>
}) => {
  const [dialogOpen, setDialogOpen] = useState(false)
  const [keyName, setKeyName] = useState('')
  const [keyType, setKeyType] = useState('primary')
  const [keyColumns, setKeyColumns] = useState('')
  const [targetType, setTargetType] = useState('')

  const keySpecsQ = useQuery({
    queryKey: govKeys.keySpecs(dbName),
    queryFn: () => listKeySpecs(ctx, { db_name: dbName }),
  })

  const createMut = useMutation({
    mutationFn: () => createKeySpec(ctx, {
      db_name: dbName,
      name: keyName,
      key_type: keyType,
      columns: keyColumns.split(',').map((s) => s.trim()).filter(Boolean),
      target_type: targetType || undefined,
    }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: govKeys.keySpecs(dbName) })
      setDialogOpen(false)
      setKeyName('')
      setKeyColumns('')
    },
  })

  const keySpecs = keySpecsQ.data ?? []

  return (
    <div style={{ marginTop: 12 }}>
      <div className="form-row" style={{ justifyContent: 'space-between', marginBottom: 12 }}>
        <Tag minimal>{keySpecs.length} key specs</Tag>
        <Button icon="plus" intent={Intent.PRIMARY} onClick={() => setDialogOpen(true)}>
          New Key Spec
        </Button>
      </div>

      {keySpecsQ.isLoading && <Spinner size={20} />}

      {keySpecs.length === 0 && !keySpecsQ.isLoading && (
        <Card>
          <Callout icon="info-sign">
            No key specifications defined. Key specs enforce uniqueness and referential integrity.
          </Callout>
        </Card>
      )}

      {keySpecs.length > 0 && (
        <Card>
          <HTMLTable compact striped style={{ width: '100%' }}>
            <thead>
              <tr>
                <th>Name</th>
                <th>Type</th>
                <th>Columns</th>
                <th>Target</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {keySpecs.map((ks, i) => (
                <tr key={i}>
                  <td style={{ fontFamily: 'monospace', fontSize: 12 }}>{String(ks.name ?? ks.key_spec_id ?? `key_${i}`)}</td>
                  <td><Tag minimal>{String(ks.key_type ?? ks.type ?? '—')}</Tag></td>
                  <td style={{ fontSize: 12 }}>
                    {Array.isArray(ks.columns) ? (ks.columns as string[]).join(', ') : String(ks.columns ?? '—')}
                  </td>
                  <td style={{ fontSize: 12 }}>{String(ks.target_type ?? '—')}</td>
                  <td>
                    <Tag
                      minimal
                      intent={String(ks.status) === 'active' ? Intent.SUCCESS : Intent.NONE}
                    >
                      {String(ks.status ?? 'active')}
                    </Tag>
                  </td>
                </tr>
              ))}
            </tbody>
          </HTMLTable>
        </Card>
      )}

      <Dialog
        isOpen={dialogOpen}
        onClose={() => setDialogOpen(false)}
        title="Create Key Spec"
        style={{ width: 500 }}
      >
        <div style={{ padding: 20 }}>
          <FormGroup label="Key Name">
            <InputGroup
              value={keyName}
              onChange={(e) => setKeyName(e.target.value)}
              placeholder="e.g. pk_order_id"
            />
          </FormGroup>
          <FormGroup label="Key Type">
            <HTMLSelect
              value={keyType}
              onChange={(e) => setKeyType(e.target.value)}
              fill
              options={[
                { value: 'primary', label: 'Primary Key' },
                { value: 'unique', label: 'Unique Key' },
                { value: 'foreign', label: 'Foreign Key' },
                { value: 'composite', label: 'Composite Key' },
              ]}
            />
          </FormGroup>
          <FormGroup label="Columns (comma-separated)">
            <InputGroup
              value={keyColumns}
              onChange={(e) => setKeyColumns(e.target.value)}
              placeholder="e.g. order_id, customer_id"
            />
          </FormGroup>
          <FormGroup label="Target Type (for foreign keys)">
            <InputGroup
              value={targetType}
              onChange={(e) => setTargetType(e.target.value)}
              placeholder="e.g. Customer"
            />
          </FormGroup>
          <div className="form-row" style={{ justifyContent: 'flex-end', gap: 8 }}>
            <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
            <Button
              intent={Intent.PRIMARY}
              loading={createMut.isPending}
              disabled={!keyName || !keyColumns}
              onClick={() => createMut.mutate()}
            >
              Create Key Spec
            </Button>
          </div>
          {createMut.error && (
            <Callout intent={Intent.DANGER} style={{ marginTop: 8 }}>Failed to create key spec.</Callout>
          )}
        </div>
      </Dialog>
    </div>
  )
}

/* ── Schema Changes Tab ─────────────────────────────── */
const SchemaChangesTab = ({
  dbName,
  ctx,
  queryClient,
}: {
  dbName: string
  ctx: ReturnType<typeof useRequestContext>
  queryClient: ReturnType<typeof useQueryClient>
}) => {
  const historyQ = useQuery({
    queryKey: govKeys.schemaHistory(dbName),
    queryFn: () => listSchemaChangeHistory(ctx, { db_name: dbName, limit: 50 }),
  })

  const statsQ = useQuery({
    queryKey: govKeys.schemaStats(dbName),
    queryFn: () => getSchemaChangeStats(ctx, { db_name: dbName }),
  })

  const ackMut = useMutation({
    mutationFn: (driftId: string) => acknowledgeSchemaChangeDrift(ctx, driftId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: govKeys.schemaHistory(dbName) })
    },
  })

  const history = historyQ.data ?? []
  const stats = (statsQ.data ?? {}) as Record<string, unknown>

  return (
    <div style={{ marginTop: 12 }}>
      {/* Stats Summary */}
      <div className="form-row" style={{ gap: 12, marginBottom: 12 }}>
        <Card style={{ flex: 1, textAlign: 'center', padding: 16 }}>
          <div style={{ fontSize: 24, fontWeight: 700 }}>
            {String(stats.total_changes ?? history.length ?? 0)}
          </div>
          <div style={{ fontSize: 12, opacity: 0.7 }}>Total Changes</div>
        </Card>
        <Card style={{ flex: 1, textAlign: 'center', padding: 16 }}>
          <div style={{ fontSize: 24, fontWeight: 700, color: '#d9822b' }}>
            {String(stats.pending_drifts ?? 0)}
          </div>
          <div style={{ fontSize: 12, opacity: 0.7 }}>Pending Drifts</div>
        </Card>
        <Card style={{ flex: 1, textAlign: 'center', padding: 16 }}>
          <div style={{ fontSize: 24, fontWeight: 700, color: '#29a634' }}>
            {String(stats.acknowledged_drifts ?? 0)}
          </div>
          <div style={{ fontSize: 12, opacity: 0.7 }}>Acknowledged</div>
        </Card>
        <Card style={{ flex: 1, textAlign: 'center', padding: 16 }}>
          <div style={{ fontSize: 24, fontWeight: 700, color: '#2b95d6' }}>
            {String(stats.compatible_changes ?? 0)}
          </div>
          <div style={{ fontSize: 12, opacity: 0.7 }}>Compatible</div>
        </Card>
      </div>

      {/* Change History */}
      <Card>
        <div className="card-title" style={{ marginBottom: 8 }}>Schema Change History</div>
        {historyQ.isLoading && <Spinner size={20} />}
        {historyQ.error && <Callout intent={Intent.DANGER}>Failed to load schema change history.</Callout>}

        {history.length === 0 && !historyQ.isLoading && (
          <Callout icon="info-sign">No schema changes recorded.</Callout>
        )}

        {history.length > 0 && (
          <HTMLTable compact striped style={{ width: '100%' }}>
            <thead>
              <tr>
                <th>Change</th>
                <th>Type</th>
                <th>Affected</th>
                <th>Status</th>
                <th>Time</th>
                <th style={{ width: 80 }}>Action</th>
              </tr>
            </thead>
            <tbody>
              {history.map((ch, i) => {
                const isDrift = String(ch.type ?? ch.change_type ?? '').toLowerCase().includes('drift')
                const isAcknowledged = String(ch.status).toLowerCase() === 'acknowledged'
                return (
                  <tr key={i}>
                    <td style={{ fontFamily: 'monospace', fontSize: 12, maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {String(ch.description ?? ch.change_id ?? `change_${i}`)}
                    </td>
                    <td>
                      <Tag
                        minimal
                        intent={isDrift ? Intent.WARNING : Intent.NONE}
                      >
                        {String(ch.type ?? ch.change_type ?? '—')}
                      </Tag>
                    </td>
                    <td style={{ fontSize: 12 }}>
                      {String(ch.affected_entity ?? ch.target ?? '—')}
                    </td>
                    <td>
                      <Tag
                        minimal
                        intent={
                          isAcknowledged ? Intent.SUCCESS :
                          isDrift ? Intent.WARNING :
                          Intent.NONE
                        }
                      >
                        {String(ch.status ?? 'recorded')}
                      </Tag>
                    </td>
                    <td style={{ fontSize: 12 }}>
                      {ch.created_at ? new Date(String(ch.created_at)).toLocaleString() : '—'}
                    </td>
                    <td>
                      {isDrift && !isAcknowledged && (
                        <Button
                          minimal
                          icon="tick"
                          intent={Intent.SUCCESS}
                          loading={ackMut.isPending}
                          onClick={() => {
                            const driftId = String(ch.drift_id ?? ch.change_id ?? '')
                            if (driftId) ackMut.mutate(driftId)
                          }}
                        />
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </HTMLTable>
        )}
      </Card>
    </div>
  )
}
