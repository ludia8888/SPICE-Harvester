import { useState } from 'react'
import {
  Button,
  Card,
  Callout,
  Dialog,
  DialogBody,
  DialogFooter,
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
import { StatusBadge } from '../components/ux/StatusBadge'
import { KeyValueEditor } from '../components/ux/KeyValueEditor'
import { DangerConfirmDialog } from '../components/DangerConfirmDialog'
import { useRequestContext } from '../api/useRequestContext'
import type {
  RequestContext,
  FoundryConnectionRecordV2,
  FoundryConnectivityImportRecordV2,
  FoundryConnectionExportRunRecordV2,
} from '../api/bff'
import {
  listConnectionsV2,
  createConnectionV2,
  deleteConnectionV2,
  testConnectionV2,
  getConnectionConfigurationV2,
  listTableImportsV2,
  createTableImportV2,
  executeTableImportV2,
  deleteTableImportV2,
  listFileImportsV2,
  createFileImportV2,
  executeFileImportV2,
  deleteFileImportV2,
  listVirtualTablesV2,
  createVirtualTableV2,
  executeVirtualTableV2,
  deleteVirtualTableV2,
  listConnectionExportRunsV2,
  createConnectionExportRunV2,
  updateConnectionSecretsV2,
} from '../api/bff'

/* ── query keys ─────────────────────────────────────── */
const connKeys = {
  list: () => ['connections'] as const,
  config: (rid: string) => ['connections', 'config', rid] as const,
  tableImports: (rid: string) => ['connections', 'table-imports', rid] as const,
  fileImports: (rid: string) => ['connections', 'file-imports', rid] as const,
  virtualTables: (rid: string) => ['connections', 'virtual-tables', rid] as const,
  exportRuns: (rid: string) => ['connections', 'export-runs', rid] as const,
}

/* ── type helpers ───────────────────────────────────── */
const CONNECTION_TYPES = [
  { value: 'postgresql', label: 'PostgreSQL' },
  { value: 'mysql', label: 'MySQL' },
  { value: 'snowflake', label: 'Snowflake' },
  { value: 'bigquery', label: 'BigQuery' },
  { value: 's3', label: 'Amazon S3' },
  { value: 'gcs', label: 'Google Cloud Storage' },
  { value: 'jdbc', label: 'JDBC (Custom)' },
  { value: 'rest_api', label: 'REST API' },
] as const

function connStatusIntent(status?: string): Intent {
  if (!status) return Intent.NONE
  switch (status.toLowerCase()) {
    case 'connected':
    case 'healthy':
    case 'active':
      return Intent.SUCCESS
    case 'error':
    case 'failed':
      return Intent.DANGER
    case 'testing':
    case 'syncing':
      return Intent.WARNING
    default:
      return Intent.NONE
  }
}

/* ── page ────────────────────────────────────────────── */
function connStatusLevel(status?: string): 'success' | 'warning' | 'danger' | 'idle' {
  if (!status) return 'idle'
  switch (status.toLowerCase()) {
    case 'connected': case 'healthy': case 'active': return 'success'
    case 'error': case 'failed': return 'danger'
    case 'testing': case 'syncing': return 'warning'
    default: return 'idle'
  }
}

export const ConnectionsPage = () => {
  const ctx = useRequestContext()
  const queryClient = useQueryClient()

  const [selected, setSelected] = useState<FoundryConnectionRecordV2 | null>(null)
  const [createOpen, setCreateOpen] = useState(false)
  const [deleteOpen, setDeleteOpen] = useState(false)
  const [detailTab, setDetailTab] = useState<string>('table-imports')

  /* list connections */
  const listQ = useQuery({
    queryKey: connKeys.list(),
    queryFn: () => listConnectionsV2(ctx),
  })

  const connections: FoundryConnectionRecordV2[] = Array.isArray(listQ.data)
    ? listQ.data
    : (listQ.data as { data?: FoundryConnectionRecordV2[] })?.data ?? []

  /* connection config */
  const configQ = useQuery({
    queryKey: connKeys.config(selected?.rid ?? ''),
    queryFn: () => getConnectionConfigurationV2(ctx, selected!.rid),
    enabled: !!selected,
  })

  /* delete mutation */
  const deleteMut = useMutation({
    mutationFn: (rid: string) => deleteConnectionV2(ctx, rid),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: connKeys.list() })
      setSelected(null)
    },
  })

  /* test mutation */
  const testMut = useMutation({
    mutationFn: (rid: string) => testConnectionV2(ctx, rid),
  })

  return (
    <div>
      <PageHeader
        title="Connections"
        subtitle={`${connections.length} data source connections`}
        actions={
          <div className="form-row">
            <Button icon="plus" intent={Intent.PRIMARY} onClick={() => setCreateOpen(true)}>
              New Connection
            </Button>
            <Button icon="refresh" minimal loading={listQ.isFetching} onClick={() => listQ.refetch()}>
              Refresh
            </Button>
          </div>
        }
      />

      <div className="two-col-grid">
        {/* Left: connection list */}
        <div className="card-stack">
          <Card>
            <div className="card-title">Data Sources</div>
            {listQ.isLoading && <Spinner size={20} />}
            {listQ.error && <Callout intent={Intent.DANGER}>Failed to load connections.</Callout>}
            {connections.length === 0 && !listQ.isLoading && (
              <Callout intent={Intent.NONE}>
                No connections configured. Click &quot;New Connection&quot; to connect an external data source.
              </Callout>
            )}
            <HTMLTable striped interactive compact style={{ width: '100%' }}>
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Type</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {connections.map((conn) => {
                  const isActive = selected?.rid === conn.rid
                  const displayName = conn.displayName ?? conn.rid
                  const connType = (conn as Record<string, unknown>).connectionType as string | undefined
                  const status = (conn as Record<string, unknown>).status as string | undefined
                  return (
                    <tr
                      key={conn.rid}
                      onClick={() => { setSelected(conn); setDetailTab('table-imports') }}
                      style={{ fontWeight: isActive ? 600 : 400, cursor: 'pointer' }}
                    >
                      <td>{displayName}</td>
                      <td><Tag minimal>{connType ?? 'unknown'}</Tag></td>
                      <td><StatusBadge status={connStatusLevel(status)} label={status ?? '—'} /></td>
                    </tr>
                  )
                })}
              </tbody>
            </HTMLTable>
          </Card>
        </div>

        {/* Right: detail */}
        <div className="card-stack">
          {!selected ? (
            <Card>
              <Callout icon="info-sign">Select a connection from the list to view details.</Callout>
            </Card>
          ) : (
            <Card>
              <div className="card-title">{selected.displayName ?? selected.rid}</div>
              <div className="form-row" style={{ marginBottom: 12 }}>
                <Tag icon="data-connection">
                  {(selected as Record<string, unknown>).connectionType as string ?? 'connection'}
                </Tag>
                <StatusBadge
                  status={connStatusLevel((selected as Record<string, unknown>).status as string)}
                  label={(selected as Record<string, unknown>).status as string ?? 'unknown'}
                />
                <Button
                  small
                  icon="diagnosis"
                  loading={testMut.isPending}
                  onClick={() => testMut.mutate(selected.rid)}
                >
                  Test
                </Button>
                <Button
                  small
                  icon="trash"
                  intent={Intent.DANGER}
                  minimal
                  loading={deleteMut.isPending}
                  onClick={() => setDeleteOpen(true)}
                >
                  Delete
                </Button>
              </div>

              {testMut.data && (
                <Callout
                  intent={
                    (testMut.data as Record<string, unknown>).success === true
                      ? Intent.SUCCESS
                      : Intent.DANGER
                  }
                  icon="diagnosis"
                  style={{ marginBottom: 12 }}
                >
                  {(testMut.data as Record<string, unknown>).success === true
                    ? 'Connection test succeeded.'
                    : `Connection test failed: ${(testMut.data as Record<string, unknown>).message ?? 'Unknown error'}`}
                </Callout>
              )}

              <Tabs selectedTabId={detailTab} onChange={(id) => setDetailTab(id as string)}>
                <Tab
                  id="table-imports"
                  title="Table Imports"
                  panel={
                    <ImportPanel
                      connectionRid={selected.rid}
                      ctx={ctx}
                      kind="table"
                    />
                  }
                />
                <Tab
                  id="file-imports"
                  title="File Imports"
                  panel={
                    <ImportPanel
                      connectionRid={selected.rid}
                      ctx={ctx}
                      kind="file"
                    />
                  }
                />
                <Tab
                  id="virtual-tables"
                  title="Virtual Tables"
                  panel={
                    <ImportPanel
                      connectionRid={selected.rid}
                      ctx={ctx}
                      kind="virtual"
                    />
                  }
                />
                <Tab
                  id="export-runs"
                  title="Export Runs"
                  panel={
                    <ExportRunsPanel
                      connectionRid={selected.rid}
                      ctx={ctx}
                    />
                  }
                />
                <Tab
                  id="config"
                  title="Configuration"
                  panel={
                    <div>
                      {configQ.isLoading && <Spinner size={20} />}
                      {configQ.error && <Callout intent={Intent.DANGER}>Failed to load configuration.</Callout>}
                      {configQ.data && <JsonViewer value={configQ.data} />}
                    </div>
                  }
                />
                <Tab id="raw" title="Raw JSON" panel={<JsonViewer value={selected} />} />
              </Tabs>
            </Card>
          )}
        </div>
      </div>

      {/* Delete Confirmation */}
      <DangerConfirmDialog
        isOpen={deleteOpen}
        title="Delete Connection"
        description={`Are you sure you want to delete "${selected?.displayName ?? selected?.rid}"? This action cannot be undone.`}
        confirmLabel="Delete"
        cancelLabel="Cancel"
        reasonLabel="Reason for deletion"
        reasonPlaceholder="e.g. No longer needed, migrated to another source"
        typedLabel="Type connection name to confirm"
        typedPlaceholder={selected?.displayName ?? ''}
        confirmTextToType={selected?.displayName ?? ''}
        loading={deleteMut.isPending}
        onCancel={() => setDeleteOpen(false)}
        onConfirm={() => {
          if (selected) deleteMut.mutate(selected.rid)
          setDeleteOpen(false)
        }}
      />

      {/* Create Connection Dialog */}
      <CreateConnectionDialog
        isOpen={createOpen}
        onClose={() => setCreateOpen(false)}
        ctx={ctx}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: connKeys.list() })
          setCreateOpen(false)
        }}
      />
    </div>
  )
}

/* ── Import Panel (Table / File / Virtual Table) ─────── */
const ImportPanel = ({
  connectionRid,
  ctx,
  kind,
}: {
  connectionRid: string
  ctx: RequestContext
  kind: 'table' | 'file' | 'virtual'
}) => {
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)

  const queryKey =
    kind === 'table'
      ? connKeys.tableImports(connectionRid)
      : kind === 'file'
        ? connKeys.fileImports(connectionRid)
        : connKeys.virtualTables(connectionRid)

  const listFn =
    kind === 'table'
      ? () => listTableImportsV2(ctx, connectionRid)
      : kind === 'file'
        ? () => listFileImportsV2(ctx, connectionRid)
        : () => listVirtualTablesV2(ctx, connectionRid)

  const listQ = useQuery({ queryKey, queryFn: listFn })

  const items: FoundryConnectivityImportRecordV2[] = Array.isArray(listQ.data)
    ? listQ.data
    : (listQ.data as { data?: FoundryConnectivityImportRecordV2[] })?.data ?? []

  const executeFn =
    kind === 'table'
      ? (rid: string) => executeTableImportV2(ctx, connectionRid, rid)
      : kind === 'file'
        ? (rid: string) => executeFileImportV2(ctx, connectionRid, rid)
        : (rid: string) => executeVirtualTableV2(ctx, connectionRid, rid)

  const deleteFn =
    kind === 'table'
      ? (rid: string) => deleteTableImportV2(ctx, connectionRid, rid)
      : kind === 'file'
        ? (rid: string) => deleteFileImportV2(ctx, connectionRid, rid)
        : (rid: string) => deleteVirtualTableV2(ctx, connectionRid, rid)

  const execMut = useMutation({
    mutationFn: executeFn,
    onSuccess: () => queryClient.invalidateQueries({ queryKey }),
  })

  const delMut = useMutation({
    mutationFn: deleteFn,
    onSuccess: () => queryClient.invalidateQueries({ queryKey }),
  })

  const createFn =
    kind === 'table'
      ? (input: Record<string, unknown>) => createTableImportV2(ctx, connectionRid, input)
      : kind === 'file'
        ? (input: Record<string, unknown>) => createFileImportV2(ctx, connectionRid, input)
        : (input: Record<string, unknown>) => createVirtualTableV2(ctx, connectionRid, input)

  const createMut = useMutation({
    mutationFn: createFn,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey })
      setCreateOpen(false)
    },
  })

  const kindLabel = kind === 'table' ? 'Table Import' : kind === 'file' ? 'File Import' : 'Virtual Table'

  return (
    <div>
      <div className="form-row" style={{ marginBottom: 8 }}>
        <Button small icon="plus" intent={Intent.PRIMARY} onClick={() => setCreateOpen(true)}>
          New {kindLabel}
        </Button>
        <Button small icon="refresh" minimal loading={listQ.isFetching} onClick={() => listQ.refetch()}>
          Refresh
        </Button>
      </div>

      {listQ.isLoading && <Spinner size={20} />}
      {listQ.error && <Callout intent={Intent.DANGER}>Failed to load {kindLabel.toLowerCase()}s.</Callout>}

      {items.length === 0 && !listQ.isLoading && (
        <Callout intent={Intent.NONE}>No {kindLabel.toLowerCase()}s configured.</Callout>
      )}

      {items.length > 0 && (
        <HTMLTable striped compact style={{ width: '100%' }}>
          <thead>
            <tr>
              <th>Name</th>
              <th>Status</th>
              <th style={{ width: 140 }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => {
              const name = item.displayName ?? item.rid
              const status = (item as Record<string, unknown>).status as string | undefined
              return (
                <tr key={item.rid}>
                  <td>{name}</td>
                  <td>
                    <Tag intent={connStatusIntent(status)} minimal>
                      {status ?? '—'}
                    </Tag>
                  </td>
                  <td>
                    <div className="form-row">
                      <Button
                        small
                        icon="play"
                        intent={Intent.SUCCESS}
                        minimal
                        loading={execMut.isPending}
                        onClick={() => execMut.mutate(item.rid)}
                      >
                        Run
                      </Button>
                      <Button
                        small
                        icon="trash"
                        intent={Intent.DANGER}
                        minimal
                        loading={delMut.isPending}
                        onClick={() => {
                          if (confirm(`Delete ${kindLabel.toLowerCase()} "${name}"?`)) {
                            delMut.mutate(item.rid)
                          }
                        }}
                      >
                        Delete
                      </Button>
                    </div>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </HTMLTable>
      )}

      {execMut.error && (
        <Callout intent={Intent.DANGER} style={{ marginTop: 8 }}>Execution failed.</Callout>
      )}
      {execMut.data && (
        <Callout intent={Intent.SUCCESS} style={{ marginTop: 8 }}>
          Execution started. Task ID: {String(execMut.data)}
        </Callout>
      )}

      {/* Quick Create Dialog */}
      <CreateImportDialog
        isOpen={createOpen}
        onClose={() => setCreateOpen(false)}
        kindLabel={kindLabel}
        loading={createMut.isPending}
        error={!!createMut.error}
        onSubmit={(input) => createMut.mutate(input)}
      />
    </div>
  )
}

/* ── Export Runs Panel ────────────────────────────────── */
const ExportRunsPanel = ({
  connectionRid,
  ctx,
}: {
  connectionRid: string
  ctx: RequestContext
}) => {
  const queryClient = useQueryClient()

  const listQ = useQuery({
    queryKey: connKeys.exportRuns(connectionRid),
    queryFn: () => listConnectionExportRunsV2(ctx, connectionRid),
  })

  const runs: FoundryConnectionExportRunRecordV2[] = Array.isArray(listQ.data)
    ? listQ.data
    : (listQ.data as { data?: FoundryConnectionExportRunRecordV2[] })?.data ?? []

  const createMut = useMutation({
    mutationFn: () => createConnectionExportRunV2(ctx, connectionRid, {}),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: connKeys.exportRuns(connectionRid) }),
  })

  return (
    <div>
      <div className="form-row" style={{ marginBottom: 8 }}>
        <Button
          small
          icon="export"
          intent={Intent.PRIMARY}
          loading={createMut.isPending}
          onClick={() => createMut.mutate()}
        >
          New Export Run
        </Button>
        <Button small icon="refresh" minimal loading={listQ.isFetching} onClick={() => listQ.refetch()}>
          Refresh
        </Button>
      </div>

      {listQ.isLoading && <Spinner size={20} />}
      {listQ.error && <Callout intent={Intent.DANGER}>Failed to load export runs.</Callout>}

      {runs.length === 0 && !listQ.isLoading && (
        <Callout intent={Intent.NONE}>No export runs yet.</Callout>
      )}

      {runs.length > 0 && (
        <HTMLTable striped compact style={{ width: '100%' }}>
          <thead>
            <tr>
              <th>Run ID</th>
              <th>Status</th>
              <th>Created</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((run) => {
              const status = (run as Record<string, unknown>).status as string | undefined
              const created = (run as Record<string, unknown>).createdAt as string | undefined
              return (
                <tr key={run.rid}>
                  <td style={{ fontFamily: 'monospace', fontSize: 12 }}>{run.rid}</td>
                  <td>
                    <Tag intent={connStatusIntent(status)} minimal>
                      {status ?? '—'}
                    </Tag>
                  </td>
                  <td>{created ? new Date(created).toLocaleString() : '—'}</td>
                </tr>
              )
            })}
          </tbody>
        </HTMLTable>
      )}

      {createMut.error && (
        <Callout intent={Intent.DANGER} style={{ marginTop: 8 }}>Failed to create export run.</Callout>
      )}
    </div>
  )
}

/* ── Create Connection Dialog ────────────────────────── */
const CreateConnectionDialog = ({
  isOpen,
  onClose,
  ctx,
  onSuccess,
}: {
  isOpen: boolean
  onClose: () => void
  ctx: RequestContext
  onSuccess: () => void
}) => {
  const [displayName, setDisplayName] = useState('')
  const [connType, setConnType] = useState<string>('postgresql')
  const [host, setHost] = useState('')
  const [port, setPort] = useState('')
  const [database, setDatabase] = useState('')
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')

  const createMut = useMutation({
    mutationFn: async () => {
      if (!displayName) throw new Error('Name required')
      return createConnectionV2(ctx, {
        displayName,
        connectionType: connType,
        configuration: {
          host: host || undefined,
          port: port ? Number(port) : undefined,
          database: database || undefined,
          username: username || undefined,
          password: password || undefined,
        },
      })
    },
    onSuccess: () => {
      setDisplayName('')
      setHost('')
      setPort('')
      setDatabase('')
      setUsername('')
      setPassword('')
      onSuccess()
    },
  })

  return (
    <Dialog isOpen={isOpen} onClose={onClose} title="New Connection" icon="data-connection" style={{ width: 520 }}>
      <DialogBody>
        <FormGroup label="Connection Name" labelFor="conn-name">
          <InputGroup
            id="conn-name"
            value={displayName}
            onChange={(e) => setDisplayName(e.target.value)}
            placeholder="e.g. Production PostgreSQL"
          />
        </FormGroup>
        <FormGroup label="Connection Type" labelFor="conn-type">
          <HTMLSelect
            id="conn-type"
            value={connType}
            onChange={(e) => setConnType(e.target.value)}
            options={CONNECTION_TYPES.map((t) => ({ value: t.value, label: t.label }))}
            fill
          />
        </FormGroup>
        <FormGroup label="Host" labelFor="conn-host">
          <InputGroup
            id="conn-host"
            value={host}
            onChange={(e) => setHost(e.target.value)}
            placeholder="e.g. db.example.com"
          />
        </FormGroup>
        <div className="form-row">
          <FormGroup label="Port" labelFor="conn-port" style={{ flex: 1 }}>
            <InputGroup
              id="conn-port"
              value={port}
              onChange={(e) => setPort(e.target.value)}
              placeholder="e.g. 5432"
              type="number"
            />
          </FormGroup>
          <FormGroup label="Database" labelFor="conn-db" style={{ flex: 2 }}>
            <InputGroup
              id="conn-db"
              value={database}
              onChange={(e) => setDatabase(e.target.value)}
              placeholder="e.g. analytics"
            />
          </FormGroup>
        </div>
        <FormGroup label="Username" labelFor="conn-user">
          <InputGroup
            id="conn-user"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="e.g. readonly_user"
          />
        </FormGroup>
        <FormGroup label="Password" labelFor="conn-pass">
          <InputGroup
            id="conn-pass"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            type="password"
            placeholder="••••••••"
          />
        </FormGroup>
        {createMut.error && <Callout intent={Intent.DANGER} style={{ marginTop: 8 }}>Failed to create connection.</Callout>}
      </DialogBody>
      <DialogFooter
        actions={
          <>
            <Button onClick={onClose}>Cancel</Button>
            <Button
              intent={Intent.PRIMARY}
              icon="plus"
              loading={createMut.isPending}
              disabled={!displayName}
              onClick={() => createMut.mutate()}
            >
              Create Connection
            </Button>
          </>
        }
      />
    </Dialog>
  )
}

/* ── Create Import Dialog (shared for table/file/virtual) */
const CreateImportDialog = ({
  isOpen,
  onClose,
  kindLabel,
  loading,
  error,
  onSubmit,
}: {
  isOpen: boolean
  onClose: () => void
  kindLabel: string
  loading: boolean
  error: boolean
  onSubmit: (input: Record<string, unknown>) => void
}) => {
  const [name, setName] = useState('')
  const [configKv, setConfigKv] = useState<Record<string, string>>({})

  return (
    <Dialog isOpen={isOpen} onClose={onClose} title={`New ${kindLabel}`} icon="import">
      <DialogBody>
        <FormGroup label="Display Name" labelFor="imp-name">
          <InputGroup
            id="imp-name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder={`e.g. orders_${kindLabel.toLowerCase().replace(' ', '_')}`}
          />
        </FormGroup>
        <FormGroup label={
          <Tooltip content="Key-value pairs for import configuration (e.g. tableName, query, schema)" placement="top">
            <span className="tooltip-label">Configuration</span>
          </Tooltip>
        }>
          <KeyValueEditor
            value={configKv}
            onChange={setConfigKv}
            keyPlaceholder="e.g. tableName"
            valuePlaceholder="e.g. public.orders"
            addLabel="Add config field"
          />
        </FormGroup>
        {error && <Callout intent={Intent.DANGER} style={{ marginTop: 8 }}>Failed to create {kindLabel.toLowerCase()}.</Callout>}
      </DialogBody>
      <DialogFooter
        actions={
          <>
            <Button onClick={onClose}>Cancel</Button>
            <Button
              intent={Intent.PRIMARY}
              icon="plus"
              loading={loading}
              disabled={!name}
              onClick={() => onSubmit({ displayName: name, ...configKv })}
            >
              Create
            </Button>
          </>
        }
      />
    </Dialog>
  )
}
