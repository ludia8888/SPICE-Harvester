import { useState, useMemo, useRef, useCallback } from 'react'
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
  Icon,
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
import { useAppStore } from '../store/useAppStore'
import type {
  RequestContext,
  FoundryConnectionRecordV2,
  FoundryConnectivityImportRecordV2,
  FoundryConnectionExportRunRecordV2,
  DatasetRecord,
} from '../api/bff'
import {
  listConnectionsV2,
  listPipelineDatasets,
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
  deletePipelineDataset,
  uploadCsvDataset,
  uploadExcelDataset,
} from '../api/bff'

/* ── query keys ─────────────────────────────────────── */
const connKeys = {
  list: () => ['connections'] as const,
  datasets: (db: string, branch: string) => ['datasets', db, branch] as const,
  config: (rid: string) => ['connections', 'config', rid] as const,
  tableImports: (rid: string) => ['connections', 'table-imports', rid] as const,
  fileImports: (rid: string) => ['connections', 'file-imports', rid] as const,
  virtualTables: (rid: string) => ['connections', 'virtual-tables', rid] as const,
  exportRuns: (rid: string) => ['connections', 'export-runs', rid] as const,
}

/* ── unified data source item ──────────────────────── */
type DataSourceItem = {
  kind: 'connection' | 'dataset'
  id: string
  name: string
  type: string
  status?: string
  rowCount?: number
  updatedAt?: string
  raw: FoundryConnectionRecordV2 | DatasetRecord
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
  const storeProject = useAppStore((s) => s.context.project)
  const storeBranch = useAppStore((s) => s.context.branch)
  const adminMode = useAppStore((s) => s.adminMode)
  const devMode = useAppStore((s) => s.devMode)
  const skipConfirm = adminMode && devMode

  const [selected, setSelected] = useState<DataSourceItem | null>(null)
  const [newOpen, setNewOpen] = useState(false)
  const [newTab, setNewTab] = useState<'upload' | 'connector'>('upload')
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

  /* list datasets (only when project is selected) */
  const datasetsQ = useQuery({
    queryKey: connKeys.datasets(storeProject ?? '', storeBranch),
    queryFn: () => listPipelineDatasets(ctx, { db_name: storeProject!, branch: storeBranch }),
    enabled: !!storeProject,
  })

  const datasets: DatasetRecord[] = Array.isArray(datasetsQ.data) ? datasetsQ.data : []

  /* unified data source list */
  const dataSources: DataSourceItem[] = useMemo(() => {
    const connItems: DataSourceItem[] = connections.map((conn) => ({
      kind: 'connection' as const,
      id: conn.rid,
      name: conn.displayName ?? conn.rid,
      type: (conn as Record<string, unknown>).connectionType as string ?? 'unknown',
      status: (conn as Record<string, unknown>).status as string | undefined,
      updatedAt: conn.updatedTime ?? undefined,
      raw: conn,
    }))
    const dsItems: DataSourceItem[] = datasets.map((ds) => ({
      kind: 'dataset' as const,
      id: ds.dataset_id,
      name: ds.name,
      type: ds.source_type,
      rowCount: ds.row_count ?? undefined,
      updatedAt: ds.updated_at ?? undefined,
      raw: ds,
    }))
    return [...dsItems, ...connItems]
  }, [connections, datasets])

  /* connection config */
  const selectedConn = selected?.kind === 'connection' ? selected.raw as FoundryConnectionRecordV2 : null
  const configQ = useQuery({
    queryKey: connKeys.config(selectedConn?.rid ?? ''),
    queryFn: () => getConnectionConfigurationV2(ctx, selectedConn!.rid),
    enabled: !!selectedConn,
  })

  /* delete mutation */
  const deleteMut = useMutation({
    mutationFn: (rid: string) => deleteConnectionV2(ctx, rid),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: connKeys.list() })
      setSelected(null)
    },
  })

  /* dataset delete mutation */
  const deleteDatasetMut = useMutation({
    mutationFn: (datasetId: string) => deletePipelineDataset(ctx, datasetId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['datasets'] })
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
        title="Data Sources"
        subtitle={`${dataSources.length} items`}
        actions={
          <div className="form-row">
            <Button icon="plus" intent={Intent.PRIMARY} onClick={() => setNewOpen(true)}>
              New
            </Button>
            <Button icon="refresh" minimal loading={listQ.isFetching || datasetsQ.isFetching} onClick={() => { listQ.refetch(); datasetsQ.refetch() }}>
              Refresh
            </Button>
          </div>
        }
      />

      <div className="two-col-grid">
            {/* Left: unified list */}
            <div className="card-stack">
              <Card>
                <div className="card-title">Data Sources</div>
                {(listQ.isLoading || datasetsQ.isLoading) && <Spinner size={20} />}
                {listQ.error && <Callout intent={Intent.DANGER}>Failed to load connections.</Callout>}
                {datasetsQ.error && <Callout intent={Intent.DANGER}>Failed to load datasets.</Callout>}
                {dataSources.length === 0 && !listQ.isLoading && !datasetsQ.isLoading && (
                  <Callout intent={Intent.NONE}>
                    No data sources yet. Upload a file or create a connection.
                  </Callout>
                )}
                <HTMLTable striped interactive compact style={{ width: '100%' }}>
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Type</th>
                      <th>Status</th>
                      <th>Updated</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dataSources.map((item) => {
                      const isActive = selected?.id === item.id
                      return (
                        <tr
                          key={`${item.kind}-${item.id}`}
                          onClick={() => { setSelected(item); setDetailTab(item.kind === 'connection' ? 'table-imports' : 'schema') }}
                          style={{ fontWeight: isActive ? 600 : 400, cursor: 'pointer' }}
                        >
                          <td>{item.name}</td>
                          <td><Tag minimal>{item.type}</Tag></td>
                          <td>
                            {item.kind === 'connection'
                              ? <StatusBadge status={connStatusLevel(item.status)} label={item.status ?? '—'} />
                              : item.rowCount != null
                                ? <Tag minimal icon="th">{item.rowCount.toLocaleString()} rows</Tag>
                                : <Tag minimal>—</Tag>
                            }
                          </td>
                          <td className="muted small">
                            {item.updatedAt ? new Date(item.updatedAt).toLocaleDateString() : '—'}
                          </td>
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
                  <Callout icon="info-sign">Select an item from the list to view details.</Callout>
                </Card>
              ) : selected.kind === 'connection' ? (
                <ConnectionDetailPanel
                  conn={selected.raw as FoundryConnectionRecordV2}
                  ctx={ctx}
                  configQ={configQ}
                  testMut={testMut}
                  deleteMut={deleteMut}
                  detailTab={detailTab}
                  setDetailTab={setDetailTab}
                  onDelete={() => {
                    if (skipConfirm) { deleteMut.mutate((selected.raw as FoundryConnectionRecordV2).rid); return }
                    setDeleteOpen(true)
                  }}
                />
              ) : (
                <DatasetDetailPanel
                  dataset={selected.raw as DatasetRecord}
                  ctx={ctx}
                  detailTab={detailTab}
                  setDetailTab={setDetailTab}
                  deleteMut={deleteDatasetMut}
                  onDelete={() => {
                    if (skipConfirm) { deleteDatasetMut.mutate((selected.raw as DatasetRecord).dataset_id); return }
                    setDeleteOpen(true)
                  }}
                />
              )}
            </div>
          </div>

          {/* Delete Confirmation */}
          <DangerConfirmDialog
            isOpen={deleteOpen}
            title={selected?.kind === 'dataset' ? 'Delete Dataset' : 'Delete Connection'}
            description={`Are you sure you want to delete "${selected?.name}"? This action cannot be undone.`}
            confirmLabel="Delete"
            cancelLabel="Cancel"
            reasonLabel="Reason for deletion"
            reasonPlaceholder="e.g. No longer needed, migrated to another source"
            typedLabel={`Type ${selected?.kind === 'dataset' ? 'dataset' : 'connection'} name to confirm`}
            typedPlaceholder={selected?.name ?? ''}
            confirmTextToType={selected?.name ?? ''}
            loading={selected?.kind === 'dataset' ? deleteDatasetMut.isPending : deleteMut.isPending}
            onCancel={() => setDeleteOpen(false)}
            onConfirm={() => {
              if (selected?.kind === 'dataset') {
                deleteDatasetMut.mutate((selected.raw as DatasetRecord).dataset_id)
              } else if (selectedConn) {
                deleteMut.mutate(selectedConn.rid)
              }
              setDeleteOpen(false)
            }}
          />

          {/* New Data Source Dialog */}
          <Dialog
            isOpen={newOpen}
            onClose={() => setNewOpen(false)}
            title="New Data Source"
            icon="plus"
            style={{ width: 580 }}
          >
            <DialogBody>
              <Tabs selectedTabId={newTab} onChange={(id) => setNewTab(id as 'upload' | 'connector')}>
                <Tab id="upload" title="Upload" panel={
                  <UploadPanel ctx={ctx} onSuccess={() => {
                    if (storeProject) queryClient.invalidateQueries({ queryKey: connKeys.datasets(storeProject, storeBranch) })
                    setNewOpen(false)
                  }} />
                } />
                <Tab id="connector" title="Connector" panel={
                  <CreateConnectionForm ctx={ctx} onSuccess={() => {
                    queryClient.invalidateQueries({ queryKey: connKeys.list() })
                    setNewOpen(false)
                  }} />
                } />
              </Tabs>
            </DialogBody>
          </Dialog>
    </div>
  )
}

/* ── Connection Detail Panel ────────────────────────── */
const ConnectionDetailPanel = ({
  conn,
  ctx,
  configQ,
  testMut,
  deleteMut,
  detailTab,
  setDetailTab,
  onDelete,
}: {
  conn: FoundryConnectionRecordV2
  ctx: RequestContext
  configQ: ReturnType<typeof useQuery>
  testMut: ReturnType<typeof useMutation<unknown, Error, string>>
  deleteMut: ReturnType<typeof useMutation<unknown, Error, string>>
  detailTab: string
  setDetailTab: (tab: string) => void
  onDelete: () => void
}) => {
  const connType = (conn as Record<string, unknown>).connectionType as string | undefined
  const status = (conn as Record<string, unknown>).status as string | undefined

  return (
    <Card>
      <div className="card-title">{conn.displayName ?? conn.rid}</div>
      <div className="form-row" style={{ marginBottom: 12 }}>
        <Tag icon="data-connection">{connType ?? 'connection'}</Tag>
        <StatusBadge status={connStatusLevel(status)} label={status ?? 'unknown'} />
        <Button small icon="diagnosis" loading={testMut.isPending} onClick={() => testMut.mutate(conn.rid)}>
          Test
        </Button>
        <Button small icon="trash" intent={Intent.DANGER} minimal loading={deleteMut.isPending} onClick={onDelete}>
          Delete
        </Button>
      </div>

      {Boolean(testMut.data) && (
        <Callout
          intent={(testMut.data as Record<string, unknown>).success === true ? Intent.SUCCESS : Intent.DANGER}
          icon="diagnosis"
          style={{ marginBottom: 12 }}
        >
          {(testMut.data as Record<string, unknown>).success === true
            ? 'Connection test succeeded.'
            : `Connection test failed: ${(testMut.data as Record<string, unknown>).message ?? 'Unknown error'}`}
        </Callout>
      )}

      <Tabs selectedTabId={detailTab} onChange={(id) => setDetailTab(id as string)}>
        <Tab id="table-imports" title="Table Imports" panel={<ImportPanel connectionRid={conn.rid} ctx={ctx} kind="table" />} />
        <Tab id="file-imports" title="File Imports" panel={<ImportPanel connectionRid={conn.rid} ctx={ctx} kind="file" />} />
        <Tab id="virtual-tables" title="Virtual Tables" panel={<ImportPanel connectionRid={conn.rid} ctx={ctx} kind="virtual" />} />
        <Tab id="export-runs" title="Export Runs" panel={<ExportRunsPanel connectionRid={conn.rid} ctx={ctx} />} />
        <Tab id="config" title="Configuration" panel={
          <div>
            {configQ.isLoading && <Spinner size={20} />}
            {Boolean(configQ.error) && <Callout intent={Intent.DANGER}>Failed to load configuration.</Callout>}
            {configQ.data != null && <JsonViewer value={configQ.data} />}
          </div>
        } />
        <Tab id="raw" title="Raw JSON" panel={<JsonViewer value={conn} />} />
      </Tabs>
    </Card>
  )
}

/* ── Dataset Detail Panel ──────────────────────────── */
const DatasetDetailPanel = ({
  dataset,
  ctx,
  detailTab,
  setDetailTab,
  deleteMut,
  onDelete,
}: {
  dataset: DatasetRecord
  ctx: RequestContext
  detailTab: string
  setDetailTab: (tab: string) => void
  deleteMut: ReturnType<typeof useMutation<unknown, Error, string>>
  onDelete: () => void
}) => {
  const rawQ = useQuery({
    queryKey: ['datasets', 'raw-file', dataset.dataset_id],
    queryFn: async () => {
      const { getPipelineDatasetRawFile } = await import('../api/bff')
      return getPipelineDatasetRawFile(ctx, dataset.dataset_id)
    },
  })

  const schema = dataset.schema_json
  const schemaColumns = (schema?.columns ?? schema?.fields ?? schema?.properties ?? []) as Array<{
    name?: string; column_name?: string; data_type?: string; type?: string; nullable?: boolean; required?: boolean
  }>

  return (
    <Card>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div className="card-title">{dataset.name}</div>
        <Button small icon="trash" intent={Intent.DANGER} minimal loading={deleteMut.isPending} onClick={onDelete}>
          Delete
        </Button>
      </div>
      <div className="form-row" style={{ marginBottom: 12 }}>
        <Tag icon="database">{dataset.source_type}</Tag>
        <Tag icon="git-branch">{dataset.branch}</Tag>
        {dataset.row_count != null && <Tag icon="th">{dataset.row_count} rows</Tag>}
        {dataset.stage && <Tag>{dataset.stage}</Tag>}
      </div>

      <Tabs selectedTabId={detailTab} onChange={(id) => setDetailTab(id as string)}>
        <Tab id="schema" title="Schema" panel={
          <div>
            {Array.isArray(schemaColumns) && schemaColumns.length > 0 ? (
              <HTMLTable striped compact style={{ width: '100%' }}>
                <thead><tr><th>Column</th><th>Type</th><th>Nullable</th></tr></thead>
                <tbody>
                  {schemaColumns.map((col, i) => (
                    <tr key={i}>
                      <td style={{ fontFamily: 'monospace', fontSize: 12 }}>{col.name ?? col.column_name ?? `col_${i}`}</td>
                      <td><Tag minimal>{col.data_type ?? col.type ?? 'unknown'}</Tag></td>
                      <td>{col.nullable !== false && col.required !== true ? 'Yes' : 'No'}</td>
                    </tr>
                  ))}
                </tbody>
              </HTMLTable>
            ) : (
              <Callout>No schema available.</Callout>
            )}
          </div>
        } />
        <Tab id="preview" title="Preview" panel={
          <div>
            {rawQ.isLoading && <Spinner size={20} />}
            {rawQ.error && <Callout intent={Intent.DANGER}>Failed to load file.</Callout>}
            {rawQ.data && (() => {
              const raw = rawQ.data as { file?: { content: string; encoding?: string; content_type?: string; filename?: string; size_bytes?: number }; content?: string; encoding?: string; content_type?: string; filename?: string; size_bytes?: number }
              const data = raw.file ?? raw
              if (data.encoding === 'base64') return <Callout>Binary file ({data.content_type}). Download to view.</Callout>
              const lines = (data.content ?? '').split('\n').slice(0, 50)
              return (
                <div>
                  <div className="form-row" style={{ marginBottom: 8 }}>
                    {data.filename && <Tag>{data.filename}</Tag>}
                    {data.size_bytes != null && <Tag minimal>{(data.size_bytes / 1024).toFixed(1)} KB</Tag>}
                  </div>
                  <pre style={{ maxHeight: 400, overflow: 'auto', fontSize: 12, padding: 8, borderRadius: 4 }}>
                    {lines.join('\n')}
                  </pre>
                  {lines.length >= 50 && <Callout icon="info-sign">Showing first 50 lines.</Callout>}
                </div>
              )
            })()}
          </div>
        } />
        <Tab id="raw" title="Raw JSON" panel={<JsonViewer value={dataset} />} />
      </Tabs>
    </Card>
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
const CreateConnectionForm = ({
  ctx,
  onSuccess,
}: {
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
    <div>
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
      <div className="form-row" style={{ marginTop: 16, justifyContent: 'flex-end' }}>
        <Button
          intent={Intent.PRIMARY}
          icon="plus"
          loading={createMut.isPending}
          disabled={!displayName}
          onClick={() => createMut.mutate()}
        >
          Create Connection
        </Button>
      </div>
    </div>
  )
}

/* ── Upload Panel (drag-and-drop CSV / Excel, multi-file) ─ */
const UploadPanel = ({ ctx, onSuccess }: { ctx: RequestContext; onSuccess?: () => void }) => {
  const storeProject = useAppStore((s) => s.context.project)
  const storeBranch = useAppStore((s) => s.context.branch)

  const [project, setProject] = useState(storeProject ?? '')
  const [branch, setBranch] = useState(storeBranch ?? 'main')
  const [datasetName, setDatasetName] = useState('')
  const [files, setFiles] = useState<File[]>([])
  const [isDragging, setIsDragging] = useState(false)
  const [uploadProgress, setUploadProgress] = useState<{ done: number; failed: number; total: number } | null>(null)
  const fileRef = useRef<HTMLInputElement>(null)

  const isSingle = files.length <= 1

  const VALID_EXTENSIONS = ['csv', 'xlsx', 'xls']
  const isValidFile = (f: File) => {
    const ext = f.name.split('.').pop()?.toLowerCase()
    return ext != null && VALID_EXTENSIONS.includes(ext)
  }

  const handleFiles = useCallback((incoming: File[]) => {
    const valid = incoming.filter(isValidFile)
    if (valid.length === 0) return
    setFiles((prev) => {
      const existingNames = new Set(prev.map((f) => f.name))
      const deduped = valid.filter((f) => !existingNames.has(f.name))
      const merged = [...prev, ...deduped]
      // Auto-set dataset name only when exactly 1 file total
      if (merged.length === 1 && !datasetName) {
        setDatasetName(merged[0].name.replace(/\.(csv|xlsx|xls)$/i, ''))
      }
      // Clear custom name when switching to multi-file
      if (merged.length > 1) {
        setDatasetName('')
      }
      return merged
    })
  }, [datasetName])

  const removeFile = useCallback((index: number) => {
    setFiles((prev) => {
      const next = prev.filter((_, i) => i !== index)
      if (next.length === 1) {
        setDatasetName(next[0].name.replace(/\.(csv|xlsx|xls)$/i, ''))
      }
      if (next.length === 0) {
        setDatasetName('')
      }
      return next
    })
  }, [])

  const onDragEnter = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    setIsDragging(true)
  }, [])

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
  }, [])

  const onDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    setIsDragging(false)
  }, [])

  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    setIsDragging(false)
    const dropped = Array.from(e.dataTransfer.files)
    handleFiles(dropped)
  }, [handleFiles])

  const onFileChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const selected = Array.from(e.target.files ?? [])
    if (selected.length > 0) handleFiles(selected)
    // Reset input so the same files can be re-selected
    if (fileRef.current) fileRef.current.value = ''
  }, [handleFiles])

  const uploadMut = useMutation({
    mutationFn: async () => {
      if (files.length === 0 || !project) throw new Error('All fields required')
      // Single file: use custom dataset name
      if (files.length === 1) {
        const f = files[0]
        const name = datasetName || f.name.replace(/\.(csv|xlsx|xls)$/i, '')
        if (!name) throw new Error('Dataset name required')
        const isExcel = f.name.endsWith('.xlsx') || f.name.endsWith('.xls')
        if (isExcel) {
          return uploadExcelDataset(ctx, { db_name: project, name, branch, file: f })
        }
        return uploadCsvDataset(ctx, { db_name: project, name, branch, file: f })
      }
      // Multi-file: upload each sequentially
      let done = 0
      let failed = 0
      const total = files.length
      setUploadProgress({ done: 0, failed: 0, total })
      for (const f of files) {
        const name = f.name.replace(/\.(csv|xlsx|xls)$/i, '')
        try {
          const isExcel = f.name.endsWith('.xlsx') || f.name.endsWith('.xls')
          if (isExcel) {
            await uploadExcelDataset(ctx, { db_name: project, name, branch, file: f })
          } else {
            await uploadCsvDataset(ctx, { db_name: project, name, branch, file: f })
          }
          done++
        } catch {
          failed++
        }
        setUploadProgress({ done, failed, total })
      }
      if (failed > 0 && done === 0) throw new Error(`All ${failed} files failed to upload`)
      return { done, failed, total }
    },
    onSuccess: () => {
      setDatasetName('')
      setFiles([])
      setUploadProgress(null)
      if (fileRef.current) fileRef.current.value = ''
      onSuccess?.()
    },
    onError: () => {
      setUploadProgress(null)
    },
  })

  const formatSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  }

  return (
    <div>
        {!storeProject && !project && (
          <Callout intent={Intent.WARNING} icon="warning-sign" style={{ marginBottom: 16 }}>
            No project selected. Enter a target project name below or select a project from the sidebar first.
          </Callout>
        )}

        <FormGroup label="Target Project" labelFor="up-project">
          <InputGroup
            id="up-project"
            value={project}
            onChange={(e) => setProject(e.target.value)}
            placeholder="e.g. my_database"
          />
        </FormGroup>

        <div className="form-row">
          <FormGroup label="Branch" labelFor="up-branch" style={{ flex: 1 }}>
            <InputGroup
              id="up-branch"
              value={branch}
              onChange={(e) => setBranch(e.target.value)}
              placeholder="e.g. main"
            />
          </FormGroup>
          <FormGroup label="Dataset Name" labelFor="up-ds-name" style={{ flex: 2 }}>
            <InputGroup
              id="up-ds-name"
              value={datasetName}
              onChange={(e) => setDatasetName(e.target.value)}
              placeholder={isSingle ? 'e.g. orders_2026q1' : '(auto from filename)'}
              disabled={!isSingle}
            />
          </FormGroup>
        </div>

        {/* Drag-and-drop zone */}
        <div
          className={`upload-dropzone${isDragging ? ' is-dragging' : ''}`}
          onDragEnter={onDragEnter}
          onDragOver={onDragOver}
          onDragLeave={onDragLeave}
          onDrop={onDrop}
          onClick={() => fileRef.current?.click()}
        >
          <Icon icon="document" size={32} className="upload-dropzone-icon" />
          <div className="upload-dropzone-text">
            Drag &amp; drop CSV or Excel files here
          </div>
          <button type="button" className="upload-dropzone-link">
            or browse files
          </button>
          <div className="upload-dropzone-text" style={{ fontSize: 11 }}>
            Supported: .csv, .xlsx, .xls &mdash; multiple files OK
          </div>
        </div>
        <input
          ref={fileRef}
          type="file"
          accept=".csv,.xlsx,.xls"
          multiple
          className="upload-file-input"
          onChange={onFileChange}
        />

        {/* File list */}
        {files.length > 0 && (
          <div className="upload-file-list" style={{ marginTop: 12 }}>
            {files.map((f, idx) => (
              <div className="upload-file-row" key={`${f.name}-${idx}`}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <Icon icon="document" />
                  <div>
                    <div style={{ fontWeight: 500 }}>{f.name}</div>
                    <div style={{ fontSize: 11, color: 'var(--foundry-text-muted)' }}>
                      {formatSize(f.size)}
                    </div>
                  </div>
                </div>
                <Button
                  small
                  minimal
                  icon="cross"
                  disabled={uploadMut.isPending}
                  onClick={() => removeFile(idx)}
                />
              </div>
            ))}
          </div>
        )}

        {/* Upload progress */}
        {uploadProgress && (
          <div style={{ marginTop: 8 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
              <Spinner size={14} />
              <span style={{ fontSize: 12 }}>
                Uploading {uploadProgress.done + uploadProgress.failed}/{uploadProgress.total}
                {uploadProgress.failed > 0 && <span style={{ color: 'var(--red5)' }}> ({uploadProgress.failed} failed)</span>}
              </span>
            </div>
            <div style={{ height: 3, background: 'var(--pt-divider-black, #ddd)', borderRadius: 2, overflow: 'hidden' }}>
              <div style={{
                height: '100%',
                width: `${((uploadProgress.done + uploadProgress.failed) / uploadProgress.total) * 100}%`,
                background: uploadProgress.failed > 0 ? 'var(--orange5, #d9822b)' : 'var(--blue5, #2b95d6)',
                transition: 'width 0.3s ease',
              }} />
            </div>
          </div>
        )}

        {/* Upload button */}
        <div className="form-row" style={{ marginTop: 16, justifyContent: 'flex-end' }}>
          <Button
            intent={Intent.PRIMARY}
            icon="upload"
            loading={uploadMut.isPending}
            disabled={!project || files.length === 0 || (isSingle && !datasetName)}
            onClick={() => uploadMut.mutate()}
          >
            {files.length <= 1 ? 'Upload Dataset' : `Upload ${files.length} Datasets`}
          </Button>
        </div>

        {/* Status messages */}
        {uploadMut.error && (
          <Callout intent={Intent.DANGER} icon="error" style={{ marginTop: 12 }}>
            Upload failed: {(uploadMut.error as Error).message ?? 'Unknown error'}
          </Callout>
        )}
        {uploadMut.isSuccess && (
          <Callout intent={Intent.SUCCESS} icon="tick-circle" style={{ marginTop: 12 }}>
            {files.length <= 1
              ? <>Successfully uploaded &quot;{datasetName || 'dataset'}&quot; to project &quot;{project}&quot;.</>
              : <>Successfully uploaded {(uploadMut.data as { done: number; failed: number; total: number })?.done ?? files.length} dataset(s) to project &quot;{project}&quot;.
                  {(uploadMut.data as { failed?: number })?.failed ? ` (${(uploadMut.data as { failed: number }).failed} failed)` : ''}</>
            }
          </Callout>
        )}
    </div>
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
