import { useState, useRef } from 'react'
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
import { useRequestContext } from '../api/useRequestContext'
import { useAppStore } from '../store/useAppStore'
import { StatusBadge } from '../components/ux/StatusBadge'
import type { RequestContext, DatasetRecord, DatasetRawFile, DatasetIngestRequestRecord } from '../api/bff'
import {
  listPipelineDatasets,
  createPipelineDataset,
  uploadCsvDataset,
  uploadExcelDataset,
  getPipelineDatasetRawFile,
  getPipelineDatasetIngestRequest,
  approvePipelineDatasetSchema,
} from '../api/bff'

/* ── query keys ─────────────────────────────────────────────── */
const dsKeys = {
  list: (db: string, branch: string) => ['datasets', db, branch] as const,
  rawFile: (id: string) => ['datasets', 'raw-file', id] as const,
  ingestReq: (id: string) => ['datasets', 'ingest-req', id] as const,
}

/* ── page ────────────────────────────────────────────────────── */
export const DatasetsPage = ({ dbName }: { dbName: string }) => {
  const ctx = useRequestContext()
  const branch = useAppStore((s) => s.context.branch)
  const queryClient = useQueryClient()

  /* state */
  const [selected, setSelected] = useState<DatasetRecord | null>(null)
  const [uploadOpen, setUploadOpen] = useState(false)
  const [detailTab, setDetailTab] = useState<string>('schema')

  /* list datasets */
  const listQ = useQuery({
    queryKey: dsKeys.list(dbName, branch),
    queryFn: () => listPipelineDatasets(ctx, { db_name: dbName, branch }),
  })

  const datasets: DatasetRecord[] = Array.isArray(listQ.data)
    ? listQ.data
    : (listQ.data as { data?: DatasetRecord[] })?.data ?? []

  /* raw file preview */
  const rawQ = useQuery({
    queryKey: dsKeys.rawFile(selected?.dataset_id ?? ''),
    queryFn: () => getPipelineDatasetRawFile(ctx, selected!.dataset_id),
    enabled: !!selected,
  })

  /* ingest request */
  const ingestQ = useQuery({
    queryKey: dsKeys.ingestReq(selected?.dataset_id ?? ''),
    queryFn: () => getPipelineDatasetIngestRequest(ctx, selected!.dataset_id),
    enabled: !!selected,
  })

  return (
    <div>
      <PageHeader
        title="Datasets"
        subtitle={`${datasets.length} datasets in ${dbName}`}
        actions={
          <div className="form-row">
            <Button icon="upload" intent={Intent.PRIMARY} onClick={() => setUploadOpen(true)}>
              Upload CSV / Excel
            </Button>
            <Button icon="refresh" minimal loading={listQ.isFetching} onClick={() => listQ.refetch()}>
              Refresh
            </Button>
          </div>
        }
      />

      <div className="two-col-grid">
        {/* Left: dataset list */}
        <div className="card-stack">
          <Card>
            <div className="card-title">
              <Tooltip content="Data tables ingested from CSV, Excel, or API sources" placement="top">
                <span className="tooltip-label">Datasets</span>
              </Tooltip>
            </div>
            {listQ.isLoading && <Spinner size={20} />}
            {listQ.error && <Callout intent={Intent.DANGER}>Failed to load datasets.</Callout>}
            {datasets.length === 0 && !listQ.isLoading && (
              <Callout intent={Intent.NONE}>No datasets yet. Upload a CSV or Excel file to get started.</Callout>
            )}
            <HTMLTable striped interactive compact style={{ width: '100%' }}>
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Source</th>
                  <th>Rows</th>
                  <th>Updated</th>
                </tr>
              </thead>
              <tbody>
                {datasets.map((ds) => {
                  const isActive = selected?.dataset_id === ds.dataset_id
                  return (
                    <tr
                      key={ds.dataset_id}
                      onClick={() => setSelected(ds)}
                      style={{ fontWeight: isActive ? 600 : 400, cursor: 'pointer' }}
                    >
                      <td>{ds.name}</td>
                      <td><Tag minimal>{ds.source_type}</Tag></td>
                      <td>{ds.row_count ?? '—'}</td>
                      <td>{ds.updated_at ? new Date(ds.updated_at).toLocaleDateString() : '—'}</td>
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
              <Callout icon="info-sign">Select a dataset from the list to view details.</Callout>
            </Card>
          ) : (
            <Card>
              <div className="card-title">{selected.name}</div>
              <div className="form-row" style={{ marginBottom: 12 }}>
                <Tag icon="database">{selected.source_type}</Tag>
                <Tag icon="git-branch">{selected.branch}</Tag>
                {selected.row_count != null && <Tag icon="th">{selected.row_count} rows</Tag>}
                {selected.stage && <Tag>{selected.stage}</Tag>}
              </div>

              <Tabs selectedTabId={detailTab} onChange={(id) => setDetailTab(id as string)}>
                <Tab id="schema" title={<Tooltip content="Column definitions and data types for this dataset"><span>Schema</span></Tooltip>} panel={
                  <div>
                    {selected.schema_json ? (
                      <SchemaTable schema={selected.schema_json} />
                    ) : (
                      <Callout>No schema available. Upload data to trigger type inference.</Callout>
                    )}
                  </div>
                } />
                <Tab id="preview" title="Preview" panel={
                  <div>
                    {rawQ.isLoading && <Spinner size={20} />}
                    {rawQ.error && <Callout intent={Intent.DANGER}>Failed to load file.</Callout>}
                    {rawQ.data && <DataPreview data={rawQ.data as DatasetRawFile} />}
                  </div>
                } />
                <Tab id="ingest" title="Ingest" panel={
                  <div>
                    {ingestQ.isLoading && <Spinner size={20} />}
                    {ingestQ.data && (
                      <IngestRequestPanel
                        ingestReq={ingestQ.data as DatasetIngestRequestRecord}
                        ctx={ctx}
                        onApproved={() => {
                          queryClient.invalidateQueries({ queryKey: dsKeys.list(dbName, branch) })
                          ingestQ.refetch()
                        }}
                      />
                    )}
                    {!ingestQ.data && !ingestQ.isLoading && (
                      <Callout>No ingest request for this dataset.</Callout>
                    )}
                  </div>
                } />
                <Tab id="raw" title="Raw JSON" panel={<JsonViewer value={selected} />} />
              </Tabs>
            </Card>
          )}
        </div>
      </div>

      {/* Upload Dialog */}
      <UploadDialog
        isOpen={uploadOpen}
        onClose={() => setUploadOpen(false)}
        dbName={dbName}
        branch={branch}
        ctx={ctx}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: dsKeys.list(dbName, branch) })
          setUploadOpen(false)
        }}
      />
    </div>
  )
}

/* ── Schema Table ────────────────────────────────────────────── */
const typeColorMap: Record<string, Intent> = {
  string: Intent.NONE, varchar: Intent.NONE, text: Intent.NONE, char: Intent.NONE,
  integer: Intent.PRIMARY, int: Intent.PRIMARY, long: Intent.PRIMARY, bigint: Intent.PRIMARY, smallint: Intent.PRIMARY,
  double: Intent.WARNING, float: Intent.WARNING, decimal: Intent.WARNING, numeric: Intent.WARNING,
  boolean: Intent.SUCCESS, bool: Intent.SUCCESS,
  date: Intent.DANGER, timestamp: Intent.DANGER, datetime: Intent.DANGER, time: Intent.DANGER,
}

const typeIconMap: Record<string, string> = {
  string: 'font', varchar: 'font', text: 'font', char: 'font',
  integer: 'numerical', int: 'numerical', long: 'numerical', bigint: 'numerical',
  double: 'floating-point', float: 'floating-point', decimal: 'floating-point',
  boolean: 'segmented-control', bool: 'segmented-control',
  date: 'calendar', timestamp: 'calendar', datetime: 'calendar',
  array: 'array', json: 'code', object: 'code-block',
}

const SchemaTable = ({ schema }: { schema: Record<string, unknown> }) => {
  const columns = (schema.columns ?? schema.fields ?? schema.properties ?? []) as Array<{
    name?: string
    column_name?: string
    data_type?: string
    type?: string
    nullable?: boolean
    required?: boolean
  }>

  if (!Array.isArray(columns) || columns.length === 0) {
    return <JsonViewer value={schema} />
  }

  return (
    <HTMLTable striped compact style={{ width: '100%' }}>
      <thead>
        <tr>
          <th>Column</th>
          <th>Type</th>
          <th>Nullable</th>
        </tr>
      </thead>
      <tbody>
        {columns.map((col, i) => {
          const rawType = (col.data_type ?? col.type ?? 'unknown').toLowerCase()
          const intent = typeColorMap[rawType] ?? Intent.NONE
          const icon = typeIconMap[rawType] as any
          return (
            <tr key={i}>
              <td style={{ fontFamily: 'monospace', fontSize: 12 }}>{col.name ?? col.column_name ?? `col_${i}`}</td>
              <td>
                <Tag minimal intent={intent} icon={icon}>
                  {col.data_type ?? col.type ?? 'unknown'}
                </Tag>
              </td>
              <td>{col.nullable !== false && col.required !== true ? 'Yes' : 'No'}</td>
            </tr>
          )
        })}
      </tbody>
    </HTMLTable>
  )
}

/* ── Data Preview ────────────────────────────────────────────── */
const DataPreview = ({ data }: { data: DatasetRawFile }) => {
  if (data.encoding === 'base64') {
    return <Callout>Binary file ({data.content_type}). Download to view.</Callout>
  }
  const lines = data.content.split('\n').slice(0, 50)
  return (
    <div>
      <div className="form-row" style={{ marginBottom: 8 }}>
        <Tag>{data.filename}</Tag>
        {data.size_bytes != null && <Tag minimal>{(data.size_bytes / 1024).toFixed(1)} KB</Tag>}
      </div>
      <pre style={{ maxHeight: 400, overflow: 'auto', fontSize: 12, background: '#f5f5f5', padding: 8, borderRadius: 4 }}>
        {lines.join('\n')}
      </pre>
      {lines.length >= 50 && <Callout icon="info-sign" intent={Intent.NONE}>Showing first 50 lines.</Callout>}
    </div>
  )
}

/* ── Ingest Request Panel ────────────────────────────────────── */
const IngestRequestPanel = ({
  ingestReq,
  ctx,
  onApproved,
}: {
  ingestReq: DatasetIngestRequestRecord
  ctx: RequestContext
  onApproved: () => void
}) => {
  const approveMut = useMutation({
    mutationFn: () => approvePipelineDatasetSchema(ctx, ingestReq.ingest_request_id, ingestReq.schema_json ?? {}),
    onSuccess: onApproved,
  })

  return (
    <div>
      <div className="form-row" style={{ marginBottom: 8 }}>
        <StatusBadge
          status={ingestReq.schema_status === 'approved' ? 'success' : 'warning'}
          label={ingestReq.schema_status ?? 'pending'}
        />
        {ingestReq.row_count != null && <Tag minimal>{ingestReq.row_count} rows detected</Tag>}
      </div>
      {ingestReq.schema_json && (
        <>
          <div className="card-title" style={{ fontSize: 13, marginBottom: 4 }}>Inferred Schema</div>
          <SchemaTable schema={ingestReq.schema_json} />
        </>
      )}
      {ingestReq.schema_status !== 'approved' && (
        <Button
          intent={Intent.SUCCESS}
          icon="tick"
          style={{ marginTop: 12 }}
          loading={approveMut.isPending}
          onClick={() => approveMut.mutate()}
        >
          Approve Schema
        </Button>
      )}
      {approveMut.error && <Callout intent={Intent.DANGER} style={{ marginTop: 8 }}>Approval failed.</Callout>}
      <div style={{ marginTop: 12 }}>
        <div className="card-title" style={{ fontSize: 13, marginBottom: 4 }}>Sample Data</div>
        <JsonViewer value={ingestReq.sample_json} />
      </div>
    </div>
  )
}

/* ── Upload Dialog ───────────────────────────────────────────── */
const UploadDialog = ({
  isOpen,
  onClose,
  dbName,
  branch,
  ctx,
  onSuccess,
}: {
  isOpen: boolean
  onClose: () => void
  dbName: string
  branch: string
  ctx: RequestContext
  onSuccess: () => void
}) => {
  const [name, setName] = useState('')
  const [file, setFile] = useState<File | null>(null)
  const fileRef = useRef<HTMLInputElement>(null)

  const uploadMut = useMutation({
    mutationFn: async () => {
      if (!file || !name) throw new Error('Name and file required')
      const isExcel = file.name.endsWith('.xlsx') || file.name.endsWith('.xls')
      if (isExcel) {
        return uploadExcelDataset(ctx, { db_name: dbName, name, branch, file })
      }
      return uploadCsvDataset(ctx, { db_name: dbName, name, branch, file })
    },
    onSuccess: () => {
      setName('')
      setFile(null)
      onSuccess()
    },
  })

  return (
    <Dialog isOpen={isOpen} onClose={onClose} title="Upload Dataset" icon="upload">
      <DialogBody>
        <FormGroup label="Dataset name" labelFor="ds-name">
          <InputGroup id="ds-name" value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. orders_2026q1" />
        </FormGroup>
        <FormGroup label="File (CSV or Excel)" labelFor="ds-file">
          <input
            ref={fileRef}
            id="ds-file"
            type="file"
            accept=".csv,.xlsx,.xls"
            onChange={(e) => setFile(e.target.files?.[0] ?? null)}
          />
        </FormGroup>
        {file && (
          <Callout intent={Intent.NONE} icon="document">
            {file.name} ({(file.size / 1024).toFixed(1)} KB)
          </Callout>
        )}
        {uploadMut.error && <Callout intent={Intent.DANGER} style={{ marginTop: 8 }}>Upload failed.</Callout>}
      </DialogBody>
      <DialogFooter
        actions={
          <>
            <Button onClick={onClose}>Cancel</Button>
            <Button
              intent={Intent.PRIMARY}
              icon="upload"
              loading={uploadMut.isPending}
              disabled={!name || !file}
              onClick={() => uploadMut.mutate()}
            >
              Upload
            </Button>
          </>
        }
      />
    </Dialog>
  )
}
