export type ApiResponse<T> = {
  status: string
  message?: string
  data?: T
  errors?: string[]
}

export type DatabaseRecord = {
  name?: string
  db_name?: string
  id?: string
  label?: string
  display_name?: string
  description?: string
  created_at?: string
  updated_at?: string
  dataset_count?: number
  datasetCount?: number
  datasets?: unknown[]
  owner_id?: string
  owner_name?: string
  role?: 'Owner' | 'Editor' | 'Viewer' | string
  shared?: boolean
  shared_with?: string[]
  sharedWith?: string[]
}

export type DatasetRecord = {
  dataset_id: string
  db_name: string
  name: string
  description?: string
  source_type: string
  source_ref?: string
  branch: string
  schema_json?: Record<string, unknown>
  created_at?: string
  updated_at?: string
  latest_commit_id?: string
  artifact_key?: string
  row_count?: number
  sample_json?: Record<string, unknown>
  version_created_at?: string
}

export type DatasetRawFile = {
  dataset_id: string
  filename: string
  content_type?: string
  size_bytes?: number
  artifact_key?: string
  s3_uri?: string
  encoding: 'utf-8' | 'base64'
  content: string
}

export type DatasetIngestRequestRecord = {
  ingest_request_id: string
  dataset_id: string
  db_name: string
  branch?: string
  status?: string
  schema_json?: Record<string, unknown>
  schema_status?: string
  schema_approved_at?: string
  schema_approved_by?: string
  sample_json?: Record<string, unknown>
  row_count?: number
  source_metadata?: Record<string, unknown>
  created_at?: string
  updated_at?: string
  published_at?: string | null
}

export type PipelineRecord = {
  pipeline_id: string
  db_name: string
  name: string
  description?: string
  pipeline_type: string
  location?: string
  status?: string
  branch?: string
  updated_at?: string
}

export type PipelineDefinition = {
  nodes?: Array<Record<string, unknown>>
  edges?: Array<Record<string, unknown>>
  parameters?: unknown[]
  settings?: Record<string, unknown>
  dependencies?: unknown[]
}

export type PipelineDetailRecord = PipelineRecord & {
  definition_json?: PipelineDefinition
  version_id?: string | null
  version?: string | null
  commit_id?: string | null
  dependencies?: unknown[]
}

export type PipelineArtifactRecord = {
  artifact_id: string
  pipeline_id: string
  job_id: string
  run_id?: string | null
  mode: string
  status: string
  definition_hash?: string | null
  definition_commit_id?: string | null
  pipeline_spec_hash?: string | null
  pipeline_spec_commit_id?: string | null
  inputs?: Record<string, unknown>
  lakefs_repository?: string | null
  lakefs_branch?: string | null
  lakefs_commit_id?: string | null
  outputs?: Array<Record<string, unknown>>
  declared_outputs?: Array<Record<string, unknown>>
  sampling_strategy?: Record<string, unknown>
  error?: Record<string, unknown>
  created_at?: string
  updated_at?: string
}

export type PipelineReadinessInput = {
  node_id?: string | null
  dataset_id?: string | null
  dataset_name?: string | null
  requested_branch?: string | null
  resolved_branch?: string | null
  status?: string | null
  detail?: string | null
  used_fallback?: boolean
  latest_commit_id?: string | null
  latest_version?: string | null
  artifact_key?: string | null
}

export type PipelineReadiness = {
  pipeline_id?: string
  branch?: string
  version_id?: string | null
  commit_id?: string | null
  version?: string | null
  status?: string | null
  inputs?: PipelineReadinessInput[]
  fallback_branches?: string[]
}

export type UploadMode = 'structured' | 'media' | 'unstructured' | 'raw'

export type GoogleSheetRegisteredSheet = {
  sheet_id: string
  sheet_url: string
  sheet_title?: string | null
  worksheet_name: string
  polling_interval: number
  database_name?: string | null
  branch?: string | null
  class_label?: string | null
  auto_import?: boolean
  max_import_rows?: number | null
  last_polled?: string | null
  last_hash?: string | null
  is_active?: boolean
  registered_at?: string | null
}

export type GoogleSheetPreview = {
  sheet_id?: string
  sheet_title?: string
  worksheet_title?: string
  worksheet_name?: string
  columns?: string[]
  sample_rows?: Array<Record<string, unknown>>
  total_rows?: number
  total_columns?: number
  [key: string]: unknown
}

export type AgentClarificationQuestion = {
  id: string
  question: string
  required?: boolean
  type?: string
  options?: string[] | null
  default?: unknown
}

export type AgentPlanStep = {
  step_id: string
  tool_id: string
  method?: string | null
  path_params?: Record<string, unknown>
  query?: Record<string, unknown>
  body?: Record<string, unknown> | null
  requires_approval?: boolean
  idempotency_key?: string | null
  data_scope?: Record<string, unknown>
  description?: string | null
  expected_output?: string | null
}

export type AgentPlan = {
  plan_id?: string | null
  goal?: string
  created_at?: string | null
  created_by?: string | null
  risk_level?: string
  requires_approval?: boolean
  data_scope?: Record<string, unknown>
  steps?: AgentPlanStep[]
  policy_notes?: string[]
  warnings?: string[]
}

type DatabaseListPayload = {
  databases?: Array<DatabaseRecord | string>
  count?: number
}

type DatasetListPayload = {
  datasets?: DatasetRecord[]
  count?: number
}

type PipelineListPayload = {
  pipelines?: PipelineRecord[]
  count?: number
}

type PipelineCreatePayload = {
  pipeline?: PipelineRecord
}

type PipelineGetPayload = {
  pipeline?: PipelineDetailRecord
}

type PipelineArtifactListPayload = {
  artifacts?: PipelineArtifactRecord[]
  count?: number
}

type PipelineReadinessPayload = {
  pipeline_id?: string
  branch?: string
  version_id?: string | null
  commit_id?: string | null
  version?: string | null
  status?: string | null
  inputs?: PipelineReadinessInput[]
  fallback_branches?: string[]
}

type DatasetUploadPayload = {
  dataset?: DatasetRecord
  ingest_request_id?: string
  schema_status?: string
  schema_suggestion?: Record<string, unknown>
}

export type DatasetUploadResult = {
  dataset: DatasetRecord
  ingest_request_id?: string
  schema_status?: string
  schema_suggestion?: Record<string, unknown>
}

type DatasetSchemaApprovalPayload = {
  dataset?: DatasetRecord
  ingest_request?: DatasetIngestRequestRecord
}

type GoogleSheetsRegisteredListPayload = {
  sheets?: GoogleSheetRegisteredSheet[]
  count?: number
  database_filter?: string | null
}

const API_BASE = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? ''
const API_TOKEN =
  (import.meta.env.VITE_BFF_TOKEN as string | undefined) ??
  (import.meta.env.VITE_ADMIN_TOKEN as string | undefined) ??
  ''
const API_USER_ID = (import.meta.env.VITE_USER_ID as string | undefined) ?? ''
const API_USER_NAME = (import.meta.env.VITE_USER_NAME as string | undefined) ?? ''

const buildUrl = (path: string) => {
  if (!API_BASE) {
    return path
  }
  const normalizedBase = API_BASE.replace(/\/$/, '')
  const normalizedPath = path.startsWith('/') ? path : `/${path}`
  if (normalizedBase.endsWith('/api/v1') && normalizedPath.startsWith('/api/v1')) {
    return `${normalizedBase}${normalizedPath.replace(/^\/api\/v1/, '')}`
  }
  return `${normalizedBase}${normalizedPath}`
}

const parseApiResponse = <T>(payload: ApiResponse<T> | T | null, fallbackMessage: string) => {
  if (!payload || typeof payload !== 'object') {
    throw new Error(fallbackMessage)
  }
  if ('status' in payload) {
    if (payload.status && payload.status !== 'error') {
      return (payload.data ?? payload) as T
    }
    throw new Error(payload.message || fallbackMessage)
  }
  return payload as T
}

const requestApi = async <T>(
  path: string,
  options?: RequestInit,
  fallbackMessage = 'Request failed',
): Promise<T> => {
  const headers = new Headers()
  headers.set('Accept', 'application/json')
  if (API_TOKEN) {
    headers.set('Authorization', `Bearer ${API_TOKEN}`)
  }
  if (API_USER_ID) {
    headers.set('X-User-ID', API_USER_ID)
  }
  if (API_USER_NAME) {
    headers.set('X-User-Name', API_USER_NAME)
  }
  new Headers(options?.headers).forEach((value, key) => headers.set(key, value))
  const response = await fetch(buildUrl(path), {
    ...options,
    headers,
  })

  const payload = await response.json().catch(() => null)
  if (!response.ok) {
    const message = (payload && (payload.message || payload.detail)) || response.statusText || fallbackMessage
    throw new Error(typeof message === 'string' ? message : fallbackMessage)
  }

  return parseApiResponse(payload as ApiResponse<T>, fallbackMessage)
}

const createIdempotencyKey = () => {
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
    return crypto.randomUUID()
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`
}

const getDatasetBaseName = (fileName: string) => fileName.replace(/\.[^/.]+$/, '')

const inferUploadEndpoint = (mode: UploadMode, fileName: string) => {
  const lower = fileName.toLowerCase()
  if (mode === 'structured') {
    if (lower.endsWith('.csv')) {
      return '/api/v1/pipelines/datasets/csv-upload'
    }
    if (lower.endsWith('.xlsx') || lower.endsWith('.xlsm') || lower.endsWith('.xls')) {
      return '/api/v1/pipelines/datasets/excel-upload'
    }
    return null
  }
  return '/api/v1/pipelines/datasets/media-upload'
}

export const listDatabases = async () => {
  const data = await requestApi<DatabaseListPayload>('/api/v1/databases', undefined, 'Failed to load databases')
  return data.databases ?? []
}

export const createDatabase = async (name: string, description?: string) => {
  const data = await requestApi<{ name?: string }>('/api/v1/databases',
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ name, description }),
    },
    'Failed to create database',
  )
  return data
}

export const deleteDatabase = async (name: string) => {
  const encoded = encodeURIComponent(name)
  const data = await requestApi(`/api/v1/databases/${encoded}`, { method: 'DELETE' }, 'Failed to delete database')
  return data
}

export const listDatasets = async (dbName: string) => {
  const data = await requestApi<DatasetListPayload>(
    `/api/v1/pipelines/datasets?db_name=${encodeURIComponent(dbName)}`,
    {
      headers: {
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    'Failed to load datasets',
  )
  return data.datasets ?? []
}

export const getDatasetRawFile = async (params: {
  dbName: string
  datasetId: string
  fileName?: string
  fileIndex?: number
}) => {
  const query = new URLSearchParams()
  if (params.fileName) {
    query.set('file_name', params.fileName)
  }
  if (params.fileIndex !== undefined) {
    query.set('file_index', String(params.fileIndex))
  }
  const suffix = query.toString()
  const data = await requestApi<{ file?: DatasetRawFile }>(
    `/api/v1/pipelines/datasets/${encodeURIComponent(params.datasetId)}/raw-file${suffix ? `?${suffix}` : ''}`,
    {
      headers: {
        'X-DB-Name': params.dbName,
        'X-Project': params.dbName,
      },
    },
    'Failed to load raw file content',
  )
  return data.file ?? null
}

export const listPipelines = async (dbName: string) => {
  const data = await requestApi<PipelineListPayload>(
    `/api/v1/pipelines?db_name=${encodeURIComponent(dbName)}`,
    {
      headers: {
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    'Failed to load pipelines',
  )
  return data.pipelines ?? []
}

export const listPipelineArtifacts = async (
  pipelineId: string,
  params?: { mode?: string; limit?: number; dbName?: string },
) => {
  const query = new URLSearchParams()
  if (params?.mode) {
    query.set('mode', params.mode)
  }
  if (params?.limit) {
    query.set('limit', String(params.limit))
  }
  const suffix = query.toString()
  const path = suffix ? `/api/v1/pipelines/${pipelineId}/artifacts?${suffix}` : `/api/v1/pipelines/${pipelineId}/artifacts`
  const data = await requestApi<PipelineArtifactListPayload>(
    path,
    params?.dbName
      ? {
          headers: {
            'X-DB-Name': params.dbName,
            'X-Project': params.dbName,
          },
        }
      : undefined,
    'Failed to load pipeline artifacts',
  )
  return data.artifacts ?? []
}

export const getPipelineReadiness = async (
  pipelineId: string,
  params?: { branch?: string; dbName?: string },
) => {
  const query = new URLSearchParams()
  if (params?.branch) {
    query.set('branch', params.branch)
  }
  const suffix = query.toString()
  const path = suffix
    ? `/api/v1/pipelines/${pipelineId}/readiness?${suffix}`
    : `/api/v1/pipelines/${pipelineId}/readiness`
  const data = await requestApi<PipelineReadinessPayload>(
    path,
    params?.dbName
      ? {
          headers: {
            'X-DB-Name': params.dbName,
            'X-Project': params.dbName,
          },
        }
      : undefined,
    'Failed to load pipeline readiness',
  )
  return data
}

export const getPipeline = async (
  pipelineId: string,
  params?: { branch?: string; previewNodeId?: string; dbName?: string },
) => {
  const query = new URLSearchParams()
  if (params?.branch) {
    query.set('branch', params.branch)
  }
  if (params?.previewNodeId) {
    query.set('preview_node_id', params.previewNodeId)
  }
  const suffix = query.toString()
  const path = suffix ? `/api/v1/pipelines/${pipelineId}?${suffix}` : `/api/v1/pipelines/${pipelineId}`
  const data = await requestApi<PipelineGetPayload>(
    path,
    params?.dbName
      ? {
          headers: {
            'X-DB-Name': params.dbName,
            'X-Project': params.dbName,
          },
        }
      : undefined,
    'Failed to load pipeline',
  )
  return data.pipeline
}

export const updatePipeline = async (
  pipelineId: string,
  params: {
    definition_json?: PipelineDefinition
    dbName?: string
  },
) => {
  const data = await requestApi<{ pipeline?: PipelineDetailRecord }>(
    `/api/v1/pipelines/${pipelineId}`,
    {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        ...(params.dbName
          ? {
              'X-DB-Name': params.dbName,
              'X-Project': params.dbName,
            }
          : {}),
      },
      body: JSON.stringify({
        definition_json: params.definition_json,
      }),
    },
    'Failed to update pipeline',
  )
  return data.pipeline
}

export const submitPipelineProposal = async (
  pipelineId: string,
  params: {
    title: string
    description?: string
    buildJobId?: string
    mappingSpecIds?: string[]
    dbName?: string
  },
) => {
  const data = await requestApi<{ proposal?: Record<string, unknown> }>(
    `/api/v1/pipelines/${pipelineId}/proposals`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(params.dbName
          ? {
              'X-DB-Name': params.dbName,
              'X-Project': params.dbName,
            }
          : {}),
      },
      body: JSON.stringify({
        title: params.title,
        description: params.description,
        build_job_id: params.buildJobId,
        mapping_spec_ids: params.mappingSpecIds,
      }),
    },
    'Failed to submit proposal',
  )
  return data.proposal ?? null
}

export const deployPipeline = async (
  pipelineId: string,
  params: {
    promoteBuild: boolean
    buildJobId?: string
    artifactId?: string
    nodeId?: string
    replayOnDeploy?: boolean
    dbName?: string
  },
) => {
  const data = await requestApi<Record<string, unknown>>(
    `/api/v1/pipelines/${pipelineId}/deploy`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(params.dbName
          ? {
              'X-DB-Name': params.dbName,
              'X-Project': params.dbName,
            }
          : {}),
      },
      body: JSON.stringify({
        promote_build: params.promoteBuild,
        build_job_id: params.buildJobId,
        artifact_id: params.artifactId,
        node_id: params.nodeId,
        replay_on_deploy: params.replayOnDeploy,
      }),
    },
    'Failed to deploy pipeline',
  )
  return data
}

export const createPipeline = async (params: {
  dbName: string
  name: string
  pipelineType: string
  location?: string
}) => {
  const data = await requestApi<PipelineCreatePayload>(
    '/api/v1/pipelines',
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-DB-Name': params.dbName,
        'X-Project': params.dbName,
      },
      body: JSON.stringify({
        db_name: params.dbName,
        name: params.name,
        pipeline_type: params.pipelineType,
        location: params.location,
      }),
    },
    'Failed to create pipeline',
  )
  return data.pipeline
}

export const uploadDataset = async (params: { dbName: string; file: File; mode: UploadMode }) => {
  const endpoint = inferUploadEndpoint(params.mode, params.file.name)
  if (!endpoint) {
    throw new Error('Only .csv, .xls, .xlsx, or .xlsm files are supported for structured uploads.')
  }

  const datasetName = getDatasetBaseName(params.file.name)
  const formData = new FormData()
  if (endpoint.includes('media-upload')) {
    formData.append('files', params.file)
  } else {
    formData.append('file', params.file)
  }
  formData.append('dataset_name', datasetName)
  formData.append('description', 'Uploaded from Files')

  const data = await requestApi<DatasetUploadPayload>(
    `${endpoint}?db_name=${encodeURIComponent(params.dbName)}`,
    {
      method: 'POST',
      headers: {
        'Idempotency-Key': createIdempotencyKey(),
        'X-DB-Name': params.dbName,
        'X-Project': params.dbName,
      },
      body: formData,
    },
    'Failed to upload dataset',
  )

  if (!data.dataset) {
    throw new Error('Dataset upload returned no dataset information.')
  }
  return {
    dataset: data.dataset,
    ingest_request_id: data.ingest_request_id,
    schema_status: data.schema_status,
    schema_suggestion: data.schema_suggestion,
  }
}

export const approveDatasetSchema = async (params: {
  ingestRequestId: string
  dbName: string
  schemaJson?: Record<string, unknown>
}) => {
  const data = await requestApi<DatasetSchemaApprovalPayload>(
    `/api/v1/pipelines/datasets/ingest-requests/${encodeURIComponent(params.ingestRequestId)}/schema/approve`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-DB-Name': params.dbName,
        'X-Project': params.dbName,
      },
      body: JSON.stringify(params.schemaJson ? { schema_json: params.schemaJson } : {}),
    },
    'Failed to approve dataset schema',
  )
  return data
}

export const listRegisteredGoogleSheets = async (params?: { databaseName?: string }) => {
  const query = new URLSearchParams()
  if (params?.databaseName) {
    query.set('database_name', params.databaseName)
  }
  const suffix = query.toString()
  const path = suffix
    ? `/api/v1/data-connectors/google-sheets/registered?${suffix}`
    : '/api/v1/data-connectors/google-sheets/registered'
  const data = await requestApi<GoogleSheetsRegisteredListPayload>(path, undefined, 'Failed to load registered sheets')
  return {
    sheets: data.sheets ?? [],
    count: data.count ?? (data.sheets?.length ?? 0),
    database_filter: data.database_filter ?? null,
  }
}

export const registerGoogleSheet = async (payload: Record<string, unknown>) => {
  const data = await requestApi<Record<string, unknown>>(
    '/api/v1/data-connectors/google-sheets/register',
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
      },
      body: JSON.stringify(payload),
    },
    'Failed to register Google Sheet',
  )
  return data
}

export const previewRegisteredGoogleSheet = async (
  sheetId: string,
  params?: { worksheetName?: string; limit?: number },
) => {
  const query = new URLSearchParams()
  if (params?.worksheetName) {
    query.set('worksheet_name', params.worksheetName)
  }
  if (params?.limit) {
    query.set('limit', String(params.limit))
  }
  const suffix = query.toString()
  const path = suffix
    ? `/api/v1/data-connectors/google-sheets/${encodeURIComponent(sheetId)}/preview?${suffix}`
    : `/api/v1/data-connectors/google-sheets/${encodeURIComponent(sheetId)}/preview`
  const data = await requestApi<GoogleSheetPreview>(path, undefined, 'Failed to preview Google Sheet')
  return data
}

export const startPipeliningGoogleSheet = async (sheetId: string, payload: Record<string, unknown>) => {
  const data = await requestApi<Record<string, unknown>>(
    `/api/v1/data-connectors/google-sheets/${encodeURIComponent(sheetId)}/start-pipelining`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
      },
      body: JSON.stringify(payload),
    },
    'Failed to start pipelining',
  )
  return data
}

export const unregisterGoogleSheet = async (sheetId: string) => {
  const data = await requestApi<Record<string, unknown>>(
    `/api/v1/data-connectors/google-sheets/${encodeURIComponent(sheetId)}`,
    {
      method: 'DELETE',
      headers: { 'Idempotency-Key': createIdempotencyKey() },
    },
    'Failed to unregister Google Sheet',
  )
  return data
}

export type AIQueryMode = 'auto' | 'label_query' | 'graph_query'

export type AIQueryRequest = {
  question: string
  branch?: string
  mode?: AIQueryMode
  limit?: number
  include_documents?: boolean
  include_provenance?: boolean
}

export const aiQuery = async (dbName: string, payload: AIQueryRequest) => {
  const encoded = encodeURIComponent(dbName)
  const data = await requestApi<Record<string, unknown>>(
    `/api/v1/ai/query/${encoded}`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
      body: JSON.stringify(payload),
    },
    'AI query failed',
  )
  return data
}

export const runGraphQuery = async (dbName: string, payload: Record<string, unknown>, params?: { branch?: string }) => {
  const encoded = encodeURIComponent(dbName)
  const branch = (params?.branch ?? 'main').trim() || 'main'
  const data = await requestApi<Record<string, unknown>>(
    `/api/v1/graph-query/${encoded}?branch=${encodeURIComponent(branch)}`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
      body: JSON.stringify(payload),
    },
    'Graph query failed',
  )
  return data
}

export const compileAgentPlan = async (payload: {
  goal: string
  data_scope?: Record<string, unknown>
  answers?: Record<string, unknown>
}) => {
  const data = await requestApi<Record<string, unknown>>(
    '/api/v1/agent-plans/compile',
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
      },
      body: JSON.stringify(payload),
    },
    'Failed to compile agent plan',
  )
  return data as unknown as {
    status?: string
    plan_id?: string
    plan?: AgentPlan | null
    questions?: AgentClarificationQuestion[]
    validation_errors?: string[]
    validation_warnings?: string[]
    compilation_report?: Record<string, unknown> | null
    planner?: Record<string, unknown> | null
  }
}

export const approveAgentPlan = async (
  planId: string,
  payload: { decision: string; step_id?: string; comment?: string; metadata?: Record<string, unknown> },
) => {
  const data = await requestApi<Record<string, unknown>>(
    `/api/v1/agent-plans/${encodeURIComponent(planId)}/approvals`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
      },
      body: JSON.stringify(payload),
    },
    'Failed to record plan approval',
  )
  return data
}

export const executeAgentPlan = async (planId: string) => {
  const data = await requestApi<Record<string, unknown>>(
    `/api/v1/agent-plans/${encodeURIComponent(planId)}/execute`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
      },
      body: JSON.stringify({}),
    },
    'Failed to execute agent plan',
  )
  return data
}

export const getAgentPlan = async (planId: string) => {
  const data = await requestApi<{
    plan?: Record<string, unknown>
    plan_id?: string
    status?: string
    created_at?: string
    updated_at?: string
  }>(`/api/v1/agent-plans/${encodeURIComponent(planId)}`, undefined, 'Failed to load agent plan')
  return data
}

export type AgentSessionRecord = {
  session_id: string
  tenant_id?: string
  created_by?: string
  status?: string
  selected_model?: string | null
  enabled_tools?: string[] | null
  started_at?: string
  terminated_at?: string | null
  created_at?: string
  updated_at?: string
  metadata?: Record<string, unknown> | null
}

export type AgentSessionEvent = {
  event_id: string
  event_type: string
  occurred_at: string
  data?: Record<string, unknown>
}

export const createAgentSession = async (payload: {
  selected_model?: string | null
  enabled_tools?: string[] | null
  metadata?: Record<string, unknown> | null
}) => {
  const data = await requestApi<{ session: AgentSessionRecord }>(
    '/api/v1/agent-sessions',
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
      },
      body: JSON.stringify(payload),
    },
    'Failed to create agent session',
  )
  return data
}

export const postAgentSessionMessage = async (
  sessionId: string,
  payload: {
    content: string
    data_scope?: Record<string, unknown>
    execute?: boolean
    answers?: Record<string, unknown>
  },
) => {
  const data = await requestApi<Record<string, unknown>>(
    `/api/v1/agent-sessions/${encodeURIComponent(sessionId)}/messages`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
      },
      body: JSON.stringify(payload),
    },
    'Failed to send agent message',
  )
  return data
}

export const listAgentSessionEvents = async (
  sessionId: string,
  params?: {
    limit?: number
    include_messages?: boolean
    include_jobs?: boolean
    include_approvals?: boolean
    include_agent_steps?: boolean
    include_tool_calls?: boolean
    include_llm_calls?: boolean
    include_ci_results?: boolean
  },
) => {
  const query = new URLSearchParams()
  if (params?.limit) {
    query.set('limit', String(params.limit))
  }
  if (typeof params?.include_messages === 'boolean') {
    query.set('include_messages', String(params.include_messages))
  }
  if (typeof params?.include_jobs === 'boolean') {
    query.set('include_jobs', String(params.include_jobs))
  }
  if (typeof params?.include_approvals === 'boolean') {
    query.set('include_approvals', String(params.include_approvals))
  }
  if (typeof params?.include_agent_steps === 'boolean') {
    query.set('include_agent_steps', String(params.include_agent_steps))
  }
  if (typeof params?.include_tool_calls === 'boolean') {
    query.set('include_tool_calls', String(params.include_tool_calls))
  }
  if (typeof params?.include_llm_calls === 'boolean') {
    query.set('include_llm_calls', String(params.include_llm_calls))
  }
  if (typeof params?.include_ci_results === 'boolean') {
    query.set('include_ci_results', String(params.include_ci_results))
  }
  const suffix = query.toString()
  const path = suffix
    ? `/api/v1/agent-sessions/${encodeURIComponent(sessionId)}/events?${suffix}`
    : `/api/v1/agent-sessions/${encodeURIComponent(sessionId)}/events`
  const data = await requestApi<{ session_id: string; count: number; events: AgentSessionEvent[] }>(
    path,
    undefined,
    'Failed to load agent session events',
  )
  return data
}

export const attachAgentSessionContextItem = async (
  sessionId: string,
  payload: {
    item_type: string
    include_mode?: string
    ref?: Record<string, unknown>
    metadata?: Record<string, unknown>
    token_count?: number
  },
) => {
  const data = await requestApi<Record<string, unknown>>(
    `/api/v1/agent-sessions/${encodeURIComponent(sessionId)}/context/items`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
      },
      body: JSON.stringify(payload),
    },
    'Failed to attach agent context',
  )
  return data
}

export const decideAgentSessionApproval = async (
  sessionId: string,
  approvalRequestId: string,
  payload: { decision: string; comment?: string; metadata?: Record<string, unknown> },
) => {
  const data = await requestApi<Record<string, unknown>>(
    `/api/v1/agent-sessions/${encodeURIComponent(sessionId)}/approvals/${encodeURIComponent(approvalRequestId)}`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
      },
      body: JSON.stringify(payload),
    },
    'Failed to record approval',
  )
  return data
}
