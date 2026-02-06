// Mock 모드 설정 - .env에서 VITE_MOCK_API=true로 설정
export const isMockMode = import.meta.env.VITE_MOCK_API === 'true'

type MockDataModule = typeof import('./mockData')

let mockDataModulePromise: Promise<MockDataModule> | null = null
const MOCK_DATA_SPECIFIER = './mockData'

const DEFAULT_MOCK_DELAY_MS = import.meta.env.MODE === 'test' ? 0 : 300
const MOCK_DELAY_MS = (() => {
  const configured = import.meta.env.VITE_MOCK_API_DELAY_MS
  if (!configured) {
    return DEFAULT_MOCK_DELAY_MS
  }
  const parsed = Number.parseInt(configured, 10)
  return Number.isFinite(parsed) ? parsed : DEFAULT_MOCK_DELAY_MS
})()

const loadMockData = async () => {
  if (!mockDataModulePromise) {
    const specifier = MOCK_DATA_SPECIFIER
    mockDataModulePromise = (import(/* @vite-ignore */ specifier) as Promise<MockDataModule>).catch((err) => {
      throw new Error(
        `VITE_MOCK_API=true but the mock data module (${MOCK_DATA_SPECIFIER}) could not be loaded: ${String(err)}`,
      )
    })
  }
  return mockDataModulePromise
}

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
  stage?: 'raw' | 'clean'
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
const API_TOKEN_RAW =
  (import.meta.env.VITE_BFF_TOKEN as string | undefined) ??
  (import.meta.env.VITE_ADMIN_TOKEN as string | undefined) ??
  ''
const API_USER_JWT_RAW = (import.meta.env.VITE_USER_JWT as string | undefined) ?? ''
const API_USER_ID = (import.meta.env.VITE_USER_ID as string | undefined) ?? ''
const API_USER_NAME = (import.meta.env.VITE_USER_NAME as string | undefined) ?? ''

const stripBearerPrefix = (value: string) => {
  const raw = String(value || '').trim()
  if (!raw) {
    return ''
  }
  return raw.replace(/^bearer\s+/i, '').trim()
}

const API_TOKEN = stripBearerPrefix(API_TOKEN_RAW)
const API_USER_JWT = stripBearerPrefix(API_USER_JWT_RAW)

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
    headers.set('X-Admin-Token', API_TOKEN)
  }
  if (API_USER_JWT) {
    headers.set('X-Delegated-Authorization', `Bearer ${API_USER_JWT}`)
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
  if (isMockMode) {
    const mockData = await loadMockData()
    await mockData.mockDelay(MOCK_DELAY_MS)
    return mockData.mockDatabases
  }
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

export const listDatasets = async (dbName: string, stage?: 'raw' | 'clean') => {
  if (isMockMode) {
    const mockData = await loadMockData()
    await mockData.mockDelay(MOCK_DELAY_MS)
    return mockData.mockDatasets.filter(d => d.db_name === dbName)
  }
  // stage 파라미터가 있으면 해당 stage만, 없으면 전체 (raw + clean)
  const stageParam = stage ? `&stage=${stage}` : ''
  const data = await requestApi<DatasetListPayload>(
    `/api/v1/pipelines/datasets?db_name=${encodeURIComponent(dbName)}${stageParam}`,
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
  if (isMockMode) {
    const mockData = await loadMockData()
    await mockData.mockDelay(MOCK_DELAY_MS)
    // Mock raw file data based on dataset
    const dataset = mockData.mockDatasets.find(d => d.dataset_id === params.datasetId)
    if (!dataset) return null
    return {
      dataset_id: params.datasetId,
      filename: `${dataset.name}.csv`,
      content_type: 'text/csv',
      encoding: 'utf-8',
      content: mockData.getMockRawContent(params.datasetId),
    } as DatasetRawFile
  }
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

// Transform Node Preview API
export type TransformPreviewResponse = {
  node_id: string
  schema_json: { columns: Array<{ name: string; type?: string }> }
  sample_json: { rows: Record<string, unknown>[] }
  row_count: number
}

export const getTransformPreview = async (
  dbName: string,
  nodeId: string
): Promise<TransformPreviewResponse | null> => {
  try {
    const data = await requestApi<TransformPreviewResponse>(
      `/api/v1/pipelines/transform-preview/${encodeURIComponent(nodeId)}`,
      {
        headers: {
          'X-DB-Name': dbName,
          'X-Project': dbName,
        },
      },
      'Failed to load transform preview',
    )
    return data
  } catch {
    return null
  }
}

export const listPipelines = async (dbName: string) => {
  if (isMockMode) {
    const mockData = await loadMockData()
    await mockData.mockDelay(MOCK_DELAY_MS)
    return mockData.mockPipelines.filter(p => p.db_name === dbName || dbName === 'demo-project')
  }
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
  if (isMockMode) {
    const mockData = await loadMockData()
    await mockData.mockDelay(MOCK_DELAY_MS)
    return mockData.mockPipelineArtifacts.filter(a => a.pipeline_id === pipelineId)
  }
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
        'Idempotency-Key': createIdempotencyKey(),
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

export type ProposalRecord = {
  proposal_id: string
  pipeline_id: string
  title: string
  description?: string
  status: string
  from_branch?: string
  to_branch?: string
  build_job_id?: string
  mapping_spec_ids?: string[]
  created_by?: string
  reviewed_by?: string
  review_comment?: string
  created_at?: string
  updated_at?: string
}

export const listPipelineProposals = async (params: {
  dbName: string
  branch?: string
  status?: string
}) => {
  const query = new URLSearchParams()
  query.set('db_name', params.dbName)
  if (params.branch) {
    query.set('branch', params.branch)
  }
  if (params.status) {
    query.set('status', params.status)
  }
  const data = await requestApi<{ proposals?: ProposalRecord[] }>(
    `/api/v1/pipelines/proposals?${query.toString()}`,
    {
      method: 'GET',
      headers: {
        'X-DB-Name': params.dbName,
        'X-Project': params.dbName,
      },
    },
    'Failed to list proposals',
  )
  return data.proposals ?? []
}

export const approvePipelineProposal = async (
  pipelineId: string,
  params: { proposalId: string; comment?: string; dbName?: string },
) => {
  const data = await requestApi<Record<string, unknown>>(
    `/api/v1/pipelines/${pipelineId}/proposals/approve`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
        ...(params.dbName
          ? {
              'X-DB-Name': params.dbName,
              'X-Project': params.dbName,
            }
          : {}),
      },
      body: JSON.stringify({
        proposal_id: params.proposalId,
        review_comment: params.comment,
      }),
    },
    'Failed to approve proposal',
  )
  return data
}

export const rejectPipelineProposal = async (
  pipelineId: string,
  params: { proposalId: string; comment?: string; dbName?: string },
) => {
  const data = await requestApi<Record<string, unknown>>(
    `/api/v1/pipelines/${pipelineId}/proposals/reject`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
        ...(params.dbName
          ? {
              'X-DB-Name': params.dbName,
              'X-Project': params.dbName,
            }
          : {}),
      },
      body: JSON.stringify({
        proposal_id: params.proposalId,
        review_comment: params.comment,
      }),
    },
    'Failed to reject proposal',
  )
  return data
}

export type PipelineBranchRecord = {
  branch: string
  db_name?: string
  status?: string
  created_at?: string
  updated_at?: string
}

export const listPipelineBranches = async (dbName: string) => {
  const data = await requestApi<{ branches?: PipelineBranchRecord[]; count?: number }>(
    `/api/v1/pipelines/branches?db_name=${encodeURIComponent(dbName)}`,
    {
      method: 'GET',
      headers: {
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    'Failed to list pipeline branches',
  )
  return data.branches ?? []
}

export const createPipelineBranch = async (
  pipelineId: string,
  params: { branch: string; dbName?: string },
) => {
  const data = await requestApi<{ branch?: Record<string, unknown> }>(
    `/api/v1/pipelines/${pipelineId}/branches`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
        ...(params.dbName
          ? {
              'X-DB-Name': params.dbName,
              'X-Project': params.dbName,
            }
          : {}),
      },
      body: JSON.stringify({
        branch: params.branch,
      }),
    },
    'Failed to create pipeline branch',
  )
  return data.branch ?? null
}

export const buildPipeline = async (
  pipelineId: string,
  params: {
    nodeId?: string
    limit?: number
    dbName?: string
  },
) => {
  const data = await requestApi<{ job_id?: string; artifact_id?: string }>(
    `/api/v1/pipelines/${pipelineId}/build`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Idempotency-Key': `build-${pipelineId}-${Date.now()}`,
        ...(params.dbName
          ? {
              'X-DB-Name': params.dbName,
              'X-Project': params.dbName,
            }
          : {}),
      },
      body: JSON.stringify({
        node_id: params.nodeId,
        limit: params.limit ?? 200,
      }),
    },
    'Failed to build pipeline',
  )
  return data
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
        'Idempotency-Key': createIdempotencyKey(),
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
  branch?: string
  description?: string
  definitionJson?: PipelineDefinition
}) => {
  const data = await requestApi<PipelineCreatePayload>(
    '/api/v1/pipelines',
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
        'X-DB-Name': params.dbName,
        'X-Project': params.dbName,
      },
      body: JSON.stringify({
        db_name: params.dbName,
        name: params.name,
        pipeline_type: params.pipelineType,
        location: params.location,
        branch: params.branch,
        description: params.description,
        definition_json: params.definitionJson,
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

export type AIQueryMode = 'auto' | 'label_query' | 'graph_query' | 'dataset_list'

export type AIQueryRequest = {
  question: string
  branch?: string
  mode?: AIQueryMode
  limit?: number
  include_documents?: boolean
  include_provenance?: boolean
  session_id?: string | null
}

export type AIIntentRoute = 'chat' | 'query' | 'plan' | 'pipeline'
export type AIIntentType = 'greeting' | 'small_talk' | 'help' | 'data_query' | 'plan_request' | 'unknown'

export type AIIntentRequest = {
  question: string
  db_name?: string | null
  project_name?: string | null
  pipeline_name?: string | null
  language?: string | null
  context?: Record<string, unknown> | null
  session_id?: string | null
}

export type AIIntentResponse = {
  intent: AIIntentType
  route: AIIntentRoute
  confidence: number
  requires_clarification: boolean
  clarifying_question?: string
  reply?: string
  missing_fields?: string[]
  llm?: Record<string, unknown>
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

export const aiIntent = async (payload: AIIntentRequest) => {
  const data = await requestApi<AIIntentResponse>(
    '/api/v1/ai/intent',
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
      },
      body: JSON.stringify(payload),
    },
    'AI intent routing failed',
  )
  return data
}

export const runPipelineAgent = async (payload: {
  goal: string
  data_scope: Record<string, unknown>
  planner_hints?: Record<string, unknown>
  answers?: Record<string, unknown>
  apply_specs?: boolean
  max_transform?: number
  max_cleansing?: number
  max_repairs?: number
}) => {
  const data = await requestApi<Record<string, unknown>>(
    '/api/v1/agent/pipeline-runs',
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
      },
      body: JSON.stringify(payload),
    },
    'Failed to run pipeline agent',
  )
  return data
}

// === Pipeline Agent SSE Streaming Types ===
export type AgentStreamEventType =
  | 'start'
  | 'thinking'
  | 'tool_start'
  | 'tool_end'
  | 'plan_update'
  | 'preview_update'
  | 'ontology_update'
  | 'clarification'
  | 'error'
  | 'complete'

export type AgentStreamEvent = {
  type: AgentStreamEventType
  data: {
    run_id?: string
    step?: number
    tool?: string
    args?: Record<string, unknown>
    success?: boolean
    observation?: Record<string, unknown>
    error?: string
    plan?: {
      definition_json?: {
        nodes?: Array<{
          id?: string
          type?: string
          operation?: string
          metadata?: Record<string, unknown>
        }>
        edges?: Array<{
          id?: string
          from?: string
          to?: string
        }>
      }
      outputs?: Array<Record<string, unknown>>
    }
    plan_id?: string
    status?: string
    node_count?: number
    edge_count?: number
    questions?: Array<Record<string, unknown>>
    validation_errors?: string[]
    validation_warnings?: string[]
    // preview_update 이벤트용
    preview?: {
      columns?: Array<{ name: string; type?: string }>
      rows?: Array<Record<string, unknown>>
      row_count?: number
    }
    // ontology_update 이벤트용
    ontology?: Record<string, unknown>
    schema_inference?: Record<string, unknown>
    mapping_suggestions?: Record<string, unknown>
    objectify_status?: {
      job_id?: string
      status?: string
      instances_created?: number
    }
    [key: string]: unknown
  }
}

export type AgentStreamCallbacks = {
  onStart?: (data: AgentStreamEvent['data']) => void
  onThinking?: (data: AgentStreamEvent['data']) => void
  onToolStart?: (data: AgentStreamEvent['data']) => void
  onToolEnd?: (data: AgentStreamEvent['data']) => void
  onPlanUpdate?: (data: AgentStreamEvent['data']) => void
  onPreviewUpdate?: (data: AgentStreamEvent['data']) => void
  onOntologyUpdate?: (data: AgentStreamEvent['data']) => void
  onClarification?: (data: AgentStreamEvent['data']) => void
  onError?: (data: AgentStreamEvent['data']) => void
  onComplete?: (data: AgentStreamEvent['data']) => void
}

// SSE 스트리밍 Pipeline Agent 실행
export const runPipelineAgentStreaming = (
  payload: {
    goal: string
    data_scope: Record<string, unknown>
    planner_hints?: Record<string, unknown>
    answers?: Record<string, unknown>
    apply_specs?: boolean
    max_transform?: number
    max_cleansing?: number
    max_repairs?: number
  },
  callbacks: AgentStreamCallbacks,
): { abort: () => void } => {
  const controller = new AbortController()

  const splitSseBuffer = (buffer: string): { frames: string[]; remainder: string } => {
    // SSE frames are separated by a blank line, which can be "\n\n" or "\r\n\r\n".
    // We normalize by detecting the earliest delimiter occurrence and iterating.
    const frames: string[] = []
    let rest = buffer
    while (true) {
      const lfIndex = rest.indexOf('\n\n')
      const crlfIndex = rest.indexOf('\r\n\r\n')
      let index = -1
      let delimLen = 0
      if (lfIndex !== -1 && (crlfIndex === -1 || lfIndex < crlfIndex)) {
        index = lfIndex
        delimLen = 2
      } else if (crlfIndex !== -1) {
        index = crlfIndex
        delimLen = 4
      }

      if (index === -1) {
        break
      }
      const frame = rest.slice(0, index)
      if (frame.trim().length > 0) {
        frames.push(frame)
      }
      rest = rest.slice(index + delimLen)
    }
    return { frames, remainder: rest }
  }

  const parseSseFrame = (frame: string): AgentStreamEvent | null => {
    // Supports multi-line `data:` payloads and CRLF.
    let eventType: string | null = null
    const dataLines: string[] = []
    const lines = frame.split(/\r?\n/)
    for (const rawLine of lines) {
      const line = rawLine ?? ''
      if (!line) continue
      if (line.startsWith(':')) {
        // Comment line per SSE spec.
        continue
      }
      if (line.startsWith('event:')) {
        eventType = line.slice('event:'.length).trim()
        continue
      }
      if (line.startsWith('data:')) {
        dataLines.push(line.slice('data:'.length).trimStart())
        continue
      }
      // Ignore id:/retry: and unknown fields for now.
    }

    if (!eventType || dataLines.length === 0) {
      return null
    }

    const dataText = dataLines.join('\n')
    try {
      const data = JSON.parse(dataText)
      return { type: eventType as AgentStreamEventType, data }
    } catch {
      // Some events may not be JSON-encoded; treat as error-friendly raw payload.
      return { type: eventType as AgentStreamEventType, data: { raw: dataText } }
    }
  }

  const fetchSSE = async () => {
    const headers: HeadersInit = {
      'Content-Type': 'application/json',
      'Accept': 'text/event-stream',
    }
    if (API_TOKEN) {
      headers['X-Admin-Token'] = API_TOKEN
    }
    if (API_USER_JWT) {
      headers['X-Delegated-Authorization'] = `Bearer ${API_USER_JWT}`
    }
    if (API_USER_ID) {
      headers['X-User-ID'] = API_USER_ID
    }
    if (API_USER_NAME) {
      headers['X-User-Name'] = API_USER_NAME
    }

    // Enterprise hardening: satisfy BFF db-scope enforcement when enabled.
    const scopeDbName =
      typeof payload.data_scope?.['db_name'] === 'string' ? payload.data_scope['db_name'] : undefined
    const dbName = String(scopeDbName ?? '').trim()
    if (dbName) {
      headers['X-DB-Name'] = dbName
      headers['X-Project'] = dbName
    }

    try {
      const response = await fetch(buildUrl('/api/v1/agent/pipeline-runs/stream'), {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
        signal: controller.signal,
      })

      if (!response.ok) {
        const errorText = await response.text()
        let message = errorText || response.statusText
        try {
          const parsed = JSON.parse(errorText)
          const msg = typeof parsed?.message === 'string' ? parsed.message : ''
          const detail = typeof parsed?.detail === 'string' ? parsed.detail : ''
          if (msg && detail) {
            message = `${msg} (${detail})`
          } else if (msg) {
            message = msg
          } else if (detail) {
            message = detail
          }
        } catch {
          // ignore - keep raw errorText
        }
        callbacks.onError?.({ error: message })
        return
      }

      const reader = response.body?.getReader()
      if (!reader) {
        callbacks.onError?.({ error: 'No response body' })
        return
      }

      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })

        const { frames, remainder } = splitSseBuffer(buffer)
        buffer = remainder
        for (const frame of frames) {
          const parsed = parseSseFrame(frame)
          if (!parsed) {
            continue
          }
          const data = parsed.data
          switch (parsed.type) {
            case 'start':
              callbacks.onStart?.(data)
              break
            case 'thinking':
              callbacks.onThinking?.(data)
              break
            case 'tool_start':
              callbacks.onToolStart?.(data)
              break
            case 'tool_end':
              callbacks.onToolEnd?.(data)
              break
            case 'plan_update':
              callbacks.onPlanUpdate?.(data)
              break
            case 'preview_update':
              callbacks.onPreviewUpdate?.(data)
              break
            case 'ontology_update':
              callbacks.onOntologyUpdate?.(data)
              break
            case 'clarification':
              callbacks.onClarification?.(data)
              break
            case 'error':
              callbacks.onError?.(data)
              break
            case 'complete':
              callbacks.onComplete?.(data)
              break
            default:
              // Unknown event types are ignored to keep smoke streaming resilient.
              break
          }
        }
      }
    } catch (error) {
      if ((error as Error).name !== 'AbortError') {
        callbacks.onError?.({ error: (error as Error).message })
      }
    }
  }

  fetchSSE()

  return {
    abort: () => controller.abort(),
  }
}

export const previewPipelinePlan = async (
  planId: string,
  payload: {
    node_id?: string
    limit?: number
    include_run_tables?: boolean
    run_table_limit?: number
  },
) => {
  const encoded = encodeURIComponent(planId)
  const data = await requestApi<Record<string, unknown>>(
    `/api/v1/pipeline-plans/${encoded}/preview`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
      },
      body: JSON.stringify(payload),
    },
    'Failed to preview pipeline plan',
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

// === Lineage Types ===
export type LineageNode = {
  id: string
  label: string
  type: 'dataset' | 'pipeline' | 'object' | 'report' | 'source'
  metadata?: Record<string, unknown>
}

export type LineageEdge = {
  id: string
  source: string
  target: string
  label?: string
}

export type LineageGraphResponse = {
  nodes: LineageNode[]
  edges: LineageEdge[]
  rootId: string
}

export type LineageImpactResponse = {
  affectedReports: number
  affectedPipelines: number
  lastUpdated?: string
  downstream: Array<{ id: string; name: string; type: string }>
}

// === Instance/Explorer Types ===
export type OntologyClass = {
  id: string
  label: string
  description?: string
  propertyCount?: number
  instanceCount?: number
}

export type Instance = {
  id: string
  classId: string
  label: string
  properties: Record<string, unknown>
  createdAt?: string
  updatedAt?: string
}

export type InstanceListResponse = {
  instances: Instance[]
  total: number
  limit: number
  offset: number
}

export type Relationship = {
  id: string
  predicate: string
  predicateLabel?: string
  sourceId: string
  sourceLabel: string
  sourceClass: string
  targetId: string
  targetLabel: string
  targetClass: string
  properties?: Record<string, unknown>
}

export type NLQueryResponse = {
  results: Array<Record<string, unknown>>
  columns: string[]
  query?: string
  explanation?: string
  totalCount?: number
}

// === Action Types ===
export type ActionParameter = {
  name: string
  label: string
  type: 'string' | 'number' | 'boolean' | 'select' | 'instance'
  required?: boolean
  options?: Array<{ value: string; label: string }>
  instanceClassId?: string
  defaultValue?: unknown
  description?: string
}

export type ActionType = {
  id: string
  name: string
  description?: string
  targetClass?: string
  parameters: ActionParameter[]
  createdAt?: string
  updatedAt?: string
}

export type ActionTypeDetail = ActionType & {
  examples?: Array<{ description: string; params: Record<string, unknown> }>
  relatedActions?: string[]
}

export type SimulationResult = {
  success: boolean
  changes: Array<{
    entityId: string
    entityLabel: string
    field: string
    before: unknown
    after: unknown
  }>
  warnings?: string[]
  errors?: string[]
}

export type ExecutionResult = {
  success: boolean
  executionId: string
  timestamp: string
  changes: Array<{
    entityId: string
    entityLabel: string
    field: string
    before: unknown
    after: unknown
  }>
  errors?: string[]
}

export type ActionLog = {
  id: string
  actionTypeId: string
  actionTypeName: string
  executedBy: string
  executedAt: string
  status: 'success' | 'failed' | 'pending'
  params: Record<string, unknown>
  result?: Record<string, unknown>
}

// === Link Type Types ===
export type LinkType = {
  id: string
  name: string
  predicate: string
  sourceClassId: string
  sourceClassName: string
  targetClassId: string
  targetClassName: string
  cardinality?: 'one-to-one' | 'one-to-many' | 'many-to-many'
  description?: string
}

export type LinkTypeDetail = LinkType & {
  properties?: Array<{ name: string; type: string }>
  instanceCount?: number
}

// === Detected Relationships (Objectify) ===
export type DetectedRelationship = {
  sourceColumn: string
  targetDataset: string
  targetColumn: string
  confidence: number
  suggestedPredicate?: string
}

export type DetectedRelationships = {
  datasetId: string
  relationships: DetectedRelationship[]
}

// === Lineage API ===
export const getLineageGraph = async (params: {
  dbName: string
  rootId: string
  direction?: 'both' | 'upstream' | 'downstream'
  maxDepth?: number
  maxNodes?: number
}) => {
  const query = new URLSearchParams()
  query.set('root_id', params.rootId)
  if (params.direction) {
    query.set('direction', params.direction)
  }
  if (params.maxDepth !== undefined) {
    query.set('max_depth', String(params.maxDepth))
  }
  if (params.maxNodes !== undefined) {
    query.set('max_nodes', String(params.maxNodes))
  }
  const encoded = encodeURIComponent(params.dbName)
  const data = await requestApi<LineageGraphResponse>(
    `/api/v1/lineage/${encoded}?${query.toString()}`,
    {
      headers: {
        'X-DB-Name': params.dbName,
        'X-Project': params.dbName,
      },
    },
    '계보 정보를 불러오는데 실패했습니다',
  )
  return data
}

export const getLineageImpact = async (dbName: string, rootId: string) => {
  const encoded = encodeURIComponent(dbName)
  const data = await requestApi<LineageImpactResponse>(
    `/api/v1/lineage/${encoded}/impact?root_id=${encodeURIComponent(rootId)}`,
    {
      headers: {
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    '영향 분석을 불러오는데 실패했습니다',
  )
  return data
}

// === Instance/Explorer API ===
export const listOntologyClasses = async (dbName: string) => {
  if (isMockMode) {
    const mockData = await loadMockData()
    await mockData.mockDelay(MOCK_DELAY_MS)
    return mockData.mockOntologyClasses
  }
  const encoded = encodeURIComponent(dbName)
  const data = await requestApi<{ classes: OntologyClass[] }>(
    `/api/v1/ontology/${encoded}/classes`,
    {
      headers: {
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    '클래스 목록을 불러오는데 실패했습니다',
  )
  return data.classes ?? []
}

export const listInstances = async (
  dbName: string,
  classId: string,
  params?: { search?: string; limit?: number; offset?: number },
) => {
  const query = new URLSearchParams()
  if (params?.search) {
    query.set('search', params.search)
  }
  if (params?.limit !== undefined) {
    query.set('limit', String(params.limit))
  }
  if (params?.offset !== undefined) {
    query.set('offset', String(params.offset))
  }
  const encoded = encodeURIComponent(dbName)
  const classEncoded = encodeURIComponent(classId)
  const suffix = query.toString()
  const path = suffix
    ? `/api/v1/ontology/${encoded}/classes/${classEncoded}/instances?${suffix}`
    : `/api/v1/ontology/${encoded}/classes/${classEncoded}/instances`
  const data = await requestApi<InstanceListResponse>(
    path,
    {
      headers: {
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    '인스턴스 목록을 불러오는데 실패했습니다',
  )
  return data
}

export const getInstance = async (dbName: string, classId: string, instanceId: string) => {
  const encoded = encodeURIComponent(dbName)
  const classEncoded = encodeURIComponent(classId)
  const instanceEncoded = encodeURIComponent(instanceId)
  const data = await requestApi<{ instance: Instance }>(
    `/api/v1/ontology/${encoded}/classes/${classEncoded}/instances/${instanceEncoded}`,
    {
      headers: {
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    '인스턴스 정보를 불러오는데 실패했습니다',
  )
  return data.instance
}

export const getInstanceRelationships = async (
  dbName: string,
  classId: string,
  instanceId: string,
) => {
  const encoded = encodeURIComponent(dbName)
  const classEncoded = encodeURIComponent(classId)
  const instanceEncoded = encodeURIComponent(instanceId)
  const data = await requestApi<{ relationships: Relationship[] }>(
    `/api/v1/ontology/${encoded}/classes/${classEncoded}/instances/${instanceEncoded}/relationships`,
    {
      headers: {
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    '관계 정보를 불러오는데 실패했습니다',
  )
  return data.relationships ?? []
}

// === Natural Language Query API ===
export const naturalLanguageQuery = async (dbName: string, question: string) => {
  const encoded = encodeURIComponent(dbName)
  const data = await requestApi<NLQueryResponse>(
    `/api/v1/ai/nl-query/${encoded}`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
      body: JSON.stringify({ question }),
    },
    '자연어 쿼리 실행에 실패했습니다',
  )
  return data
}

// === Action API ===
export const listActionTypes = async (dbName: string) => {
  if (isMockMode) {
    const mockData = await loadMockData()
    await mockData.mockDelay(MOCK_DELAY_MS)
    return mockData.mockActionTypes
  }
  const encoded = encodeURIComponent(dbName)
  const data = await requestApi<{ actionTypes: ActionType[] }>(
    `/api/v1/ontology/${encoded}/action-types`,
    {
      headers: {
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    'Action 목록을 불러오는데 실패했습니다',
  )
  return data.actionTypes ?? []
}

export const getActionType = async (dbName: string, actionTypeId: string) => {
  const encoded = encodeURIComponent(dbName)
  const actionEncoded = encodeURIComponent(actionTypeId)
  const data = await requestApi<{ actionType: ActionTypeDetail }>(
    `/api/v1/ontology/${encoded}/action-types/${actionEncoded}`,
    {
      headers: {
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    'Action 정보를 불러오는데 실패했습니다',
  )
  return data.actionType
}

export const simulateAction = async (
  dbName: string,
  actionTypeId: string,
  params: Record<string, unknown>,
) => {
  const encoded = encodeURIComponent(dbName)
  const actionEncoded = encodeURIComponent(actionTypeId)
  const data = await requestApi<SimulationResult>(
    `/api/v1/ontology/${encoded}/action-types/${actionEncoded}/simulate`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
      body: JSON.stringify({ params }),
    },
    '시뮬레이션 실행에 실패했습니다',
  )
  return data
}

export const executeAction = async (
  dbName: string,
  actionTypeId: string,
  params: Record<string, unknown>,
) => {
  const encoded = encodeURIComponent(dbName)
  const actionEncoded = encodeURIComponent(actionTypeId)
  const data = await requestApi<ExecutionResult>(
    `/api/v1/ontology/${encoded}/action-types/${actionEncoded}/execute`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
      body: JSON.stringify({ params }),
    },
    'Action 실행에 실패했습니다',
  )
  return data
}

export const listActionLogs = async (dbName: string, params?: { limit?: number; offset?: number }) => {
  const query = new URLSearchParams()
  if (params?.limit !== undefined) {
    query.set('limit', String(params.limit))
  }
  if (params?.offset !== undefined) {
    query.set('offset', String(params.offset))
  }
  const encoded = encodeURIComponent(dbName)
  const suffix = query.toString()
  const path = suffix
    ? `/api/v1/ontology/${encoded}/action-logs?${suffix}`
    : `/api/v1/ontology/${encoded}/action-logs`
  const data = await requestApi<{ logs: ActionLog[] }>(
    path,
    {
      headers: {
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    'Action 기록을 불러오는데 실패했습니다',
  )
  return data.logs ?? []
}

// === Link Type API ===
export const listLinkTypes = async (dbName: string) => {
  if (isMockMode) {
    const mockData = await loadMockData()
    await mockData.mockDelay(MOCK_DELAY_MS)
    return mockData.mockLinkTypes
  }
  const encoded = encodeURIComponent(dbName)
  const data = await requestApi<{ linkTypes: LinkType[] }>(
    `/api/v1/ontology/${encoded}/link-types`,
    {
      headers: {
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    'Link Type 목록을 불러오는데 실패했습니다',
  )
  return data.linkTypes ?? []
}

export const getLinkType = async (dbName: string, linkTypeId: string) => {
  const encoded = encodeURIComponent(dbName)
  const linkEncoded = encodeURIComponent(linkTypeId)
  const data = await requestApi<{ linkType: LinkTypeDetail }>(
    `/api/v1/ontology/${encoded}/link-types/${linkEncoded}`,
    {
      headers: {
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    'Link Type 정보를 불러오는데 실패했습니다',
  )
  return data.linkType
}

// === Objectify API ===
export const detectRelationships = async (dbName: string, datasetId: string) => {
  const encoded = encodeURIComponent(dbName)
  const datasetEncoded = encodeURIComponent(datasetId)
  const data = await requestApi<DetectedRelationships>(
    `/api/v1/ontology/${encoded}/datasets/${datasetEncoded}/detect-relationships`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': createIdempotencyKey(),
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    '관계 감지에 실패했습니다',
  )
  return data
}

// === UDF (User Defined Function) API ===
export type UdfRecord = {
  udf_id: string
  db_name: string
  name: string
  description?: string
  latest_version: number
  created_at?: string
  updated_at?: string
  code?: string
}

export type UdfVersionRecord = {
  version_id: string
  udf_id: string
  version: number
  code: string
  created_at?: string
}

export const listUdfs = async (dbName: string): Promise<UdfRecord[]> => {
  if (isMockMode) {
    const mockData = await loadMockData()
    await mockData.mockDelay(MOCK_DELAY_MS)
    return mockData.mockUdfs.filter(u => u.db_name === dbName || dbName === 'demo-project')
  }
  const data = await requestApi<UdfRecord[]>(
    `/api/v1/pipelines/udfs?db_name=${encodeURIComponent(dbName)}`,
    {
      method: 'GET',
      headers: {
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
    },
    'UDF 목록 조회에 실패했습니다',
  )
  return data
}

export const createUdf = async (
  dbName: string,
  params: { name: string; code: string; description?: string },
): Promise<UdfRecord> => {
  const data = await requestApi<UdfRecord>(
    `/api/v1/pipelines/udfs?db_name=${encodeURIComponent(dbName)}`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-DB-Name': dbName,
        'X-Project': dbName,
      },
      body: JSON.stringify(params),
    },
    'UDF 생성에 실패했습니다',
  )
  return data
}

export const getUdf = async (udfId: string): Promise<UdfRecord> => {
  const data = await requestApi<UdfRecord>(
    `/api/v1/pipelines/udfs/${encodeURIComponent(udfId)}`,
    { method: 'GET' },
    'UDF 조회에 실패했습니다',
  )
  return data
}

export const createUdfVersion = async (
  udfId: string,
  code: string,
): Promise<UdfVersionRecord> => {
  const data = await requestApi<UdfVersionRecord>(
    `/api/v1/pipelines/udfs/${encodeURIComponent(udfId)}/versions`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code }),
    },
    'UDF 버전 생성에 실패했습니다',
  )
  return data
}

export const getUdfVersion = async (
  udfId: string,
  version: number,
): Promise<UdfVersionRecord> => {
  const data = await requestApi<UdfVersionRecord>(
    `/api/v1/pipelines/udfs/${encodeURIComponent(udfId)}/versions/${version}`,
    { method: 'GET' },
    'UDF 버전 조회에 실패했습니다',
  )
  return data
}
