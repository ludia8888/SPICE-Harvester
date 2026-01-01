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

const parseApiResponse = <T>(payload: ApiResponse<T> | null, fallbackMessage: string) => {
  if (!payload || typeof payload !== 'object') {
    throw new Error(fallbackMessage)
  }
  if (payload.status && payload.status !== 'error') {
    return (payload.data ?? payload) as T
  }
  throw new Error(payload.message || fallbackMessage)
}

const requestApi = async <T>(
  path: string,
  options?: RequestInit,
  fallbackMessage = 'Request failed',
): Promise<T> => {
  const authHeaders = API_TOKEN ? { Authorization: `Bearer ${API_TOKEN}` } : {}
  const userHeaders = {
    ...(API_USER_ID ? { 'X-User-ID': API_USER_ID } : {}),
    ...(API_USER_NAME ? { 'X-User-Name': API_USER_NAME } : {}),
  }
  const response = await fetch(buildUrl(path), {
    ...options,
    headers: {
      Accept: 'application/json',
      ...authHeaders,
      ...userHeaders,
      ...(options?.headers ?? {}),
    },
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
    throw new Error('Only .csv or .xlsx files are supported for structured uploads.')
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
  return data.dataset
}
