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

type DatasetUploadPayload = {
  dataset?: DatasetRecord
}

const API_BASE = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? ''

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
  const response = await fetch(buildUrl(path), {
    ...options,
    headers: {
      Accept: 'application/json',
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

export const listDatasets = async (dbName: string) => {
  const data = await requestApi<DatasetListPayload>(
    `/api/v1/pipelines/datasets?db_name=${encodeURIComponent(dbName)}`,
    undefined,
    'Failed to load datasets',
  )
  return data.datasets ?? []
}

export const listPipelines = async (dbName: string) => {
  const data = await requestApi<PipelineListPayload>(
    `/api/v1/pipelines?db_name=${encodeURIComponent(dbName)}`,
    undefined,
    'Failed to load pipelines',
  )
  return data.pipelines ?? []
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
