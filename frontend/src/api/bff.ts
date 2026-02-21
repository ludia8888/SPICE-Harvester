import type { Language } from '../types/app'
import { useAppStore } from '../store/useAppStore'
import { API_BASE_URL } from './config'
import { LEGACY_ENDPOINT_BLOCKLIST_PATTERNS } from './legacyEndpointBlocklist'

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
  version_id?: string
  lakefs_commit_id?: string
  version_created_at?: string
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

export type FoundryDatasetRecordV2 = {
  rid: string
  name: string
  description?: string | null
  parentFolderRid?: string
  branchName?: string
  sourceType?: string
  createdTime?: string | null
  createdBy?: string
}

export type FoundryDatasetTransactionRecordV2 = {
  rid: string
  datasetRid?: string | null
  transactionType?: string
  status?: string
  createdTime?: string | null
  closedTime?: string | null
}

export type FoundryReadTableResponseV2 = {
  columns: Array<{ name?: string; type?: string } | string>
  rows: unknown[]
  totalRowCount: number
}

export type FoundryConnectionRecordV2 = {
  rid: string
  displayName?: string
  connectionConfiguration?: Record<string, unknown>
  configuration?: Record<string, unknown>
  parentFolderRid?: string
  exportSettings?: Record<string, unknown>
  status?: string
  createdTime?: string | null
  updatedTime?: string | null
}

export type FoundryConnectionListResponseV2 = {
  data: FoundryConnectionRecordV2[]
  nextPageToken?: string | null
}

export type FoundryConnectionTestResultV2 = {
  connectionRid?: string
  status?: string
  message?: string
  details?: Record<string, unknown>
}

export type FoundryConnectionConfigurationResponseV2 = {
  connectionRid?: string
  connectionConfiguration?: Record<string, unknown>
  configuration?: Record<string, unknown>
}

export type FoundryConnectionConfigurationBatchResponseV2 = {
  data: Record<string, Record<string, unknown>>
}

export type FoundryConnectionExportRunRecordV2 = {
  rid: string
  connectionRid?: string
  status?: string
  taskId?: string
  createdTime?: string | null
  startedTime?: string | null
  completedTime?: string | null
  auditLogId?: string
  sideEffectDelivery?: Record<string, unknown>
  writebackStatus?: string
  error?: string
}

export type FoundryConnectionExportRunListResponseV2 = {
  data: FoundryConnectionExportRunRecordV2[]
  nextPageToken?: string | null
}

export type FoundryQueryTypeRecordV2 = {
  apiName?: string
  displayName?: string
  description?: string
  parameters?: Record<string, unknown>
  output?: unknown
  version?: string
  [key: string]: unknown
}

export type FoundryQueryTypeListResponseV2 = {
  data: FoundryQueryTypeRecordV2[]
  nextPageToken?: string | null
}

export type FoundryQueryTypeExecuteResponseV2 = Record<string, unknown> & {
  value?: Record<string, unknown> | unknown[] | null
}

export type FoundryActionProjectPolicyInheritanceV2 = {
  enabled?: boolean
  scope?: string
  subjectType?: string
  subjectId?: string
  requirePolicy?: boolean
}

export type FoundryActionDynamicSecurityV2 = {
  permissionModel?: string
  editsBeyondActions?: boolean
  permissionPolicy?: Record<string, unknown>
  projectPolicyInheritance?: FoundryActionProjectPolicyInheritanceV2
}

export type FoundryActionTypeRecordV2 = {
  apiName?: string
  displayName?: string
  description?: string
  status?: string
  rid?: string
  parameters?: Record<string, unknown>
  operations?: unknown[]
  toolDescription?: string
  targetObjectType?: string
  writebackTarget?: Record<string, unknown>
  conflictPolicy?: string
  validationRules?: unknown[]
  permissionModel?: string
  editsBeyondActions?: boolean
  permissionPolicy?: Record<string, unknown>
  projectPolicyInheritance?: FoundryActionProjectPolicyInheritanceV2
  dynamicSecurity?: FoundryActionDynamicSecurityV2
  [key: string]: unknown
}

export type FoundryActionTypeListResponseV2 = {
  data: FoundryActionTypeRecordV2[]
  nextPageToken?: string | null
}

export type AccessPolicyRecordV1 = {
  policy_id?: string
  db_name?: string
  scope?: string
  subject_type?: string
  subject_id?: string
  policy?: Record<string, unknown>
  status?: string
  created_at?: string
  updated_at?: string
}

export type FoundryConnectivityImportRecordV2 = {
  rid: string
  name?: string
  parentRid?: string
  displayName?: string
  connectionRid?: string
  datasetRid?: string
  branchName?: string | null
  importMode?: string
  allowSchemaChanges?: boolean
  config?: Record<string, unknown>
  markings?: unknown[]
}

export type FoundryConnectivityImportListResponseV2 = {
  data: FoundryConnectivityImportRecordV2[]
  nextPageToken?: string | null
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

export type LineageImpactResponse = {
  affectedReports: number
  affectedPipelines: number
  lastUpdated?: string
  downstream: Array<{ id: string; name: string; type: string }>
}

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

const normalizeApiPathForLegacyGuard = (path: string) => {
  const raw = String(path || '').trim()
  if (!raw) {
    return '/'
  }
  const fromUrl = (() => {
    try {
      return new URL(raw).pathname
    } catch {
      return null
    }
  })()
  if (fromUrl) {
    return fromUrl
  }
  const noQuery = raw.split('?')[0]
  if (noQuery.startsWith('/api/')) {
    return noQuery
  }
  if (noQuery.startsWith('/')) {
    return noQuery
  }
  if (noQuery.startsWith('api/')) {
    return `/${noQuery}`
  }
  return `/api/v1/${noQuery.replace(/^\/+/, '')}`
}

const assertLegacyEndpointNotRemoved = (path: string) => {
  const normalized = normalizeApiPathForLegacyGuard(path)
  if (LEGACY_ENDPOINT_BLOCKLIST_PATTERNS.some((pattern) => pattern.test(normalized))) {
    throw new Error(`Removed legacy endpoint blocked: ${normalized}`)
  }
}

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
  assertLegacyEndpointNotRemoved(path)
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

/* ═══════════════════════════════════════════════════════════════════════════
   BFF v2 API layer (RequestContext-based)
   ─────────────────────────────────────────────────────────────────────────
   The functions below use a RequestContext pattern (language, adminToken)
   and target the full BFF /api/v1 surface.  Functions whose names overlap
   with the legacy mock-based layer above are suffixed with "Ctx".
   ═══════════════════════════════════════════════════════════════════════════ */


export class HttpError extends Error {
  status: number
  detail: unknown
  retryAfter: number | null

  constructor(status: number, message: string, detail: unknown, retryAfter: number | null = null) {
    super(message)
    this.name = 'HttpError'
    this.status = status
    this.detail = detail
    this.retryAfter = retryAfter
  }
}

export type RequestContext = {
  language: Language
  adminToken: string
  adminActor?: string
}

type Bff2SearchParams = Record<string, string | number | boolean | null | undefined>

const bff2BuildApiUrl = (path: string, language: Language, searchParams?: Bff2SearchParams) => {
  const rawPath = String(path || '').trim()
  const normalizedPath = rawPath.replace(/^\/+/, '')
  const base = API_BASE_URL.replace(/\/+$/, '')
  const isAbsoluteApiPath = normalizedPath.startsWith('api/')
  let url: URL

  if (isAbsoluteApiPath) {
    if (base.startsWith('http')) {
      const baseUrl = new URL(base)
      url = new URL(`/${normalizedPath}`, `${baseUrl.protocol}//${baseUrl.host}`)
    } else {
      url = new URL(`/${normalizedPath}`, window.location.origin)
    }
  } else {
    url = base.startsWith('http')
      ? new URL(`${base}/${normalizedPath}`)
      : new URL(`${base}/${normalizedPath}`, window.location.origin)
  }

  url.searchParams.set('lang', language)
  if (searchParams) {
    Object.entries(searchParams).forEach(([key, value]) => {
      if (value === null || value === undefined) {
        return
      }
      url.searchParams.set(key, String(value))
    })
  }

  return url.toString()
}

const bff2BuildHeaders = (language: Language, adminToken: string, json = false) => {
  const headers = new Headers({ 'Accept-Language': language })
  if (adminToken) {
    headers.set('X-Admin-Token', adminToken)
  }
  if (json) {
    headers.set('Content-Type', 'application/json')
  }
  return headers
}

const bff2ParseJson = async (response: Response) => {
  try {
    return (await response.json()) as unknown
  } catch {
    return null
  }
}

const bff2ParseRetryAfterSeconds = (value: string | null) => {
  if (!value) {
    return null
  }
  const numeric = Number(value)
  if (Number.isFinite(numeric)) {
    return Math.max(0, Math.round(numeric))
  }
  const date = new Date(value)
  if (!Number.isNaN(date.valueOf())) {
    const diff = Math.ceil((date.getTime() - Date.now()) / 1000)
    return Math.max(0, diff)
  }
  return null
}

const bff2RequestJson = async <T>(
  path: string,
  init: RequestInit,
  context: RequestContext,
  searchParams?: Bff2SearchParams,
  extraHeaders?: HeadersInit,
): Promise<{ status: number; payload: T | null }> => {
  assertLegacyEndpointNotRemoved(path)
  const isJsonBody = init.body !== undefined && !(init.body instanceof FormData)
  const headers = bff2BuildHeaders(context.language, context.adminToken, isJsonBody)
  if (context.adminActor) {
    headers.set('X-Admin-Actor', context.adminActor)
  }
  if (extraHeaders) {
    const extra = new Headers(extraHeaders)
    extra.forEach((value, key) => headers.set(key, value))
  }

  const response = await fetch(bff2BuildApiUrl(path, context.language, searchParams), {
    ...init,
    headers,
  })

  const payload = (await bff2ParseJson(response)) as T | null

  if (!response.ok) {
    const retryAfter = bff2ParseRetryAfterSeconds(response.headers.get('Retry-After'))
    if (response.status === 401 || response.status === 403 || response.status === 503) {
      useAppStore.getState().setSettingsOpen(true)
    }
    throw new HttpError(response.status, `HTTP ${response.status}`, payload, retryAfter)
  }

  return { status: response.status, payload }
}

const bff2RequestRaw = async (
  path: string,
  init: RequestInit,
  context: RequestContext,
  searchParams?: Bff2SearchParams,
  extraHeaders?: HeadersInit,
): Promise<Response> => {
  assertLegacyEndpointNotRemoved(path)
  const headers = bff2BuildHeaders(context.language, context.adminToken, false)
  if (context.adminActor) {
    headers.set('X-Admin-Actor', context.adminActor)
  }
  if (extraHeaders) {
    const extra = new Headers(extraHeaders)
    extra.forEach((value, key) => headers.set(key, value))
  }

  const response = await fetch(bff2BuildApiUrl(path, context.language, searchParams), {
    ...init,
    headers,
  })

  if (!response.ok) {
    const payload = await bff2ParseJson(response)
    const retryAfter = bff2ParseRetryAfterSeconds(response.headers.get('Retry-After'))
    if (response.status === 401 || response.status === 403 || response.status === 503) {
      useAppStore.getState().setSettingsOpen(true)
    }
    throw new HttpError(response.status, `HTTP ${response.status}`, payload, retryAfter)
  }

  return response
}

const bff2DbContextHeaders = (dbName?: string): HeadersInit | undefined => {
  const normalized = String(dbName ?? '').trim()
  if (!normalized) {
    return undefined
  }
  return {
    'X-DB-Name': normalized,
    'X-Project': normalized,
  }
}

type Bff2DatabaseListResponse = {
  data?: { databases?: Array<{ name?: string }> }
}

type Bff2AcceptedContract = {
  data?: { command_id?: string }
  command_id?: string
}

export type CreateDatabaseInput = {
  name: string
  description?: string
}

export type WriteResult = {
  status: number
  commandId?: string
}

export type CommandResult = {
  command_id: string
  status: 'PENDING' | 'PROCESSING' | 'COMPLETED' | 'FAILED' | 'CANCELLED' | 'RETRYING'
  result?: Record<string, unknown> | null
  error?: string | null
  completed_at?: string | null
  retry_count?: number
}

export const listDatabasesCtx = async (context: RequestContext): Promise<string[]> => {
  const { payload } = await bff2RequestJson<Bff2DatabaseListResponse>(
    'databases',
    { method: 'GET' },
    context,
  )

  const names =
    payload?.data?.databases
      ?.map((db) => db?.name)
      .filter((name): name is string => Boolean(name)) ?? []

  return names
}

export const openDatabase = async (context: RequestContext, dbName: string) => {
  await bff2RequestJson<unknown>(
    `databases/${encodeURIComponent(dbName)}`,
    { method: 'GET' },
    context,
  )
}

export const createDatabaseCtx = async (
  context: RequestContext,
  input: CreateDatabaseInput,
  extraHeaders?: HeadersInit,
): Promise<WriteResult> => {
  const { status, payload } = await bff2RequestJson<Bff2AcceptedContract>(
    'databases',
    { method: 'POST', body: JSON.stringify(input) },
    context,
    undefined,
    extraHeaders,
  )

  const commandId = payload?.data?.command_id ?? payload?.command_id
  return { status, commandId }
}

export const deleteDatabaseCtx = async (
  context: RequestContext,
  dbName: string,
  expectedSeq: number,
  extraHeaders?: HeadersInit,
): Promise<WriteResult> => {
  const { status, payload } = await bff2RequestJson<Bff2AcceptedContract>(
    `databases/${encodeURIComponent(dbName)}`,
    { method: 'DELETE' },
    context,
    { expected_seq: expectedSeq },
    extraHeaders,
  )

  const commandId = payload?.data?.command_id ?? payload?.command_id
  return { status, commandId }
}

export const getDatabaseExpectedSeq = async (
  context: RequestContext,
  dbName: string,
): Promise<number> => {
  const { payload } = await bff2RequestJson<{ data?: { expected_seq?: number } }>(
    `databases/${encodeURIComponent(dbName)}/expected-seq`,
    { method: 'GET' },
    context,
  )

  const value = payload?.data?.expected_seq
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new HttpError(500, 'Invalid expected_seq response', payload)
  }
  return value
}

export type Summary = {
  data?: Record<string, unknown>
}

export const getSummary = async (
  context: RequestContext,
  params: { dbName?: string | null; branch?: string | null },
): Promise<Summary> => {
  const { payload } = await bff2RequestJson<Summary>(
    'summary',
    { method: 'GET' },
    context,
    { db: params.dbName ?? undefined, branch: params.branch ?? undefined },
  )

  return payload ?? {}
}

export const getCommandStatus = async (
  context: RequestContext,
  commandId: string,
): Promise<CommandResult> => {
  const { payload } = await bff2RequestJson<CommandResult>(
    `commands/${encodeURIComponent(commandId)}/status`,
    { method: 'GET' },
    context,
  )

  if (!payload) {
    throw new HttpError(500, 'Command status payload missing', payload)
  }

  return payload
}

export type ApiEnvelope<T = Record<string, unknown>> = {
  status?: string
  message?: string
  data?: T
  [key: string]: unknown
}

type FoundryObjectTypeV2 = {
  apiName?: string
  displayName?: string
  description?: string
  properties?: Record<
    string,
    {
      displayName?: string
      dataType?: { type?: string }
      required?: boolean
    }
  >
}

const asRecord = (value: unknown): Record<string, unknown> | null =>
  value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : null

const toLegacyOntologyShape = (objectType: FoundryObjectTypeV2): Record<string, unknown> => {
  const classId = String(objectType.apiName ?? '').trim()
  const properties = Object.entries(objectType.properties ?? {}).map(([name, spec]) => ({
    name,
    label: String(spec?.displayName ?? name),
    type: String(spec?.dataType?.type ?? 'xsd:string'),
    required: Boolean(spec?.required),
  }))

  return {
    id: classId,
    label: String(objectType.displayName ?? classId),
    description: String(objectType.description ?? ''),
    properties,
    relationships: [],
  }
}

const toLegacyInstanceShape = (value: unknown, classIdHint?: string): Record<string, unknown> => {
  const row = asRecord(value) ?? {}
  const instanceIdRaw = row.__primaryKey ?? row.instance_id ?? row.id ?? row['@id']
  const instanceId = typeof instanceIdRaw === 'string' || typeof instanceIdRaw === 'number'
    ? String(instanceIdRaw)
    : ''
  const classIdRaw = row.__apiName ?? row.class_id ?? row.classId ?? classIdHint
  const classId = typeof classIdRaw === 'string' ? classIdRaw : String(classIdHint ?? '')
  const displayLabelRaw = row.__title ?? row.label ?? row.name ?? instanceId
  const displayLabel = typeof displayLabelRaw === 'string' || typeof displayLabelRaw === 'number'
    ? String(displayLabelRaw)
    : instanceId
  const currentDisplay = asRecord(row.display)
  return {
    ...row,
    instance_id: instanceId,
    id: instanceId || row.id,
    class_id: classId,
    display: {
      ...(currentDisplay ?? {}),
      label: displayLabel,
    },
  }
}

export const createDatasetV2 = async (
  context: RequestContext,
  input: {
    dbName: string
    name: string
    description?: string
    branchName?: string
  },
): Promise<FoundryDatasetRecordV2> => {
  const dbName = String(input.dbName).trim()
  const branchName = String(input.branchName ?? 'main').trim() || 'main'
  const body = {
    name: input.name,
    description: input.description,
    branchName,
    parentFolderRid: `ri.spice.main.folder.${dbName}`,
  }
  const { payload } = await bff2RequestJson<FoundryDatasetRecordV2>(
    '/api/v2/datasets',
    { method: 'POST', body: JSON.stringify(body) },
    context,
    undefined,
    bff2DbContextHeaders(dbName),
  )
  return payload ?? { rid: '', name: input.name, branchName }
}

export const createDatasetTransactionV2 = async (
  context: RequestContext,
  datasetRid: string,
  params?: {
    dbName?: string
    transactionType?: string
    idempotencyKey?: string
  },
): Promise<FoundryDatasetTransactionRecordV2> => {
  const body = {
    transactionType: String(params?.transactionType ?? 'APPEND'),
    idempotencyKey: params?.idempotencyKey,
  }
  const { payload } = await bff2RequestJson<FoundryDatasetTransactionRecordV2>(
    `/api/v2/datasets/${encodeURIComponent(datasetRid)}/transactions`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
    undefined,
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: '', status: 'OPEN' }
}

export const uploadDatasetFileV2 = async (
  context: RequestContext,
  datasetRid: string,
  params: {
    filePath: string
    content: BodyInit
    branchName?: string
    contentType?: string
    dbName?: string
  },
) => {
  const branchName = String(params.branchName ?? 'main').trim() || 'main'
  const extraHeaders = new Headers(bff2DbContextHeaders(params.dbName))
  extraHeaders.set('Content-Type', params.contentType ?? 'application/octet-stream')
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `/api/v2/datasets/${encodeURIComponent(datasetRid)}/files:upload`,
    { method: 'POST', body: params.content },
    context,
    { filePath: params.filePath, branchName },
    extraHeaders,
  )
  return payload ?? {}
}

export const commitDatasetTransactionV2 = async (
  context: RequestContext,
  datasetRid: string,
  transactionRid: string,
  params?: { dbName?: string },
): Promise<FoundryDatasetTransactionRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryDatasetTransactionRecordV2>(
    `/api/v2/datasets/${encodeURIComponent(datasetRid)}/transactions/${encodeURIComponent(transactionRid)}/commit`,
    { method: 'POST' },
    context,
    undefined,
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: transactionRid, status: 'COMMITTED' }
}

export const abortDatasetTransactionV2 = async (
  context: RequestContext,
  datasetRid: string,
  transactionRid: string,
  params?: { dbName?: string },
): Promise<FoundryDatasetTransactionRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryDatasetTransactionRecordV2>(
    `/api/v2/datasets/${encodeURIComponent(datasetRid)}/transactions/${encodeURIComponent(transactionRid)}/abort`,
    { method: 'POST' },
    context,
    undefined,
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: transactionRid, status: 'ABORTED' }
}

export const readDatasetTableV2 = async (
  context: RequestContext,
  datasetRid: string,
  input?: {
    rowLimit?: number
    columns?: string[]
  },
  params?: { dbName?: string },
): Promise<FoundryReadTableResponseV2> => {
  const { payload } = await bff2RequestJson<FoundryReadTableResponseV2>(
    `/api/v2/datasets/${encodeURIComponent(datasetRid)}/readTable`,
    { method: 'POST', body: JSON.stringify(input ?? {}) },
    context,
    undefined,
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { columns: [], rows: [], totalRowCount: 0 }
}

const withConnectivityPreview = (searchParams?: Bff2SearchParams): Bff2SearchParams => ({
  preview: true,
  ...(searchParams ?? {}),
})

const extractConnectivityExecuteRid = (payload: unknown) => {
  if (typeof payload === 'string') {
    return payload
  }
  const record = asRecord(payload)
  if (!record) {
    return ''
  }
  const direct =
    record.rid ??
    record.buildRid ??
    record.build_rid ??
    asRecord(record.data)?.rid ??
    asRecord(record.data)?.buildRid ??
    asRecord(record.data)?.build_rid
  return typeof direct === 'string' ? direct : ''
}

export const createConnectionV2 = async (
  context: RequestContext,
  input: Record<string, unknown>,
  params?: { dbName?: string },
): Promise<FoundryConnectionRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectionRecordV2>(
    '/api/v2/connectivity/connections',
    { method: 'POST', body: JSON.stringify(input) },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return (
    payload ?? {
      rid: '',
      displayName: String(input['displayName'] ?? input['name'] ?? ''),
    }
  )
}

export const getConnectionV2 = async (
  context: RequestContext,
  connectionRid: string,
  params?: { dbName?: string },
): Promise<FoundryConnectionRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectionRecordV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}`,
    { method: 'GET' },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: connectionRid }
}

export const listConnectionsV2 = async (
  context: RequestContext,
  params?: { dbName?: string; pageSize?: number; pageToken?: string | null },
): Promise<FoundryConnectionListResponseV2> => {
  const pageSize = Number.isFinite(params?.pageSize) ? Math.max(1, Math.min(500, Number(params?.pageSize))) : 100
  const pageToken = String(params?.pageToken ?? '').trim() || undefined
  const { payload } = await bff2RequestJson<FoundryConnectionListResponseV2>(
    '/api/v2/connectivity/connections',
    { method: 'GET' },
    context,
    withConnectivityPreview({ pageSize, pageToken }),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { data: [], nextPageToken: null }
}

export const deleteConnectionV2 = async (
  context: RequestContext,
  connectionRid: string,
  params?: { dbName?: string },
): Promise<void> => {
  await bff2RequestJson<unknown>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}`,
    { method: 'DELETE' },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
}

export const testConnectionV2 = async (
  context: RequestContext,
  connectionRid: string,
  params?: { dbName?: string },
): Promise<FoundryConnectionTestResultV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectionTestResultV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/test`,
    { method: 'POST' },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? {}
}

export const getConnectionConfigurationV2 = async (
  context: RequestContext,
  connectionRid: string,
  params?: { dbName?: string },
): Promise<FoundryConnectionConfigurationResponseV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectionConfigurationResponseV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/getConfiguration`,
    { method: 'GET' },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? {}
}

export const getConnectionConfigurationBatchV2 = async (
  context: RequestContext,
  input: Array<{ connectionRid: string }>,
  params?: { dbName?: string },
): Promise<FoundryConnectionConfigurationBatchResponseV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectionConfigurationBatchResponseV2>(
    '/api/v2/connectivity/connections/getConfigurationBatch',
    { method: 'POST', body: JSON.stringify(input) },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { data: {} }
}

export const updateConnectionSecretsV2 = async (
  context: RequestContext,
  connectionRid: string,
  input: Record<string, unknown>,
  params?: { dbName?: string },
): Promise<void> => {
  await bff2RequestJson<unknown>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/updateSecrets`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
}

export const updateConnectionExportSettingsV2 = async (
  context: RequestContext,
  connectionRid: string,
  input: Record<string, unknown>,
  params?: { dbName?: string },
): Promise<void> => {
  await bff2RequestJson<unknown>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/updateExportSettings`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
}

export const createConnectionExportRunV2 = async (
  context: RequestContext,
  connectionRid: string,
  input: Record<string, unknown>,
  params?: { dbName?: string },
): Promise<FoundryConnectionExportRunRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectionExportRunRecordV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/exportRuns`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: '' }
}

export const getConnectionExportRunV2 = async (
  context: RequestContext,
  connectionRid: string,
  exportRunRid: string,
  params?: { dbName?: string },
): Promise<FoundryConnectionExportRunRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectionExportRunRecordV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/exportRuns/${encodeURIComponent(exportRunRid)}`,
    { method: 'GET' },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: exportRunRid }
}

export const listConnectionExportRunsV2 = async (
  context: RequestContext,
  connectionRid: string,
  params?: { dbName?: string; pageSize?: number; pageToken?: string | null },
): Promise<FoundryConnectionExportRunListResponseV2> => {
  const pageSize = Number.isFinite(params?.pageSize) ? Math.max(1, Math.min(500, Number(params?.pageSize))) : 100
  const pageToken = String(params?.pageToken ?? '').trim() || undefined
  const { payload } = await bff2RequestJson<FoundryConnectionExportRunListResponseV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/exportRuns`,
    { method: 'GET' },
    context,
    withConnectivityPreview({ pageSize, pageToken }),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { data: [], nextPageToken: null }
}

export const uploadCustomJdbcDriversV2 = async (
  context: RequestContext,
  connectionRid: string,
  input: {
    fileName: string
    content: BodyInit
    contentType?: string
    dbName?: string
  },
): Promise<FoundryConnectionRecordV2> => {
  const extraHeaders = new Headers(bff2DbContextHeaders(input.dbName))
  extraHeaders.set('Content-Type', input.contentType ?? 'application/octet-stream')
  const { payload } = await bff2RequestJson<FoundryConnectionRecordV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/uploadCustomJdbcDrivers`,
    { method: 'POST', body: input.content },
    context,
    withConnectivityPreview({ fileName: input.fileName }),
    extraHeaders,
  )
  return payload ?? { rid: connectionRid }
}

export const createTableImportV2 = async (
  context: RequestContext,
  connectionRid: string,
  input: Record<string, unknown>,
  params?: { dbName?: string },
): Promise<FoundryConnectivityImportRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectivityImportRecordV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/tableImports`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: '' }
}

export const getTableImportV2 = async (
  context: RequestContext,
  connectionRid: string,
  tableImportRid: string,
  params?: { dbName?: string },
): Promise<FoundryConnectivityImportRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectivityImportRecordV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/tableImports/${encodeURIComponent(tableImportRid)}`,
    { method: 'GET' },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: tableImportRid }
}

export const listTableImportsV2 = async (
  context: RequestContext,
  connectionRid: string,
  params?: { dbName?: string; pageSize?: number; pageToken?: string | null },
): Promise<FoundryConnectivityImportListResponseV2> => {
  const pageSize = Number.isFinite(params?.pageSize) ? Math.max(1, Math.min(500, Number(params?.pageSize))) : 100
  const pageToken = String(params?.pageToken ?? '').trim() || undefined
  const { payload } = await bff2RequestJson<FoundryConnectivityImportListResponseV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/tableImports`,
    { method: 'GET' },
    context,
    withConnectivityPreview({ pageSize, pageToken }),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { data: [], nextPageToken: null }
}

export const replaceTableImportV2 = async (
  context: RequestContext,
  connectionRid: string,
  tableImportRid: string,
  input: Record<string, unknown>,
  params?: { dbName?: string },
): Promise<FoundryConnectivityImportRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectivityImportRecordV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/tableImports/${encodeURIComponent(tableImportRid)}`,
    { method: 'PUT', body: JSON.stringify(input) },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: tableImportRid }
}

export const deleteTableImportV2 = async (
  context: RequestContext,
  connectionRid: string,
  tableImportRid: string,
  params?: { dbName?: string },
): Promise<void> => {
  await bff2RequestJson<unknown>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/tableImports/${encodeURIComponent(tableImportRid)}`,
    { method: 'DELETE' },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
}

export const executeTableImportV2 = async (
  context: RequestContext,
  connectionRid: string,
  tableImportRid: string,
  params?: { dbName?: string },
): Promise<string> => {
  const { payload } = await bff2RequestJson<unknown>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/tableImports/${encodeURIComponent(tableImportRid)}/execute`,
    { method: 'POST' },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return extractConnectivityExecuteRid(payload)
}

export const createFileImportV2 = async (
  context: RequestContext,
  connectionRid: string,
  input: Record<string, unknown>,
  params?: { dbName?: string },
): Promise<FoundryConnectivityImportRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectivityImportRecordV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/fileImports`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: '' }
}

export const getFileImportV2 = async (
  context: RequestContext,
  connectionRid: string,
  fileImportRid: string,
  params?: { dbName?: string },
): Promise<FoundryConnectivityImportRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectivityImportRecordV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/fileImports/${encodeURIComponent(fileImportRid)}`,
    { method: 'GET' },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: fileImportRid }
}

export const listFileImportsV2 = async (
  context: RequestContext,
  connectionRid: string,
  params?: { dbName?: string; pageSize?: number; pageToken?: string | null },
): Promise<FoundryConnectivityImportListResponseV2> => {
  const pageSize = Number.isFinite(params?.pageSize) ? Math.max(1, Math.min(500, Number(params?.pageSize))) : 100
  const pageToken = String(params?.pageToken ?? '').trim() || undefined
  const { payload } = await bff2RequestJson<FoundryConnectivityImportListResponseV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/fileImports`,
    { method: 'GET' },
    context,
    withConnectivityPreview({ pageSize, pageToken }),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { data: [], nextPageToken: null }
}

export const replaceFileImportV2 = async (
  context: RequestContext,
  connectionRid: string,
  fileImportRid: string,
  input: Record<string, unknown>,
  params?: { dbName?: string },
): Promise<FoundryConnectivityImportRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectivityImportRecordV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/fileImports/${encodeURIComponent(fileImportRid)}`,
    { method: 'PUT', body: JSON.stringify(input) },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: fileImportRid }
}

export const deleteFileImportV2 = async (
  context: RequestContext,
  connectionRid: string,
  fileImportRid: string,
  params?: { dbName?: string },
): Promise<void> => {
  await bff2RequestJson<unknown>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/fileImports/${encodeURIComponent(fileImportRid)}`,
    { method: 'DELETE' },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
}

export const executeFileImportV2 = async (
  context: RequestContext,
  connectionRid: string,
  fileImportRid: string,
  params?: { dbName?: string },
): Promise<string> => {
  const { payload } = await bff2RequestJson<unknown>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/fileImports/${encodeURIComponent(fileImportRid)}/execute`,
    { method: 'POST' },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return extractConnectivityExecuteRid(payload)
}

export const createVirtualTableV2 = async (
  context: RequestContext,
  connectionRid: string,
  input: Record<string, unknown>,
  params?: { dbName?: string },
): Promise<FoundryConnectivityImportRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectivityImportRecordV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/virtualTables`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: '' }
}

export const getVirtualTableV2 = async (
  context: RequestContext,
  connectionRid: string,
  virtualTableRid: string,
  params?: { dbName?: string },
): Promise<FoundryConnectivityImportRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectivityImportRecordV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/virtualTables/${encodeURIComponent(virtualTableRid)}`,
    { method: 'GET' },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: virtualTableRid }
}

export const listVirtualTablesV2 = async (
  context: RequestContext,
  connectionRid: string,
  params?: { dbName?: string; pageSize?: number; pageToken?: string | null },
): Promise<FoundryConnectivityImportListResponseV2> => {
  const pageSize = Number.isFinite(params?.pageSize) ? Math.max(1, Math.min(500, Number(params?.pageSize))) : 100
  const pageToken = String(params?.pageToken ?? '').trim() || undefined
  const { payload } = await bff2RequestJson<FoundryConnectivityImportListResponseV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/virtualTables`,
    { method: 'GET' },
    context,
    withConnectivityPreview({ pageSize, pageToken }),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { data: [], nextPageToken: null }
}

export const replaceVirtualTableV2 = async (
  context: RequestContext,
  connectionRid: string,
  virtualTableRid: string,
  input: Record<string, unknown>,
  params?: { dbName?: string },
): Promise<FoundryConnectivityImportRecordV2> => {
  const { payload } = await bff2RequestJson<FoundryConnectivityImportRecordV2>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/virtualTables/${encodeURIComponent(virtualTableRid)}`,
    { method: 'PUT', body: JSON.stringify(input) },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return payload ?? { rid: virtualTableRid }
}

export const deleteVirtualTableV2 = async (
  context: RequestContext,
  connectionRid: string,
  virtualTableRid: string,
  params?: { dbName?: string },
): Promise<void> => {
  await bff2RequestJson<unknown>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/virtualTables/${encodeURIComponent(virtualTableRid)}`,
    { method: 'DELETE' },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
}

export const executeVirtualTableV2 = async (
  context: RequestContext,
  connectionRid: string,
  virtualTableRid: string,
  params?: { dbName?: string },
): Promise<string> => {
  const { payload } = await bff2RequestJson<unknown>(
    `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/virtualTables/${encodeURIComponent(virtualTableRid)}/execute`,
    { method: 'POST' },
    context,
    withConnectivityPreview(),
    bff2DbContextHeaders(params?.dbName),
  )
  return extractConnectivityExecuteRid(payload)
}

export const listDatabaseClasses = async (context: RequestContext, dbName: string) => {
  const { payload } = await bff2RequestJson<{ data?: FoundryObjectTypeV2[] }>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/objectTypes`,
    { method: 'GET' },
    context,
    { branch: 'main', pageSize: 1000 },
  )
  const classes = (payload?.data ?? []).map(toLegacyOntologyShape)
  return {
    status: 'success',
    classes,
    count: classes.length,
    data: { classes },
  }
}

export const listOntology = async (
  context: RequestContext,
  dbName: string,
  branch: string,
) => {
  const { payload } = await bff2RequestJson<{ data?: FoundryObjectTypeV2[] }>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/objectTypes`,
    { method: 'GET' },
    context,
    { branch, pageSize: 1000 },
  )
  const classes = (payload?.data ?? []).map(toLegacyOntologyShape)
  return {
    status: 'success',
    ontologies: classes,
    data: { ontologies: classes },
    branch,
  }
}

export const getOntology = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  branch: string,
) => {
  const { payload } = await bff2RequestJson<FoundryObjectTypeV2>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/objectTypes/${encodeURIComponent(classLabel)}`,
    { method: 'GET' },
    context,
    { branch },
  )
  if (!payload) {
    return {}
  }
  return toLegacyOntologyShape(payload)
}

export const listObjectTypesV2 = async (
  context: RequestContext,
  dbName: string,
  params?: { branch?: string; pageSize?: number },
) => {
  const branch = String(params?.branch ?? 'main').trim() || 'main'
  const pageSize = Number.isFinite(params?.pageSize) ? Math.max(1, Math.min(1000, Number(params?.pageSize))) : 1000
  const { payload } = await bff2RequestJson<{ data?: unknown[]; nextPageToken?: string | null }>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/objectTypes`,
    { method: 'GET' },
    context,
    { branch, pageSize },
  )
  return payload ?? { data: [], nextPageToken: null }
}

export type ObjectTypeContractCreateInputV2 = {
  apiName: string
  status?: string
  primaryKey?: string
  titleProperty?: string
  pkSpec?: Record<string, unknown>
  backingSource?: Record<string, unknown>
  backingSources?: Record<string, unknown>[]
  backingDatasetId?: string
  backingDatasourceId?: string
  backingDatasourceVersionId?: string
  datasetVersionId?: string
  schemaHash?: string
  mappingSpecId?: string
  mappingSpecVersion?: number
  autoGenerateMapping?: boolean
  metadata?: Record<string, unknown>
}

export type ObjectTypeContractUpdateInputV2 = Omit<ObjectTypeContractCreateInputV2, 'apiName'> & {
  migration?: Record<string, unknown>
}

export const createObjectTypeV2 = async (
  context: RequestContext,
  dbName: string,
  input: ObjectTypeContractCreateInputV2,
  params?: { branch?: string; expectedHeadCommit?: string },
) => {
  const branch = String(params?.branch ?? 'main').trim() || 'main'
  const expectedHeadCommit = String(params?.expectedHeadCommit ?? '').trim() || undefined
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/objectTypes`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    { branch, expectedHeadCommit },
  )
  return payload ?? {}
}

export const updateObjectTypeV2 = async (
  context: RequestContext,
  dbName: string,
  objectType: string,
  input: ObjectTypeContractUpdateInputV2,
  params?: { branch?: string; expectedHeadCommit?: string },
) => {
  const branch = String(params?.branch ?? 'main').trim() || 'main'
  const expectedHeadCommit = String(params?.expectedHeadCommit ?? '').trim() || undefined
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/objectTypes/${encodeURIComponent(objectType)}`,
    { method: 'PATCH', body: JSON.stringify(input) },
    context,
    { branch, expectedHeadCommit },
  )
  return payload ?? {}
}

export const getObjectTypeFullMetadataV2 = async (
  context: RequestContext,
  dbName: string,
  objectType: string,
  params?: { branch?: string },
) => {
  const branch = String(params?.branch ?? 'main').trim() || 'main'
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/objectTypes/${encodeURIComponent(objectType)}/fullMetadata`,
    { method: 'GET' },
    context,
    { branch },
  )
  return payload ?? {}
}

export const searchObjectsV2 = async (
  context: RequestContext,
  dbName: string,
  objectType: string,
  input: Record<string, unknown>,
  params?: { branch?: string },
) => {
  const branch = String(params?.branch ?? 'main').trim() || 'main'
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/objects/${encodeURIComponent(objectType)}/search`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    { branch },
  )
  return payload ?? {}
}

export const listActionTypesV2 = async (
  context: RequestContext,
  dbName: string,
  params?: { branch?: string; pageSize?: number },
): Promise<FoundryActionTypeListResponseV2> => {
  const branch = String(params?.branch ?? 'main').trim() || 'main'
  const pageSize = Number.isFinite(params?.pageSize) ? Math.max(1, Math.min(1000, Number(params?.pageSize))) : 100
  const { payload } = await bff2RequestJson<Record<string, unknown> | unknown[]>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/actionTypes`,
    { method: 'GET' },
    context,
    { branch, pageSize },
  )
  if (Array.isArray(payload)) {
    return { data: payload as FoundryActionTypeRecordV2[], nextPageToken: null }
  }

  const record = payload && typeof payload === 'object' ? payload : {}
  const container =
    record && typeof record === 'object' && !Array.isArray(record.data) && record.data && typeof record.data === 'object'
      ? (record.data as Record<string, unknown>)
      : null

  const data = Array.isArray((record as Record<string, unknown>).data)
    ? ((record as Record<string, unknown>).data as FoundryActionTypeRecordV2[])
    : (Array.isArray(container?.actionTypes) ? (container?.actionTypes as FoundryActionTypeRecordV2[]) : [])

  const nextPageTokenRaw =
    (record as Record<string, unknown>).nextPageToken ??
    (container ? container.nextPageToken : null)
  const nextPageToken =
    typeof nextPageTokenRaw === 'string' ? nextPageTokenRaw : (nextPageTokenRaw == null ? null : String(nextPageTokenRaw))

  return { data, nextPageToken }
}

export const getActionTypeV2 = async (
  context: RequestContext,
  dbName: string,
  actionType: string,
  params?: { branch?: string },
): Promise<FoundryActionTypeRecordV2> => {
  const branch = String(params?.branch ?? 'main').trim() || 'main'
  const { payload } = await bff2RequestJson<FoundryActionTypeRecordV2>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/actionTypes/${encodeURIComponent(actionType)}`,
    { method: 'GET' },
    context,
    { branch },
  )
  return payload ?? {}
}

export const listQueryTypesV2 = async (
  context: RequestContext,
  dbName: string,
  params?: { pageSize?: number; pageToken?: string | null },
): Promise<FoundryQueryTypeListResponseV2> => {
  const pageSize = Number.isFinite(params?.pageSize) ? Math.max(1, Math.min(1000, Number(params?.pageSize))) : 100
  const pageToken = String(params?.pageToken ?? '').trim() || undefined
  const { payload } = await bff2RequestJson<Record<string, unknown> | unknown[]>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/queryTypes`,
    { method: 'GET' },
    context,
    { pageSize, pageToken },
  )
  if (Array.isArray(payload)) {
    return { data: payload as FoundryQueryTypeRecordV2[], nextPageToken: null }
  }

  const record = payload && typeof payload === 'object' ? payload : {}
  const container =
    record && typeof record === 'object' && !Array.isArray(record.data) && record.data && typeof record.data === 'object'
      ? (record.data as Record<string, unknown>)
      : null

  const data = Array.isArray((record as Record<string, unknown>).data)
    ? ((record as Record<string, unknown>).data as FoundryQueryTypeRecordV2[])
    : (Array.isArray(container?.queryTypes) ? (container?.queryTypes as FoundryQueryTypeRecordV2[]) : [])

  const nextPageTokenRaw =
    (record as Record<string, unknown>).nextPageToken ??
    (container ? container.nextPageToken : null)
  const nextPageToken =
    typeof nextPageTokenRaw === 'string' ? nextPageTokenRaw : (nextPageTokenRaw == null ? null : String(nextPageTokenRaw))

  return { data, nextPageToken }
}

export const getQueryTypeV2 = async (
  context: RequestContext,
  dbName: string,
  queryApiName: string,
  params?: { version?: string | null },
): Promise<FoundryQueryTypeRecordV2> => {
  const version = String(params?.version ?? '').trim() || undefined
  const { payload } = await bff2RequestJson<FoundryQueryTypeRecordV2>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/queryTypes/${encodeURIComponent(queryApiName)}`,
    { method: 'GET' },
    context,
    { version },
  )
  return payload ?? {}
}

export const executeQueryTypeV2 = async (
  context: RequestContext,
  dbName: string,
  queryApiName: string,
  input?: {
    parameters?: Record<string, unknown>
    options?: Record<string, unknown>
  },
  params?: { version?: string | null },
): Promise<FoundryQueryTypeExecuteResponseV2> => {
  const version = String(params?.version ?? '').trim() || undefined
  const { payload } = await bff2RequestJson<FoundryQueryTypeExecuteResponseV2>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/queries/${encodeURIComponent(queryApiName)}/execute`,
    {
      method: 'POST',
      body: JSON.stringify({
        parameters: input?.parameters ?? {},
        options: input?.options ?? {},
      }),
    },
    context,
    { version },
  )
  return payload ?? {}
}

export const upsertAccessPolicyV1 = async (
  context: RequestContext,
  input: {
    db_name: string
    scope?: string
    subject_type: string
    subject_id: string
    policy: Record<string, unknown>
    status?: string
  },
): Promise<AccessPolicyRecordV1> => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    '/api/v1/access-policies',
    {
      method: 'POST',
      body: JSON.stringify(input),
    },
    context,
  )
  const root = payload && typeof payload === 'object' ? payload : {}
  const data = root.data && typeof root.data === 'object' ? (root.data as Record<string, unknown>) : {}
  const record = data.access_policy && typeof data.access_policy === 'object'
    ? (data.access_policy as AccessPolicyRecordV1)
    : (data as AccessPolicyRecordV1)
  return record ?? {}
}

export const listAccessPoliciesV1 = async (
  context: RequestContext,
  params?: {
    db_name?: string
    scope?: string
    subject_type?: string
    subject_id?: string
    status?: string
  },
): Promise<AccessPolicyRecordV1[]> => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    '/api/v1/access-policies',
    { method: 'GET' },
    context,
    params ?? {},
  )
  const root = payload && typeof payload === 'object' ? payload : {}
  const data = root.data && typeof root.data === 'object' ? (root.data as Record<string, unknown>) : {}
  const rows = Array.isArray(data.access_policies)
    ? (data.access_policies as AccessPolicyRecordV1[])
    : (Array.isArray(root.access_policies) ? (root.access_policies as AccessPolicyRecordV1[]) : [])
  return rows
}

export const listOntologyResourcesV1 = async (
  context: RequestContext,
  dbName: string,
  resourceType: string,
  params?: { branch?: string; limit?: number; offset?: number },
) => {
  const branch = String(params?.branch ?? 'main').trim() || 'main'
  const limit = Number.isFinite(params?.limit) ? Math.max(1, Math.min(1000, Number(params?.limit))) : 200
  const offset = Number.isFinite(params?.offset) ? Math.max(0, Number(params?.offset)) : 0
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `databases/${encodeURIComponent(dbName)}/ontology/resources/${encodeURIComponent(resourceType)}`,
    { method: 'GET' },
    context,
    { branch, limit, offset },
  )
  return payload ?? {}
}

export const createOntologyResourceV1 = async (
  context: RequestContext,
  dbName: string,
  resourceType: string,
  input: Record<string, unknown>,
  params?: { branch?: string; expectedHeadCommit?: string },
) => {
  const branch = String(params?.branch ?? 'main').trim() || 'main'
  const expectedHeadCommit = String(
    params?.expectedHeadCommit ?? (branch.startsWith('branch:') ? branch : `branch:${branch}`),
  ).trim()
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `databases/${encodeURIComponent(dbName)}/ontology/resources/${encodeURIComponent(resourceType)}`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    { branch, expected_head_commit: expectedHeadCommit },
  )
  return payload ?? {}
}

export const getOntologyResourceV1 = async (
  context: RequestContext,
  dbName: string,
  resourceType: string,
  resourceId: string,
  params?: { branch?: string },
) => {
  const branch = String(params?.branch ?? 'main').trim() || 'main'
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `databases/${encodeURIComponent(dbName)}/ontology/resources/${encodeURIComponent(resourceType)}/${encodeURIComponent(resourceId)}`,
    { method: 'GET' },
    context,
    { branch },
  )
  return payload ?? {}
}

export const updateOntologyResourceV1 = async (
  context: RequestContext,
  dbName: string,
  resourceType: string,
  resourceId: string,
  input: Record<string, unknown>,
  params?: { branch?: string; expectedHeadCommit?: string },
) => {
  const branch = String(params?.branch ?? 'main').trim() || 'main'
  const expectedHeadCommit = String(
    params?.expectedHeadCommit ?? (branch.startsWith('branch:') ? branch : `branch:${branch}`),
  ).trim()
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `databases/${encodeURIComponent(dbName)}/ontology/resources/${encodeURIComponent(resourceType)}/${encodeURIComponent(resourceId)}`,
    { method: 'PUT', body: JSON.stringify(input) },
    context,
    { branch, expected_head_commit: expectedHeadCommit },
  )
  return payload ?? {}
}

export const deleteOntologyResourceV1 = async (
  context: RequestContext,
  dbName: string,
  resourceType: string,
  resourceId: string,
  params?: { branch?: string; expectedHeadCommit?: string },
) => {
  const branch = String(params?.branch ?? 'main').trim() || 'main'
  const expectedHeadCommit = String(
    params?.expectedHeadCommit ?? (branch.startsWith('branch:') ? branch : `branch:${branch}`),
  ).trim()
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `databases/${encodeURIComponent(dbName)}/ontology/resources/${encodeURIComponent(resourceType)}/${encodeURIComponent(resourceId)}`,
    { method: 'DELETE' },
    context,
    { branch, expected_head_commit: expectedHeadCommit },
  )
  return payload ?? {}
}

export const recordOntologyDeploymentV1 = async (
  context: RequestContext,
  dbName: string,
  input?: {
    target_branch?: string
    ontology_commit_id?: string
    snapshot_rid?: string
    deployed_by?: string
    metadata?: Record<string, unknown>
  },
) => {
  const branch = String(input?.target_branch ?? 'main').trim() || 'main'
  const commitId = String(
    input?.ontology_commit_id ?? (branch.startsWith('branch:') ? branch : `branch:${branch}`),
  ).trim()
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `databases/${encodeURIComponent(dbName)}/ontology/records/deployments`,
    {
      method: 'POST',
      body: JSON.stringify({
        target_branch: branch,
        ontology_commit_id: commitId,
        snapshot_rid: input?.snapshot_rid ?? null,
        deployed_by: input?.deployed_by ?? 'system',
        metadata: input?.metadata ?? {},
      }),
    },
    context,
  )
  return payload ?? {}
}

export type ActionApplyResponseV2 = Record<string, unknown> & {
  auditLogId: string | null
  sideEffectDelivery: unknown | null
  writebackStatus: 'confirmed' | 'missing' | 'not_configured'
}

const normalizeActionApplyResponseV2 = (payload: Record<string, unknown> | null): ActionApplyResponseV2 => {
  const record = payload ?? {}
  const nested = asRecord(record.data)
  const auditLogIdCandidate = (
    record.auditLogId ??
    record.action_log_id ??
    record.command_id ??
    nested?.auditLogId ??
    nested?.action_log_id ??
    nested?.command_id ??
    null
  )
  const auditLogId = String(auditLogIdCandidate ?? '').trim() || null
  const sideEffectDelivery = (record.sideEffectDelivery ?? nested?.sideEffectDelivery ?? null) as unknown
  const rawWritebackStatus = String(record.writebackStatus ?? nested?.writebackStatus ?? '').trim().toLowerCase()
  const writebackStatus: ActionApplyResponseV2['writebackStatus'] =
    rawWritebackStatus === 'confirmed' || rawWritebackStatus === 'missing' || rawWritebackStatus === 'not_configured'
      ? rawWritebackStatus
      : (auditLogId || sideEffectDelivery !== null ? 'confirmed' : 'not_configured')

  return {
    ...record,
    auditLogId,
    action_log_id: auditLogId,
    sideEffectDelivery,
    writebackStatus,
  }
}

export const applyActionV2 = async (
  context: RequestContext,
  dbName: string,
  actionType: string,
  input: Record<string, unknown>,
  params?: { branch?: string },
): Promise<ActionApplyResponseV2> => {
  const branch = String(params?.branch ?? 'main').trim() || 'main'
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/actions/${encodeURIComponent(actionType)}/apply`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    { branch },
  )
  return normalizeActionApplyResponseV2(payload)
}

export const createObjectifyMappingSpec = async (
  context: RequestContext,
  input: Record<string, unknown>,
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'objectify/mapping-specs',
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export type RunObjectifyDatasetInput = Record<string, unknown> & {
  dataset_version_id: string
}

export const runObjectifyDataset = async (
  context: RequestContext,
  datasetId: string,
  input: RunObjectifyDatasetInput,
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `objectify/datasets/${encodeURIComponent(datasetId)}/run`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export const getMappingsSummary = async (context: RequestContext, dbName: string) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `databases/${encodeURIComponent(dbName)}/mappings/`,
    { method: 'GET' },
    context,
  )
  return payload ?? {}
}

export const exportMappings = async (context: RequestContext, dbName: string) => {
  const response = await bff2RequestRaw(
    `databases/${encodeURIComponent(dbName)}/mappings/export`,
    { method: 'POST' },
    context,
  )
  const blob = await response.blob()
  const disposition = response.headers.get('Content-Disposition') ?? ''
  return { blob, disposition }
}

export const validateMappings = async (
  context: RequestContext,
  dbName: string,
  file: File,
) => {
  const body = new FormData()
  body.append('file', file)
  const { payload } = await bff2RequestJson<ApiEnvelope>(
    `databases/${encodeURIComponent(dbName)}/mappings/validate`,
    { method: 'POST', body },
    context,
  )
  return payload ?? {}
}

export const importMappings = async (
  context: RequestContext,
  dbName: string,
  file: File,
) => {
  const body = new FormData()
  body.append('file', file)
  const { payload } = await bff2RequestJson<ApiEnvelope>(
    `databases/${encodeURIComponent(dbName)}/mappings/import`,
    { method: 'POST', body },
    context,
  )
  return payload ?? {}
}

export const clearMappings = async (context: RequestContext, dbName: string) => {
  const { payload } = await bff2RequestJson<ApiEnvelope>(
    `databases/${encodeURIComponent(dbName)}/mappings/`,
    { method: 'DELETE' },
    context,
  )
  return payload ?? {}
}

export const suggestMappingsFromExcel = async (
  context: RequestContext,
  dbName: string,
  params: { target_class_id: string; sheet_name?: string; table_id?: string; table_top?: number; table_left?: number; table_bottom?: number; table_right?: number },
  file: File,
  targetSchemaJson?: string,
) => {
  const body = new FormData()
  body.append('file', file)
  if (targetSchemaJson) {
    body.append('target_schema_json', targetSchemaJson)
  }
  if (params.sheet_name) {
    body.append('sheet_name', params.sheet_name)
  }
  if (params.table_id) {
    body.append('table_id', params.table_id)
  }
  if (params.table_top !== undefined) {
    body.append('table_top', String(params.table_top))
    body.append('table_left', String(params.table_left ?? 0))
    body.append('table_bottom', String(params.table_bottom ?? 0))
    body.append('table_right', String(params.table_right ?? 0))
  }
  const { payload } = await bff2RequestJson<ApiEnvelope>(
    `databases/${encodeURIComponent(dbName)}/suggest-mappings-from-excel`,
    { method: 'POST', body },
    context,
    { target_class_id: params.target_class_id },
  )
  return payload ?? {}
}

export const dryRunImportFromExcel = async (
  context: RequestContext,
  dbName: string,
  payload: {
    file: File
    target_class_id: string
    target_schema_json: string
    mappings_json: string
    sheet_name?: string
    table_id?: string
    table_top?: number
    table_left?: number
    table_bottom?: number
    table_right?: number
    max_tables?: number
    max_rows?: number
    max_cols?: number
    dry_run_rows?: number
    max_import_rows?: number
    options_json?: string
  },
) => {
  const body = new FormData()
  body.append('file', payload.file)
  body.append('target_class_id', payload.target_class_id)
  body.append('target_schema_json', payload.target_schema_json)
  body.append('mappings_json', payload.mappings_json)
  if (payload.sheet_name) body.append('sheet_name', payload.sheet_name)
  if (payload.table_id) body.append('table_id', payload.table_id)
  if (payload.table_top !== undefined) {
    body.append('table_top', String(payload.table_top))
    body.append('table_left', String(payload.table_left ?? 0))
    body.append('table_bottom', String(payload.table_bottom ?? 0))
    body.append('table_right', String(payload.table_right ?? 0))
  }
  if (payload.max_tables !== undefined) body.append('max_tables', String(payload.max_tables))
  if (payload.max_rows !== undefined) body.append('max_rows', String(payload.max_rows))
  if (payload.max_cols !== undefined) body.append('max_cols', String(payload.max_cols))
  if (payload.dry_run_rows !== undefined) body.append('dry_run_rows', String(payload.dry_run_rows))
  if (payload.max_import_rows !== undefined) body.append('max_import_rows', String(payload.max_import_rows))
  if (payload.options_json) body.append('options_json', payload.options_json)

  const { payload: result } = await bff2RequestJson<ApiEnvelope>(
    `databases/${encodeURIComponent(dbName)}/import-from-excel/dry-run`,
    { method: 'POST', body },
    context,
  )
  return result ?? {}
}

export const commitImportFromExcel = async (
  context: RequestContext,
  dbName: string,
  payload: {
    file: File
    target_class_id: string
    target_schema_json: string
    mappings_json: string
    sheet_name?: string
    table_id?: string
    table_top?: number
    table_left?: number
    table_bottom?: number
    table_right?: number
    max_tables?: number
    max_rows?: number
    max_cols?: number
    max_import_rows?: number
    options_json?: string
  },
) => {
  const body = new FormData()
  body.append('file', payload.file)
  body.append('target_class_id', payload.target_class_id)
  body.append('target_schema_json', payload.target_schema_json)
  body.append('mappings_json', payload.mappings_json)
  if (payload.sheet_name) body.append('sheet_name', payload.sheet_name)
  if (payload.table_id) body.append('table_id', payload.table_id)
  if (payload.table_top !== undefined) {
    body.append('table_top', String(payload.table_top))
    body.append('table_left', String(payload.table_left ?? 0))
    body.append('table_bottom', String(payload.table_bottom ?? 0))
    body.append('table_right', String(payload.table_right ?? 0))
  }
  if (payload.max_tables !== undefined) body.append('max_tables', String(payload.max_tables))
  if (payload.max_rows !== undefined) body.append('max_rows', String(payload.max_rows))
  if (payload.max_cols !== undefined) body.append('max_cols', String(payload.max_cols))
  if (payload.max_import_rows !== undefined) body.append('max_import_rows', String(payload.max_import_rows))
  if (payload.options_json) body.append('options_json', payload.options_json)

  const { payload: result } = await bff2RequestJson<ApiEnvelope>(
    `databases/${encodeURIComponent(dbName)}/import-from-excel/commit`,
    { method: 'POST', body },
    context,
  )
  return result ?? {}
}

export const suggestSchemaFromData = async (
  context: RequestContext,
  dbName: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await bff2RequestJson<ApiEnvelope>(
    `databases/${encodeURIComponent(dbName)}/suggest-schema-from-data`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export const suggestSchemaFromExcel = async (
  context: RequestContext,
  dbName: string,
  file: File,
  params?: {
    sheet_name?: string
    class_name?: string
    table_id?: string
    table_top?: number
    table_left?: number
    table_bottom?: number
    table_right?: number
    include_complex_types?: boolean
    max_tables?: number
    max_rows?: number
    max_cols?: number
  },
) => {
  const body = new FormData()
  body.append('file', file)
  if (params?.sheet_name) body.append('sheet_name', params.sheet_name)
  if (params?.class_name) body.append('class_name', params.class_name)
  if (params?.table_id) body.append('table_id', params.table_id)
  if (params?.table_top !== undefined) {
    body.append('table_top', String(params.table_top))
    body.append('table_left', String(params.table_left ?? 0))
    body.append('table_bottom', String(params.table_bottom ?? 0))
    body.append('table_right', String(params.table_right ?? 0))
  }
  if (params?.include_complex_types !== undefined) {
    body.append('include_complex_types', String(params.include_complex_types))
  }
  if (params?.max_tables !== undefined) body.append('max_tables', String(params.max_tables))
  if (params?.max_rows !== undefined) body.append('max_rows', String(params.max_rows))
  if (params?.max_cols !== undefined) body.append('max_cols', String(params.max_cols))
  const { payload } = await bff2RequestJson<ApiEnvelope>(
    `databases/${encodeURIComponent(dbName)}/suggest-schema-from-excel`,
    { method: 'POST', body },
    context,
  )
  return payload ?? {}
}

export const listInstancesCtx = async (
  context: RequestContext,
  dbName: string,
  classId: string,
  params: { limit?: number; offset?: number; search?: string },
) => {
  const limit = Number.isFinite(params?.limit) ? Math.max(1, Math.min(500, Number(params.limit))) : 100
  const offset = Number.isFinite(params?.offset) ? Math.max(0, Number(params.offset)) : 0
  const { payload } = await bff2RequestJson<{ data?: unknown[]; totalCount?: string | number }>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/objects/${encodeURIComponent(classId)}/search`,
    { method: 'POST', body: JSON.stringify({ pageSize: Math.max(200, limit + offset) }) },
    context,
    { branch: 'main' },
  )
  const raw = Array.isArray(payload?.data) ? payload.data : []
  const normalized = raw.map((item) => toLegacyInstanceShape(item, classId))
  const search = String(params?.search ?? '').trim().toLowerCase()
  const filtered = search
    ? normalized.filter((item) => JSON.stringify(item).toLowerCase().includes(search))
    : normalized
  const paged = filtered.slice(offset, offset + limit)
  const totalCount = payload?.totalCount
  const totalFromPayload =
    typeof totalCount === 'number'
      ? totalCount
      : (typeof totalCount === 'string' && Number.isFinite(Number(totalCount)) ? Number(totalCount) : filtered.length)
  return {
    status: 'success',
    instances: paged,
    total: totalFromPayload,
    limit,
    offset,
    data: {
      instances: paged,
      total: totalFromPayload,
      limit,
      offset,
    },
  }
}

export const getInstanceCtx = async (
  context: RequestContext,
  dbName: string,
  classId: string,
  instanceId: string,
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/objects/${encodeURIComponent(classId)}/${encodeURIComponent(instanceId)}`,
    { method: 'GET' },
    context,
    { branch: 'main' },
  )
  const normalized = toLegacyInstanceShape(payload, classId)
  return {
    status: 'success',
    data: normalized,
  }
}

export const getSampleValues = async (
  context: RequestContext,
  dbName: string,
  classId: string,
) => {
  const { payload } = await bff2RequestJson<{ data?: unknown[] }>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/objects/${encodeURIComponent(classId)}/search`,
    { method: 'POST', body: JSON.stringify({ pageSize: 200 }) },
    context,
    { branch: 'main' },
  )
  const records = Array.isArray(payload?.data) ? payload.data.map((item) => asRecord(item) ?? {}) : []
  const samples: Record<string, unknown[]> = {}
  records.forEach((row) => {
    Object.entries(row).forEach(([key, rawValue]) => {
      if (key.startsWith('__')) {
        return
      }
      if (rawValue === null || rawValue === undefined || typeof rawValue === 'object') {
        return
      }
      if (!samples[key]) {
        samples[key] = []
      }
      const nextValue = String(rawValue)
      if (!samples[key].includes(nextValue) && samples[key].length < 12) {
        samples[key].push(nextValue)
      }
    })
  })
  return {
    status: 'success',
    data: {
      samples,
      count: records.length,
    },
  }
}

export const createInstance = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  branch: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await bff2RequestJson<CommandResult>(
    `databases/${encodeURIComponent(dbName)}/instances/${encodeURIComponent(classLabel)}/create`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    { branch },
  )
  return payload ?? ({} as CommandResult)
}

export const updateInstance = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  instanceId: string,
  branch: string,
  expectedSeq: number,
  input: Record<string, unknown>,
) => {
  const { payload } = await bff2RequestJson<CommandResult>(
    `databases/${encodeURIComponent(dbName)}/instances/${encodeURIComponent(classLabel)}/${encodeURIComponent(instanceId)}/update`,
    { method: 'PUT', body: JSON.stringify(input) },
    context,
    { branch, expected_seq: expectedSeq },
  )
  return payload ?? ({} as CommandResult)
}

export const deleteInstance = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  instanceId: string,
  branch: string,
  expectedSeq: number,
) => {
  const { payload } = await bff2RequestJson<CommandResult>(
    `databases/${encodeURIComponent(dbName)}/instances/${encodeURIComponent(classLabel)}/${encodeURIComponent(instanceId)}/delete`,
    { method: 'DELETE' },
    context,
    { branch, expected_seq: expectedSeq },
  )
  return payload ?? ({} as CommandResult)
}

export const bulkCreateInstances = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  branch: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await bff2RequestJson<CommandResult>(
    `databases/${encodeURIComponent(dbName)}/instances/${encodeURIComponent(classLabel)}/bulk-create`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    { branch },
  )
  return payload ?? ({} as CommandResult)
}

export const runGraphQueryCtx = async (
  context: RequestContext,
  dbName: string,
  branch: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `graph-query/${encodeURIComponent(dbName)}`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    { branch },
  )
  return payload ?? {}
}

export const getGraphPaths = async (
  context: RequestContext,
  dbName: string,
  params: { source_class: string; target_class?: string; max_depth?: number; branch?: string },
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `graph-query/${encodeURIComponent(dbName)}/paths`,
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getGraphHealth = async (context: RequestContext) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'graph-query/health',
    { method: 'GET' },
    context,
  )
  return payload ?? {}
}

export const queryBuilderInfo = async (context: RequestContext, dbName: string) => {
  const classesPayload = await listDatabaseClasses(context, dbName)
  return {
    status: 'success',
    source: 'v2_object_search_adapter',
    operators: {
      common: ['=', '!=', 'IN', 'IS_NULL', 'IS_NOT_NULL', 'LIKE', 'STARTS_WITH', 'CONTAINS'],
      string: ['=', '!=', 'LIKE', 'STARTS_WITH', 'CONTAINS', 'IS_NULL', 'IS_NOT_NULL'],
      number: ['=', '!=', 'IN', 'IS_NULL', 'IS_NOT_NULL'],
      boolean: ['=', '!=', 'IS_NULL', 'IS_NOT_NULL'],
    },
    classes: (classesPayload as { classes?: unknown[] }).classes ?? [],
  }
}

export const runQuery = async (
  context: RequestContext,
  dbName: string,
  input: Record<string, unknown>,
) => {
  const classLabelRaw = input.class_label ?? input.classId ?? input.objectType
  const classLabel = typeof classLabelRaw === 'string' ? classLabelRaw.trim() : ''
  if (!classLabel) {
    throw new HttpError(400, 'class_label is required for query', {
      errorCode: 'INVALID_ARGUMENT',
    })
  }

  const normalizeWhereClause = (filter: unknown): Record<string, unknown> | null => {
    const row = asRecord(filter)
    if (!row) {
      return null
    }
    const field = String(row.field ?? '').trim()
    const operator = String(row.operator ?? '').trim().toLowerCase()
    const value = row.value
    if (!field || !operator) {
      return null
    }

    if (operator === 'eq' || operator === '=') {
      return { type: 'eq', field, value }
    }
    if (operator === 'like') {
      const text = String(value ?? '').replace(/%/g, '').trim()
      if (!text) return null
      return { type: 'containsAnyTerm', field, value: text }
    }
    if (operator === 'starts_with') {
      return { type: 'startsWith', field, value: String(value ?? '') }
    }
    if (operator === 'contains') {
      return { type: 'containsAnyTerm', field, value: String(value ?? '') }
    }
    if (operator === 'in') {
      const list = Array.isArray(value) ? value : String(value ?? '').split(',').map((item) => item.trim()).filter(Boolean)
      if (list.length === 0) {
        return null
      }
      return { type: 'in', field, value: list }
    }
    if (operator === 'is_null') {
      return { type: 'isNull', field, value: true }
    }
    if (operator === 'is_not_null') {
      return { type: 'isNull', field, value: false }
    }
    return null
  }

  const filtersRaw = Array.isArray(input.filters) ? input.filters : []
  const whereClauses = filtersRaw.map((item) => normalizeWhereClause(item)).filter((item): item is Record<string, unknown> => Boolean(item))
  const where =
    whereClauses.length === 0
      ? undefined
      : (whereClauses.length === 1 ? whereClauses[0] : { type: 'and', value: whereClauses })

  const limitRaw = Number(input.limit)
  const pageSize = Number.isFinite(limitRaw) ? Math.max(1, Math.min(500, limitRaw)) : 100

  const select = Array.isArray(input.select)
    ? input.select.map((item) => String(item)).filter(Boolean)
    : []

  const orderField = typeof input.order_by === 'string' ? input.order_by.trim() : ''
  const orderDirection = String(input.order_direction ?? 'asc').toLowerCase() === 'desc' ? 'desc' : 'asc'
  const orderBy = orderField
    ? {
        orderType: 'fields',
        fields: [{ field: orderField, direction: orderDirection }],
      }
    : undefined

  const searchPayload: Record<string, unknown> = {
    pageSize,
    ...(where ? { where } : {}),
    ...(select.length ? { select } : {}),
    ...(orderBy ? { orderBy } : {}),
  }

  const { payload } = await bff2RequestJson<{ data?: unknown[]; totalCount?: string | number }>(
    `/api/v2/ontologies/${encodeURIComponent(dbName)}/objects/${encodeURIComponent(classLabel)}/search`,
    { method: 'POST', body: JSON.stringify(searchPayload) },
    context,
    { branch: 'main' },
  )

  const records = Array.isArray(payload?.data) ? payload.data.map((item) => toLegacyInstanceShape(item, classLabel)) : []
  const totalCount = payload?.totalCount
  const total =
    typeof totalCount === 'number'
      ? totalCount
      : (typeof totalCount === 'string' && Number.isFinite(Number(totalCount)) ? Number(totalCount) : records.length)

  return {
    status: 'success',
    results: records,
    total,
    data: { results: records, total },
  }
}

export const listAuditLogs = async (
  context: RequestContext,
  params: Record<string, unknown>,
) => {
  const { payload } = await bff2RequestJson<ApiEnvelope>(
    'audit/logs',
    { method: 'GET' },
    context,
    params as Bff2SearchParams,
  )
  return payload ?? {}
}

export const getAuditChainHead = async (
  context: RequestContext,
  partitionKey: string,
) => {
  const { payload } = await bff2RequestJson<ApiEnvelope>(
    'audit/chain-head',
    { method: 'GET' },
    context,
    { partition_key: partitionKey },
  )
  return payload ?? {}
}

export const getLineageGraphCtx = async (
  context: RequestContext,
  params: {
    root: string
    db_name: string
    branch?: string
    direction?: 'upstream' | 'downstream' | 'both'
    max_depth?: number
    max_nodes?: number
    max_edges?: number
    as_of?: string
  },
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'lineage/graph',
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getLineageImpactCtx = async (
  context: RequestContext,
  params: {
    root: string
    db_name: string
    branch?: string
    direction?: 'upstream' | 'downstream' | 'both'
    max_depth?: number
    max_nodes?: number
    max_edges?: number
    as_of?: string
    artifact_kind?: string
  },
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'lineage/impact',
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getLineageMetrics = async (
  context: RequestContext,
  params: { db_name: string; branch?: string; window_minutes?: number },
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'lineage/metrics',
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getLineagePathCtx = async (
  context: RequestContext,
  params: {
    source: string
    target: string
    db_name: string
    branch?: string
    direction?: 'upstream' | 'downstream' | 'both'
    max_depth?: number
    max_nodes?: number
    max_edges?: number
  },
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'lineage/path',
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getLineageDiffCtx = async (
  context: RequestContext,
  params: {
    root: string
    from_as_of: string
    db_name: string
    to_as_of?: string
    branch?: string
    direction?: 'upstream' | 'downstream' | 'both'
    max_depth?: number
    max_nodes?: number
    max_edges?: number
  },
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'lineage/diff',
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getLineageRunsCtx = async (
  context: RequestContext,
  params: {
    db_name: string
    branch?: string
    since?: string
    until?: string
    edge_type?: string
    run_limit?: number
    freshness_slo_minutes?: number
    include_impact_preview?: boolean
    impact_preview_runs_limit?: number
    impact_preview_artifacts_limit?: number
    impact_preview_edge_limit?: number
  },
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'lineage/runs',
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getLineageRunImpactCtx = async (
  context: RequestContext,
  params: {
    run_id: string
    db_name: string
    branch?: string
    since?: string
    until?: string
    event_limit?: number
    artifact_preview_limit?: number
  },
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'lineage/run-impact',
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getLineageTimelineCtx = async (
  context: RequestContext,
  params: {
    db_name: string
    branch?: string
    since?: string
    until?: string
    edge_type?: string
    projection_name?: string
    bucket_minutes?: number
    event_limit?: number
    event_preview_limit?: number
  },
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'lineage/timeline',
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getLineageColumnLineageCtx = async (
  context: RequestContext,
  params: {
    db_name: string
    branch?: string
    run_id?: string
    source_field?: string
    target_field?: string
    target_class_id?: string
    since?: string
    until?: string
    edge_limit?: number
    pair_limit?: number
  },
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'lineage/column-lineage',
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getLineageOutOfDateCtx = async (
  context: RequestContext,
  params: {
    db_name: string
    branch?: string
    artifact_kind?: string
    as_of?: string
    freshness_slo_minutes?: number
    artifact_limit?: number
    stale_preview_limit?: number
    projection_limit?: number
    projection_preview_limit?: number
  },
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'lineage/out-of-date',
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const listTasks = async (
  context: RequestContext,
  params?: { status?: string; task_type?: string; limit?: number },
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'tasks/',
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getTask = async (context: RequestContext, taskId: string) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `tasks/${encodeURIComponent(taskId)}`,
    { method: 'GET' },
    context,
  )
  return payload ?? {}
}

export const getTaskResult = async (context: RequestContext, taskId: string) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `tasks/${encodeURIComponent(taskId)}/result`,
    { method: 'GET' },
    context,
  )
  return payload ?? {}
}

export const cancelTask = async (context: RequestContext, taskId: string) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `tasks/${encodeURIComponent(taskId)}`,
    { method: 'DELETE' },
    context,
  )
  return payload ?? {}
}

export const getTaskMetrics = async (context: RequestContext) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'tasks/metrics/summary',
    { method: 'GET' },
    context,
  )
  return payload ?? {}
}

export const replayInstanceState = async (
  context: RequestContext,
  input: Record<string, unknown>,
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'admin/replay-instance-state',
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export const recomputeProjection = async (
  context: RequestContext,
  input: Record<string, unknown>,
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'admin/recompute-projection',
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export const cleanupOldReplays = async (
  context: RequestContext,
  input: { older_than_hours?: number },
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'admin/cleanup-old-replays',
    { method: 'POST' },
    context,
    { older_than_hours: input.older_than_hours ?? 24 },
  )
  return payload ?? {}
}

export const getSystemHealth = async (context: RequestContext) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    'admin/system-health',
    { method: 'GET' },
    context,
  )
  return payload ?? {}
}

export const translateQueryPlan = async (
  context: RequestContext,
  dbName: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `ai/translate/query-plan/${encodeURIComponent(dbName)}`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export const runAiQuery = async (
  context: RequestContext,
  dbName: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await bff2RequestJson<Record<string, unknown>>(
    `ai/query/${encodeURIComponent(dbName)}`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}
