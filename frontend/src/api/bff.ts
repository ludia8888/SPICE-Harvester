import type { Language } from '../types/app'
import { useAppStore, type SettingsDialogReason } from '../store/useAppStore'
import { API_BASE_URL } from './config'

export class HttpError extends Error {
  status: number
  detail: unknown
  retryAfterSeconds?: number

  constructor(status: number, message: string, detail: unknown, retryAfterSeconds?: number) {
    super(message)
    this.name = 'HttpError'
    this.status = status
    this.detail = detail
    this.retryAfterSeconds = retryAfterSeconds
  }
}

type RequestContext = {
  language: Language
  authToken?: string
  adminToken?: string
  adminActor?: string
  changeReason?: string
}

type SearchParams = Record<string, string | number | boolean | null | undefined>

type RetryPolicy = {
  retries: number
  retryOnStatus?: number[]
}

type RequestOptions = {
  retry?: RetryPolicy
  extraHeaders?: HeadersInit
  skipAuthRetry?: boolean
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

type PendingAuthRetry = {
  path: string
  init: RequestInit
  searchParams?: SearchParams
  options?: RequestOptions
}

let pendingAuthRetry: PendingAuthRetry | null = null

const shouldQueueAuthRetry = (status: number, context: RequestContext) => {
  if (status === 401 || status === 403) {
    return true
  }
  if (status === 503) {
    return !(context.authToken || context.adminToken)
  }
  return false
}

const resolveAuthReason = (status: number, context: RequestContext): SettingsDialogReason | null => {
  if (status === 401) {
    return 'UNAUTHORIZED'
  }
  if (status === 403) {
    return 'FORBIDDEN'
  }
  if (status === 503 && !(context.authToken || context.adminToken)) {
    return 'TOKEN_MISSING'
  }
  return null
}

const queueAuthRetry = (
  status: number,
  context: RequestContext,
  request: PendingAuthRetry,
) => {
  if (request.options?.skipAuthRetry) {
    return
  }
  if (!shouldQueueAuthRetry(status, context)) {
    return
  }
  const reason = resolveAuthReason(status, context)
  if (!reason) {
    return
  }
  pendingAuthRetry = {
    path: request.path,
    init: { ...request.init },
    searchParams: request.searchParams,
    options: request.options,
  }
  useAppStore.getState().openSettingsDialog(reason)
}

const buildApiUrl = (path: string, language: Language, searchParams?: SearchParams) => {
  const normalizedPath = path.replace(/^\/+/, '')
  const base = API_BASE_URL.replace(/\/+$/, '')
  const url = base.startsWith('http')
    ? new URL(`${base}/${normalizedPath}`)
    : new URL(`${base}/${normalizedPath}`, window.location.origin)

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

const buildHeaders = (context: RequestContext, json = false, extraHeaders?: HeadersInit) => {
  const headers = new Headers({ 'Accept-Language': context.language })
  if (context.authToken) {
    headers.set('Authorization', `Bearer ${context.authToken}`)
  }
  if (context.adminToken) {
    headers.set('X-Admin-Token', context.adminToken)
  }
  if (context.adminActor) {
    headers.set('X-Admin-Actor', context.adminActor)
  }
  if (context.changeReason) {
    headers.set('X-Change-Reason', context.changeReason)
  }
  if (json) {
    headers.set('Content-Type', 'application/json')
  }
  if (extraHeaders) {
    const extra = new Headers(extraHeaders)
    extra.forEach((value, key) => headers.set(key, value))
  }
  return headers
}

const parseJson = async (response: Response) => {
  try {
    return (await response.json()) as unknown
  } catch {
    return null
  }
}

const parseRetryAfter = (value: string | null) => {
  if (!value) {
    return undefined
  }
  const asNumber = Number(value)
  if (!Number.isNaN(asNumber) && Number.isFinite(asNumber)) {
    return Math.max(0, Math.ceil(asNumber))
  }
  const date = new Date(value)
  if (!Number.isNaN(date.valueOf())) {
    const diffSeconds = Math.ceil((date.getTime() - Date.now()) / 1000)
    return diffSeconds > 0 ? diffSeconds : 0
  }
  return undefined
}

const requestJson = async <T>(
  path: string,
  init: RequestInit,
  context: RequestContext,
  searchParams?: SearchParams,
  options?: RequestOptions,
): Promise<{ status: number; payload: T | null; headers: Headers }> => {
  const attempt = async (retriesLeft: number): Promise<{ status: number; payload: T | null; headers: Headers }> => {
    const hasJsonBody =
      init.body !== undefined && init.body !== null && !(init.body instanceof FormData)
    const headers = buildHeaders(context, hasJsonBody, options?.extraHeaders)
    const response = await fetch(buildApiUrl(path, context.language, searchParams), {
      ...init,
      headers,
    })

    const payload = (await parseJson(response)) as T | null

    if (!response.ok) {
      if (response.status === 429) {
        const retryAfter = parseRetryAfter(response.headers.get('Retry-After'))
        if (retriesLeft > 0) {
          const delayMs = retryAfter ? retryAfter * 1000 : 1000
          await sleep(delayMs)
          return attempt(retriesLeft - 1)
        }
        throw new HttpError(response.status, `HTTP ${response.status}`, payload, retryAfter)
      }
      queueAuthRetry(response.status, context, { path, init, searchParams, options })
      throw new HttpError(response.status, `HTTP ${response.status}`, payload)
    }

    return { status: response.status, payload, headers: response.headers }
  }

  const retryOn = options?.retry?.retryOnStatus ?? [429]
  const retries = options?.retry?.retries ?? 0
  if (retries > 0 && retryOn.includes(429)) {
    return attempt(retries)
  }
  return attempt(0)
}

const requestBlob = async (
  path: string,
  init: RequestInit,
  context: RequestContext,
  searchParams?: SearchParams,
): Promise<{ status: number; blob: Blob; filename?: string }> => {
  const headers = buildHeaders(context, false)
  const response = await fetch(buildApiUrl(path, context.language, searchParams), {
    ...init,
    headers,
  })

  if (!response.ok) {
    const payload = await parseJson(response)
    queueAuthRetry(response.status, context, { path, init, searchParams })
    throw new HttpError(response.status, `HTTP ${response.status}`, payload)
  }

  const blob = await response.blob()
  const disposition = response.headers.get('Content-Disposition') ?? ''
  const match = disposition.match(/filename=([^;]+)/i)
  const filename = match ? match[1].replace(/"/g, '') : undefined
  return { status: response.status, blob, filename }
}

export const retryPendingAuthRequest = async (context: RequestContext) => {
  if (!pendingAuthRetry) {
    return null
  }
  const pending = pendingAuthRetry
  pendingAuthRetry = null
  const { payload } = await requestJson<unknown>(
    pending.path,
    pending.init,
    context,
    pending.searchParams,
    { ...pending.options, skipAuthRetry: true },
  )
  return payload
}

type DatabaseListResponse = {
  data?: { databases?: Array<{ name?: string }> }
}

type AcceptedContract = {
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

const extractCommandId = (payload: AcceptedContract | null) =>
  payload?.data?.command_id ?? payload?.command_id

const extractBatchCommandIds = (payload: any): string[] => {
  if (!payload || typeof payload !== 'object') {
    return []
  }
  const write = (payload as { write?: { commands?: any[] } }).write
  if (!write || !Array.isArray(write.commands)) {
    return []
  }
  return write.commands
    .map((command) => {
      if (!command) {
        return null
      }
      if (typeof command.command_id === 'string') {
        return command.command_id
      }
      if (command.command && typeof command.command.command_id === 'string') {
        return command.command.command_id
      }
      if (command.command && typeof command.command.id === 'string') {
        return command.command.id
      }
      return null
    })
    .filter((value): value is string => Boolean(value))
}

export const listDatabases = async (context: RequestContext): Promise<string[]> => {
  const { payload } = await requestJson<DatabaseListResponse>('databases', { method: 'GET' }, context)

  const names =
    payload?.data?.databases
      ?.map((db) => db?.name)
      .filter((name): name is string => Boolean(name)) ?? []

  return names
}

export const openDatabase = async (context: RequestContext, dbName: string) => {
  await requestJson<unknown>(`databases/${encodeURIComponent(dbName)}`, { method: 'GET' }, context)
}

export const createDatabase = async (
  context: RequestContext,
  input: CreateDatabaseInput,
  extraHeaders?: HeadersInit,
): Promise<WriteResult> => {
  const { status, payload } = await requestJson<AcceptedContract>(
    'databases',
    { method: 'POST', body: JSON.stringify(input) },
    context,
    undefined,
    { extraHeaders },
  )

  return { status, commandId: extractCommandId(payload) }
}

export const deleteDatabase = async (
  context: RequestContext,
  dbName: string,
  expectedSeq: number,
  extraHeaders?: HeadersInit,
): Promise<WriteResult> => {
  const { status, payload } = await requestJson<AcceptedContract>(
    `databases/${encodeURIComponent(dbName)}`,
    { method: 'DELETE' },
    context,
    { expected_seq: expectedSeq },
    { extraHeaders },
  )

  return { status, commandId: extractCommandId(payload) }
}

export const getDatabaseExpectedSeq = async (
  context: RequestContext,
  dbName: string,
): Promise<number> => {
  const { payload } = await requestJson<{ data?: { expected_seq?: number } }>(
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
  const { payload } = await requestJson<Summary>(
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
  const { payload } = await requestJson<CommandResult>(
    `commands/${encodeURIComponent(commandId)}/status`,
    { method: 'GET' },
    context,
  )

  if (!payload) {
    throw new HttpError(500, 'Command status payload missing', payload)
  }

  return payload
}

export const listBranches = async (context: RequestContext, dbName: string) => {
  const { payload } = await requestJson<{ branches?: Array<{ name?: string }>; count?: number }>(
    `databases/${encodeURIComponent(dbName)}/branches`,
    { method: 'GET' },
    context,
  )
  return payload ?? {}
}

export const createBranch = async (
  context: RequestContext,
  dbName: string,
  input: { name: string; from_branch?: string },
) => {
  const { payload } = await requestJson<unknown>(
    `databases/${encodeURIComponent(dbName)}/branches`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload
}

export const deleteBranch = async (context: RequestContext, dbName: string, branchName: string) => {
  const { payload } = await requestJson<unknown>(
    `databases/${encodeURIComponent(dbName)}/branches/${encodeURIComponent(branchName)}`,
    { method: 'DELETE' },
    context,
  )
  return payload
}

export const listOntologies = async (context: RequestContext, dbName: string, branch: string) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/ontology/list`,
    { method: 'GET' },
    context,
    { branch },
  )
  return payload
}

export const getOntology = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  branch: string,
) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/ontology/${encodeURIComponent(classLabel)}`,
    { method: 'GET' },
    context,
    { branch },
  )
  return payload
}

export const validateOntology = async (
  context: RequestContext,
  dbName: string,
  branch: string,
  body: unknown,
) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/ontology/validate`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
    { branch },
  )
  return payload
}

export const validateOntologyUpdate = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  branch: string,
  body: unknown,
) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/ontology/${encodeURIComponent(classLabel)}/validate`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
    { branch },
  )
  return payload
}

export const createOntology = async (
  context: RequestContext,
  dbName: string,
  branch: string,
  body: unknown,
  extraHeaders?: HeadersInit,
) => {
  const { payload } = await requestJson<AcceptedContract>(
    `database/${encodeURIComponent(dbName)}/ontology`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
    { branch },
    { extraHeaders },
  )
  return { payload, commandId: extractCommandId(payload) }
}

export const updateOntology = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  branch: string,
  expectedSeq: number,
  body: unknown,
  extraHeaders?: HeadersInit,
) => {
  const { payload } = await requestJson<AcceptedContract>(
    `database/${encodeURIComponent(dbName)}/ontology/${encodeURIComponent(classLabel)}`,
    { method: 'PUT', body: JSON.stringify(body) },
    context,
    { branch, expected_seq: expectedSeq },
    { extraHeaders },
  )
  return { payload, commandId: extractCommandId(payload) }
}

export const deleteOntology = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  branch: string,
  expectedSeq: number,
  extraHeaders?: HeadersInit,
) => {
  const { payload } = await requestJson<AcceptedContract>(
    `database/${encodeURIComponent(dbName)}/ontology/${encodeURIComponent(classLabel)}`,
    { method: 'DELETE' },
    context,
    { branch, expected_seq: expectedSeq },
    { extraHeaders },
  )
  return { payload, commandId: extractCommandId(payload) }
}

export const getOntologySchema = async (
  context: RequestContext,
  dbName: string,
  classId: string,
  branch: string,
  format = 'json',
) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/ontology/${encodeURIComponent(classId)}/schema`,
    { method: 'GET' },
    context,
    { branch, format },
  )
  return payload
}

export const saveMappingMetadata = async (
  context: RequestContext,
  dbName: string,
  classId: string,
  body: unknown,
) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/ontology/${encodeURIComponent(classId)}/mapping-metadata`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
  )
  return payload
}

export const getMappingsSummary = async (context: RequestContext, dbName: string) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/mappings/`,
    { method: 'GET' },
    context,
  )
  return payload
}

export const exportMappings = async (context: RequestContext, dbName: string) =>
  requestBlob(`database/${encodeURIComponent(dbName)}/mappings/export`, { method: 'POST' }, context)

export const validateMappings = async (context: RequestContext, dbName: string, file: File) => {
  const form = new FormData()
  form.append('file', file)
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/mappings/validate`,
    { method: 'POST', body: form },
    context,
  )
  return payload
}

export const importMappings = async (context: RequestContext, dbName: string, file: File) => {
  const form = new FormData()
  form.append('file', file)
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/mappings/import`,
    { method: 'POST', body: form },
    context,
  )
  return payload
}

export const previewSheet = async (
  context: RequestContext,
  input: { sheet_url: string; worksheet_name?: string; api_key?: string },
  limit = 10,
) => {
  const { payload } = await requestJson<unknown>(
    'data-connectors/google-sheets/preview',
    { method: 'POST', body: JSON.stringify(input) },
    context,
    { limit },
    { retry: { retries: 1 } },
  )
  return payload
}

export const gridSheet = async (
  context: RequestContext,
  input: {
    sheet_url: string
    worksheet_name?: string
    api_key?: string
    max_rows?: number
    max_cols?: number
    trim_trailing_empty?: boolean
  },
) => {
  const { payload } = await requestJson<unknown>(
    'data-connectors/google-sheets/grid',
    { method: 'POST', body: JSON.stringify(input) },
    context,
    undefined,
    { retry: { retries: 1 } },
  )
  return payload
}

export const registerSheet = async (context: RequestContext, input: Record<string, unknown>) => {
  const { payload } = await requestJson<unknown>(
    'data-connectors/google-sheets/register',
    { method: 'POST', body: JSON.stringify(input) },
    context,
    undefined,
    { retry: { retries: 1 } },
  )
  return payload
}

export const listRegisteredSheets = async (context: RequestContext, dbName?: string) => {
  const { payload } = await requestJson<unknown>(
    'data-connectors/google-sheets/registered',
    { method: 'GET' },
    context,
    { database_name: dbName ?? undefined },
  )
  return payload
}

export const previewRegisteredSheet = async (
  context: RequestContext,
  sheetId: string,
  worksheetName?: string,
  limit = 10,
) => {
  const { payload } = await requestJson<unknown>(
    `data-connectors/google-sheets/${encodeURIComponent(sheetId)}/preview`,
    { method: 'GET' },
    context,
    { worksheet_name: worksheetName ?? undefined, limit },
  )
  return payload
}

export const deleteRegisteredSheet = async (context: RequestContext, sheetId: string) => {
  const { payload } = await requestJson<unknown>(
    `data-connectors/google-sheets/${encodeURIComponent(sheetId)}`,
    { method: 'DELETE' },
    context,
  )
  return payload
}

export const suggestMappingsFromSheets = async (context: RequestContext, dbName: string, body: unknown) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/suggest-mappings-from-google-sheets`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
    undefined,
    { retry: { retries: 1 } },
  )
  return payload
}

export const importFromSheetsDryRun = async (context: RequestContext, dbName: string, body: unknown) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/import-from-google-sheets/dry-run`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
    undefined,
    { retry: { retries: 1 } },
  )
  return payload
}

export const importFromSheetsCommit = async (context: RequestContext, dbName: string, body: unknown) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/import-from-google-sheets/commit`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
    undefined,
    { retry: { retries: 1 } },
  )
  return payload
}

export const suggestMappingsFromExcel = async (
  context: RequestContext,
  dbName: string,
  targetClassId: string,
  file: File,
  targetSchemaJson?: string,
) => {
  const form = new FormData()
  form.append('file', file)
  if (targetSchemaJson) {
    form.append('target_schema_json', targetSchemaJson)
  }
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/suggest-mappings-from-excel`,
    { method: 'POST', body: form },
    context,
    { target_class_id: targetClassId },
    { retry: { retries: 1 } },
  )
  return payload
}

export const importFromExcelDryRun = async (
  context: RequestContext,
  dbName: string,
  payload: {
    file: File
    target_class_id: string
    target_schema_json?: string
    mappings_json: string
    sheet_name?: string
    table_id?: string
    table_top?: number
    table_left?: number
    table_bottom?: number
    table_right?: number
    max_rows?: number
  },
) => {
  const form = new FormData()
  form.append('file', payload.file)
  form.append('target_class_id', payload.target_class_id)
  form.append('mappings_json', payload.mappings_json)
  if (payload.target_schema_json) {
    form.append('target_schema_json', payload.target_schema_json)
  }
  if (payload.sheet_name) {
    form.append('sheet_name', payload.sheet_name)
  }
  if (payload.table_id) {
    form.append('table_id', payload.table_id)
  }
  if (typeof payload.table_top === 'number') {
    form.append('table_top', String(payload.table_top))
  }
  if (typeof payload.table_left === 'number') {
    form.append('table_left', String(payload.table_left))
  }
  if (typeof payload.table_bottom === 'number') {
    form.append('table_bottom', String(payload.table_bottom))
  }
  if (typeof payload.table_right === 'number') {
    form.append('table_right', String(payload.table_right))
  }
  if (typeof payload.max_rows === 'number') {
    form.append('max_rows', String(payload.max_rows))
  }

  const { payload: response } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/import-from-excel/dry-run`,
    { method: 'POST', body: form },
    context,
    undefined,
    { retry: { retries: 1 } },
  )
  return response
}

export const importFromExcelCommit = async (
  context: RequestContext,
  dbName: string,
  payload: {
    file: File
    target_class_id: string
    target_schema_json?: string
    mappings_json: string
    sheet_name?: string
    table_id?: string
    table_top?: number
    table_left?: number
    table_bottom?: number
    table_right?: number
    max_rows?: number
  },
) => {
  const form = new FormData()
  form.append('file', payload.file)
  form.append('target_class_id', payload.target_class_id)
  form.append('mappings_json', payload.mappings_json)
  if (payload.target_schema_json) {
    form.append('target_schema_json', payload.target_schema_json)
  }
  if (payload.sheet_name) {
    form.append('sheet_name', payload.sheet_name)
  }
  if (payload.table_id) {
    form.append('table_id', payload.table_id)
  }
  if (typeof payload.table_top === 'number') {
    form.append('table_top', String(payload.table_top))
  }
  if (typeof payload.table_left === 'number') {
    form.append('table_left', String(payload.table_left))
  }
  if (typeof payload.table_bottom === 'number') {
    form.append('table_bottom', String(payload.table_bottom))
  }
  if (typeof payload.table_right === 'number') {
    form.append('table_right', String(payload.table_right))
  }
  if (typeof payload.max_rows === 'number') {
    form.append('max_rows', String(payload.max_rows))
  }

  const { payload: response } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/import-from-excel/commit`,
    { method: 'POST', body: form },
    context,
    undefined,
    { retry: { retries: 1 } },
  )
  return response
}

export const suggestSchemaFromSheets = async (context: RequestContext, dbName: string, body: unknown) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/suggest-schema-from-google-sheets`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
    undefined,
    { retry: { retries: 1 } },
  )
  return payload
}

export const suggestSchemaFromData = async (context: RequestContext, dbName: string, body: unknown) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/suggest-schema-from-data`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
  )
  return payload
}

export const listInstances = async (
  context: RequestContext,
  dbName: string,
  classId: string,
  params: { limit?: number; offset?: number; search?: string },
) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/class/${encodeURIComponent(classId)}/instances`,
    { method: 'GET' },
    context,
    params,
  )
  return payload
}

export const getInstance = async (context: RequestContext, dbName: string, classId: string, instanceId: string) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/class/${encodeURIComponent(classId)}/instance/${encodeURIComponent(instanceId)}`,
    { method: 'GET' },
    context,
  )
  return payload
}

export const getSampleValues = async (context: RequestContext, dbName: string, classId: string) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/class/${encodeURIComponent(classId)}/sample-values`,
    { method: 'GET' },
    context,
  )
  return payload
}

export const createInstance = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  branch: string,
  body: unknown,
) => {
  const { payload } = await requestJson<CommandResult>(
    `database/${encodeURIComponent(dbName)}/instances/${encodeURIComponent(classLabel)}/create`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
    { branch },
  )
  return payload
}

export const bulkCreateInstances = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  branch: string,
  body: unknown,
) => {
  const { payload } = await requestJson<CommandResult>(
    `database/${encodeURIComponent(dbName)}/instances/${encodeURIComponent(classLabel)}/bulk-create`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
    { branch },
  )
  return payload
}

export const updateInstance = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  instanceId: string,
  branch: string,
  expectedSeq: number,
  body: unknown,
) => {
  const { payload } = await requestJson<CommandResult>(
    `database/${encodeURIComponent(dbName)}/instances/${encodeURIComponent(classLabel)}/${encodeURIComponent(instanceId)}/update`,
    { method: 'PUT', body: JSON.stringify(body) },
    context,
    { branch, expected_seq: expectedSeq },
  )
  return payload
}

export const deleteInstance = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  instanceId: string,
  branch: string,
  expectedSeq: number,
) => {
  const { payload } = await requestJson<CommandResult>(
    `database/${encodeURIComponent(dbName)}/instances/${encodeURIComponent(classLabel)}/${encodeURIComponent(instanceId)}/delete`,
    { method: 'DELETE' },
    context,
    { branch, expected_seq: expectedSeq },
  )
  return payload
}

export const graphQuery = async (
  context: RequestContext,
  dbName: string,
  branch: string,
  body: unknown,
) => {
  const { payload } = await requestJson<unknown>(
    `graph-query/${encodeURIComponent(dbName)}`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
    { branch },
  )
  return payload
}

export const graphPaths = async (
  context: RequestContext,
  dbName: string,
  params: { source_class: string; target_class: string; max_depth?: number; branch?: string },
) => {
  const { payload } = await requestJson<unknown>(
    `graph-query/${encodeURIComponent(dbName)}/paths`,
    { method: 'GET' },
    context,
    params,
  )
  return payload
}

export const graphHealth = async (context: RequestContext) => {
  const { payload } = await requestJson<unknown>('graph-query/health', { method: 'GET' }, context)
  return payload
}

export const queryBuilderInfo = async (context: RequestContext, dbName: string) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/query/builder`,
    { method: 'GET' },
    context,
  )
  return payload
}

export const runQuery = async (context: RequestContext, dbName: string, body: unknown) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/query`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
  )
  return payload
}

export const runRawQuery = async (context: RequestContext, dbName: string, body: unknown) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/query/raw`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
  )
  return payload
}

export const simulateMerge = async (context: RequestContext, dbName: string, body: unknown) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/merge/simulate`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
  )
  return payload
}

export const resolveMerge = async (context: RequestContext, dbName: string, body: unknown, extraHeaders?: HeadersInit) => {
  const { payload } = await requestJson<unknown>(
    `database/${encodeURIComponent(dbName)}/merge/resolve`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
    undefined,
    { extraHeaders },
  )
  return payload
}

export const listAuditLogs = async (
  context: RequestContext,
  params: { partition_key: string } & Record<string, string | number | boolean | undefined>,
) => {
  const { payload } = await requestJson<unknown>(
    'audit/logs',
    { method: 'GET' },
    context,
    params,
  )
  return payload
}

export const getAuditChainHead = async (
  context: RequestContext,
  params: { partition_key: string },
) => {
  const { payload } = await requestJson<unknown>(
    'audit/chain-head',
    { method: 'GET' },
    context,
    params,
  )
  return payload
}

export const getLineageGraph = async (
  context: RequestContext,
  params: { root: string; db_name: string; max_depth?: number; direction?: string },
) => {
  const { payload } = await requestJson<unknown>('lineage/graph', { method: 'GET' }, context, params)
  return payload
}

export const getLineageImpact = async (
  context: RequestContext,
  params: { root: string; db_name: string; max_depth?: number; direction?: string },
) => {
  const { payload } = await requestJson<unknown>('lineage/impact', { method: 'GET' }, context, params)
  return payload
}

export const getLineageMetrics = async (
  context: RequestContext,
  params: { db_name: string; window_minutes?: number },
) => {
  const { payload } = await requestJson<unknown>('lineage/metrics', { method: 'GET' }, context, params)
  return payload
}

export const listTasks = async (
  context: RequestContext,
  params: { status?: string; task_type?: string; limit?: number },
) => {
  const { payload } = await requestJson<unknown>('tasks/', { method: 'GET' }, context, params)
  return payload
}

export const getTask = async (context: RequestContext, taskId: string) => {
  const { payload } = await requestJson<unknown>(
    `tasks/${encodeURIComponent(taskId)}`,
    { method: 'GET' },
    context,
  )
  return payload
}

export const getTaskResult = async (context: RequestContext, taskId: string) => {
  const { payload } = await requestJson<unknown>(
    `tasks/${encodeURIComponent(taskId)}/result`,
    { method: 'GET' },
    context,
  )
  return payload
}

export const cancelTask = async (context: RequestContext, taskId: string) => {
  const { payload } = await requestJson<unknown>(
    `tasks/${encodeURIComponent(taskId)}`,
    { method: 'DELETE' },
    context,
  )
  return payload
}

export const getTaskMetrics = async (context: RequestContext) => {
  const { payload } = await requestJson<unknown>('tasks/metrics/summary', { method: 'GET' }, context)
  return payload
}

export const replayInstanceState = async (context: RequestContext, body: unknown) => {
  const { payload } = await requestJson<unknown>(
    'admin/replay-instance-state',
    { method: 'POST', body: JSON.stringify(body) },
    context,
  )
  return payload
}

export const getReplayResult = async (context: RequestContext, taskId: string) => {
  const { payload } = await requestJson<unknown>(
    `admin/replay-instance-state/${encodeURIComponent(taskId)}/result`,
    { method: 'GET' },
    context,
  )
  return payload
}

export const getReplayTrace = async (context: RequestContext, taskId: string) => {
  const { payload } = await requestJson<unknown>(
    `admin/replay-instance-state/${encodeURIComponent(taskId)}/trace`,
    { method: 'GET' },
    context,
  )
  return payload
}

export const recomputeProjection = async (context: RequestContext, body: unknown) => {
  const { payload } = await requestJson<unknown>(
    'admin/recompute-projection',
    { method: 'POST', body: JSON.stringify(body) },
    context,
  )
  return payload
}

export const getRecomputeResult = async (context: RequestContext, taskId: string) => {
  const { payload } = await requestJson<unknown>(
    `admin/recompute-projection/${encodeURIComponent(taskId)}/result`,
    { method: 'GET' },
    context,
  )
  return payload
}

export const cleanupOldReplays = async (context: RequestContext, olderThanHours: number) => {
  const { payload } = await requestJson<unknown>(
    'admin/cleanup-old-replays',
    { method: 'POST' },
    context,
    { older_than_hours: olderThanHours },
  )
  return payload
}

export const getSystemHealth = async (context: RequestContext) => {
  const { payload } = await requestJson<unknown>('admin/system-health', { method: 'GET' }, context)
  return payload
}

export const aiTranslatePlan = async (context: RequestContext, dbName: string, body: unknown) => {
  const { payload } = await requestJson<unknown>(
    `ai/translate/query-plan/${encodeURIComponent(dbName)}`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
    undefined,
    { retry: { retries: 1 } },
  )
  return payload
}

export const aiQuery = async (context: RequestContext, dbName: string, body: unknown) => {
  const { payload } = await requestJson<unknown>(
    `ai/query/${encodeURIComponent(dbName)}`,
    { method: 'POST', body: JSON.stringify(body) },
    context,
    undefined,
    { retry: { retries: 1 } },
  )
  return payload
}

export const extractWriteCommandIds = (payload: unknown) => extractBatchCommandIds(payload)
