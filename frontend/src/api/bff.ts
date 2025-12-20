import type { Language } from '../types/app'
import { useAppStore } from '../store/useAppStore'
import { API_BASE_URL } from './config'

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

type SearchParams = Record<string, string | number | boolean | null | undefined>

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

const buildHeaders = (language: Language, adminToken: string, json = false) => {
  const headers = new Headers({ 'Accept-Language': language })
  if (adminToken) {
    headers.set('X-Admin-Token', adminToken)
  }
  if (json) {
    headers.set('Content-Type', 'application/json')
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

const parseRetryAfterSeconds = (value: string | null) => {
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

const requestJson = async <T>(
  path: string,
  init: RequestInit,
  context: RequestContext,
  searchParams?: SearchParams,
  extraHeaders?: HeadersInit,
): Promise<{ status: number; payload: T | null }> => {
  const isJsonBody = init.body !== undefined && !(init.body instanceof FormData)
  const headers = buildHeaders(context.language, context.adminToken, isJsonBody)
  if (context.adminActor) {
    headers.set('X-Admin-Actor', context.adminActor)
  }
  if (extraHeaders) {
    const extra = new Headers(extraHeaders)
    extra.forEach((value, key) => headers.set(key, value))
  }

  const response = await fetch(buildApiUrl(path, context.language, searchParams), {
    ...init,
    headers,
  })

  const payload = (await parseJson(response)) as T | null

  if (!response.ok) {
    const retryAfter = parseRetryAfterSeconds(response.headers.get('Retry-After'))
    if (response.status === 401 || response.status === 403 || response.status === 503) {
      useAppStore.getState().setSettingsOpen(true)
    }
    throw new HttpError(response.status, `HTTP ${response.status}`, payload, retryAfter)
  }

  return { status: response.status, payload }
}

const requestRaw = async (
  path: string,
  init: RequestInit,
  context: RequestContext,
  searchParams?: SearchParams,
  extraHeaders?: HeadersInit,
): Promise<Response> => {
  const headers = buildHeaders(context.language, context.adminToken, false)
  if (context.adminActor) {
    headers.set('X-Admin-Actor', context.adminActor)
  }
  if (extraHeaders) {
    const extra = new Headers(extraHeaders)
    extra.forEach((value, key) => headers.set(key, value))
  }

  const response = await fetch(buildApiUrl(path, context.language, searchParams), {
    ...init,
    headers,
  })

  if (!response.ok) {
    const payload = await parseJson(response)
    const retryAfter = parseRetryAfterSeconds(response.headers.get('Retry-After'))
    if (response.status === 401 || response.status === 403 || response.status === 503) {
      useAppStore.getState().setSettingsOpen(true)
    }
    throw new HttpError(response.status, `HTTP ${response.status}`, payload, retryAfter)
  }

  return response
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

export const listDatabases = async (context: RequestContext): Promise<string[]> => {
  const { payload } = await requestJson<DatabaseListResponse>(
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
  await requestJson<unknown>(
    `databases/${encodeURIComponent(dbName)}`,
    { method: 'GET' },
    context,
  )
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
    extraHeaders,
  )

  const commandId = payload?.data?.command_id ?? payload?.command_id
  return { status, commandId }
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
    extraHeaders,
  )

  const commandId = payload?.data?.command_id ?? payload?.command_id
  return { status, commandId }
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

export type ApiEnvelope<T = Record<string, unknown>> = {
  status?: string
  message?: string
  data?: T
  [key: string]: unknown
}

export const listBranches = async (context: RequestContext, dbName: string) => {
  const { payload } = await requestJson<{ branches?: unknown[]; count?: number }>(
    `databases/${encodeURIComponent(dbName)}/branches`,
    { method: 'GET' },
    context,
  )
  return payload ?? { branches: [], count: 0 }
}

export const createBranch = async (
  context: RequestContext,
  dbName: string,
  input: { name: string; from_branch?: string },
): Promise<ApiEnvelope> => {
  const { payload } = await requestJson<ApiEnvelope>(
    `databases/${encodeURIComponent(dbName)}/branches`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export const deleteBranch = async (
  context: RequestContext,
  dbName: string,
  branchName: string,
): Promise<ApiEnvelope> => {
  const { payload } = await requestJson<ApiEnvelope>(
    `databases/${encodeURIComponent(dbName)}/branches/${encodeURIComponent(branchName)}`,
    { method: 'DELETE' },
    context,
  )
  return payload ?? {}
}

export const listDatabaseClasses = async (context: RequestContext, dbName: string) => {
  const { payload } = await requestJson<{ classes?: unknown[]; count?: number }>(
    `databases/${encodeURIComponent(dbName)}/classes`,
    { method: 'GET' },
    context,
  )
  return payload ?? { classes: [], count: 0 }
}

export const listOntology = async (
  context: RequestContext,
  dbName: string,
  branch: string,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/ontology/list`,
    { method: 'GET' },
    context,
    { branch },
  )
  return payload ?? {}
}

export const getOntology = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  branch: string,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/ontology/${encodeURIComponent(classLabel)}`,
    { method: 'GET' },
    context,
    { branch },
  )
  return payload ?? {}
}

export const validateOntologyCreate = async (
  context: RequestContext,
  dbName: string,
  branch: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/ontology/validate`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    { branch },
  )
  return payload ?? {}
}

export const createOntology = async (
  context: RequestContext,
  dbName: string,
  branch: string,
  input: Record<string, unknown>,
  extraHeaders?: HeadersInit,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/ontology`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    { branch },
    extraHeaders,
  )
  return payload ?? {}
}

export const validateOntologyUpdate = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  branch: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/ontology/${encodeURIComponent(classLabel)}/validate`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    { branch },
  )
  return payload ?? {}
}

export const updateOntology = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  branch: string,
  expectedSeq: number,
  input: Record<string, unknown>,
  extraHeaders?: HeadersInit,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/ontology/${encodeURIComponent(classLabel)}`,
    { method: 'PUT', body: JSON.stringify(input) },
    context,
    { branch, expected_seq: expectedSeq },
    extraHeaders,
  )
  return payload ?? {}
}

export const deleteOntology = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  branch: string,
  expectedSeq: number,
  extraHeaders?: HeadersInit,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/ontology/${encodeURIComponent(classLabel)}`,
    { method: 'DELETE' },
    context,
    { branch, expected_seq: expectedSeq },
    extraHeaders,
  )
  return payload ?? {}
}

export const getOntologySchema = async (
  context: RequestContext,
  dbName: string,
  classId: string,
  branch: string,
  format: 'json' | 'jsonld' | 'owl' = 'json',
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/ontology/${encodeURIComponent(classId)}/schema`,
    { method: 'GET' },
    context,
    { branch, format },
  )
  return payload ?? {}
}

export const getMappingsSummary = async (context: RequestContext, dbName: string) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    `database/${encodeURIComponent(dbName)}/mappings/`,
    { method: 'GET' },
    context,
  )
  return payload ?? {}
}

export const exportMappings = async (context: RequestContext, dbName: string) => {
  const response = await requestRaw(
    `database/${encodeURIComponent(dbName)}/mappings/export`,
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
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/mappings/validate`,
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
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/mappings/import`,
    { method: 'POST', body },
    context,
  )
  return payload ?? {}
}

export const clearMappings = async (context: RequestContext, dbName: string) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/mappings/`,
    { method: 'DELETE' },
    context,
  )
  return payload ?? {}
}

export const previewGoogleSheet = async (
  context: RequestContext,
  input: { sheet_url: string; worksheet_name?: string; api_key?: string },
  limit = 10,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    'data-connectors/google-sheets/preview',
    { method: 'POST', body: JSON.stringify(input) },
    context,
    { limit },
  )
  return payload ?? {}
}

export const gridGoogleSheet = async (
  context: RequestContext,
  input: { sheet_url: string; worksheet_name?: string; api_key?: string; max_rows?: number; max_cols?: number; trim_trailing_empty?: boolean },
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    'data-connectors/google-sheets/grid',
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export const registerGoogleSheet = async (
  context: RequestContext,
  input: Record<string, unknown>,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    'data-connectors/google-sheets/register',
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export const listRegisteredSheets = async (
  context: RequestContext,
  databaseName?: string,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    'data-connectors/google-sheets/registered',
    { method: 'GET' },
    context,
    { database_name: databaseName },
  )
  return payload ?? {}
}

export const previewRegisteredSheet = async (
  context: RequestContext,
  sheetId: string,
  params?: { worksheet_name?: string; limit?: number },
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `data-connectors/google-sheets/${encodeURIComponent(sheetId)}/preview`,
    { method: 'GET' },
    context,
    { worksheet_name: params?.worksheet_name, limit: params?.limit },
  )
  return payload ?? {}
}

export const unregisterSheet = async (context: RequestContext, sheetId: string) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `data-connectors/google-sheets/${encodeURIComponent(sheetId)}`,
    { method: 'DELETE' },
    context,
  )
  return payload ?? {}
}

export const suggestMappingsFromGoogleSheets = async (
  context: RequestContext,
  dbName: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/suggest-mappings-from-google-sheets`,
    { method: 'POST', body: JSON.stringify(input) },
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
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/suggest-mappings-from-excel`,
    { method: 'POST', body },
    context,
    { target_class_id: params.target_class_id },
  )
  return payload ?? {}
}

export const dryRunImportFromGoogleSheets = async (
  context: RequestContext,
  dbName: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/import-from-google-sheets/dry-run`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export const commitImportFromGoogleSheets = async (
  context: RequestContext,
  dbName: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/import-from-google-sheets/commit`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
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

  const { payload: result } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/import-from-excel/dry-run`,
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

  const { payload: result } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/import-from-excel/commit`,
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
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/suggest-schema-from-data`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export const suggestSchemaFromGoogleSheets = async (
  context: RequestContext,
  dbName: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/suggest-schema-from-google-sheets`,
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
  const { payload } = await requestJson<ApiEnvelope>(
    `database/${encodeURIComponent(dbName)}/suggest-schema-from-excel`,
    { method: 'POST', body },
    context,
  )
  return payload ?? {}
}

export const listInstances = async (
  context: RequestContext,
  dbName: string,
  classId: string,
  params: { limit?: number; offset?: number; search?: string },
) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    `database/${encodeURIComponent(dbName)}/class/${encodeURIComponent(classId)}/instances`,
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getInstance = async (
  context: RequestContext,
  dbName: string,
  classId: string,
  instanceId: string,
) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    `database/${encodeURIComponent(dbName)}/class/${encodeURIComponent(classId)}/instance/${encodeURIComponent(instanceId)}`,
    { method: 'GET' },
    context,
  )
  return payload ?? {}
}

export const getSampleValues = async (
  context: RequestContext,
  dbName: string,
  classId: string,
) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    `database/${encodeURIComponent(dbName)}/class/${encodeURIComponent(classId)}/sample-values`,
    { method: 'GET' },
    context,
  )
  return payload ?? {}
}

export const createInstance = async (
  context: RequestContext,
  dbName: string,
  classLabel: string,
  branch: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await requestJson<CommandResult>(
    `database/${encodeURIComponent(dbName)}/instances/${encodeURIComponent(classLabel)}/create`,
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
  const { payload } = await requestJson<CommandResult>(
    `database/${encodeURIComponent(dbName)}/instances/${encodeURIComponent(classLabel)}/${encodeURIComponent(instanceId)}/update`,
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
  const { payload } = await requestJson<CommandResult>(
    `database/${encodeURIComponent(dbName)}/instances/${encodeURIComponent(classLabel)}/${encodeURIComponent(instanceId)}/delete`,
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
  const { payload } = await requestJson<CommandResult>(
    `database/${encodeURIComponent(dbName)}/instances/${encodeURIComponent(classLabel)}/bulk-create`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
    { branch },
  )
  return payload ?? ({} as CommandResult)
}

export const runGraphQuery = async (
  context: RequestContext,
  dbName: string,
  branch: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await requestJson<Record<string, unknown>>(
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
  const { payload } = await requestJson<Record<string, unknown>>(
    `graph-query/${encodeURIComponent(dbName)}/paths`,
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getGraphHealth = async (context: RequestContext) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    'graph-query/health',
    { method: 'GET' },
    context,
  )
  return payload ?? {}
}

export const queryBuilderInfo = async (context: RequestContext, dbName: string) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    `database/${encodeURIComponent(dbName)}/query/builder`,
    { method: 'GET' },
    context,
  )
  return payload ?? {}
}

export const runQuery = async (
  context: RequestContext,
  dbName: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    `database/${encodeURIComponent(dbName)}/query`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export const runRawQuery = async (
  context: RequestContext,
  dbName: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    `database/${encodeURIComponent(dbName)}/query/raw`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export const simulateMerge = async (
  context: RequestContext,
  dbName: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    `database/${encodeURIComponent(dbName)}/merge/simulate`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export const resolveMerge = async (
  context: RequestContext,
  dbName: string,
  input: Record<string, unknown>,
) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    `database/${encodeURIComponent(dbName)}/merge/resolve`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}

export const listAuditLogs = async (
  context: RequestContext,
  params: Record<string, unknown>,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    'audit/logs',
    { method: 'GET' },
    context,
    params as SearchParams,
  )
  return payload ?? {}
}

export const getAuditChainHead = async (
  context: RequestContext,
  partitionKey: string,
) => {
  const { payload } = await requestJson<ApiEnvelope>(
    'audit/chain-head',
    { method: 'GET' },
    context,
    { partition_key: partitionKey },
  )
  return payload ?? {}
}

export const getLineageGraph = async (
  context: RequestContext,
  params: { root: string; db_name: string },
) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    'lineage/graph',
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getLineageImpact = async (
  context: RequestContext,
  params: { root: string; db_name: string },
) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    'lineage/impact',
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getLineageMetrics = async (
  context: RequestContext,
  params: { db_name: string; window_minutes?: number },
) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    'lineage/metrics',
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
  const { payload } = await requestJson<Record<string, unknown>>(
    'tasks/',
    { method: 'GET' },
    context,
    params,
  )
  return payload ?? {}
}

export const getTask = async (context: RequestContext, taskId: string) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    `tasks/${encodeURIComponent(taskId)}`,
    { method: 'GET' },
    context,
  )
  return payload ?? {}
}

export const getTaskResult = async (context: RequestContext, taskId: string) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    `tasks/${encodeURIComponent(taskId)}/result`,
    { method: 'GET' },
    context,
  )
  return payload ?? {}
}

export const cancelTask = async (context: RequestContext, taskId: string) => {
  const { payload } = await requestJson<Record<string, unknown>>(
    `tasks/${encodeURIComponent(taskId)}`,
    { method: 'DELETE' },
    context,
  )
  return payload ?? {}
}

export const getTaskMetrics = async (context: RequestContext) => {
  const { payload } = await requestJson<Record<string, unknown>>(
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
  const { payload } = await requestJson<Record<string, unknown>>(
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
  const { payload } = await requestJson<Record<string, unknown>>(
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
  const { payload } = await requestJson<Record<string, unknown>>(
    'admin/cleanup-old-replays',
    { method: 'POST' },
    context,
    { older_than_hours: input.older_than_hours ?? 24 },
  )
  return payload ?? {}
}

export const getSystemHealth = async (context: RequestContext) => {
  const { payload } = await requestJson<Record<string, unknown>>(
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
  const { payload } = await requestJson<Record<string, unknown>>(
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
  const { payload } = await requestJson<Record<string, unknown>>(
    `ai/query/${encodeURIComponent(dbName)}`,
    { method: 'POST', body: JSON.stringify(input) },
    context,
  )
  return payload ?? {}
}
