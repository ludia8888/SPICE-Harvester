import type { Language } from '../types/app'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '/api/v1'

export class HttpError extends Error {
  status: number
  detail: unknown

  constructor(status: number, message: string, detail: unknown) {
    super(message)
    this.name = 'HttpError'
    this.status = status
    this.detail = detail
  }
}

type RequestContext = {
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

const requestJson = async <T>(
  path: string,
  init: RequestInit,
  context: RequestContext,
  searchParams?: SearchParams,
  extraHeaders?: HeadersInit,
): Promise<{ status: number; payload: T | null }> => {
  const headers = buildHeaders(context.language, context.adminToken, init.body !== undefined)
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
    throw new HttpError(response.status, `HTTP ${response.status}`, payload)
  }

  return { status: response.status, payload }
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
