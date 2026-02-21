import { randomUUID } from 'node:crypto'
import { access, readFile, writeFile } from 'node:fs/promises'
import path from 'node:path'
import type { APIRequestContext } from '@playwright/test'
import type { QABugRecord, QABugSeverity } from './foundryQaTypes'
import { assertAllowedFoundryQaEndpoint } from './foundryApiAllowlist'

type ApiRequestOptions = {
  method?: 'GET' | 'POST' | 'PUT' | 'DELETE'
  headers?: Record<string, string>
  query?: Record<string, string | number | boolean | undefined>
  json?: unknown
  body?: string | Buffer | ArrayBuffer | Uint8Array
  multipart?: Record<string, unknown>
}

export type ApiCallResult = {
  ok: boolean
  status: number
  body: unknown
  text: string
}

const guessBugFilePath = () => {
  const cwd = process.cwd()
  const fromFrontend = path.resolve(cwd, '../qa_bugs.json')
  const local = path.resolve(cwd, 'qa_bugs.json')
  return cwd.endsWith('/frontend') ? fromFrontend : local
}

const makeUrl = (endpoint: string, query?: ApiRequestOptions['query']) => {
  const params = new URLSearchParams()
  Object.entries(query ?? {}).forEach(([key, value]) => {
    if (value === undefined || value === null) {
      return
    }
    params.set(key, String(value))
  })
  if (!params.size) {
    return endpoint
  }
  return `${endpoint}${endpoint.includes('?') ? '&' : '?'}${params.toString()}`
}

const parseBody = (text: string) => {
  if (!text) {
    return null
  }
  try {
    return JSON.parse(text) as unknown
  } catch {
    return null
  }
}

const asRecord = (value: unknown) =>
  value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : null

export const buildAuthHeaders = (dbName?: string) => {
  const headers: Record<string, string> = {
    'Accept-Language': 'en',
    'X-Admin-Token': 'change_me',
    'X-User-ID': 'system',
    'X-User-Name': 'system',
    'X-User-Type': 'user',
  }
  if (dbName) {
    headers['X-DB-Name'] = dbName
    headers['X-Project'] = dbName
  }
  return headers
}

export const apiCall = async (
  request: APIRequestContext,
  endpoint: string,
  options: ApiRequestOptions = {},
): Promise<ApiCallResult> => {
  const method = options.method ?? 'GET'
  assertAllowedFoundryQaEndpoint(method, endpoint)
  const response = await request.fetch(makeUrl(endpoint, options.query), {
    method,
    headers: options.headers,
    data: options.json ?? options.body,
    multipart: options.multipart,
    failOnStatusCode: false,
  })
  const text = await response.text()
  return {
    ok: response.ok(),
    status: response.status(),
    body: parseBody(text),
    text,
  }
}

export const extractApiData = <T = Record<string, unknown>>(body: unknown): T | null => {
  const record = asRecord(body)
  if (!record) {
    return null
  }
  if ('data' in record) {
    return (record.data as T) ?? null
  }
  return body as T
}

export const extractCommandId = (body: unknown) => {
  const data = extractApiData<Record<string, unknown>>(body)
  if (!data) {
    return null
  }
  const direct = data.command_id ?? (asRecord(data.command)?.command_id ?? null)
  if (typeof direct === 'string' && direct.trim()) {
    return direct
  }
  return null
}

export const waitForCommandCompletion = async (
  request: APIRequestContext,
  commandId: string,
  headers: Record<string, string>,
  timeoutMs = 180_000,
) => {
  const deadline = Date.now() + timeoutMs
  let lastBody: unknown = null

  while (Date.now() < deadline) {
    const res = await apiCall(request, `/api/v1/commands/${encodeURIComponent(commandId)}/status`, {
      method: 'GET',
      headers,
    })
    lastBody = res.body
    if (res.ok) {
      const payload = asRecord(res.body) ?? {}
      const nested = asRecord(payload.data)
      const status = String(payload.status ?? nested?.status ?? '').toUpperCase()
      if (['COMPLETED', 'SUCCESS', 'SUCCEEDED', 'DONE'].includes(status)) {
        return { ok: true, status, body: res.body }
      }
      if (['FAILED', 'ERROR', 'CANCELLED'].includes(status)) {
        return { ok: false, status, body: res.body }
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }

  return { ok: false, status: 'TIMEOUT', body: lastBody }
}

export class QABugCollector {
  private readonly bugs: QABugRecord[] = []
  private readonly qaBugFilePath: string

  constructor() {
    this.qaBugFilePath = guessBugFilePath()
  }

  add(input: Omit<QABugRecord, 'id' | 'timestamp' | 'source'>) {
    this.bugs.push({
      id: `frontend-qa-${randomUUID()}`,
      timestamp: new Date().toISOString(),
      source: 'frontend_e2e',
      ...input,
    })
  }

  getAll() {
    return [...this.bugs]
  }

  countBySeverity(severity: QABugSeverity) {
    return this.bugs.filter((bug) => bug.severity === severity).length
  }

  async flush() {
    let existing: unknown[] = []
    try {
      await access(this.qaBugFilePath)
      const raw = await readFile(this.qaBugFilePath, 'utf-8')
      const parsed = JSON.parse(raw) as unknown
      existing = Array.isArray(parsed) ? parsed : []
    } catch {
      existing = []
    }
    const merged = [...existing, ...this.bugs]
    await writeFile(this.qaBugFilePath, `${JSON.stringify(merged, null, 2)}\n`, 'utf-8')
  }
}
