import { randomUUID } from 'node:crypto'
import { access, mkdir, readFile, writeFile } from 'node:fs/promises'
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

const QA_BUG_ARCHIVE_DIRNAME = 'qa_bugs_archive'
const QA_BUG_ARCHIVE_FILE_PREFIX = 'qa_bugs_run_'
const preparedQaBugFiles = new Set<string>()
const preparingQaBugFiles = new Map<string, Promise<void>>()

const parseBugList = (raw: string): unknown[] => {
  try {
    const parsed = JSON.parse(raw) as unknown
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

const archiveAndResetQaBugFileForRun = async (qaBugFilePath: string) => {
  await mkdir(path.dirname(qaBugFilePath), { recursive: true })

  let existing: unknown[] = []
  try {
    await access(qaBugFilePath)
    const raw = await readFile(qaBugFilePath, 'utf-8')
    existing = parseBugList(raw)
  } catch {
    existing = []
  }

  if (existing.length) {
    const archiveDir = path.join(path.dirname(qaBugFilePath), QA_BUG_ARCHIVE_DIRNAME)
    await mkdir(archiveDir, { recursive: true })
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
    const suffix = randomUUID().split('-')[0]
    const archivePath = path.join(archiveDir, `${QA_BUG_ARCHIVE_FILE_PREFIX}${timestamp}_${suffix}.json`)
    await writeFile(archivePath, `${JSON.stringify(existing, null, 2)}\n`, 'utf-8')
  }

  await writeFile(qaBugFilePath, '[]\n', 'utf-8')
}

const ensureQaBugFileRunScope = async (qaBugFilePath: string) => {
  if (preparedQaBugFiles.has(qaBugFilePath)) {
    return
  }
  const inFlight = preparingQaBugFiles.get(qaBugFilePath)
  if (inFlight) {
    await inFlight
    return
  }

  const task = (async () => {
    await archiveAndResetQaBugFileForRun(qaBugFilePath)
    preparedQaBugFiles.add(qaBugFilePath)
  })()

  preparingQaBugFiles.set(qaBugFilePath, task)
  try {
    await task
  } finally {
    preparingQaBugFiles.delete(qaBugFilePath)
  }
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
    await ensureQaBugFileRunScope(this.qaBugFilePath)

    let existing: unknown[] = []
    try {
      await access(this.qaBugFilePath)
      const raw = await readFile(this.qaBugFilePath, 'utf-8')
      existing = parseBugList(raw)
    } catch {
      existing = []
    }
    const merged = [...existing, ...this.bugs]
    await writeFile(this.qaBugFilePath, `${JSON.stringify(merged, null, 2)}\n`, 'utf-8')
    this.bugs.length = 0
  }
}
