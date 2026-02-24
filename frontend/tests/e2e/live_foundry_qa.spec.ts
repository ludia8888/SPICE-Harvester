import { createHash } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import path from 'node:path'
import { expect, test } from '@playwright/test'
import type { APIRequestContext, Locator, Page, TestInfo } from '@playwright/test'
import { seedLocalStorage } from '../utils/mockBff'
import { FOUNDRY_QA_API_CONTRACT } from './foundryApiContract'
import { isAllowedFoundryQaEndpoint, normalizeApiPath } from './foundryApiAllowlist'
import type {
  ActionSideEffectEvidence,
  ClosedLoopVerificationResult,
  DatasetIntegrityEvidence,
} from './foundryQaTypes'
import {
  QABugCollector,
  apiCall,
  buildAuthHeaders,
  extractApiData,
  extractCommandId,
  waitForCommandCompletion,
} from './foundryQaUtils'

const makeDbName = () => `qa_live_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`

const waitForDatabaseRow = async (page: Page, dbName: string) => {
  for (let attempt = 0; attempt < 45; attempt += 1) {
    const row = page.locator('tbody tr', { hasText: dbName })
    if ((await row.count()) > 0) {
      qaLog(`database row visible attempt=${attempt + 1}`)
      return row.first()
    }
    if (attempt === 0 || (attempt + 1) % 10 === 0) {
      qaLog(`waiting database row attempt=${attempt + 1}`)
    }
    await page.getByRole('button', { name: 'Refresh', exact: true }).click()
    await page.waitForTimeout(1000)
  }
  qaLog('database row wait exhausted')
  return null
}

const ensureDatabaseExistsViaApi = async (request: APIRequestContext, dbName: string) => {
  qaLog('fallback create database via api start')
  let create: Awaited<ReturnType<typeof apiCall>> | null = null
  for (let attempt = 0; attempt < 10; attempt += 1) {
    create = await apiCall(request, '/api/v1/databases', {
      method: 'POST',
      headers: {
        ...buildAuthHeaders(),
        'Content-Type': 'application/json',
      },
      json: {
        name: dbName,
        description: 'frontend live foundry qa fallback create',
      },
    })
    if (create.ok || create.status === 409) {
      break
    }
    if (create.status >= 500 || create.status === 429) {
      qaLog(`fallback create transient status=${create.status} attempt=${attempt + 1}`)
      await new Promise((resolve) => setTimeout(resolve, 1500))
      continue
    }
    qaLog(`fallback create failed status=${create.status}`)
    return false
  }
  if (!create || (!create.ok && create.status !== 409)) {
    qaLog(`fallback create exhausted status=${create?.status ?? 0}`)
    return false
  }

  const commandId = extractCommandId(create.body)
  if (commandId) {
    qaLog(`fallback command poll start command_id=${commandId}`)
    await waitForCommandCompletion(request, commandId, buildAuthHeaders(), 30_000)
    qaLog('fallback command poll done')
  }

  for (let attempt = 0; attempt < 60; attempt += 1) {
    const list = await apiCall(request, '/api/v1/databases', {
      method: 'GET',
      headers: buildAuthHeaders(),
    })
    if (list.ok) {
      const data = extractApiData<Record<string, unknown>>(list.body)
      const rows = Array.isArray(data?.databases) ? data.databases : []
      const names = rows.map((entry) => (typeof entry === 'string'
        ? entry
        : String((entry as Record<string, unknown>).name ?? (entry as Record<string, unknown>).db_name ?? '')))
      if (names.includes(dbName)) {
        qaLog(`fallback database visible via api attempt=${attempt + 1}`)
        return true
      }
    }
    if (attempt === 0 || (attempt + 1) % 15 === 0) {
      qaLog(`fallback waiting api list attempt=${attempt + 1}`)
    }
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }

  qaLog('fallback api list exhausted')
  return false
}

const openDatabaseFromList = async (page: Page, request: APIRequestContext, dbName: string) => {
  let row: Locator | null = page.locator('tbody tr', { hasText: dbName }).first()
  if ((await row.count()) === 0) {
    qaLog('database row missing, creating via frontend api')
    const ensured = await ensureDatabaseExistsViaApi(request, dbName)
    if (!ensured) {
      throw new Error(`Database create fallback failed: ${dbName}`)
    }
    row = await waitForDatabaseRow(page, dbName)
    if (!row) {
      qaLog('database row still missing after api create; navigating directly')
      await page.goto(`/db/${encodeURIComponent(dbName)}/overview?lang=en`)
      return
    }
  }
  if (!row || (await row.count()) === 0) {
    throw new Error(`Database row not found after retries: ${dbName}`)
  }
  await row.getByRole('button', { name: 'Open', exact: true }).click()
}

const fixturePath = (name: string) => path.resolve(process.cwd(), '../backend/tests/fixtures/kaggle_data', name)

const readFixtureCsv = async (name: string) => {
  const fullPath = fixturePath(name)
  return readFile(fullPath)
}

const csvFromRows = (header: string[], rows: Array<Array<string | number | null | undefined>>) => {
  const escape = (value: string | number | null | undefined) => {
    const raw = value === null || value === undefined ? '' : String(value)
    if (raw.includes(',') || raw.includes('"') || raw.includes('\n')) {
      return `"${raw.replace(/"/g, '""')}"`
    }
    return raw
  }
  return [header.map(escape).join(','), ...rows.map((row) => row.map(escape).join(','))].join('\n')
}

const parseCsv = (text: string) => {
  const rows: string[][] = []
  let row: string[] = []
  let field = ''
  let inQuotes = false
  const pushField = () => {
    row.push(field)
    field = ''
  }
  const pushRow = () => {
    rows.push(row)
    row = []
  }
  for (let i = 0; i < text.length; i += 1) {
    const char = text[i] ?? ''
    if (inQuotes) {
      if (char === '"') {
        const next = text[i + 1]
        if (next === '"') {
          field += '"'
          i += 1
          continue
        }
        inQuotes = false
        continue
      }
      field += char
      continue
    }
    if (char === '"') {
      inQuotes = true
      continue
    }
    if (char === ',') {
      pushField()
      continue
    }
    if (char === '\n') {
      pushField()
      pushRow()
      continue
    }
    if (char === '\r') {
      continue
    }
    field += char
  }
  if (inQuotes) {
    field = `${field}`
  }
  if (field.length > 0 || row.length > 0) {
    pushField()
    pushRow()
  }
  return rows
}

const sha256Hex = (buffer: Buffer) => createHash('sha256').update(buffer).digest('hex')

const decodeDatasetRawContent = (file: Record<string, unknown> | null): Buffer | null => {
  if (!file) {
    return null
  }
  const encoding = String(file.encoding ?? 'utf-8').trim().toLowerCase()
  const content = file.content
  if (typeof content !== 'string' || !content.length) {
    return null
  }
  if (encoding === 'base64') {
    return Buffer.from(content, 'base64')
  }
  if (encoding === 'utf-8' || encoding === 'utf8') {
    return Buffer.from(content, 'utf-8')
  }
  return null
}

const sleep = async (ms: number) =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

const fetchJsonWithRetry = async <T>(
  urls: string[],
  sourceLabel: string,
  rounds: number = 3,
): Promise<T> => {
  let lastError: unknown = null

  for (let round = 0; round < rounds; round += 1) {
    for (const url of urls) {
      try {
        const response = await fetch(url)
        if (!response.ok) {
          throw new Error(`${sourceLabel} fetch failed (${response.status})`)
        }
        return (await response.json()) as T
      } catch (error) {
        lastError = error
      }
    }
    if (round < rounds - 1) {
      await sleep((round + 1) * 750)
    }
  }

  if (lastError instanceof Error) {
    throw lastError
  }
  throw new Error(`${sourceLabel} fetch failed`)
}

const buildLiveWeatherCsv = async () => {
  const payload = await fetchJsonWithRetry<{
    hourly?: {
      time?: string[]
      temperature_2m?: number[]
      precipitation?: number[]
    }
  }>(
    [
      'https://api.open-meteo.com/v1/forecast?latitude=37.57&longitude=126.98&hourly=temperature_2m,precipitation&forecast_days=1&timezone=UTC',
      'https://api.open-meteo.com/v1/forecast?latitude=40.71&longitude=-74.01&hourly=temperature_2m,precipitation&forecast_days=1&timezone=UTC',
    ],
    'Open-Meteo',
  )
  const time = payload.hourly?.time ?? []
  const temp = payload.hourly?.temperature_2m ?? []
  const rain = payload.hourly?.precipitation ?? []
  const rows = time.slice(0, 24).map((ts, index) => [ts, temp[index] ?? null, rain[index] ?? null])
  return Buffer.from(csvFromRows(['timestamp', 'temperature_2m', 'precipitation'], rows), 'utf-8')
}

const buildLiveFxCsv = async () => {
  const payload = await fetchJsonWithRetry<{
    date?: string
    rates?: Record<string, number>
  }>(
    [
      'https://api.frankfurter.app/latest?from=USD',
      'https://api.frankfurter.app/latest?from=EUR',
    ],
    'Frankfurter',
  )
  const date = payload.date ?? new Date().toISOString().slice(0, 10)
  const rates = payload.rates ?? {}
  const base = rates.USD ? 'EUR' : 'USD'
  const rows = Object.entries(rates)
    .slice(0, 20)
    .map(([currency, rate]) => [date, base, currency, rate])
  return Buffer.from(csvFromRows(['date', 'base', 'quote', 'rate'], rows), 'utf-8')
}

const buildLiveEarthquakeCsv = async () => {
  const payload = await fetchJsonWithRetry<{
    features?: Array<{
      id?: string
      properties?: {
        mag?: number
        place?: string
        time?: number
      }
      geometry?: {
        coordinates?: number[]
      }
    }>
  }>(
    [
      'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson',
      'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson',
    ],
    'USGS',
  )
  const rows = (payload.features ?? []).slice(0, 100).map((feature) => {
    const coordinates = feature.geometry?.coordinates ?? []
    return [
      feature.id ?? '',
      feature.properties?.mag ?? null,
      feature.properties?.place ?? '',
      feature.properties?.time ?? null,
      coordinates[1] ?? null,
      coordinates[0] ?? null,
      coordinates[2] ?? null,
    ]
  })
  return Buffer.from(csvFromRows(['event_id', 'magnitude', 'place', 'epoch_ms', 'latitude', 'longitude', 'depth_km'], rows), 'utf-8')
}

const uploadCsvDataset = async (params: {
  request: APIRequestContext
  dbName: string
  datasetName: string
  fileName: string
  fileBuffer: Buffer
  bugs: QABugCollector
  phase: string
  step: string
}) => {
  const authHeaders = buildAuthHeaders(params.dbName)
  const jsonHeaders = {
    ...authHeaders,
    'Content-Type': 'application/json',
  }

  const createResponse = await apiCall(params.request, '/api/v2/datasets', {
    method: 'POST',
    headers: jsonHeaders,
    json: {
      name: params.datasetName,
      parentFolderRid: `ri.foundry.main.folder.${params.dbName}`,
    },
  })
  if (!createResponse.ok) {
    params.bugs.add({
      severity: 'P0',
      phase: params.phase,
      repro_steps: [
        'Open Databases page',
        'Create database',
        `Create dataset (${params.datasetName})`,
      ],
      expected: 'Dataset creation succeeds and returns dataset rid',
      actual: `status=${createResponse.status}; body=${createResponse.text.slice(0, 400)}`,
      endpoint: 'POST /api/v2/datasets',
      ui_path: `/db/${encodeURIComponent(params.dbName)}/overview`,
      evidence: {
        status: createResponse.status,
        body: createResponse.body,
        step: params.step,
      },
      hypothesis: 'Foundry dataset creation contract mismatch or registry write failure.',
    })
    return null
  }

  const createPayload = extractApiData<Record<string, unknown>>(createResponse.body)
  const datasetRid = typeof createPayload?.rid === 'string' ? createPayload.rid : null
  if (!datasetRid) {
    params.bugs.add({
      severity: 'P1',
      phase: params.phase,
      repro_steps: [`Create dataset (${params.datasetName})`],
      expected: 'rid exists in create response',
      actual: `Missing rid in response: ${JSON.stringify(createResponse.body).slice(0, 400)}`,
      endpoint: 'POST /api/v2/datasets',
      ui_path: `/db/${encodeURIComponent(params.dbName)}/overview`,
      evidence: { response: createResponse.body, step: params.step },
      hypothesis: 'Create succeeded partially but dataset registry payload is incomplete.',
    })
    return null
  }

  const transactionResponse = await apiCall(
    params.request,
    `/api/v2/datasets/${encodeURIComponent(datasetRid)}/transactions`,
    {
      method: 'POST',
      headers: jsonHeaders,
      json: { transactionType: 'APPEND' },
    },
  )
  const transactionPayload = extractApiData<Record<string, unknown>>(transactionResponse.body)
  const transactionRid = typeof transactionPayload?.rid === 'string' ? transactionPayload.rid : null
  if (!transactionResponse.ok || !transactionRid) {
    params.bugs.add({
      severity: 'P0',
      phase: params.phase,
      repro_steps: [`Create transaction (${params.datasetName})`],
      expected: 'Transaction creation succeeds and returns transaction rid',
      actual: `status=${transactionResponse.status}; body=${transactionResponse.text.slice(0, 400)}`,
      endpoint: 'POST /api/v2/datasets/{datasetRid}/transactions',
      ui_path: `/db/${encodeURIComponent(params.dbName)}/overview`,
      evidence: {
        dataset_rid: datasetRid,
        response: transactionResponse.body,
        step: params.step,
      },
      hypothesis: 'Transaction lifecycle initialization failed.',
    })
    return null
  }

  const uploadResponse = await apiCall(
    params.request,
    `/api/v2/datasets/${encodeURIComponent(datasetRid)}/files/${encodeURIComponent(params.fileName).replace(/%2F/g, '/')}/upload`,
    {
      method: 'POST',
      headers: {
        ...authHeaders,
        'Content-Type': 'text/csv',
      },
      query: {
        transactionRid,
        branchName: 'master',
      },
      body: params.fileBuffer,
    },
  )
  if (!uploadResponse.ok) {
    params.bugs.add({
      severity: 'P0',
      phase: params.phase,
      repro_steps: [`Upload file for dataset (${params.datasetName})`],
      expected: 'Dataset file upload succeeds',
      actual: `status=${uploadResponse.status}; body=${uploadResponse.text.slice(0, 400)}`,
      endpoint: 'POST /api/v2/datasets/{datasetRid}/files/{filePath}/upload',
      ui_path: `/db/${encodeURIComponent(params.dbName)}/overview`,
      evidence: {
        dataset_rid: datasetRid,
        response: uploadResponse.body,
        step: params.step,
      },
      hypothesis: 'Dataset file upload storage path failed.',
    })
    return null
  }

  const commitResponse = await apiCall(
    params.request,
    `/api/v2/datasets/${encodeURIComponent(datasetRid)}/transactions/${encodeURIComponent(transactionRid)}/commit`,
    {
      method: 'POST',
      headers: authHeaders,
    },
  )
  if (!commitResponse.ok) {
    params.bugs.add({
      severity: 'P0',
      phase: params.phase,
      repro_steps: [`Commit transaction (${params.datasetName})`],
      expected: 'Transaction commit succeeds',
      actual: `status=${commitResponse.status}; body=${commitResponse.text.slice(0, 400)}`,
      endpoint: 'POST /api/v2/datasets/{datasetRid}/transactions/{transactionRid}/commit',
      ui_path: `/db/${encodeURIComponent(params.dbName)}/overview`,
      evidence: {
        dataset_rid: datasetRid,
        transaction_rid: transactionRid,
        response: commitResponse.body,
        step: params.step,
      },
      hypothesis: 'Dataset version commit failed before publication.',
    })
    return null
  }

  const datasetId = String(datasetRid).replace(/^ri\.(?:spice|foundry)\.main\.dataset\./, '').trim()
  return {
    datasetId,
    ingestRequestId: null,
    rawResponse: {
      create: createResponse.body,
      transaction: transactionResponse.body,
      upload: uploadResponse.body,
      commit: commitResponse.body,
    },
  }
}

const createScreenshot = async (page: Page, testInfo: TestInfo, name: string) => {
  const path = testInfo.outputPath(`${name}.png`)
  await page.screenshot({ path, fullPage: true })
  return path
}

const qaLog = (message: string) => {
  // Keep runtime logs terse so stalled phases are easy to identify in CI/local runs.
  // eslint-disable-next-line no-console
  console.log(`[live_foundry_qa] ${new Date().toISOString()} ${message}`)
}

const parseDatasetRows = (body: unknown) => {
  const payload = extractApiData<Record<string, unknown>>(body)
  const rows = Array.isArray(payload?.datasets) ? payload.datasets : []
  return rows
    .map((entry) => (entry && typeof entry === 'object' && !Array.isArray(entry)
      ? (entry as Record<string, unknown>)
      : null))
    .filter((entry): entry is Record<string, unknown> => Boolean(entry))
}

const extractDatasetId = (row: Record<string, unknown>) =>
  String(row.dataset_id ?? row.datasetId ?? row.id ?? '').trim()

const extractDatasetIdFromRid = (datasetRid: string) =>
  String(datasetRid ?? '').replace(/^ri\.(?:spice|foundry)\.main\.dataset\./, '').trim()

const extractConnectionIdFromRid = (connectionRid: string) =>
  String(connectionRid ?? '').replace(/^ri\.(?:spice|foundry)\.main\.connection\./, '').trim()

const asObject = (value: unknown): Record<string, unknown> | null =>
  value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null

const asObjectFromApi = (value: unknown): Record<string, unknown> => {
  const direct = asObject(value)
  if (direct) {
    return direct
  }
  const extracted = extractApiData<Record<string, unknown>>(value)
  return asObject(extracted) ?? {}
}

const resolveQaBffOrigin = () => {
  const candidate = String(
    process.env.E2E_VITE_PROXY_TARGET
      ?? process.env.VITE_PROXY_TARGET
      ?? 'http://127.0.0.1:18012',
  ).trim()
  if (!candidate) {
    return 'http://127.0.0.1:18012'
  }
  if (candidate.startsWith('http://') || candidate.startsWith('https://')) {
    return candidate.replace(/\/+$/, '')
  }
  return `http://${candidate}`.replace(/\/+$/, '')
}

const resolveOrderPrimaryKey = (row: Record<string, unknown>): string | null => {
  const properties = asObject(row.properties)
  const candidate = String(
    row.__primaryKey
      ?? row.primaryKey
      ?? row.instance_id
      ?? row.order_id
      ?? properties?.order_id
      ?? properties?.instance_id
      ?? '',
  ).trim()
  return candidate || null
}

const resolveOrderStatus = (row: Record<string, unknown>): string | null => {
  const properties = asObject(row.properties)
  const candidate = String(
    row.order_status
      ?? properties?.order_status
      ?? '',
  ).trim()
  return candidate || null
}

test.describe.serial('Live Foundry QA', () => {
  test('foundry lifecycle via frontend-exposed api only', async ({ page, request, browserName }, testInfo) => {
    test.skip(browserName !== 'chromium', 'Foundry live QA is run in chromium only.')
    test.setTimeout(20 * 60 * 1000)

    const bugs = new QABugCollector()
    const dbName = makeDbName()
    const datasets = new Map<string, string>()
    const datasetVersions = new Map<string, string>()
    const datasetInputDigestByName = new Map<string, { sha256: string; bytes: number; fileName: string }>()
    const datasetIntegrityEvidence: Record<string, DatasetIntegrityEvidence> = {}
    let createdOrderInstanceId: string | null = null
    let pipelineBuildJobId: string | null = null
    let connectionExportEvidence: Record<string, unknown> | null = null
    let dynamicSecurityEvidence: Record<string, unknown> | null = null
    let kineticQueryEvidence: Record<string, unknown> | null = null
    let selectedActionTypeId: string | null = null
    const uiSurfaceFailures: Array<{ status: number; method: string; url: string; pageUrl: string }> = []
    const apiAllowlistViolations: Array<{ method: string; path: string; url: string; pageUrl: string }> = []
    let collectUiSurfaceFailures = false

    page.on('request', (requestInfo) => {
      if (!collectUiSurfaceFailures) {
        return
      }
      const method = requestInfo.method().toUpperCase()
      const url = requestInfo.url()
      const path = normalizeApiPath(url)
      if (!path.startsWith('/api/')) {
        return
      }
      if (isAllowedFoundryQaEndpoint(method, path)) {
        return
      }
      apiAllowlistViolations.push({
        method,
        path,
        url,
        pageUrl: page.url(),
      })
    })

    page.on('response', (response) => {
      if (!collectUiSurfaceFailures) {
        return
      }
      const status = response.status()
      if (status !== 404 && status < 500) {
        return
      }
      const url = response.url()
      if (!url.includes('/api/')) {
        return
      }
      uiSurfaceFailures.push({
        status,
        method: response.request().method(),
        url,
        pageUrl: page.url(),
      })
    })

    await seedLocalStorage(page, {
      adminToken: 'change_me',
      language: 'en',
      branch: 'main',
      project: null,
      rememberToken: true,
      theme: 'light',
    })

    await page.goto('/?lang=en')
    qaLog(`started db=${dbName}`)
    await expect(page.getByRole('heading', { level: 1, name: 'Projects' })).toBeVisible()

    qaLog('opening database from list')
    await openDatabaseFromList(page, request, dbName)
    qaLog('database opened')
    await expect(page).toHaveURL(new RegExp(`/db/${dbName}/overview`))

    // Frontend surface smoke: navigate through mandatory screens like a user.
    const requiredScreens = [
      `/db/${encodeURIComponent(dbName)}/overview`,
      `/db/${encodeURIComponent(dbName)}/ontology`,
      `/db/${encodeURIComponent(dbName)}/mappings`,
      `/db/${encodeURIComponent(dbName)}/instances`,
      `/db/${encodeURIComponent(dbName)}/explore/graph`,
      `/db/${encodeURIComponent(dbName)}/explore/query`,
      `/db/${encodeURIComponent(dbName)}/audit`,
      `/db/${encodeURIComponent(dbName)}/lineage`,
    ]
    collectUiSurfaceFailures = true
    for (const [index, route] of requiredScreens.entries()) {
      await page.goto(`${route}?lang=en`)
      await page.waitForTimeout(600)
      await createScreenshot(page, testInfo, `ui_surface_${index + 1}`)
    }
    qaLog('ui surface sweep done')
    collectUiSurfaceFailures = false
    await page.goto(`/db/${encodeURIComponent(dbName)}/overview?lang=en`)

    const seenUiFailure = new Set<string>()
    for (const failure of uiSurfaceFailures) {
      const endpointPath = (() => {
        try {
          return new URL(failure.url).pathname
        } catch {
          return failure.url
        }
      })()
      const key = `${failure.status}:${failure.method}:${endpointPath}`
      if (seenUiFailure.has(key)) {
        continue
      }
      seenUiFailure.add(key)
      bugs.add({
        severity: failure.status >= 500 ? 'P0' : 'P1',
        phase: 'Frontend surface',
        repro_steps: ['Open mandatory QA screens in sequence', `Observe failed API call (${failure.method} ${endpointPath})`],
        expected: 'Frontend-exposed API should not return 404/500 for mandatory QA routes.',
        actual: `status=${failure.status}; method=${failure.method}; url=${failure.url}`,
        endpoint: `${failure.method} ${endpointPath}`,
        ui_path: failure.pageUrl,
        evidence: failure,
        hypothesis: 'Frontend route is still using removed or incompatible API contract.',
      })
    }

    for (const violation of apiAllowlistViolations) {
      bugs.add({
        severity: 'P0',
        phase: 'Frontend surface',
        repro_steps: ['Open mandatory QA screens in sequence', `Observe API call outside allowlist (${violation.method} ${violation.path})`],
        expected: 'Frontend screens must call only allowlisted BFF/public API paths.',
        actual: `allowlist_violation=${violation.method} ${violation.path}`,
        endpoint: `${violation.method} ${violation.path}`,
        ui_path: violation.pageUrl,
        evidence: violation,
        hypothesis: 'Legacy or undocumented frontend API call is still wired in UI.',
      })
    }

    // Phase 1: Raw ingest (Kaggle fixtures)
    const kaggleFiles: Array<{ file: string; datasetName: string }> = [
      { file: 'olist_orders.csv', datasetName: 'olist_orders' },
      { file: 'olist_order_items.csv', datasetName: 'olist_order_items' },
      { file: 'olist_customers.csv', datasetName: 'olist_customers' },
      { file: 'olist_products.csv', datasetName: 'olist_products' },
      { file: 'olist_sellers.csv', datasetName: 'olist_sellers' },
    ]

    for (const [index, item] of kaggleFiles.entries()) {
      try {
        const csv = await readFixtureCsv(item.file)
        datasetInputDigestByName.set(item.datasetName, {
          sha256: sha256Hex(csv),
          bytes: csv.byteLength,
          fileName: item.file,
        })
        const uploaded = await uploadCsvDataset({
          request,
          dbName,
          datasetName: item.datasetName,
          fileName: item.file,
          fileBuffer: csv,
          bugs,
          phase: 'Raw ingest',
          step: `kaggle-${index + 1}`,
        })
        if (uploaded?.datasetId) {
          datasets.set(item.datasetName, uploaded.datasetId)
        }
      } catch (error) {
        bugs.add({
          severity: 'P0',
          phase: 'Raw ingest',
          repro_steps: [`Load fixture file ${item.file}`, 'Upload CSV'],
          expected: 'Kaggle fixture file can be loaded and uploaded',
          actual: error instanceof Error ? error.message : String(error),
          endpoint: 'local fixture + Foundry v2 dataset lifecycle',
          ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
          evidence: { file: item.file },
          hypothesis: 'Fixture path or upload contract is invalid.',
        })
      }
    }
    qaLog(`kaggle ingest done datasets=${datasets.size}`)

    // Phase 1b: Live API ingest
    const liveSources = [
      { key: 'live_weather', fileName: 'live_weather.csv', build: buildLiveWeatherCsv },
      { key: 'live_fx', fileName: 'live_fx.csv', build: buildLiveFxCsv },
      { key: 'live_earthquakes', fileName: 'live_earthquakes.csv', build: buildLiveEarthquakeCsv },
    ]

    for (const item of liveSources) {
      try {
        const csv = await item.build()
        datasetInputDigestByName.set(item.key, {
          sha256: sha256Hex(csv),
          bytes: csv.byteLength,
          fileName: item.fileName,
        })
        const uploaded = await uploadCsvDataset({
          request,
          dbName,
          datasetName: item.key,
          fileName: item.fileName,
          fileBuffer: csv,
          bugs,
          phase: 'Live ingest',
          step: item.key,
        })
        if (uploaded?.datasetId) {
          datasets.set(item.key, uploaded.datasetId)
        }
      } catch (error) {
        bugs.add({
          severity: 'P1',
          phase: 'Live ingest',
          repro_steps: [`Fetch source for ${item.key}`, 'Upload converted CSV'],
          expected: 'Live source can be converted and ingested as dataset',
          actual: error instanceof Error ? error.message : String(error),
          endpoint: `fetch external + Foundry v2 dataset lifecycle (${item.key})`,
          ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
          evidence: { source: item.key },
          hypothesis: 'External source temporary failure or parser mismatch.',
        })
      }
    }
    qaLog(`live ingest done datasets=${datasets.size}`)

    // Phase 1c: Foundry datasets v2 lifecycle ingest (create -> transaction -> upload -> commit -> readTable)
    {
      const datasetName = `v2_orders_${Date.now().toString(36)}`
      const csvBuffer = Buffer.from(
        csvFromRows(
          ['order_id', 'order_status', 'customer_id'],
          [
            [`qa_v2_order_${Date.now().toString(36)}`, 'PENDING', `qa_v2_customer_${Date.now().toString(36)}`],
            [`qa_v2_order_${Date.now().toString(36)}`, 'ON_HOLD', `qa_v2_customer_${Date.now().toString(36)}`],
          ],
        ),
        'utf-8',
      )
      datasetInputDigestByName.set('v2_orders', {
        sha256: sha256Hex(csvBuffer),
        bytes: csvBuffer.byteLength,
        fileName: 'source.csv',
      })
      const baseHeaders = {
        ...buildAuthHeaders(dbName),
        'Content-Type': 'application/json',
      }

      const createDatasetV2 = await apiCall(request, '/api/v2/datasets', {
        method: 'POST',
        headers: baseHeaders,
        json: {
          name: datasetName,
          parentFolderRid: `ri.foundry.main.folder.${dbName}`,
        },
      })
      if (!createDatasetV2.ok) {
        bugs.add({
          severity: 'P0',
          phase: 'Raw ingest',
          repro_steps: ['Create dataset via Foundry v2 API'],
          expected: 'POST /api/v2/datasets succeeds',
          actual: `status=${createDatasetV2.status}; body=${createDatasetV2.text.slice(0, 400)}`,
          endpoint: 'POST /api/v2/datasets',
          ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
          evidence: { response: createDatasetV2.body },
          hypothesis: 'Foundry datasets v2 create contract is broken.',
        })
      } else {
        const createPayload = extractApiData<Record<string, unknown>>(createDatasetV2.body)
        const datasetRid = typeof createPayload?.rid === 'string' ? createPayload.rid : null
        if (!datasetRid) {
          bugs.add({
            severity: 'P1',
            phase: 'Raw ingest',
            repro_steps: ['Create dataset via Foundry v2 API'],
            expected: 'Create response contains dataset rid',
            actual: `payload=${JSON.stringify(createDatasetV2.body).slice(0, 400)}`,
            endpoint: 'POST /api/v2/datasets',
            ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
            evidence: { response: createDatasetV2.body },
            hypothesis: 'Foundry v2 create response shape drifted.',
          })
        } else {
          const createTxn = await apiCall(
            request,
            `/api/v2/datasets/${encodeURIComponent(datasetRid)}/transactions`,
            {
              method: 'POST',
              headers: baseHeaders,
              json: { transactionType: 'APPEND' },
            },
          )
          const txnPayload = extractApiData<Record<string, unknown>>(createTxn.body)
          const transactionRid = typeof txnPayload?.rid === 'string' ? txnPayload.rid : null
          if (!createTxn.ok || !transactionRid) {
            bugs.add({
              severity: 'P0',
              phase: 'Raw ingest',
              repro_steps: ['Create Foundry v2 transaction'],
              expected: 'Transaction creation succeeds and returns transaction rid',
              actual: `status=${createTxn.status}; body=${createTxn.text.slice(0, 400)}`,
              endpoint: 'POST /api/v2/datasets/{datasetRid}/transactions',
              ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
              evidence: { dataset_rid: datasetRid, response: createTxn.body },
              hypothesis: 'Foundry v2 transaction lifecycle could not be initialized.',
            })
          } else {
            const uploadV2 = await apiCall(
              request,
              `/api/v2/datasets/${encodeURIComponent(datasetRid)}/files/source.csv/upload`,
              {
                method: 'POST',
                headers: {
                  ...buildAuthHeaders(dbName),
                  'Content-Type': 'text/csv',
                },
                query: {
                  transactionRid,
                  branchName: 'master',
                },
                body: csvBuffer,
              },
            )
            if (!uploadV2.ok) {
              bugs.add({
                severity: 'P0',
                phase: 'Raw ingest',
                repro_steps: ['Upload source.csv through Foundry v2 dataset file upload'],
                expected: 'CSV upload succeeds for transaction commit preparation',
                actual: `status=${uploadV2.status}; body=${uploadV2.text.slice(0, 400)}`,
                endpoint: 'POST /api/v2/datasets/{datasetRid}/files/{filePath}/upload',
                ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
                evidence: { dataset_rid: datasetRid, response: uploadV2.body },
                hypothesis: 'Foundry v2 file upload path is not writable.',
              })
            } else {
              const commitTxn = await apiCall(
                request,
                `/api/v2/datasets/${encodeURIComponent(datasetRid)}/transactions/${encodeURIComponent(transactionRid)}/commit`,
                {
                  method: 'POST',
                  headers: buildAuthHeaders(dbName),
                },
              )
              if (!commitTxn.ok) {
                bugs.add({
                  severity: 'P0',
                  phase: 'Raw ingest',
                  repro_steps: ['Commit Foundry v2 transaction after uploading source.csv'],
                  expected: 'Transaction commit succeeds and publishes dataset version',
                  actual: `status=${commitTxn.status}; body=${commitTxn.text.slice(0, 400)}`,
                  endpoint: 'POST /api/v2/datasets/{datasetRid}/transactions/{transactionRid}/commit',
                  ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
                  evidence: {
                    dataset_rid: datasetRid,
                    transaction_rid: transactionRid,
                    response: commitTxn.body,
                  },
                  hypothesis: 'Commit path failed while publishing dataset version metadata.',
                })
              } else {
                const readTableV2 = await apiCall(
                  request,
                  `/api/v2/datasets/${encodeURIComponent(datasetRid)}/readTable`,
                  {
                    method: 'GET',
                    headers: buildAuthHeaders(dbName),
                    query: {
                      rowLimit: 10,
                      branchName: 'master',
                      format: 'CSV',
                    },
                  },
                )
                const csvRows = parseCsv(readTableV2.text ?? '')
                const headerRow = csvRows[0] ?? []
                const tableRows = csvRows.slice(1)
                const totalRowCount = tableRows.length
                const columnNames = headerRow
                  .map((col) => String(col ?? '').trim())
                  .filter((name) => Boolean(name))
                const hasExpectedColumns = ['order_id', 'order_status', 'customer_id'].every((name) => columnNames.includes(name))
                if (!readTableV2.ok || tableRows.length < 2 || !hasExpectedColumns || totalRowCount !== 2) {
                  bugs.add({
                    severity: 'P1',
                    phase: 'Raw ingest',
                    repro_steps: ['Commit Foundry v2 transaction', 'Read table via Foundry v2 readTable'],
                    expected: 'readTable returns uploaded rows with expected schema columns',
                    actual: `status=${readTableV2.status}; columns=${columnNames.join(',')}; rows=${tableRows.length}; total=${totalRowCount}`,
                    endpoint: 'GET /api/v2/datasets/{datasetRid}/readTable',
                    ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
                    evidence: {
                      dataset_rid: datasetRid,
                      totalRowCount,
                      response: readTableV2.text.slice(0, 400),
                    },
                    hypothesis: 'Dataset version was committed but preview/sample projection is missing.',
                  })
                } else {
                  const v2DatasetId = extractDatasetIdFromRid(datasetRid)
                  if (v2DatasetId) {
                    datasets.set('v2_orders', v2DatasetId)
                  }
                }
              }
            }
          }
        }
      }
    }
    qaLog(`v2 ingest done datasets=${datasets.size}`)

    // Phase 1d: Connectivity writeback (connection -> export settings -> export run -> audit evidence)
    {
      const connectivityHeaders = {
        ...buildAuthHeaders(dbName),
        'Content-Type': 'application/json',
      }
      const connectionName = `qa_export_conn_${Date.now().toString(36)}`
      const qaBffOrigin = resolveQaBffOrigin()
      const exportTargetUrl = `${qaBffOrigin}/api/v1/health`

      const createConnection = await apiCall(request, '/api/v2/connectivity/connections', {
        method: 'POST',
        headers: connectivityHeaders,
        query: { preview: 'true' },
        json: {
          displayName: connectionName,
          markings: ['qa.internal'],
          connectionConfiguration: {
            type: 'GoogleSheetsConnectionConfig',
          },
        },
      })
      if (!createConnection.ok) {
        bugs.add({
          severity: 'P1',
          phase: 'Connectivity',
          repro_steps: ['Create connectivity connection via Foundry v2 API'],
          expected: 'POST /api/v2/connectivity/connections succeeds',
          actual: `status=${createConnection.status}; body=${createConnection.text.slice(0, 400)}`,
          endpoint: 'POST /api/v2/connectivity/connections',
          ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
          evidence: { response: createConnection.body },
          hypothesis: 'Foundry connectivity v2 connection create contract is broken in runtime.',
        })
      } else {
        const createConnectionPayload = asObjectFromApi(createConnection.body)
        const connectionRid = String(createConnectionPayload.rid ?? '').trim()
        if (!connectionRid) {
          bugs.add({
            severity: 'P1',
            phase: 'Connectivity',
            repro_steps: ['Create connectivity connection via Foundry v2 API'],
            expected: 'Connection create response includes connection rid',
            actual: `payload=${JSON.stringify(createConnection.body).slice(0, 400)}`,
            endpoint: 'POST /api/v2/connectivity/connections',
            ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
            evidence: { response: createConnection.body },
            hypothesis: 'Connectivity create response shape drifted and does not expose rid.',
          })
        } else {
          const updateExportSettings = await apiCall(
            request,
            `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/updateExportSettings`,
            {
              method: 'POST',
              headers: connectivityHeaders,
              query: { preview: 'true' },
              json: {
                exportSettings: {
                  exportsEnabled: true,
                  targetUrl: exportTargetUrl,
                  method: 'GET',
                  headers: {
                    'X-SPICE-QA': 'live-foundry-connectivity',
                  },
                },
              },
            },
          )
          if (!updateExportSettings.ok && updateExportSettings.status !== 204) {
            bugs.add({
              severity: 'P1',
              phase: 'Connectivity',
              repro_steps: ['Create connection', 'Update export settings'],
              expected: 'Export settings update succeeds for created connection',
              actual: `status=${updateExportSettings.status}; body=${updateExportSettings.text.slice(0, 400)}`,
              endpoint: 'POST /api/v2/connectivity/connections/{connectionRid}/updateExportSettings',
              ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
              evidence: {
                connection_rid: connectionRid,
                response: updateExportSettings.body,
              },
              hypothesis: 'Export settings normalization or persistence failed for connectivity connection.',
            })
          }

          const createExportRun = await apiCall(
            request,
            `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/exportRuns`,
            {
              method: 'POST',
              headers: connectivityHeaders,
              query: { preview: 'true' },
              json: {
                targetUrl: exportTargetUrl,
                method: 'GET',
                dryRun: false,
                timeoutMs: 15_000,
                headers: {
                  'X-SPICE-QA': 'live-foundry-connectivity',
                },
                payload: {
                  dbName,
                  phase: 'connectivity-export-run',
                  connectionRid,
                  timestamp: new Date().toISOString(),
                },
              },
            },
          )
          const createExportRunPayload = asObjectFromApi(createExportRun.body)
          const exportRunRid = String(createExportRunPayload.rid ?? '').trim()
          if (!createExportRun.ok || !exportRunRid) {
            bugs.add({
              severity: 'P1',
              phase: 'Connectivity',
              repro_steps: ['Create connection export run'],
              expected: 'POST /exportRuns accepts request and returns export run rid',
              actual: `status=${createExportRun.status}; body=${createExportRun.text.slice(0, 400)}`,
              endpoint: 'POST /api/v2/connectivity/connections/{connectionRid}/exportRuns',
              ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
              evidence: {
                connection_rid: connectionRid,
                response: createExportRun.body,
              },
              hypothesis: 'Connectivity export run task orchestration failed at creation time.',
            })
          } else {
            let exportRunStatus: string | null = null
            let exportRunTaskId: string | null = null
            let exportRunAuditLogId: string | null = null
            let sideEffectDelivery: Record<string, unknown> | null = null
            let lastExportRunResponse: unknown = null

            for (let attempt = 0; attempt < 30; attempt += 1) {
              const getExportRun = await apiCall(
                request,
                `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/exportRuns/${encodeURIComponent(exportRunRid)}`,
                {
                  method: 'GET',
                  headers: buildAuthHeaders(dbName),
                  query: { preview: 'true' },
                },
              )
              lastExportRunResponse = getExportRun.body
              if (getExportRun.ok) {
                const payload = asObjectFromApi(getExportRun.body)
                exportRunStatus = String(payload.status ?? '').trim().toUpperCase() || null
                exportRunTaskId = String(payload.taskId ?? '').trim() || null
                exportRunAuditLogId = String(payload.auditLogId ?? '').trim() || null
                sideEffectDelivery = asObject(payload.sideEffectDelivery)
                if (exportRunStatus && ['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(exportRunStatus)) {
                  break
                }
              }
              await page.waitForTimeout(1000)
            }

            if (exportRunStatus !== 'SUCCEEDED') {
              bugs.add({
                severity: 'P1',
                phase: 'Connectivity',
                repro_steps: ['Create export run', 'Poll export run status until terminal'],
                expected: 'Connectivity export run reaches SUCCEEDED state',
                actual: `status=${String(exportRunStatus)}`,
                endpoint: 'GET /api/v2/connectivity/connections/{connectionRid}/exportRuns/{exportRunRid}',
                ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
                evidence: {
                  connection_rid: connectionRid,
                  export_run_rid: exportRunRid,
                  response: lastExportRunResponse,
                },
                hypothesis: 'Export side-effect execution failed or task state transition is stuck.',
              })
            } else {
              const listExportRuns = await apiCall(
                request,
                `/api/v2/connectivity/connections/${encodeURIComponent(connectionRid)}/exportRuns`,
                {
                  method: 'GET',
                  headers: buildAuthHeaders(dbName),
                  query: {
                    preview: 'true',
                    pageSize: 50,
                  },
                },
              )
              if (!listExportRuns.ok) {
                bugs.add({
                  severity: 'P1',
                  phase: 'Connectivity',
                  repro_steps: ['Create export run', 'List export runs'],
                  expected: 'Export run list endpoint returns successfully',
                  actual: `status=${listExportRuns.status}; body=${listExportRuns.text.slice(0, 400)}`,
                  endpoint: 'GET /api/v2/connectivity/connections/{connectionRid}/exportRuns',
                  ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
                  evidence: {
                    connection_rid: connectionRid,
                    response: listExportRuns.body,
                  },
                  hypothesis: 'Connectivity export run list endpoint is not reading task registry correctly.',
                })
              } else {
                const listPayload = asObjectFromApi(listExportRuns.body)
                const rows = Array.isArray(listPayload.data)
                  ? listPayload.data
                      .map((entry) => asObject(entry))
                      .filter((entry): entry is Record<string, unknown> => Boolean(entry))
                  : []
                const listed = rows.some((row) => String(row.rid ?? '').trim() === exportRunRid)
                if (!listed) {
                  bugs.add({
                    severity: 'P1',
                    phase: 'Connectivity',
                    repro_steps: ['Create export run', 'List export runs'],
                    expected: 'Recently created export run appears in list endpoint',
                    actual: `missing_export_run_rid=${exportRunRid}`,
                    endpoint: 'GET /api/v2/connectivity/connections/{connectionRid}/exportRuns',
                    ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
                    evidence: {
                      connection_rid: connectionRid,
                      export_run_rid: exportRunRid,
                      response: listExportRuns.body,
                    },
                    hypothesis: 'Pagination/filtering dropped latest export run row.',
                  })
                }
              }

              const connectionId = extractConnectionIdFromRid(connectionRid)
              let auditEvidenceCount = 0
              if (connectionId && exportRunTaskId) {
                for (let attempt = 0; attempt < 20; attempt += 1) {
                  const auditLogs = await apiCall(request, '/api/v1/audit/logs', {
                    method: 'GET',
                    headers: buildAuthHeaders(dbName),
                    query: {
                      partition_key: `connectivity:${connectionId}`,
                      command_id: exportRunTaskId,
                      limit: 50,
                    },
                  })
                  if (auditLogs.ok) {
                    const payload = extractApiData<Record<string, unknown>>(auditLogs.body) ?? asObjectFromApi(auditLogs.body)
                    const items = Array.isArray(payload.items) ? payload.items : []
                    auditEvidenceCount = items.length
                    if (auditEvidenceCount > 0) {
                      break
                    }
                  }
                  await page.waitForTimeout(1000)
                }
              }

              if (exportRunTaskId && auditEvidenceCount === 0) {
                bugs.add({
                  severity: 'P1',
                  phase: 'Connectivity',
                  repro_steps: ['Create export run', 'Poll audit logs by command_id and connectivity partition'],
                  expected: 'At least one audit log is written for connectivity export run',
                  actual: `audit_count=${auditEvidenceCount}`,
                  endpoint: 'GET /api/v1/audit/logs',
                  ui_path: `/db/${encodeURIComponent(dbName)}/audit`,
                  evidence: {
                    connection_rid: connectionRid,
                    connection_id: connectionId,
                    export_run_rid: exportRunRid,
                    task_id: exportRunTaskId,
                    audit_log_id: exportRunAuditLogId,
                  },
                  hypothesis: 'Connectivity export run succeeded but audit evidence was not queryable by command id.',
                })
              }

              const deliveryStatus = String(sideEffectDelivery?.status ?? '').trim().toUpperCase()
              if (deliveryStatus !== 'DELIVERED') {
                bugs.add({
                  severity: 'P1',
                  phase: 'Connectivity',
                  repro_steps: ['Create export run', 'Inspect sideEffectDelivery in export run result'],
                  expected: 'sideEffectDelivery.status is DELIVERED for non-dry run export',
                  actual: `delivery_status=${deliveryStatus || 'n/a'}`,
                  endpoint: 'POST /api/v2/connectivity/connections/{connectionRid}/exportRuns',
                  ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
                  evidence: {
                    connection_rid: connectionRid,
                    export_run_rid: exportRunRid,
                    side_effect_delivery: sideEffectDelivery,
                  },
                  hypothesis: 'Writeback target call did not complete successfully despite terminal task status.',
                })
              }

              connectionExportEvidence = {
                connectionRid,
                connectionId,
                exportRunRid,
                taskId: exportRunTaskId,
                auditLogId: exportRunAuditLogId,
                status: exportRunStatus,
                sideEffectDelivery,
                targetUrl: exportTargetUrl,
                auditEvidenceCount,
              }
            }
          }
        }
      }
    }
    qaLog('connectivity phase done')

    // Data-flow guard: every uploaded dataset must be listed in registry and raw bytes retrievable.
    const listDatasetsResponse = await apiCall(request, '/api/v1/pipelines/datasets', {
      method: 'GET',
      headers: buildAuthHeaders(dbName),
      query: {
        db_name: dbName,
      },
    })
    if (!listDatasetsResponse.ok) {
      bugs.add({
        severity: 'P1',
        phase: 'Raw ingest',
        repro_steps: ['Ingest Kaggle + live datasets', 'List datasets from frontend API'],
        expected: 'Dataset registry returns uploaded datasets',
        actual: `status=${listDatasetsResponse.status}; body=${listDatasetsResponse.text.slice(0, 400)}`,
        endpoint: 'GET /api/v1/pipelines/datasets',
        ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
        evidence: { response: listDatasetsResponse.body },
        hypothesis: 'Ingest succeeded but dataset registry query path failed.',
      })
    } else {
      const listedRows = parseDatasetRows(listDatasetsResponse.body)
      const listedIds = new Set(
        listedRows
          .map((row) => extractDatasetId(row))
          .filter((id) => Boolean(id)),
      )
      for (const [datasetName, datasetId] of datasets.entries()) {
        if (!listedIds.has(datasetId)) {
          bugs.add({
            severity: 'P1',
            phase: 'Raw ingest',
            repro_steps: ['Upload dataset', 'List datasets from frontend API'],
            expected: `Uploaded dataset appears in registry (${datasetName})`,
            actual: `dataset_id_missing=${datasetId}`,
            endpoint: 'GET /api/v1/pipelines/datasets',
            ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
            evidence: {
              dataset_name: datasetName,
              dataset_id: datasetId,
              listed_dataset_ids: Array.from(listedIds.values()),
            },
            hypothesis: 'Dataset registry/list endpoint lost newly ingested dataset metadata.',
          })
        }
      }

      for (const [datasetName, datasetId] of datasets.entries()) {
        let rawVerified = false
        let rawVersionId: string | null = null
        let rawSha256: string | null = null
        let rawBytes: number | null = null
        let lastStatus = 0
        let lastBody: unknown = null
        for (let attempt = 0; attempt < 12; attempt += 1) {
          const rawFile = await apiCall(
            request,
            `/api/v1/pipelines/datasets/${encodeURIComponent(datasetId)}/raw-file`,
            {
              method: 'GET',
              headers: buildAuthHeaders(dbName),
            },
          )
          lastStatus = rawFile.status
          lastBody = rawFile.body
          if (rawFile.ok) {
            const payload = extractApiData<Record<string, unknown>>(rawFile.body)
            const file = (payload?.file && typeof payload.file === 'object' && !Array.isArray(payload.file))
              ? payload.file as Record<string, unknown>
              : null
            const content = typeof file?.content === 'string' ? file.content : ''
            const versionIdCandidate = String(file?.version_id ?? file?.versionId ?? '').trim()
            if (versionIdCandidate) {
              rawVersionId = versionIdCandidate
              datasetVersions.set(datasetId, versionIdCandidate)
            }
            const decodedBytes = decodeDatasetRawContent(file)
            if (decodedBytes && decodedBytes.length > 0) {
              rawSha256 = sha256Hex(decodedBytes)
              rawBytes = decodedBytes.byteLength
              rawVerified = true
              break
            }
            if (content.length > 0) {
              rawSha256 = sha256Hex(Buffer.from(content, 'utf-8'))
              rawBytes = Buffer.byteLength(content, 'utf-8')
              rawVerified = true
              break
            }
          }
          await page.waitForTimeout(1000)
        }

        if (!rawVerified) {
          bugs.add({
            severity: 'P1',
            phase: 'Raw ingest',
            repro_steps: ['Upload dataset', 'Read raw file via frontend API'],
            expected: `Raw file for ${datasetName} is retrievable and non-empty`,
            actual: `status=${lastStatus}; body=${JSON.stringify(lastBody).slice(0, 400)}`,
            endpoint: 'GET /api/v1/pipelines/datasets/{datasetId}/raw-file',
            ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
            evidence: {
              dataset_name: datasetName,
              dataset_id: datasetId,
              response_status: lastStatus,
              response_body: lastBody,
            },
            hypothesis: 'Dataset metadata exists but raw artifact retrieval path failed or artifact is empty.',
          })
          continue
        }

        const expectedDigest = datasetInputDigestByName.get(datasetName)
        datasetIntegrityEvidence[datasetName] = {
          datasetName,
          datasetId,
          expectedSha256: expectedDigest?.sha256 ?? 'n/a',
          actualSha256: rawSha256,
          expectedBytes: expectedDigest?.bytes ?? -1,
          actualBytes: rawBytes,
          checksumMatch: Boolean(expectedDigest && rawSha256 && expectedDigest.sha256 === rawSha256),
          byteLengthMatch: Boolean(expectedDigest && rawBytes !== null && expectedDigest.bytes === rawBytes),
          versionId: rawVersionId,
        }
        if (!rawVersionId) {
          bugs.add({
            severity: 'P1',
            phase: 'Raw ingest',
            repro_steps: ['Upload dataset', 'Read raw file via frontend API'],
            expected: `Raw file response for ${datasetName} contains version_id for version-pinned objectify`,
            actual: `version_id_missing dataset_id=${datasetId}`,
            endpoint: 'GET /api/v1/pipelines/datasets/{datasetId}/raw-file',
            ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
            evidence: {
              dataset_name: datasetName,
              dataset_id: datasetId,
              response_status: lastStatus,
              response_body: lastBody,
            },
            hypothesis: 'Dataset raw-file contract is missing version metadata required for deterministic objectify.',
          })
        }

        if (!expectedDigest) {
          bugs.add({
            severity: 'P2',
            phase: 'Raw ingest',
            repro_steps: ['Register dataset input digest', 'Compare with raw-file digest'],
            expected: `Expected input digest metadata exists for dataset ${datasetName}`,
            actual: `missing_input_digest dataset_id=${datasetId}`,
            endpoint: 'GET /api/v1/pipelines/datasets/{datasetId}/raw-file',
            ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
            evidence: {
              dataset_name: datasetName,
              dataset_id: datasetId,
              actual_sha256: rawSha256,
              actual_bytes: rawBytes,
            },
            hypothesis: 'QA ingest precondition tracking missed dataset input digest registration.',
          })
        } else if (!rawSha256 || rawSha256 !== expectedDigest.sha256 || rawBytes !== expectedDigest.bytes) {
          bugs.add({
            severity: 'P1',
            phase: 'Raw ingest',
            repro_steps: ['Upload dataset bytes', 'Read raw-file bytes via frontend API', 'Compare SHA-256/byte length'],
            expected: `Raw artifact bytes match uploaded source bytes for ${datasetName}`,
            actual: `expected_sha256=${expectedDigest.sha256}; actual_sha256=${rawSha256}; expected_bytes=${expectedDigest.bytes}; actual_bytes=${String(rawBytes)}`,
            endpoint: 'GET /api/v1/pipelines/datasets/{datasetId}/raw-file',
            ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
            evidence: {
              dataset_name: datasetName,
              dataset_id: datasetId,
              source_file: expectedDigest.fileName,
              expected_sha256: expectedDigest.sha256,
              actual_sha256: rawSha256,
              expected_bytes: expectedDigest.bytes,
              actual_bytes: rawBytes,
              response_status: lastStatus,
              response_body: lastBody,
            },
            hypothesis: 'Raw artifact bytes were transformed, truncated, or replaced between upload and retrieval.',
          })
        }
      }
    }

    await createScreenshot(page, testInfo, 'phase1_ingest')

    // Phase 2: Transform pipeline (join)
    const ordersDatasetId = datasets.get('olist_orders')
    const orderItemsDatasetId = datasets.get('olist_order_items')
    let pipelineId: string | null = null

    if (ordersDatasetId && orderItemsDatasetId) {
      const definition = {
        nodes: [
          { id: 'in_orders', type: 'input', metadata: { datasetId: ordersDatasetId } },
          { id: 'in_items', type: 'input', metadata: { datasetId: orderItemsDatasetId } },
          {
            id: 'join_oi',
            type: 'transform',
            metadata: {
              operation: 'join',
              leftKey: 'order_id',
              rightKey: 'order_id',
              joinType: 'inner',
            },
          },
          {
            id: 'out_enriched_orders',
            type: 'output',
            metadata: {
              outputName: 'enriched_orders',
              outputKind: 'dataset',
            },
          },
        ],
        edges: [
          { from: 'in_orders', to: 'join_oi' },
          { from: 'in_items', to: 'join_oi' },
          { from: 'join_oi', to: 'out_enriched_orders' },
        ],
        parameters: [],
        settings: { engine: 'Batch' },
      }

      const createPipeline = await apiCall(request, '/api/v1/pipelines', {
        method: 'POST',
        headers: {
          ...buildAuthHeaders(dbName),
          'Idempotency-Key': `qa-pipeline-${Date.now()}`,
          'Content-Type': 'application/json',
        },
        json: {
          db_name: dbName,
          name: `enriched_orders_${Date.now().toString(36)}`,
          description: 'Foundry QA transform pipeline',
          definition_json: definition,
          pipeline_type: 'batch',
          branch: 'main',
          location: 'qa',
        },
      })

      if (!createPipeline.ok) {
        bugs.add({
          severity: 'P0',
          phase: 'Transform',
          repro_steps: ['Create pipeline with join transform'],
          expected: 'Pipeline creation succeeds',
          actual: `status=${createPipeline.status}; body=${createPipeline.text.slice(0, 400)}`,
          endpoint: 'POST /api/v1/pipelines',
          ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
          evidence: { response: createPipeline.body },
          hypothesis: 'Pipeline definition validation or registry write failed.',
        })
      } else {
        const pipelineData = extractApiData<Record<string, unknown>>(createPipeline.body)
        const pipeline = (pipelineData?.pipeline ?? null) as Record<string, unknown> | null
        pipelineId =
          (typeof pipeline?.pipeline_id === 'string' && pipeline.pipeline_id) ||
          (typeof pipeline?.id === 'string' && pipeline.id) ||
          null
      }

      if (pipelineId) {
        const build = await apiCall(request, `/api/v1/pipelines/${encodeURIComponent(pipelineId)}/build`, {
          method: 'POST',
          headers: {
            ...buildAuthHeaders(dbName),
            'Content-Type': 'application/json',
            'Idempotency-Key': `qa-build-${Date.now()}`,
          },
          json: {
            db_name: dbName,
            branch: 'main',
            limit: 200,
          },
        })

        if (!build.ok) {
          bugs.add({
            severity: 'P0',
            phase: 'Transform',
            repro_steps: ['Build pipeline'],
            expected: 'Build request accepted',
            actual: `status=${build.status}; body=${build.text.slice(0, 400)}`,
            endpoint: 'POST /api/v1/pipelines/{pipelineId}/build',
            ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
            evidence: { pipelineId, response: build.body },
            hypothesis: 'Build worker or Spark pipeline execution failed.',
          })
        }

        const buildJobId =
          typeof (extractApiData<Record<string, unknown>>(build.body) ?? {}).job_id === 'string'
            ? String((extractApiData<Record<string, unknown>>(build.body) ?? {}).job_id)
            : null
        pipelineBuildJobId = buildJobId

        if (buildJobId) {
          const deadline = Date.now() + 5 * 60 * 1000
          let buildCompleted = false
          while (Date.now() < deadline) {
            const runs = await apiCall(request, `/api/v1/pipelines/${encodeURIComponent(pipelineId)}/runs`, {
              method: 'GET',
              headers: buildAuthHeaders(dbName),
              query: { limit: 50 },
            })
            if (runs.ok) {
              const runData = extractApiData<Record<string, unknown>>(runs.body)
              const runItems = Array.isArray(runData?.runs) ? runData.runs : []
              const matched = runItems.find((item) => {
                const row = item as Record<string, unknown>
                return String(row.job_id ?? '') === buildJobId
              }) as Record<string, unknown> | undefined
              const status = String(matched?.status ?? '').toUpperCase()
              if (['SUCCESS', 'COMPLETED', 'SUCCEEDED'].includes(status)) {
                buildCompleted = true
                break
              }
              if (status === 'FAILED') {
                bugs.add({
                  severity: 'P1',
                  phase: 'Transform',
                  repro_steps: ['Poll pipeline runs after build'],
                  expected: 'Build run completes successfully',
                  actual: `Build job failed: ${JSON.stringify(matched).slice(0, 400)}`,
                  endpoint: 'GET /api/v1/pipelines/{pipelineId}/runs',
                  ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
                  evidence: { pipelineId, buildJobId, run: matched },
                  hypothesis: 'Pipeline transform contract mismatch or execution runtime error.',
                })
                break
              }
            }
            await page.waitForTimeout(2000)
          }

          if (!buildCompleted) {
            bugs.add({
              severity: 'P1',
              phase: 'Transform',
              repro_steps: ['Build pipeline', 'Poll run status'],
              expected: 'Build reaches SUCCESS state within timeout',
              actual: 'Build did not complete within 5 minutes.',
              endpoint: 'POST /api/v1/pipelines/{pipelineId}/build + GET /runs',
              ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
              evidence: { pipelineId, buildJobId },
              hypothesis: 'Worker queue delay or blocked build execution.',
            })
          }
        }
        qaLog(`pipeline build/deploy done pipeline=${pipelineId} build_job=${String(buildJobId)}`)

        const deploy = await apiCall(request, `/api/v1/pipelines/${encodeURIComponent(pipelineId)}/deploy`, {
          method: 'POST',
          headers: {
            ...buildAuthHeaders(dbName),
            'Content-Type': 'application/json',
            'Idempotency-Key': `qa-deploy-${Date.now()}`,
          },
          json: {
            promote_build: true,
            build_job_id: buildJobId ?? undefined,
            node_id: 'out_enriched_orders',
            branch: 'main',
          },
        })

        if (!deploy.ok) {
          bugs.add({
            severity: 'P1',
            phase: 'Transform',
            repro_steps: ['Deploy pipeline after build'],
            expected: 'Deploy request succeeds',
            actual: `status=${deploy.status}; body=${deploy.text.slice(0, 400)}`,
            endpoint: 'POST /api/v1/pipelines/{pipelineId}/deploy',
            ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
            evidence: { pipelineId, response: deploy.body },
            hypothesis: 'Deploy lock conflict or promote-build constraints are unmet.',
          })
        }

        const readiness = await apiCall(request, `/api/v1/pipelines/${encodeURIComponent(pipelineId)}/readiness`, {
          method: 'GET',
          headers: buildAuthHeaders(dbName),
          query: { branch: 'main' },
        })
        if (!readiness.ok) {
          bugs.add({
            severity: 'P2',
            phase: 'Transform',
            repro_steps: ['Read pipeline readiness after build/deploy'],
            expected: 'Readiness endpoint returns input/output readiness state',
            actual: `status=${readiness.status}; body=${readiness.text.slice(0, 400)}`,
            endpoint: 'GET /api/v1/pipelines/{pipelineId}/readiness',
            ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
            evidence: { pipelineId, response: readiness.body },
            hypothesis: 'Pipeline readiness view contract failed after deployment.',
          })
        }

        const artifacts = await apiCall(request, `/api/v1/pipelines/${encodeURIComponent(pipelineId)}/artifacts`, {
          method: 'GET',
          headers: buildAuthHeaders(dbName),
          query: { mode: 'build', limit: 20 },
        })
        if (!artifacts.ok) {
          bugs.add({
            severity: 'P2',
            phase: 'Transform',
            repro_steps: ['Read pipeline artifacts after build/deploy'],
            expected: 'Artifact endpoint returns build artifacts',
            actual: `status=${artifacts.status}; body=${artifacts.text.slice(0, 400)}`,
            endpoint: 'GET /api/v1/pipelines/{pipelineId}/artifacts',
            ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
            evidence: { pipelineId, response: artifacts.body },
            hypothesis: 'Pipeline artifact index query failed in runtime.',
          })
        } else if (buildJobId) {
          const payload = extractApiData<Record<string, unknown>>(artifacts.body)
          const rows = Array.isArray(payload?.artifacts) ? payload.artifacts as Array<Record<string, unknown>> : []
          const hasBuildArtifact = rows.some((row) => String(row.job_id ?? row.jobId ?? '') === buildJobId)
          if (!hasBuildArtifact) {
            bugs.add({
              severity: 'P2',
              phase: 'Transform',
              repro_steps: ['Build pipeline', 'Read artifacts'],
              expected: `Artifact list contains build job (${buildJobId})`,
              actual: 'Build artifact not found in artifact list',
              endpoint: 'GET /api/v1/pipelines/{pipelineId}/artifacts',
              ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
              evidence: {
                pipelineId,
                build_job_id: buildJobId,
                artifacts: rows.slice(0, 10),
              },
              hypothesis: 'Build finished but artifact indexing/read endpoint is stale or incomplete.',
            })
          }
        }
      }
    } else {
      bugs.add({
        severity: 'P0',
        phase: 'Transform',
        repro_steps: ['Upload kaggle datasets', 'Create transform pipeline'],
        expected: 'orders + order_items datasets are available',
        actual: `Missing dataset ids: orders=${String(ordersDatasetId)}, items=${String(orderItemsDatasetId)}`,
        endpoint: 'ingest prerequisite',
        ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
        evidence: {
          availableDatasets: Object.fromEntries(datasets.entries()),
        },
        hypothesis: 'Raw ingest failed, blocking transformation phase.',
      })
    }
    qaLog('transform phase done')

    await createScreenshot(page, testInfo, 'phase2_transform')

    // Phase 3: Ontology object types
    const ontologyClasses = [
      {
        id: 'Customer',
        label: { en: 'Customer', ko: '고객' },
        description: { en: 'Customer object', ko: '고객 객체' },
        properties: [
          { name: 'customer_id', type: 'xsd:string', label: { en: 'Customer ID' }, required: true, primaryKey: true, titleKey: true },
          { name: 'customer_city', type: 'xsd:string', label: { en: 'City' } },
          { name: 'customer_state', type: 'xsd:string', label: { en: 'State' } },
        ],
        relationships: [],
      },
      {
        id: 'Order',
        label: { en: 'Order', ko: '주문' },
        description: { en: 'Order object', ko: '주문 객체' },
        properties: [
          { name: 'order_id', type: 'xsd:string', label: { en: 'Order ID' }, required: true, primaryKey: true, titleKey: true },
          { name: 'order_status', type: 'xsd:string', label: { en: 'Status' } },
          { name: 'customer_id', type: 'xsd:string', label: { en: 'Customer ID' } },
        ],
        relationships: [],
      },
      {
        id: 'Seller',
        label: { en: 'Seller', ko: '판매자' },
        description: { en: 'Seller object', ko: '판매자 객체' },
        properties: [
          { name: 'seller_id', type: 'xsd:string', label: { en: 'Seller ID' }, required: true, primaryKey: true, titleKey: true },
          { name: 'seller_city', type: 'xsd:string', label: { en: 'City' } },
        ],
        relationships: [],
      },
      {
        id: 'Payment',
        label: { en: 'Payment', ko: '결제' },
        description: { en: 'Payment object', ko: '결제 객체' },
        properties: [
          { name: 'payment_id', type: 'xsd:string', label: { en: 'Payment ID' }, required: true, primaryKey: true, titleKey: true },
          { name: 'order_id', type: 'xsd:string', label: { en: 'Order ID' } },
          { name: 'payment_value', type: 'xsd:string', label: { en: 'Value' } },
        ],
        relationships: [],
      },
    ]

    for (const klass of ontologyClasses) {
      const createOntology = await apiCall(request, `/api/v1/databases/${encodeURIComponent(dbName)}/ontology`, {
        method: 'POST',
        headers: {
          ...buildAuthHeaders(dbName),
          'Content-Type': 'application/json',
          'Idempotency-Key': `qa-ontology-${klass.id}-${Date.now()}`,
        },
        query: { branch: 'main' },
        json: klass,
      })

      if (!createOntology.ok && createOntology.status !== 409) {
        bugs.add({
          severity: 'P1',
          phase: 'Ontology',
          repro_steps: [`Create ontology class ${klass.id}`],
          expected: 'Ontology create command accepted',
          actual: `status=${createOntology.status}; body=${createOntology.text.slice(0, 400)}`,
          endpoint: 'POST /api/v1/databases/{db}/ontology',
          ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
          evidence: { classId: klass.id, response: createOntology.body },
          hypothesis: 'Ontology validation or OCC policy rejected class payload.',
        })
      }

      const commandId = extractCommandId(createOntology.body)
      if (commandId) {
        const commandResult = await waitForCommandCompletion(request, commandId, buildAuthHeaders(dbName))
        if (!commandResult.ok) {
          bugs.add({
            severity: 'P1',
            phase: 'Ontology',
            repro_steps: [`Wait ontology create command (${commandId})`],
            expected: 'Ontology command reaches COMPLETED',
            actual: `status=${commandResult.status}; payload=${JSON.stringify(commandResult.body).slice(0, 400)}`,
            endpoint: 'GET /api/v1/commands/{commandId}/status',
            ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
            evidence: { command_id: commandId, payload: commandResult.body },
            hypothesis: 'Ontology worker failed during asynchronous apply.',
          })
        }
      }
    }
    qaLog('ontology class create/list done')

    const orderDatasetId = datasets.get('v2_orders') ?? datasets.get('olist_orders')
    if (orderDatasetId) {
      const createDraftContract = await apiCall(request, `/api/v2/ontologies/${encodeURIComponent(dbName)}/objectTypes`, {
        method: 'POST',
        headers: {
          ...buildAuthHeaders(dbName),
          'Content-Type': 'application/json',
        },
        query: { branch: 'main' },
        json: {
          apiName: 'Order',
          status: 'DRAFT',
        },
      })

      if (!createDraftContract.ok && createDraftContract.status !== 409) {
        bugs.add({
          severity: 'P1',
          phase: 'Ontology',
          repro_steps: ['Create v2 object type contract (DRAFT) for Order'],
          expected: 'Object type contract is created via v2 endpoint',
          actual: `status=${createDraftContract.status}; body=${createDraftContract.text.slice(0, 400)}`,
          endpoint: 'POST /api/v2/ontologies/{ontology}/objectTypes',
          ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
          evidence: { response: createDraftContract.body },
          hypothesis: 'Foundry v2 object type write surface is unavailable or validation schema mismatched.',
        })
      }

      const activateContract = await apiCall(
        request,
        `/api/v2/ontologies/${encodeURIComponent(dbName)}/objectTypes/${encodeURIComponent('Order')}`,
        {
          method: 'PATCH',
          headers: {
            ...buildAuthHeaders(dbName),
            'Content-Type': 'application/json',
          },
          query: { branch: 'main' },
          json: {
            status: 'ACTIVE',
            pkSpec: {
              primary_key: ['order_id'],
              title_key: ['order_id'],
            },
            backingSources: [
              {
                dataset_id: orderDatasetId,
              },
            ],
          },
        },
      )

      if (!activateContract.ok) {
        bugs.add({
          severity: 'P1',
          phase: 'Ontology',
          repro_steps: ['Promote object type contract to ACTIVE with backing source'],
          expected: 'Order object type contract becomes ACTIVE',
          actual: `status=${activateContract.status}; body=${activateContract.text.slice(0, 400)}`,
          endpoint: 'PATCH /api/v2/ontologies/{ontology}/objectTypes/{objectType}',
          ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
          evidence: { response: activateContract.body, dataset_id: orderDatasetId },
          hypothesis: 'DRAFT→ACTIVE promotion path still rejects backing source or key contract.',
        })
      } else {
        const getOrderObjectType = await apiCall(
          request,
          `/api/v2/ontologies/${encodeURIComponent(dbName)}/objectTypes/${encodeURIComponent('Order')}`,
          {
            method: 'GET',
            headers: buildAuthHeaders(dbName),
            query: { branch: 'main' },
          },
        )
        const objectTypePayload = extractApiData<Record<string, unknown>>(getOrderObjectType.body) ?? {}
        const resolvedStatus = String(
          objectTypePayload.status
            ?? (objectTypePayload.data as Record<string, unknown> | undefined)?.status
            ?? '',
        ).toUpperCase()
        if (!getOrderObjectType.ok || resolvedStatus !== 'ACTIVE') {
          bugs.add({
            severity: 'P1',
            phase: 'Ontology',
            repro_steps: ['Fetch Order object type after ACTIVE promotion'],
            expected: 'Order object type status is ACTIVE',
            actual: `status=${getOrderObjectType.status}; object_status=${resolvedStatus || 'unknown'}`,
            endpoint: 'GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}',
            ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
            evidence: { response: getOrderObjectType.body },
            hypothesis: 'Contract promotion persisted incompletely or v2 read projection is stale.',
          })
        }
      }
    }

    const objectTypes = await apiCall(request, `/api/v2/ontologies/${encodeURIComponent(dbName)}/objectTypes`, {
      method: 'GET',
      headers: buildAuthHeaders(dbName),
      query: {
        branch: 'main',
        pageSize: 1000,
      },
    })

    if (!objectTypes.ok) {
      bugs.add({
        severity: 'P0',
        phase: 'Ontology',
        repro_steps: ['List object types'],
        expected: 'Object type list loads from v2 ontology API',
        actual: `status=${objectTypes.status}; body=${objectTypes.text.slice(0, 400)}`,
        endpoint: 'GET /api/v2/ontologies/{ontology}/objectTypes',
        ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
        evidence: { response: objectTypes.body },
        hypothesis: 'v2 ontology read surface not reachable for the newly created project.',
      })
    }

    await createScreenshot(page, testInfo, 'phase3_ontology')

    // Phase 4: Objectify mapping + run
    if (orderDatasetId) {
      let orderDatasetVersionId = datasetVersions.get(orderDatasetId) ?? null
      const mappingSpec = await apiCall(request, '/api/v1/objectify/mapping-specs', {
        method: 'POST',
        headers: {
          ...buildAuthHeaders(dbName),
          'Content-Type': 'application/json',
        },
        json: {
          dataset_id: orderDatasetId,
          dataset_branch: 'main',
          target_class_id: 'Order',
          mappings: [
            { source_field: 'order_id', target_field: 'order_id' },
            { source_field: 'order_status', target_field: 'order_status' },
            { source_field: 'customer_id', target_field: 'customer_id' },
          ],
          auto_sync: true,
        },
      })

      let mappingSpecId: string | null = null

      if (!mappingSpec.ok) {
        bugs.add({
          severity: 'P1',
          phase: 'Objectify',
          repro_steps: ['Create mapping spec for Order'],
          expected: 'Mapping spec is created',
          actual: `status=${mappingSpec.status}; body=${mappingSpec.text.slice(0, 400)}`,
          endpoint: 'POST /api/v1/objectify/mapping-specs',
          ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
          evidence: { response: mappingSpec.body },
          hypothesis: 'Mapping spec validation failed or dataset/class linkage is invalid.',
        })
      } else {
        const data = extractApiData<Record<string, unknown>>(mappingSpec.body)
        mappingSpecId =
          (typeof data?.mapping_spec_id === 'string' && data.mapping_spec_id) ||
          (typeof data?.mappingSpecId === 'string' && data.mappingSpecId) ||
          null
        const mappingSpecRecord = asObject(data?.mapping_spec)
        const mappingSpecOptions = asObject(mappingSpecRecord?.options)
        const impactScope = asObject(mappingSpecOptions?.impact_scope)
        const mappingDatasetVersionId = String(
          mappingSpecRecord?.dataset_version_id
            ?? mappingSpecRecord?.datasetVersionId
            ?? impactScope?.dataset_version_id
            ?? '',
        ).trim() || null
        if (mappingDatasetVersionId) {
          orderDatasetVersionId = mappingDatasetVersionId
          datasetVersions.set(orderDatasetId, mappingDatasetVersionId)
        }
      }

      if (mappingSpecId) {
        if (!orderDatasetVersionId) {
          bugs.add({
            severity: 'P1',
            phase: 'Objectify',
            repro_steps: ['Resolve dataset version id from raw ingest or mapping preflight'],
            expected: 'Objectify run uses explicit dataset_version_id (version-pinned execution)',
            actual: `dataset_version_missing dataset_id=${orderDatasetId}`,
            endpoint: 'POST /api/v1/objectify/datasets/{datasetId}/run',
            ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
            evidence: { mapping_spec_id: mappingSpecId, dataset_id: orderDatasetId },
            hypothesis: 'Frontend-exposed dataset/mapping contracts did not surface version_id for deterministic objectify.',
          })
        } else {
          const runObjectify = await apiCall(request, `/api/v1/objectify/datasets/${encodeURIComponent(orderDatasetId)}/run`, {
            method: 'POST',
            headers: {
              ...buildAuthHeaders(dbName),
              'Content-Type': 'application/json',
            },
            json: {
              mapping_spec_id: mappingSpecId,
              dataset_version_id: orderDatasetVersionId,
              max_rows: 2000,
              allow_partial: true,
            },
          })

          if (!runObjectify.ok) {
            bugs.add({
              severity: 'P1',
              phase: 'Objectify',
              repro_steps: ['Run objectify job for orders dataset with pinned dataset_version_id'],
              expected: 'Objectify run is queued successfully',
              actual: `status=${runObjectify.status}; body=${runObjectify.text.slice(0, 400)}`,
              endpoint: 'POST /api/v1/objectify/datasets/{datasetId}/run',
              ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
              evidence: {
                mapping_spec_id: mappingSpecId,
                dataset_version_id: orderDatasetVersionId,
                response: runObjectify.body,
              },
              hypothesis: 'Objectify queue or mapping contract mismatch blocked execution.',
            })
          }
        }
      }
    }
    qaLog('objectify phase done')

    // Phase 5: Projection + multihop query
    let graphQuery = await apiCall(request, `/api/v1/graph-query/${encodeURIComponent(dbName)}`, {
      method: 'POST',
      headers: {
        ...buildAuthHeaders(dbName),
        'Content-Type': 'application/json',
      },
      query: {
        base_branch: 'main',
      },
      json: {
        start_class: 'Order',
        hops: [],
        filters: {},
        limit: 50,
        max_nodes: 200,
        max_edges: 500,
        include_paths: true,
        no_cycles: true,
        include_documents: true,
      },
    })
    for (let attempt = 0; attempt < 20 && !graphQuery.ok; attempt += 1) {
      await page.waitForTimeout(1000)
      graphQuery = await apiCall(request, `/api/v1/graph-query/${encodeURIComponent(dbName)}`, {
        method: 'POST',
        headers: {
          ...buildAuthHeaders(dbName),
          'Content-Type': 'application/json',
        },
        query: {
          base_branch: 'main',
        },
        json: {
          start_class: 'Order',
          hops: [],
          filters: {},
          limit: 50,
          max_nodes: 200,
          max_edges: 500,
          include_paths: true,
          no_cycles: true,
          include_documents: true,
        },
      })
    }

    if (!graphQuery.ok) {
      bugs.add({
        severity: 'P1',
        phase: 'Graph/Multihop',
        repro_steps: ['Run graph query from Order object type'],
        expected: 'Graph query succeeds and returns nodes/edges',
        actual: `status=${graphQuery.status}; body=${graphQuery.text.slice(0, 400)}`,
        endpoint: 'POST /api/v1/graph-query/{db}',
        ui_path: `/db/${encodeURIComponent(dbName)}/explore/graph`,
        evidence: { response: graphQuery.body },
        hypothesis: 'Graph federation index is missing or ontology-to-index mapping is incomplete.',
      })
    }

    const createLinkedInstance = async (
      classLabel: 'Order',
      payload: Record<string, unknown>,
      stepLabel: string,
    ) => {
      const createResponse = await apiCall(
        request,
        `/api/v1/databases/${encodeURIComponent(dbName)}/instances/${encodeURIComponent(classLabel)}/create`,
        {
          method: 'POST',
          headers: {
            ...buildAuthHeaders(dbName),
            'Content-Type': 'application/json',
          },
          query: { branch: 'main' },
          json: {
            data: payload,
            metadata: {
              source: 'frontend_live_foundry_qa_multihop',
              step: stepLabel,
            },
          },
        },
      )
      if (!createResponse.ok) {
        bugs.add({
          severity: 'P1',
          phase: 'Graph/Multihop',
          repro_steps: [`Create ${classLabel} seed instance (${stepLabel}) for dedicated multihop path`],
          expected: `${classLabel} seed instance is accepted`,
          actual: `status=${createResponse.status}; body=${createResponse.text.slice(0, 300)}`,
          endpoint: 'POST /api/v1/databases/{db}/instances/{class}/create',
          ui_path: `/db/${encodeURIComponent(dbName)}/instances`,
          evidence: {
            classLabel,
            payload,
            response: createResponse.body,
          },
          hypothesis: 'Async instance write path rejected multihop seed payload.',
        })
        return false
      }

      const commandId = extractCommandId(createResponse.body)
      if (!commandId) {
        bugs.add({
          severity: 'P1',
          phase: 'Graph/Multihop',
          repro_steps: [`Create ${classLabel} seed instance (${stepLabel})`],
          expected: 'Create response contains command_id for async completion polling',
          actual: `command_id missing: ${JSON.stringify(createResponse.body).slice(0, 300)}`,
          endpoint: 'POST /api/v1/databases/{db}/instances/{class}/create',
          ui_path: `/db/${encodeURIComponent(dbName)}/instances`,
          evidence: {
            classLabel,
            payload,
            response: createResponse.body,
          },
          hypothesis: 'Async command envelope contract drifted for instance create responses.',
        })
        return false
      }

      const completion = await waitForCommandCompletion(
        request,
        commandId,
        buildAuthHeaders(dbName),
        300_000,
      )
      if (!completion.ok) {
        bugs.add({
          severity: 'P1',
          phase: 'Graph/Multihop',
          repro_steps: [`Wait for ${classLabel} seed create command completion (${commandId})`],
          expected: 'Create command reaches COMPLETED',
          actual: `status=${completion.status}; body=${JSON.stringify(completion.body).slice(0, 300)}`,
          endpoint: 'GET /api/v1/commands/{commandId}/status',
          ui_path: `/db/${encodeURIComponent(dbName)}/instances`,
          evidence: {
            classLabel,
            payload,
            command_id: commandId,
            command_response: completion.body,
          },
          hypothesis: 'Instance worker failed before graph projection materialization.',
        })
        return false
      }
      return true
    }

    const multiHopSeed = {
      orderHeadId: `-qa_multihop_order_head_${Date.now().toString(36)}`,
      orderMidId: `-qa_multihop_order_mid_${Date.now().toString(36)}`,
      orderTailId: `-qa_multihop_order_tail_${Date.now().toString(36)}`,
    }

    const tailReady = await createLinkedInstance(
      'Order',
      {
        order_id: multiHopSeed.orderTailId,
        order_status: 'PENDING',
        customer_id: `qa_multihop_customer_${Date.now().toString(36)}`,
      },
      'seed_order_tail',
    )
    const midReady = tailReady
      ? await createLinkedInstance(
        'Order',
        {
          order_id: multiHopSeed.orderMidId,
          order_status: 'PENDING',
          customer_id: `qa_multihop_customer_${Date.now().toString(36)}`,
          next_ref: `Order/${multiHopSeed.orderTailId}`,
        },
        'seed_order_mid',
      )
      : false
    const headReady = midReady
      ? await createLinkedInstance(
        'Order',
        {
          order_id: multiHopSeed.orderHeadId,
          order_status: 'PENDING',
          customer_id: `qa_multihop_customer_${Date.now().toString(36)}`,
          next_ref: `Order/${multiHopSeed.orderMidId}`,
        },
        'seed_order_head',
      )
      : false

    let dedicatedMultiHopVerified = false
    let dedicatedMultiHopEvidence: Record<string, unknown> = {
      seed: multiHopSeed,
      tail_ready: tailReady,
      mid_ready: midReady,
      head_ready: headReady,
    }

    if (headReady) {
      const toObj = (value: unknown) =>
        value && typeof value === 'object' && !Array.isArray(value)
          ? (value as Record<string, unknown>)
          : null

      for (let attempt = 0; attempt < 25; attempt += 1) {
        const dedicatedMultiHop = await apiCall(request, `/api/v1/graph-query/${encodeURIComponent(dbName)}`, {
          method: 'POST',
          headers: {
            ...buildAuthHeaders(dbName),
            'Content-Type': 'application/json',
          },
          query: {
            base_branch: 'main',
          },
          json: {
            start_class: 'Order',
            hops: [
              { predicate: 'next_ref', target_class: 'Order' },
              { predicate: 'next_ref', target_class: 'Order' },
            ],
            filters: {},
            limit: 20,
            max_nodes: 100,
            max_edges: 200,
            include_paths: true,
            max_paths: 50,
            no_cycles: false,
            include_documents: true,
          },
        })

        const graphPayload = toObj(extractApiData<Record<string, unknown>>(dedicatedMultiHop.body)) ?? toObj(dedicatedMultiHop.body)
        const nodes = Array.isArray(graphPayload?.nodes)
          ? graphPayload.nodes
            .map((entry) => toObj(entry))
            .filter((entry): entry is Record<string, unknown> => Boolean(entry))
          : []
        const edges = Array.isArray(graphPayload?.edges)
          ? graphPayload.edges
            .map((entry) => toObj(entry))
            .filter((entry): entry is Record<string, unknown> => Boolean(entry))
          : []
        const paths = Array.isArray(graphPayload?.paths) ? graphPayload.paths : []

        const nodeIds = nodes.map((row) => String(row.id ?? '').trim()).filter(Boolean)
        const predicates = edges.map((row) => String(row.predicate ?? '').trim()).filter(Boolean)
        const hasRequiredNodes = [
          `Order/${multiHopSeed.orderHeadId}`,
          `Order/${multiHopSeed.orderMidId}`,
          `Order/${multiHopSeed.orderTailId}`,
        ].every((id) => nodeIds.includes(id))
        const hasRequiredPredicates = ['next_ref'].every((predicate) => predicates.includes(predicate))
        const hasTwoHopPath = paths.some((entry) => {
          if (Array.isArray(entry)) {
            return entry.length >= 3
          }
          const row = toObj(entry)
          if (!row) {
            return false
          }
          const pathNodes = Array.isArray(row.nodes) ? row.nodes : []
          const hops = Number(row.hops ?? Math.max(0, pathNodes.length - 1))
          return Number.isFinite(hops) && hops >= 2
        })

        dedicatedMultiHopEvidence = {
          ...dedicatedMultiHopEvidence,
          attempt: attempt + 1,
          response_status: dedicatedMultiHop.status,
          node_count: nodes.length,
          edge_count: edges.length,
          path_count: paths.length,
          node_ids_sample: nodeIds.slice(0, 10),
          predicates_sample: predicates.slice(0, 10),
          has_required_nodes: hasRequiredNodes,
          has_required_predicates: hasRequiredPredicates,
          has_two_hop_path: hasTwoHopPath,
          response: graphPayload ?? dedicatedMultiHop.body,
        }

        if (dedicatedMultiHop.ok && hasRequiredNodes && hasRequiredPredicates && hasTwoHopPath) {
          dedicatedMultiHopVerified = true
          break
        }
        await page.waitForTimeout(1000)
      }
    }

    if (!dedicatedMultiHopVerified) {
      bugs.add({
        severity: 'P1',
        phase: 'Graph/Multihop',
        repro_steps: [
          'Create deterministic seed instances for Order -> Order -> Order chain',
          'Run dedicated 2-hop graph query with start_class=Order',
        ],
        expected: 'Graph query returns seeded nodes, required predicates, and at least one 2-hop path.',
        actual: `dedicated_multihop_verified=${dedicatedMultiHopVerified}`,
        endpoint: 'POST /api/v1/graph-query/{db}',
        ui_path: `/db/${encodeURIComponent(dbName)}/explore/graph`,
        evidence: dedicatedMultiHopEvidence,
        hypothesis: 'Relationships were not materialized into graph index or query traversal contract drifted.',
      })
    }

    const recomputeProjection = await apiCall(request, '/api/v1/admin/recompute-projection', {
      method: 'POST',
      headers: {
        ...buildAuthHeaders(dbName),
        'Content-Type': 'application/json',
      },
      json: {
        db_name: dbName,
        projection: 'ontologies',
        branch: 'main',
        from_ts: new Date(Date.now() - 60 * 60 * 1000).toISOString(),
        promote: false,
      },
    })

    let recomputeProjectionTaskId: string | null = null
    if (!recomputeProjection.ok) {
      bugs.add({
        severity: 'P2',
        phase: 'Projection',
        repro_steps: ['Trigger projection recompute task'],
        expected: 'Recompute task is accepted',
        actual: `status=${recomputeProjection.status}; body=${recomputeProjection.text.slice(0, 300)}`,
        endpoint: 'POST /api/v1/admin/recompute-projection',
        ui_path: '/operations/admin',
        evidence: { response: recomputeProjection.body },
        hypothesis: 'Projection task scheduling or Redis task state persistence is failing.',
      })
    } else {
      const recomputePayload = asObjectFromApi(recomputeProjection.body)
      recomputeProjectionTaskId = String(recomputePayload.task_id ?? recomputePayload.taskId ?? '').trim() || null
      if (!recomputeProjectionTaskId) {
        bugs.add({
          severity: 'P1',
          phase: 'Projection',
          repro_steps: ['Trigger projection recompute task'],
          expected: 'Recompute accept response includes task id for status polling',
          actual: `payload=${JSON.stringify(recomputeProjection.body).slice(0, 300)}`,
          endpoint: 'POST /api/v1/admin/recompute-projection',
          ui_path: '/operations/admin',
          evidence: { response: recomputeProjection.body },
          hypothesis: 'Background task id is not surfaced, so runtime success cannot be verified from frontend.',
        })
      } else {
        let recomputeFinalStatus: string | null = null
        let recomputeFinalResponse: unknown = null
        for (let attempt = 0; attempt < 240; attempt += 1) {
          const statusResponse = await apiCall(
            request,
            `/api/v1/admin/recompute-projection/${encodeURIComponent(recomputeProjectionTaskId)}/result`,
            {
              method: 'GET',
              headers: buildAuthHeaders(dbName),
            },
          )
          recomputeFinalResponse = statusResponse.body
          if (!statusResponse.ok) {
            await page.waitForTimeout(1000)
            continue
          }
          const payload = asObjectFromApi(statusResponse.body)
          const statusValue = String(payload.status ?? '').trim().toLowerCase()
          if (statusValue === 'completed' || statusValue === 'failed') {
            recomputeFinalStatus = statusValue
            break
          }
          await page.waitForTimeout(1000)
        }

        if (recomputeFinalStatus !== 'completed') {
          bugs.add({
            severity: 'P1',
            phase: 'Projection',
            repro_steps: ['Trigger projection recompute task', 'Poll task result until terminal status'],
            expected: 'Projection recompute reaches completed status',
            actual: `status=${String(recomputeFinalStatus)}`,
            endpoint: 'GET /api/v1/admin/recompute-projection/{task_id}/result',
            ui_path: '/operations/admin',
            evidence: {
              task_id: recomputeProjectionTaskId,
              response: recomputeFinalResponse,
            },
            hypothesis: 'Projection recompute accepted but background execution failed or stalled.',
          })
        }
      }
    }
    qaLog('graph/projection phase done')

    await createScreenshot(page, testInfo, 'phase5_graph_projection')

    // Phase 6: Action ontology + side effect
    let actionEvidence: ActionSideEffectEvidence | null = null
    const loadActionTypes = async () =>
      apiCall(request, `/api/v2/ontologies/${encodeURIComponent(dbName)}/actionTypes`, {
        method: 'GET',
        headers: buildAuthHeaders(dbName),
        query: {
          branch: 'main',
          pageSize: 100,
        },
      })

    const parseActionTypeList = (raw: unknown) => {
      const payload = raw as Record<string, unknown>
      if (Array.isArray(payload?.data)) {
        return payload.data as Array<Record<string, unknown>>
      }
      return [] as Array<Record<string, unknown>>
    }

    let actionTypes = await loadActionTypes()
    let actionTypeList = actionTypes.ok ? parseActionTypeList(actionTypes.body) : []

    if (actionTypeList.length === 0) {
      const ensureActionType = async (id: string, nextStatus: string) => {
        const createResponse = await apiCall(
          request,
          `/api/v1/databases/${encodeURIComponent(dbName)}/ontology/resources/action-types`,
          {
            method: 'POST',
            headers: {
              ...buildAuthHeaders(dbName),
              'Content-Type': 'application/json',
            },
            query: {
              branch: 'main',
              expected_head_commit: 'branch:main',
            },
            json: {
              id,
              label: { en: id },
              description: { en: `${id} action type (frontend QA)` },
              spec: {
                input_schema: {
                  fields: [
                    {
                      name: 'order',
                      type: 'object_ref',
                      required: true,
                      object_type: 'Order',
                    },
                  ],
                },
                permission_model: 'ontology_roles',
                edits_beyond_actions: false,
                permission_policy: {
                  effect: 'ALLOW',
                  principals: ['role:Owner', 'role:Editor', 'role:DomainModeler', 'user:system'],
                  inherit_project_policy: true,
                  project_policy_scope: 'action_access',
                  project_policy_subject_type: 'project',
                  require_project_policy: true,
                },
                writeback_target: {
                  repo: 'ontology-writeback',
                  branch: 'writeback-{db_name}',
                },
                conflict_policy: 'FAIL',
                implementation: {
                  type: 'template_v1',
                  targets: [
                    {
                      target: { from: 'input.order' },
                      changes: {
                        set: {
                          order_status: nextStatus,
                        },
                      },
                    },
                  ],
                },
              },
              metadata: {
                source: 'frontend_live_foundry_qa',
              },
            },
          },
        )
        if (!createResponse.ok && createResponse.status !== 409) {
          bugs.add({
            severity: 'P1',
            phase: 'Action',
            repro_steps: [`Create action type (${id}) via frontend ontology resource API`],
            expected: 'Action type resource is created',
            actual: `status=${createResponse.status}; body=${createResponse.text.slice(0, 400)}`,
            endpoint: 'POST /api/v1/databases/{db}/ontology/resources/action-types',
            ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
            evidence: { action_type_id: id, response: createResponse.body },
            hypothesis: 'BFF ontology resource route is not connected to OMS action_type resource creation.',
          })
          return false
        }
        return true
      }

      await ensureActionType('HoldOrder', 'ON_HOLD')
      await ensureActionType('ReleaseOrder', 'PENDING')

      for (let attempt = 0; attempt < 8; attempt += 1) {
        actionTypes = await loadActionTypes()
        actionTypeList = actionTypes.ok ? parseActionTypeList(actionTypes.body) : []
        if (actionTypeList.length > 0) {
          break
        }
        await page.waitForTimeout(1200)
      }
    }

    if (!actionTypes.ok || actionTypeList.length === 0) {
      bugs.add({
        severity: 'P1',
        phase: 'Action',
        repro_steps: ['List action types', 'Apply action validate/execute'],
        expected: 'At least one action type is available for apply',
        actual: actionTypes.ok
          ? 'No action type available in ontology. Action lifecycle cannot be verified.'
          : `status=${actionTypes.status}; body=${actionTypes.text.slice(0, 300)}`,
        endpoint: 'GET /api/v2/ontologies/{ontology}/actionTypes',
        ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
        evidence: { response: actionTypes.body },
        hypothesis: 'Action type creation/public management flow is missing from FE-exposed APIs.',
      })
    } else {
      const preferredActionType =
        actionTypeList.find((entry) => String(entry.apiName ?? entry.id ?? '').trim() === 'HoldOrder') ??
        actionTypeList[0]
      const actionTypeId = String(preferredActionType.apiName ?? preferredActionType.id ?? '').trim()

      if (actionTypeId) {
        selectedActionTypeId = actionTypeId
        const recordDeployment = await apiCall(
          request,
          `/api/v1/databases/${encodeURIComponent(dbName)}/ontology/records/deployments`,
          {
            method: 'POST',
            headers: {
              ...buildAuthHeaders(dbName),
              'Content-Type': 'application/json',
            },
            json: {
              target_branch: 'main',
              ontology_commit_id: 'branch:main',
              deployed_by: 'frontend_live_qa',
              metadata: {
                source: 'frontend_live_foundry_qa',
              },
            },
          },
        )

        if (!recordDeployment.ok) {
          bugs.add({
            severity: 'P1',
            phase: 'Action',
            repro_steps: ['Record ontology deployment before action execution'],
            expected: 'Deployment record is created for target branch',
            actual: `status=${recordDeployment.status}; body=${recordDeployment.text.slice(0, 300)}`,
            endpoint: 'POST /api/v1/databases/{db}/ontology/records/deployments',
            ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
            evidence: { response: recordDeployment.body, actionTypeId },
            hypothesis: 'Action runtime deployment precondition cannot be met from frontend surface.',
          })
        }

        const actionTargetOrderId = `qa_action_order_${Date.now().toString(36)}`
        const actionTargetCreate = await apiCall(
          request,
          `/api/v1/databases/${encodeURIComponent(dbName)}/instances/Order/create`,
          {
            method: 'POST',
            headers: {
              ...buildAuthHeaders(dbName),
              'Content-Type': 'application/json',
            },
            query: { branch: 'main' },
            json: {
              data: {
                order_id: actionTargetOrderId,
                order_status: 'PENDING',
                customer_id: `qa_customer_${Date.now().toString(36)}`,
              },
              metadata: {
                source: 'foundry_live_qa_action_target',
              },
            },
          },
        )

        let actionTargetReady = false
        if (actionTargetCreate.ok) {
          const actionTargetCommandId = extractCommandId(actionTargetCreate.body)
          if (actionTargetCommandId) {
            const completion = await waitForCommandCompletion(
              request,
              actionTargetCommandId,
              buildAuthHeaders(dbName),
              120_000,
            )
            actionTargetReady = completion.ok
          }
        } else {
          bugs.add({
            severity: 'P1',
            phase: 'Action',
            repro_steps: ['Create a target Order instance before action apply'],
            expected: 'Target instance for action is created',
            actual: `status=${actionTargetCreate.status}; body=${actionTargetCreate.text.slice(0, 300)}`,
            endpoint: 'POST /api/v1/databases/{db}/instances/Order/create',
            ui_path: `/db/${encodeURIComponent(dbName)}/instances`,
            evidence: { response: actionTargetCreate.body, actionTypeId },
            hypothesis: 'Action target instance precondition failed due write path/runtime issue.',
          })
        }

        const actionParameters = actionTargetReady
          ? {
            order: {
              class_id: 'Order',
              instance_id: actionTargetOrderId,
            },
          }
          : {}

        const validateApply = await apiCall(
          request,
          `/api/v2/ontologies/${encodeURIComponent(dbName)}/actions/${encodeURIComponent(actionTypeId)}/apply`,
          {
            method: 'POST',
            headers: {
              ...buildAuthHeaders(dbName),
              'Content-Type': 'application/json',
            },
            query: { branch: 'main' },
            json: {
              options: { mode: 'VALIDATE_ONLY' },
              parameters: actionParameters,
            },
          },
        )

        const executeApply = await apiCall(
          request,
          `/api/v2/ontologies/${encodeURIComponent(dbName)}/actions/${encodeURIComponent(actionTypeId)}/apply`,
          {
            method: 'POST',
            headers: {
              ...buildAuthHeaders(dbName),
              'Content-Type': 'application/json',
            },
            query: { branch: 'main' },
            json: {
              options: { mode: 'VALIDATE_AND_EXECUTE' },
              parameters: actionParameters,
            },
          },
        )

        const executePayload = (executeApply.body && typeof executeApply.body === 'object'
          ? (executeApply.body as Record<string, unknown>)
          : {}) as Record<string, unknown>
        const executeData = (executePayload.data && typeof executePayload.data === 'object'
          ? (executePayload.data as Record<string, unknown>)
          : null)
        const auditLogId =
          (typeof executePayload.auditLogId === 'string' && executePayload.auditLogId) ||
          (typeof executePayload.action_log_id === 'string' && executePayload.action_log_id) ||
          (typeof executeData?.auditLogId === 'string' && executeData.auditLogId) ||
          (typeof executeData?.action_log_id === 'string' && executeData.action_log_id) ||
          null
        const sideEffectDelivery = executePayload.sideEffectDelivery ?? executeData?.sideEffectDelivery ?? null

        actionEvidence = {
          actionTypeId,
          mode: 'VALIDATE_AND_EXECUTE',
          request: {
            options: { mode: 'VALIDATE_AND_EXECUTE' },
            parameters: actionParameters,
          },
          responseStatus: executeApply.status,
          responseBody: executeApply.body,
          auditLogId,
          sideEffectDelivery,
          webhookDelivery: {
            received: false,
          },
          writebackStatus:
            executeApply.ok && (auditLogId || sideEffectDelivery)
              ? 'confirmed'
              : (executeApply.ok ? 'not_configured' : 'missing'),
        }

        let actionProjectionVerified = false
        let actionObservedStatus: string | null = null
        let actionAuditEvidenceCount = 0
        let actionProjectionEvidence: Record<string, unknown> | null = null
        const expectedActionStatus = actionTypeId === 'HoldOrder' ? 'ON_HOLD' : null

        if (executeApply.ok && actionTargetReady && expectedActionStatus) {
          for (let attempt = 0; attempt < 80; attempt += 1) {
            const directCheck = await apiCall(
              request,
              `/api/v2/ontologies/${encodeURIComponent(dbName)}/objects/Order/${encodeURIComponent(actionTargetOrderId)}`,
              {
                method: 'GET',
                headers: buildAuthHeaders(dbName),
                query: { branch: 'main' },
              },
            )
            actionProjectionEvidence = {
              mode: 'direct_get',
              status: directCheck.status,
              body: directCheck.body,
              attempt,
            }
            if (directCheck.ok) {
              const directPayload = asObjectFromApi(directCheck.body)
              const directRow = asObject(directPayload.data) ?? directPayload
              actionObservedStatus = resolveOrderStatus(directRow) ?? actionObservedStatus
              if (actionObservedStatus === expectedActionStatus) {
                actionProjectionVerified = true
                break
              }
            }

            const projectionCheck = await apiCall(
              request,
              `/api/v2/ontologies/${encodeURIComponent(dbName)}/objects/Order/search`,
              {
                method: 'POST',
                headers: {
                  ...buildAuthHeaders(dbName),
                  'Content-Type': 'application/json',
                },
                query: { branch: 'main' },
                json: {
                  pageSize: 500,
                },
              },
            )
            if (projectionCheck.ok) {
              const projectionPayload = projectionCheck.body as Record<string, unknown>
              const rows = Array.isArray(projectionPayload?.data)
                ? projectionPayload.data as Array<Record<string, unknown>>
                : []
              const target = rows.find((row) => resolveOrderPrimaryKey(row) === actionTargetOrderId) ?? null
              actionObservedStatus = target ? resolveOrderStatus(target) : null
              if (actionObservedStatus === expectedActionStatus) {
                actionProjectionVerified = true
                break
              }
            }
            actionProjectionEvidence = {
              mode: 'search_fallback',
              status: projectionCheck.status,
              body: projectionCheck.body,
              observed_status: actionObservedStatus,
              attempt,
            }
            await page.waitForTimeout(1500)
          }
        }

        if (executeApply.ok && auditLogId) {
          for (let attempt = 0; attempt < 60; attempt += 1) {
            const audit = await apiCall(request, '/api/v1/audit/logs', {
              method: 'GET',
              headers: buildAuthHeaders(dbName),
              query: {
                partition_key: `db:${dbName}`,
                command_id: auditLogId,
                limit: 200,
              },
            })
            if (audit.ok) {
              const payload = extractApiData<Record<string, unknown>>(audit.body)
              const items = Array.isArray(payload?.items) ? payload.items : []
              actionAuditEvidenceCount = items.length
              if (actionAuditEvidenceCount > 0) {
                break
              }
            }
            await page.waitForTimeout(1500)
          }
        }

        if (executeApply.ok && actionTargetReady && expectedActionStatus && !actionProjectionVerified) {
          bugs.add({
            severity: 'P1',
            phase: 'Action',
            repro_steps: [
              `Apply ${actionTypeId} to target Order`,
              'Poll object search in main branch',
            ],
            expected: `Target Order status changes to ${expectedActionStatus}`,
            actual: `observed=${String(actionObservedStatus)}`,
            endpoint: 'POST /api/v2/ontologies/{ontology}/actions/{action}/apply + POST /objects/Order/search',
            ui_path: `/db/${encodeURIComponent(dbName)}/instances`,
            evidence: {
              actionTypeId,
              target_order_id: actionTargetOrderId,
              expected_status: expectedActionStatus,
              observed_status: actionObservedStatus,
              action_log_id: auditLogId,
              projection_check: actionProjectionEvidence,
              apply_response: executeApply.body,
            },
            hypothesis: 'Action projection merge is stale or writeback result is not materialized into object search.',
          })
        }

        if (executeApply.ok && auditLogId && actionAuditEvidenceCount === 0) {
          bugs.add({
            severity: 'P1',
            phase: 'Action',
            repro_steps: [
              `Apply ${actionTypeId} to target Order`,
              'Poll audit logs by command_id',
            ],
            expected: 'At least one audit log item exists for the action command',
            actual: `audit_count=${actionAuditEvidenceCount}`,
            endpoint: 'GET /api/v1/audit/logs',
            ui_path: `/db/${encodeURIComponent(dbName)}/audit`,
            evidence: {
              actionTypeId,
              action_log_id: auditLogId,
              target_order_id: actionTargetOrderId,
            },
            hypothesis: 'Action succeeded but audit indexing/filter query did not return command-linked evidence.',
          })
        }

        if (!validateApply.ok || !executeApply.ok) {
          bugs.add({
            severity: 'P1',
            phase: 'Action',
            repro_steps: ['Apply action with VALIDATE_ONLY', 'Apply action with VALIDATE_AND_EXECUTE'],
            expected: 'Both action API calls succeed',
            actual: `validate=${validateApply.status}, execute=${executeApply.status}`,
            endpoint: 'POST /api/v2/ontologies/{ontology}/actions/{action}/apply',
            ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
            evidence: {
              actionTypeId,
              parameters: actionParameters,
              validate: validateApply.body,
              execute: executeApply.body,
            },
            hypothesis: 'Action runtime preconditions (deployment/writeback target/worker contracts) are unmet.',
          })
        }
      }
    }
    qaLog('action phase done')

    // Phase 6b: Dynamic layer (project-level policy inheritance + action guardrail contract)
    {
      const upsertProjectPolicy = await apiCall(request, '/api/v1/access-policies', {
        method: 'POST',
        headers: {
          ...buildAuthHeaders(dbName),
          'Content-Type': 'application/json',
        },
        json: {
          db_name: dbName,
          scope: 'action_access',
          subject_type: 'project',
          subject_id: dbName,
          policy: {
            effect: 'ALLOW',
            principals: ['user:system'],
          },
          status: 'ACTIVE',
        },
      })

      if (!upsertProjectPolicy.ok) {
        bugs.add({
          severity: 'P1',
          phase: 'Dynamic security',
          repro_steps: ['Upsert project-level access policy for action inheritance'],
          expected: 'POST /api/v1/access-policies succeeds for project scope',
          actual: `status=${upsertProjectPolicy.status}; body=${upsertProjectPolicy.text.slice(0, 300)}`,
          endpoint: 'POST /api/v1/access-policies',
          ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
          evidence: { response: upsertProjectPolicy.body },
          hypothesis: 'Project-scope access policy surface is not reachable from FE/BFF contract.',
        })
      }

      const listProjectPolicies = await apiCall(request, '/api/v1/access-policies', {
        method: 'GET',
        headers: buildAuthHeaders(dbName),
        query: {
          db_name: dbName,
          scope: 'action_access',
          subject_type: 'project',
          subject_id: dbName,
          status: 'ACTIVE',
        },
      })

      const projectPolicyRows = (() => {
        const payload = extractApiData<Record<string, unknown>>(listProjectPolicies.body)
        const rows = Array.isArray(payload?.access_policies) ? payload.access_policies : []
        return rows
          .map((entry) => asObject(entry))
          .filter((entry): entry is Record<string, unknown> => Boolean(entry))
      })()
      const projectPolicy = projectPolicyRows.find((row) =>
        String(row.db_name ?? '').trim() === dbName &&
        String(row.scope ?? '').trim() === 'action_access' &&
        String(row.subject_type ?? '').trim() === 'project' &&
        String(row.subject_id ?? '').trim() === dbName,
      ) ?? null

      if (!listProjectPolicies.ok || !projectPolicy) {
        bugs.add({
          severity: 'P1',
          phase: 'Dynamic security',
          repro_steps: ['List project-level access policies after upsert'],
          expected: 'Project policy row is visible from FE/BFF list API',
          actual: listProjectPolicies.ok
            ? 'project_policy_missing'
            : `status=${listProjectPolicies.status}; body=${listProjectPolicies.text.slice(0, 300)}`,
          endpoint: 'GET /api/v1/access-policies',
          ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
          evidence: {
            response: listProjectPolicies.body,
          },
          hypothesis: 'Access policy list projection is stale or scope filters are not applied consistently.',
        })
      }

      let actionTypeContract: Awaited<ReturnType<typeof apiCall>> | null = null
      let permissionModel = ''
      let editsBeyondActions = false
      let inheritanceEnabled = false
      let inheritanceScope = ''
      if (selectedActionTypeId) {
        actionTypeContract = await apiCall(
          request,
          `/api/v2/ontologies/${encodeURIComponent(dbName)}/actionTypes/${encodeURIComponent(selectedActionTypeId)}`,
          {
            method: 'GET',
            headers: buildAuthHeaders(dbName),
            query: { branch: 'main' },
          },
        )
        if (actionTypeContract.ok) {
          const payload = asObjectFromApi(actionTypeContract.body)
          const dynamicSecurity = asObject(payload.dynamicSecurity)
          const inheritance = asObject(payload.projectPolicyInheritance) ?? asObject(dynamicSecurity?.projectPolicyInheritance)
          permissionModel = String(payload.permissionModel ?? dynamicSecurity?.permissionModel ?? '').trim()
          editsBeyondActions = Boolean(payload.editsBeyondActions ?? dynamicSecurity?.editsBeyondActions ?? false)
          inheritanceEnabled = Boolean(inheritance?.enabled)
          inheritanceScope = String(inheritance?.scope ?? '').trim()
        }
      }

      if (
        selectedActionTypeId &&
        (
          !actionTypeContract?.ok ||
          !permissionModel ||
          !inheritanceEnabled ||
          (inheritanceScope && inheritanceScope !== 'action_access')
        )
      ) {
        bugs.add({
          severity: 'P1',
          phase: 'Dynamic security',
          repro_steps: ['Get action type contract from v2 actionTypes surface'],
          expected: 'Action type exposes permissionModel + projectPolicyInheritance in FE/BFF contract',
          actual: actionTypeContract?.ok
            ? `permissionModel=${permissionModel}; inheritanceEnabled=${String(inheritanceEnabled)}; scope=${inheritanceScope}`
            : `status=${actionTypeContract?.status ?? 0}; body=${String(actionTypeContract?.text ?? '').slice(0, 300)}`,
          endpoint: 'GET /api/v2/ontologies/{ontology}/actionTypes/{actionType}',
          ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
          evidence: {
            actionTypeId: selectedActionTypeId,
            response: actionTypeContract?.body ?? null,
          },
          hypothesis: 'Dynamic security primitives are not surfaced as first-class action type fields on FE/BFF contract.',
        })
      }

      dynamicSecurityEvidence = {
        actionTypeId: selectedActionTypeId,
        projectPolicyUpsertStatus: upsertProjectPolicy.status,
        projectPolicyListStatus: listProjectPolicies.status,
        projectPolicyVisible: Boolean(projectPolicy),
        permissionModel: permissionModel || null,
        editsBeyondActions,
        inheritanceEnabled,
        inheritanceScope: inheritanceScope || null,
      }
    }
    qaLog('dynamic security phase done')

    // Phase 6c: Kinetic layer (query types/functions)
    {
      const queryTypeId = 'QaOrderSearch'
      const createQueryType = await apiCall(
        request,
        `/api/v1/databases/${encodeURIComponent(dbName)}/ontology/resources/functions`,
        {
          method: 'POST',
          headers: {
            ...buildAuthHeaders(dbName),
            'Content-Type': 'application/json',
          },
          query: {
            branch: 'main',
            expected_head_commit: 'branch:main',
          },
          json: {
            id: queryTypeId,
            label: { en: queryTypeId },
            description: { en: 'frontend QA kinetic query type' },
            spec: {
              expression: 'search(Order)',
              return_type_ref: 'object_type:Order',
              deterministic: true,
              version: '1',
              parameters: {},
              output: {
                type: 'objectSet',
                objectType: 'Order',
              },
              execution: {
                objectType: 'Order',
                search: {
                  pageSize: 100,
                },
              },
            },
            metadata: {
              source: 'frontend_live_foundry_qa',
            },
          },
        },
      )

      if (!createQueryType.ok && createQueryType.status !== 409) {
        bugs.add({
          severity: 'P1',
          phase: 'Kinetic query',
          repro_steps: ['Create query type via frontend ontology resource API'],
          expected: 'Query type resource is created or already exists',
          actual: `status=${createQueryType.status}; body=${createQueryType.text.slice(0, 400)}`,
          endpoint: 'POST /api/v1/databases/{db}/ontology/resources/functions',
          ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
          evidence: { query_type_id: queryTypeId, response: createQueryType.body },
          hypothesis: 'Ontology function/query resource route is not aligned with v2 query type surface.',
        })
      }

      let listResponse: Awaited<ReturnType<typeof apiCall>> | null = null
      let getResponse: Awaited<ReturnType<typeof apiCall>> | null = null
      let listContainsQueryType = false
      for (let attempt = 0; attempt < 10; attempt += 1) {
        listResponse = await apiCall(request, `/api/v2/ontologies/${encodeURIComponent(dbName)}/queryTypes`, {
          method: 'GET',
          headers: buildAuthHeaders(dbName),
          query: {
            pageSize: 200,
          },
        })

        const listPayload = asObjectFromApi(listResponse.body)
        const rows = Array.isArray(listPayload.data)
          ? listPayload.data
            .map((entry) => asObject(entry))
            .filter((entry): entry is Record<string, unknown> => Boolean(entry))
          : []
        listContainsQueryType = rows.some((entry) => String(entry.apiName ?? entry.id ?? '').trim() === queryTypeId)

        getResponse = await apiCall(
          request,
          `/api/v2/ontologies/${encodeURIComponent(dbName)}/queryTypes/${encodeURIComponent(queryTypeId)}`,
          {
            method: 'GET',
            headers: buildAuthHeaders(dbName),
          },
        )

        if (listResponse.ok && getResponse.ok && listContainsQueryType) {
          break
        }
        await page.waitForTimeout(1200)
      }

      if (!listResponse?.ok || !listContainsQueryType) {
        bugs.add({
          severity: 'P1',
          phase: 'Kinetic query',
          repro_steps: ['List query types from v2 ontology surface'],
          expected: `Query type ${queryTypeId} appears in queryTypes list`,
          actual: listResponse?.ok
            ? `query_type_missing=${queryTypeId}`
            : `status=${listResponse?.status ?? 0}; body=${String(listResponse?.text ?? '').slice(0, 300)}`,
          endpoint: 'GET /api/v2/ontologies/{ontology}/queryTypes',
          ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
          evidence: { response: listResponse?.body ?? null, query_type_id: queryTypeId },
          hypothesis: 'v2 query type listing does not reflect ontology function resources consistently.',
        })
      }

      if (!getResponse?.ok) {
        bugs.add({
          severity: 'P1',
          phase: 'Kinetic query',
          repro_steps: ['Get query type by api name from v2 ontology surface'],
          expected: `Query type ${queryTypeId} is retrievable`,
          actual: `status=${getResponse?.status ?? 0}; body=${String(getResponse?.text ?? '').slice(0, 300)}`,
          endpoint: 'GET /api/v2/ontologies/{ontology}/queryTypes/{queryApiName}',
          ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
          evidence: { response: getResponse?.body ?? null, query_type_id: queryTypeId },
          hypothesis: 'Query type retrieval projection is stale or version metadata did not materialize.',
        })
      }

      const executeQuery = await apiCall(
        request,
        `/api/v2/ontologies/${encodeURIComponent(dbName)}/queries/${encodeURIComponent(queryTypeId)}/execute`,
        {
          method: 'POST',
          headers: {
            ...buildAuthHeaders(dbName),
            'Content-Type': 'application/json',
          },
          json: {
            parameters: {},
            options: {
              pageSize: 100,
            },
          },
        },
      )

      if (!executeQuery.ok) {
        bugs.add({
          severity: 'P1',
          phase: 'Kinetic query',
          repro_steps: ['Execute query type via v2 query execution endpoint'],
          expected: 'Query execute responds successfully',
          actual: `status=${executeQuery.status}; body=${executeQuery.text.slice(0, 400)}`,
          endpoint: 'POST /api/v2/ontologies/{ontology}/queries/{queryApiName}/execute',
          ui_path: `/db/${encodeURIComponent(dbName)}/explore/query`,
          evidence: { response: executeQuery.body, query_type_id: queryTypeId },
          hypothesis: 'Kinetic query execution bridge from function resource to object search failed.',
        })
      } else {
        const executePayload = asObjectFromApi(executeQuery.body)
        const valueObj = asObject(executePayload.value)
        const rows = Array.isArray(valueObj?.data)
          ? valueObj?.data
          : (Array.isArray(executePayload.value) ? executePayload.value : [])
        const rowCount = Array.isArray(rows) ? rows.length : 0
        kineticQueryEvidence = {
          queryTypeId,
          listContainsQueryType,
          executeStatus: executeQuery.status,
          rowCount,
          resultSample: Array.isArray(rows) ? rows.slice(0, 3) : [],
        }
        if (rowCount < 1) {
          bugs.add({
            severity: 'P1',
            phase: 'Kinetic query',
            repro_steps: ['Execute query type via v2 query execution endpoint'],
            expected: 'Query execution returns at least one Order object',
            actual: `row_count=${rowCount}`,
            endpoint: 'POST /api/v2/ontologies/{ontology}/queries/{queryApiName}/execute',
            ui_path: `/db/${encodeURIComponent(dbName)}/explore/query`,
            evidence: { response: executeQuery.body, query_type_id: queryTypeId, row_count: rowCount },
            hypothesis: 'Query type executed but object search bridge returned empty set unexpectedly.',
          })
        }
      }
    }
    qaLog('kinetic query phase done')

    // Phase 7: Closed loop (instance write -> search + audit)
    let closedLoop: ClosedLoopVerificationResult | null = null
    const beforeQuery = await apiCall(request, `/api/v2/ontologies/${encodeURIComponent(dbName)}/objects/Order/search`, {
      method: 'POST',
      headers: {
        ...buildAuthHeaders(dbName),
        'Content-Type': 'application/json',
      },
      query: { branch: 'main' },
      json: { pageSize: 50 },
    })
    const beforeCount = (() => {
      const payload = beforeQuery.body as Record<string, unknown>
      const data = payload?.data
      if (Array.isArray(data)) {
        return data.length
      }
      return 0
    })()

    const syntheticOrderId = `qa_order_${Date.now().toString(36)}`
    const createInstance = await apiCall(request, `/api/v1/databases/${encodeURIComponent(dbName)}/instances/Order/create`, {
      method: 'POST',
      headers: {
        ...buildAuthHeaders(dbName),
        'Content-Type': 'application/json',
      },
      query: { branch: 'main' },
      json: {
        data: {
          order_id: syntheticOrderId,
          order_status: 'PENDING',
          customer_id: `qa_customer_${Date.now().toString(36)}`,
        },
        metadata: {
          source: 'foundry_live_qa',
        },
      },
    })

    if (!createInstance.ok) {
      bugs.add({
        severity: 'P1',
        phase: 'Closed loop',
        repro_steps: ['Create Order instance via async write API'],
        expected: 'Instance create command accepted',
        actual: `status=${createInstance.status}; body=${createInstance.text.slice(0, 400)}`,
        endpoint: 'POST /api/v1/databases/{db}/instances/{class}/create',
        ui_path: `/db/${encodeURIComponent(dbName)}/instances`,
        evidence: { response: createInstance.body },
        hypothesis: 'Instance write blocked by label mapping, ACL, or projection constraints.',
      })
    } else {
      const commandId = extractCommandId(createInstance.body)
      if (commandId) {
        const command = await waitForCommandCompletion(request, commandId, buildAuthHeaders(dbName), 120_000)
        if (!command.ok) {
          bugs.add({
            severity: 'P1',
            phase: 'Closed loop',
            repro_steps: ['Wait for create-instance command completion'],
            expected: 'Command status reaches COMPLETED',
            actual: `status=${command.status}; body=${JSON.stringify(command.body).slice(0, 400)}`,
            endpoint: 'GET /api/v1/commands/{commandId}/status',
            ui_path: `/db/${encodeURIComponent(dbName)}/instances`,
            evidence: { command_id: commandId, payload: command.body },
            hypothesis: 'Async write worker failed to materialize the instance.',
          })
        }
      }

      // allow read model catch-up
      await page.waitForTimeout(4000)
      const afterQuery = await apiCall(request, `/api/v2/ontologies/${encodeURIComponent(dbName)}/objects/Order/search`, {
        method: 'POST',
        headers: {
          ...buildAuthHeaders(dbName),
          'Content-Type': 'application/json',
        },
        query: { branch: 'main' },
        json: {
          pageSize: 100,
        },
      })

      const afterObjects = (() => {
        const payload = afterQuery.body as Record<string, unknown>
        const data = payload?.data
        return Array.isArray(data) ? data as Array<Record<string, unknown>> : []
      })()
      const pkFrequency = new Map<string, number>()
      for (const row of afterObjects) {
        const pk = resolveOrderPrimaryKey(row)
        if (!pk) {
          continue
        }
        pkFrequency.set(pk, (pkFrequency.get(pk) ?? 0) + 1)
      }
      const duplicatePrimaryKeys = Array.from(pkFrequency.entries())
        .filter(([, count]) => count > 1)
        .map(([pk]) => pk)
      if (duplicatePrimaryKeys.length > 0) {
        bugs.add({
          severity: 'P1',
          phase: 'Closed loop',
          repro_steps: ['Load Order objects after objectify/action flow', 'Check primary key uniqueness'],
          expected: 'Order projection contains unique primary keys in search result set',
          actual: `duplicate_primary_keys=${duplicatePrimaryKeys.join(',')}`,
          endpoint: 'POST /api/v2/ontologies/{ontology}/objects/Order/search',
          ui_path: `/db/${encodeURIComponent(dbName)}/instances`,
          evidence: {
            duplicate_primary_keys: duplicatePrimaryKeys,
            sampled_row_count: afterObjects.length,
          },
          hypothesis: 'Object projection or merge logic produced duplicate instances for the same primary key.',
        })
      }

      const afterCount = afterObjects.length
      const created = afterObjects.find((item) => resolveOrderPrimaryKey(item) === syntheticOrderId)
      createdOrderInstanceId = created ? syntheticOrderId : null

      // Data-flow doc alignment: search operators + strong consistency + Foundry object envelope keys.
      const eqStrongRead = await apiCall(request, `/api/v2/ontologies/${encodeURIComponent(dbName)}/objects/Order/search`, {
        method: 'POST',
        headers: {
          ...buildAuthHeaders(dbName),
          'Content-Type': 'application/json',
          'X-Consistency': 'strong',
        },
        query: { branch: 'main' },
        json: {
          where: {
            type: 'eq',
            field: 'instance_id',
            value: syntheticOrderId,
          },
          pageSize: 10,
        },
      })

      const eqStrongRows = (() => {
        const payload = eqStrongRead.body as Record<string, unknown>
        return Array.isArray(payload?.data) ? payload.data as Array<Record<string, unknown>> : []
      })()
      const eqStrongTarget =
        eqStrongRows.find((row) => resolveOrderPrimaryKey(row) === syntheticOrderId) ?? null

      if (!eqStrongRead.ok || !eqStrongTarget) {
        bugs.add({
          severity: 'P1',
          phase: 'Search operators',
          repro_steps: [
            'Create Order instance',
            'Search with eq(instance_id) + X-Consistency: strong',
          ],
          expected: 'Read-after-write should return target object via strong-consistency search.',
          actual: `status=${eqStrongRead.status}; rows=${eqStrongRows.length}; found=${Boolean(eqStrongTarget)}`,
          endpoint: 'POST /api/v2/ontologies/{ontology}/objects/Order/search',
          ui_path: `/db/${encodeURIComponent(dbName)}/instances`,
          evidence: {
            synthetic_order_id: syntheticOrderId,
            response_status: eqStrongRead.status,
            response_body: eqStrongRead.body,
          },
          hypothesis: 'Strong consistency header path or eq operator mapping does not guarantee read-your-write behavior.',
        })
      } else {
        const props = eqStrongTarget.properties
        const hasFoundryEnvelope =
          typeof eqStrongTarget.__rid === 'string' &&
          typeof eqStrongTarget.__primaryKey === 'string' &&
          typeof eqStrongTarget.__apiName === 'string' &&
          Boolean(props) &&
          typeof props === 'object' &&
          !Array.isArray(props) &&
          String((props as Record<string, unknown>).order_id ?? '') === syntheticOrderId
        if (!hasFoundryEnvelope) {
          bugs.add({
            severity: 'P1',
            phase: 'Search operators',
            repro_steps: [
              'Search with eq(order_id) after create',
              'Validate Foundry object envelope fields',
            ],
            expected: 'Search result includes __rid, __primaryKey, __apiName and properties map.',
            actual: `envelope_invalid=${JSON.stringify(eqStrongTarget).slice(0, 500)}`,
            endpoint: 'POST /api/v2/ontologies/{ontology}/objects/{objectType}/search',
            ui_path: `/db/${encodeURIComponent(dbName)}/instances`,
            evidence: {
              row: eqStrongTarget,
            },
            hypothesis: 'Search response projection does not fully normalize to Foundry-style object envelope.',
          })
        }
      }

      const containsAnyTermQuery = await apiCall(request, `/api/v2/ontologies/${encodeURIComponent(dbName)}/objects/Order/search`, {
        method: 'POST',
        headers: {
          ...buildAuthHeaders(dbName),
          'Content-Type': 'application/json',
        },
        query: { branch: 'main' },
        json: {
          where: {
            type: 'containsAnyTerm',
            field: 'order_status',
            value: 'PENDING',
          },
          pageSize: 20,
        },
      })

      if (!containsAnyTermQuery.ok) {
        bugs.add({
          severity: 'P1',
          phase: 'Search operators',
          repro_steps: ['Search Order objects using containsAnyTerm(order_status, "PENDING")'],
          expected: 'containsAnyTerm operator executes successfully.',
          actual: `status=${containsAnyTermQuery.status}; body=${containsAnyTermQuery.text.slice(0, 300)}`,
          endpoint: 'POST /api/v2/ontologies/{ontology}/objects/{objectType}/search',
          ui_path: `/db/${encodeURIComponent(dbName)}/explore/query`,
          evidence: { response: containsAnyTermQuery.body },
          hypothesis: 'SearchJsonQueryV2 text operator mapping to ES is broken at runtime.',
        })
      }

      const gteQuery = await apiCall(request, `/api/v2/ontologies/${encodeURIComponent(dbName)}/objects/Order/search`, {
        method: 'POST',
        headers: {
          ...buildAuthHeaders(dbName),
          'Content-Type': 'application/json',
        },
        query: { branch: 'main' },
        json: {
          where: {
            type: 'gte',
            field: 'created_at',
            value: '1970-01-01T00:00:00Z',
          },
          pageSize: 20,
        },
      })

      if (!gteQuery.ok) {
        bugs.add({
          severity: 'P1',
          phase: 'Search operators',
          repro_steps: ['Search Order objects using gte(created_at, "1970-01-01T00:00:00Z")'],
          expected: 'gte operator executes successfully.',
          actual: `status=${gteQuery.status}; body=${gteQuery.text.slice(0, 300)}`,
          endpoint: 'POST /api/v2/ontologies/{ontology}/objects/{objectType}/search',
          ui_path: `/db/${encodeURIComponent(dbName)}/explore/query`,
          evidence: { response: gteQuery.body },
          hypothesis: 'SearchJsonQueryV2 numeric/date range operator mapping to ES is broken at runtime.',
        })
      }

      let auditItems: Array<Record<string, unknown>> = []
      let lastAuditStatus = 0
      let lastAuditBody: unknown = null
      const isAuditHit = (item: Record<string, unknown>) => {
        const commandMatch = commandId ? String(item.command_id ?? '') === commandId : false
        const resourceMatch = String(item.resource_id ?? '') === syntheticOrderId
        const containsOrderId = JSON.stringify(item).includes(syntheticOrderId)
        return commandMatch || resourceMatch || containsOrderId
      }
      for (let attempt = 0; attempt < 40; attempt += 1) {
        const audit = await apiCall(request, '/api/v1/audit/logs', {
          method: 'GET',
          headers: buildAuthHeaders(dbName),
          query: {
            partition_key: `db:${dbName}`,
            limit: 200,
          },
        })
        lastAuditStatus = audit.status
        lastAuditBody = audit.body
        if (!audit.ok) {
          await page.waitForTimeout(1500)
          continue
        }
        const allItems = (() => {
          const payload = extractApiData<Record<string, unknown>>(audit.body)
          const items = Array.isArray(payload?.items) ? payload.items : []
          return items
            .map((entry) => (entry && typeof entry === 'object' && !Array.isArray(entry)
              ? (entry as Record<string, unknown>)
              : null))
            .filter((entry): entry is Record<string, unknown> => Boolean(entry))
        })()
        auditItems = allItems.filter((item) => isAuditHit(item))
        if (auditItems.length > 0) {
          break
        }
        await page.waitForTimeout(1500)
      }

      closedLoop = {
        datasetId: orderDatasetId ?? 'unknown',
        objectType: 'Order',
        queryBeforeCount: beforeCount,
        queryAfterCount: afterCount,
        changedObjectIds: createdOrderInstanceId ? [createdOrderInstanceId] : [],
        auditLogEvidence: {
          hasLogs: auditItems.length > 0,
          count: auditItems.length,
        },
        status: createdOrderInstanceId && auditItems.length > 0 ? 'verified' : (createdOrderInstanceId ? 'partial' : 'failed'),
      }

      if (closedLoop.status !== 'verified') {
        if (lastAuditStatus >= 400) {
          bugs.add({
            severity: 'P1',
            phase: 'Closed loop',
            repro_steps: ['Load audit logs using db partition key'],
            expected: 'Audit API returns filtered logs successfully',
            actual: `status=${lastAuditStatus}; body=${JSON.stringify(lastAuditBody).slice(0, 400)}`,
            endpoint: 'GET /api/v1/audit/logs',
            ui_path: `/db/${encodeURIComponent(dbName)}/audit`,
            evidence: {
              audit_status: lastAuditStatus,
              audit_response: lastAuditBody,
            },
            hypothesis: 'Audit API request parsing or filter handling is broken in current runtime.',
          })
        }
        bugs.add({
          severity: 'P1',
          phase: 'Closed loop',
          repro_steps: ['Create instance', 'Search objects', 'Load audit logs'],
          expected: 'Object becomes queryable and corresponding audit log exists',
          actual: `closed_loop_status=${closedLoop.status}; created=${String(createdOrderInstanceId)}; audit_count=${auditItems.length}`,
          endpoint: 'Instance write + object search + audit logs',
          ui_path: `/db/${encodeURIComponent(dbName)}/instances`,
          evidence: {
            before_count: beforeCount,
            after_count: afterCount,
            audit_count: auditItems.length,
            audit_status: lastAuditStatus,
            audit_response: lastAuditBody,
          },
          hypothesis:
            lastAuditStatus >= 400
              ? 'Audit API failed, so closed-loop evidence could not be retrieved.'
              : 'Read-model projection delay or missing audit partition linkage.',
        })
      }
    }
    qaLog(`closed-loop phase done status=${String(closedLoop?.status ?? 'n/a')}`)

    // Phase 8: Lineage API checks (all FE-exposed lineage routes via BFF)
    const lineageEvidence: Record<string, unknown> = {}
    const lineageWindowStart = new Date(Date.now() - 4 * 60 * 60 * 1000).toISOString()
    const lineageWindowEnd = new Date().toISOString()
    const asObj = (value: unknown) =>
      value && typeof value === 'object' && !Array.isArray(value)
        ? (value as Record<string, unknown>)
        : null
    const asObjRows = (value: unknown) => {
      if (!Array.isArray(value)) {
        return [] as Array<Record<string, unknown>>
      }
      return value
        .map((entry) => asObj(entry))
        .filter((entry): entry is Record<string, unknown> => Boolean(entry))
    }

    const lineageMetrics = await apiCall(request, '/api/v1/lineage/metrics', {
      method: 'GET',
      headers: buildAuthHeaders(dbName),
      query: {
        db_name: dbName,
        window_minutes: 60,
      },
    })

    if (!lineageMetrics.ok) {
      bugs.add({
        severity: 'P1',
        phase: 'Lineage',
        repro_steps: ['Load lineage metrics'],
        expected: 'Lineage metrics endpoint responds successfully with non-zero recorded lineage',
        actual: `status=${lineageMetrics.status}; body=${lineageMetrics.text.slice(0, 300)}`,
        endpoint: 'GET /api/v1/lineage/metrics',
        ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
        evidence: { response: lineageMetrics.body },
        hypothesis: 'Lineage store metrics query failed in runtime.',
      })
    } else {
      const metricsData = asObj(extractApiData<Record<string, unknown>>(lineageMetrics.body))
      const eventsAppended = Number(metricsData?.events_appended ?? 0)
      const edgesRecorded = Number(metricsData?.lineage_edges_recorded ?? 0)
      const missingRatio = Number(metricsData?.missing_lineage_ratio_estimate ?? 1)
      lineageEvidence.metrics = {
        events_appended: eventsAppended,
        lineage_edges_recorded: edgesRecorded,
        missing_lineage_ratio_estimate: missingRatio,
      }
      if (!Number.isFinite(eventsAppended) || !Number.isFinite(edgesRecorded) || eventsAppended <= 0 || edgesRecorded <= 0) {
        bugs.add({
          severity: 'P1',
          phase: 'Lineage',
          repro_steps: ['Run full ingest/transform/action flow', 'Load lineage metrics'],
          expected: 'events_appended and lineage_edges_recorded are both greater than zero',
          actual: `events_appended=${eventsAppended}; lineage_edges_recorded=${edgesRecorded}`,
          endpoint: 'GET /api/v1/lineage/metrics',
          ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
          evidence: { response: lineageMetrics.body },
          hypothesis: 'Lifecycle operations succeeded but lineage edge recording did not materialize.',
        })
      }
    }

    const lineageRuns = await apiCall(request, '/api/v1/lineage/runs', {
      method: 'GET',
      headers: buildAuthHeaders(dbName),
      query: {
        db_name: dbName,
        since: lineageWindowStart,
        until: lineageWindowEnd,
        run_limit: 200,
        freshness_slo_minutes: 120,
        include_impact_preview: true,
        impact_preview_runs_limit: 20,
        impact_preview_artifacts_limit: 20,
        impact_preview_edge_limit: 2000,
      },
    })
    if (!lineageRuns.ok) {
      bugs.add({
        severity: 'P2',
        phase: 'Lineage',
        repro_steps: ['Load lineage run timeline'],
        expected: 'Lineage run timeline endpoint responds successfully',
        actual: `status=${lineageRuns.status}; body=${lineageRuns.text.slice(0, 300)}`,
        endpoint: 'GET /api/v1/lineage/runs',
        ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
        evidence: { response: lineageRuns.body },
        hypothesis: 'Run-level lineage summarization endpoint failed in current runtime.',
      })
    }
    const runsData = asObj(extractApiData<Record<string, unknown>>(lineageRuns.body))
    const runRows = lineageRuns.ok ? asObjRows(runsData?.runs) : []
    lineageEvidence.runs = {
      run_count: runRows.length,
      status_counts: runsData?.status_counts ?? null,
      warnings: runsData?.warnings ?? null,
    }

    const lineageTimeline = await apiCall(request, '/api/v1/lineage/timeline', {
      method: 'GET',
      headers: buildAuthHeaders(dbName),
      query: {
        db_name: dbName,
        since: lineageWindowStart,
        until: lineageWindowEnd,
        bucket_minutes: 15,
        event_limit: 2000,
        event_preview_limit: 500,
      },
    })
    if (!lineageTimeline.ok) {
      bugs.add({
        severity: 'P1',
        phase: 'Lineage',
        repro_steps: ['Load lineage timeline'],
        expected: 'Lineage timeline endpoint responds with event previews',
        actual: `status=${lineageTimeline.status}; body=${lineageTimeline.text.slice(0, 300)}`,
        endpoint: 'GET /api/v1/lineage/timeline',
        ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
        evidence: { response: lineageTimeline.body },
        hypothesis: 'Lineage timeline aggregation failed in runtime.',
      })
    }

    let lineageSeedRoot: string | null = null
    let lineageSeedTarget: string | null = null
    if (lineageTimeline.ok) {
      const timelineData = asObj(extractApiData<Record<string, unknown>>(lineageTimeline.body))
      const timelineEvents = asObjRows(timelineData?.events)
      const eventCountLoaded = Number(timelineData?.event_count_loaded ?? timelineEvents.length)
      lineageEvidence.timeline = {
        event_count_loaded: eventCountLoaded,
        event_preview_count: timelineEvents.length,
        warnings: timelineData?.warnings ?? null,
      }
      if (!Number.isFinite(eventCountLoaded) || eventCountLoaded <= 0 || timelineEvents.length === 0) {
        bugs.add({
          severity: 'P1',
          phase: 'Lineage',
          repro_steps: ['Run full lifecycle', 'Load lineage timeline'],
          expected: 'Timeline contains at least one lineage event',
          actual: `event_count_loaded=${eventCountLoaded}; preview_rows=${timelineEvents.length}`,
          endpoint: 'GET /api/v1/lineage/timeline',
          ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
          evidence: { response: lineageTimeline.body },
          hypothesis: 'Lineage events were not recorded despite successful workflow operations.',
        })
      } else {
        const seed =
          timelineEvents.find((row) =>
            String(row.edge_type ?? '') === 'aggregate_emitted_event' &&
            typeof row.from_node_id === 'string' &&
            typeof row.to_node_id === 'string',
          ) ??
          timelineEvents.find((row) =>
            typeof row.from_node_id === 'string' &&
            typeof row.to_node_id === 'string',
          ) ??
          null
        lineageSeedRoot = seed ? String(seed.from_node_id ?? '').trim() : null
        lineageSeedTarget = seed ? String(seed.to_node_id ?? '').trim() : null
      }
    }

    if (!lineageSeedRoot || !lineageSeedTarget) {
      bugs.add({
        severity: 'P1',
        phase: 'Lineage',
        repro_steps: ['Load lineage timeline', 'Derive root and target nodes for graph/path assertions'],
        expected: 'Timeline should provide at least one traversable lineage edge',
        actual: `root=${String(lineageSeedRoot)}; target=${String(lineageSeedTarget)}`,
        endpoint: 'GET /api/v1/lineage/timeline',
        ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
        evidence: { timeline: lineageTimeline.body },
        hypothesis: 'Lineage timeline did not expose traversable nodes for graph/path checks.',
      })
    } else {
      const lineageGraph = await apiCall(request, '/api/v1/lineage/graph', {
        method: 'GET',
        headers: buildAuthHeaders(dbName),
        query: {
          db_name: dbName,
          root: lineageSeedRoot,
          direction: 'both',
          max_depth: 5,
          max_nodes: 2000,
          max_edges: 5000,
        },
      })
      if (!lineageGraph.ok) {
        bugs.add({
          severity: 'P1',
          phase: 'Lineage',
          repro_steps: ['Call lineage graph with timeline-derived root'],
          expected: 'Lineage graph endpoint returns nodes/edges',
          actual: `status=${lineageGraph.status}; body=${lineageGraph.text.slice(0, 300)}`,
          endpoint: 'GET /api/v1/lineage/graph',
          ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
          evidence: { root: lineageSeedRoot, response: lineageGraph.body },
          hypothesis: 'Lineage graph traversal failed for a known timeline node.',
        })
      } else {
        const graphData = asObj(extractApiData<Record<string, unknown>>(lineageGraph.body))
        const graph = asObj(graphData?.graph)
        const nodeCount = asObjRows(graph?.nodes).length
        const edgeCount = asObjRows(graph?.edges).length
        lineageEvidence.graph = { root: lineageSeedRoot, node_count: nodeCount, edge_count: edgeCount }
        if (nodeCount <= 0 || edgeCount <= 0) {
          bugs.add({
            severity: 'P1',
            phase: 'Lineage',
            repro_steps: ['Call lineage graph with timeline-derived root'],
            expected: 'Graph traversal should contain at least one node and edge',
            actual: `node_count=${nodeCount}; edge_count=${edgeCount}`,
            endpoint: 'GET /api/v1/lineage/graph',
            ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
            evidence: { root: lineageSeedRoot, response: lineageGraph.body },
            hypothesis: 'Graph projection exists but traversal returned empty topology.',
          })
        }
      }

      const lineagePath = await apiCall(request, '/api/v1/lineage/path', {
        method: 'GET',
        headers: buildAuthHeaders(dbName),
        query: {
          db_name: dbName,
          source: lineageSeedRoot,
          target: lineageSeedTarget,
          direction: 'downstream',
          max_depth: 10,
          max_nodes: 5000,
          max_edges: 15000,
        },
      })
      if (!lineagePath.ok) {
        bugs.add({
          severity: 'P1',
          phase: 'Lineage',
          repro_steps: ['Call lineage path using seed source/target from timeline'],
          expected: 'Path endpoint returns a traversable path',
          actual: `status=${lineagePath.status}; body=${lineagePath.text.slice(0, 300)}`,
          endpoint: 'GET /api/v1/lineage/path',
          ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
          evidence: { source: lineageSeedRoot, target: lineageSeedTarget, response: lineagePath.body },
          hypothesis: 'Path search failed even though timeline exposed the same edge.',
        })
      } else {
        const pathData = asObj(extractApiData<Record<string, unknown>>(lineagePath.body))
        const pathFound = Boolean(pathData?.found)
        const hops = Number(pathData?.hops ?? 0)
        lineageEvidence.path = { found: pathFound, hops }
        if (!pathFound || hops <= 0) {
          bugs.add({
            severity: 'P1',
            phase: 'Lineage',
            repro_steps: ['Call lineage path using seed source/target from timeline'],
            expected: 'Path should be found with at least one hop',
            actual: `found=${String(pathFound)}; hops=${hops}`,
            endpoint: 'GET /api/v1/lineage/path',
            ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
            evidence: { source: lineageSeedRoot, target: lineageSeedTarget, response: lineagePath.body },
            hypothesis: 'Path traversal did not materialize even for directly connected lineage nodes.',
          })
        }
      }

      const lineageImpact = await apiCall(request, '/api/v1/lineage/impact', {
        method: 'GET',
        headers: buildAuthHeaders(dbName),
        query: {
          db_name: dbName,
          root: lineageSeedRoot,
          direction: 'downstream',
          max_depth: 10,
          max_nodes: 5000,
          max_edges: 15000,
        },
      })
      if (!lineageImpact.ok) {
        bugs.add({
          severity: 'P1',
          phase: 'Lineage',
          repro_steps: ['Call lineage impact using timeline-derived root'],
          expected: 'Impact endpoint returns impacted artifacts',
          actual: `status=${lineageImpact.status}; body=${lineageImpact.text.slice(0, 300)}`,
          endpoint: 'GET /api/v1/lineage/impact',
          ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
          evidence: { root: lineageSeedRoot, response: lineageImpact.body },
          hypothesis: 'Impact traversal failed for a valid lineage root.',
        })
      } else {
        const impactData = asObj(extractApiData<Record<string, unknown>>(lineageImpact.body))
        const artifactCount = asObjRows(impactData?.artifacts).length
        lineageEvidence.impact = { artifact_count: artifactCount, counts: impactData?.counts ?? null }
        if (artifactCount <= 0) {
          bugs.add({
            severity: 'P1',
            phase: 'Lineage',
            repro_steps: ['Call lineage impact using timeline-derived root'],
            expected: 'Impact endpoint should return at least one downstream artifact',
            actual: `artifact_count=${artifactCount}`,
            endpoint: 'GET /api/v1/lineage/impact',
            ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
            evidence: { root: lineageSeedRoot, response: lineageImpact.body },
            hypothesis: 'Lineage artifacts were not linked to seed root despite timeline activity.',
          })
        }
      }

      const lineageDiff = await apiCall(request, '/api/v1/lineage/diff', {
        method: 'GET',
        headers: buildAuthHeaders(dbName),
        query: {
          db_name: dbName,
          root: lineageSeedRoot,
          from_as_of: lineageWindowStart,
          to_as_of: lineageWindowEnd,
          direction: 'downstream',
          max_depth: 10,
          max_nodes: 5000,
          max_edges: 15000,
        },
      })
      if (!lineageDiff.ok) {
        bugs.add({
          severity: 'P1',
          phase: 'Lineage',
          repro_steps: ['Call lineage diff with pre/post time window'],
          expected: 'Diff endpoint returns added/removed lineage edges',
          actual: `status=${lineageDiff.status}; body=${lineageDiff.text.slice(0, 300)}`,
          endpoint: 'GET /api/v1/lineage/diff',
          ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
          evidence: { root: lineageSeedRoot, response: lineageDiff.body },
          hypothesis: 'Diff computation failed for active lineage root.',
        })
      } else {
        const diffData = asObj(extractApiData<Record<string, unknown>>(lineageDiff.body))
        const addedNodes = asObjRows(diffData?.added_nodes).length
        const addedEdges = asObjRows(diffData?.added_edges).length
        lineageEvidence.diff = { added_nodes: addedNodes, added_edges: addedEdges }
        if (addedNodes <= 0 && addedEdges <= 0) {
          bugs.add({
            severity: 'P1',
            phase: 'Lineage',
            repro_steps: ['Call lineage diff with pre/post time window'],
            expected: 'Diff should show at least one added node or edge in active window',
            actual: `added_nodes=${addedNodes}; added_edges=${addedEdges}`,
            endpoint: 'GET /api/v1/lineage/diff',
            ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
            evidence: { root: lineageSeedRoot, response: lineageDiff.body },
            hypothesis: 'Diff window/range succeeded but no lineage change was detected unexpectedly.',
          })
        }
      }
    }

    const runIdCandidate =
      (runRows.find((row) => typeof row.run_id === 'string' && String(row.run_id).trim())?.run_id as string | undefined) ??
      pipelineBuildJobId ??
      null

    const lineageRunImpact = runIdCandidate
      ? await apiCall(request, '/api/v1/lineage/run-impact', {
        method: 'GET',
        headers: buildAuthHeaders(dbName),
        query: {
          db_name: dbName,
          run_id: runIdCandidate,
          since: lineageWindowStart,
          until: lineageWindowEnd,
          event_limit: 5000,
          artifact_preview_limit: 200,
        },
      })
      : null
    if (runIdCandidate && lineageRunImpact && !lineageRunImpact.ok) {
      bugs.add({
        severity: 'P2',
        phase: 'Lineage',
        repro_steps: ['Call lineage run-impact with derived run id'],
        expected: 'Run impact endpoint responds successfully',
        actual: `status=${lineageRunImpact.status}; body=${lineageRunImpact.text.slice(0, 300)}`,
        endpoint: 'GET /api/v1/lineage/run-impact',
        ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
        evidence: { run_id: runIdCandidate, response: lineageRunImpact.body },
        hypothesis: 'Run impact summarization failed for selected run id.',
      })
    } else if (runIdCandidate && lineageRunImpact?.ok) {
      const runImpactData = asObj(extractApiData<Record<string, unknown>>(lineageRunImpact.body))
      const edgesLoaded = Number(runImpactData?.edges_loaded ?? 0)
      lineageEvidence.run_impact = { run_id: runIdCandidate, edges_loaded: edgesLoaded }
      if (runRows.length > 0 && edgesLoaded <= 0) {
        bugs.add({
          severity: 'P1',
          phase: 'Lineage',
          repro_steps: ['Call lineage runs', 'Call run-impact using returned run_id'],
          expected: 'Run impact should include at least one edge for returned run_id',
          actual: `run_count=${runRows.length}; run_id=${runIdCandidate}; edges_loaded=${edgesLoaded}`,
          endpoint: 'GET /api/v1/lineage/run-impact',
          ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
          evidence: { run_id: runIdCandidate, response: lineageRunImpact.body },
          hypothesis: 'Run summary exists but corresponding run impact edges were not linked.',
        })
      }
    }

    const lineageColumnLineage = await apiCall(request, '/api/v1/lineage/column-lineage', {
      method: 'GET',
      headers: buildAuthHeaders(dbName),
      query: {
        db_name: dbName,
        run_id: runIdCandidate ?? undefined,
        since: lineageWindowStart,
        until: lineageWindowEnd,
        edge_limit: 5000,
        pair_limit: 2000,
      },
    })
    if (!lineageColumnLineage.ok) {
      bugs.add({
        severity: 'P2',
        phase: 'Lineage',
        repro_steps: ['Load column-level lineage'],
        expected: 'Column lineage endpoint responds successfully',
        actual: `status=${lineageColumnLineage.status}; body=${lineageColumnLineage.text.slice(0, 300)}`,
        endpoint: 'GET /api/v1/lineage/column-lineage',
        ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
        evidence: { run_id: runIdCandidate, response: lineageColumnLineage.body },
        hypothesis: 'Column lineage resolver failed at runtime.',
      })
    } else {
      const columnLineageData = asObj(extractApiData<Record<string, unknown>>(lineageColumnLineage.body))
      const pairCount = Number(columnLineageData?.pair_count ?? 0)
      lineageEvidence.column_lineage = {
        pair_count: pairCount,
        unresolved_ref_count: Number(columnLineageData?.unresolved_ref_count ?? 0),
      }
      if (!Number.isFinite(pairCount) || pairCount < 0) {
        bugs.add({
          severity: 'P1',
          phase: 'Lineage',
          repro_steps: ['Load column-level lineage'],
          expected: 'pair_count is a non-negative number',
          actual: `pair_count=${String(columnLineageData?.pair_count)}`,
          endpoint: 'GET /api/v1/lineage/column-lineage',
          ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
          evidence: { response: lineageColumnLineage.body },
          hypothesis: 'Column lineage payload schema is inconsistent.',
        })
      }
    }

    const lineageOutOfDate = await apiCall(request, '/api/v1/lineage/out-of-date', {
      method: 'GET',
      headers: buildAuthHeaders(dbName),
      query: {
        db_name: dbName,
        freshness_slo_minutes: 120,
        artifact_limit: 5000,
        stale_preview_limit: 200,
        projection_limit: 1000,
        projection_preview_limit: 200,
      },
    })
    if (!lineageOutOfDate.ok) {
      bugs.add({
        severity: 'P2',
        phase: 'Lineage',
        repro_steps: ['Load out-of-date lineage diagnostics'],
        expected: 'Out-of-date diagnostics endpoint responds successfully',
        actual: `status=${lineageOutOfDate.status}; body=${lineageOutOfDate.text.slice(0, 300)}`,
        endpoint: 'GET /api/v1/lineage/out-of-date',
        ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
        evidence: { response: lineageOutOfDate.body },
        hypothesis: 'Out-of-date diagnostics aggregation failed in runtime.',
      })
    } else {
      const outOfDateData = asObj(extractApiData<Record<string, unknown>>(lineageOutOfDate.body))
      const outOfDateSummary = asObj(outOfDateData?.summary)
      const staleArtifactCount =
        outOfDateData?.stale_artifact_count ??
        outOfDateSummary?.stale_artifact_count ??
        null
      const staleProjectionCount =
        outOfDateData?.stale_projection_count ??
        outOfDateSummary?.stale_projection_count ??
        null
      lineageEvidence.out_of_date = {
        stale_artifact_count: staleArtifactCount,
        stale_projection_count: staleProjectionCount,
        warnings: outOfDateData?.warnings ?? null,
      }
      const hasStaleArtifactCount =
        Object.prototype.hasOwnProperty.call(outOfDateData ?? {}, 'stale_artifact_count') ||
        Object.prototype.hasOwnProperty.call(outOfDateSummary ?? {}, 'stale_artifact_count')
      if (!hasStaleArtifactCount) {
        bugs.add({
          severity: 'P1',
          phase: 'Lineage',
          repro_steps: ['Load out-of-date lineage diagnostics'],
          expected: 'Payload includes stale_artifact_count field (root or summary)',
          actual: `root_keys=${Object.keys(outOfDateData ?? {}).join(',')}; summary_keys=${Object.keys(outOfDateSummary ?? {}).join(',')}`,
          endpoint: 'GET /api/v1/lineage/out-of-date',
          ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
          evidence: { response: lineageOutOfDate.body },
          hypothesis: 'Out-of-date payload shape drifted from contract.',
        })
      }
    }
    qaLog('lineage phase done')

    await createScreenshot(page, testInfo, 'phase8_lineage')

    const summaryPayload = {
      dbName,
      apiContract: FOUNDRY_QA_API_CONTRACT,
      datasets: Object.fromEntries(datasets.entries()),
      pipelineId,
      createdOrderInstanceId,
      actionEvidence,
      dynamicSecurityEvidence,
      kineticQueryEvidence,
      connectionExportEvidence,
      datasetIntegrityEvidence,
      closedLoop,
      lineageEvidence,
      bugCounts: {
        P0: bugs.countBySeverity('P0'),
        P1: bugs.countBySeverity('P1'),
        P2: bugs.countBySeverity('P2'),
        P3: bugs.countBySeverity('P3'),
      },
      bugs: bugs.getAll(),
    }

    await testInfo.attach('foundry-live-qa-summary', {
      body: Buffer.from(JSON.stringify(summaryPayload, null, 2), 'utf-8'),
      contentType: 'application/json',
    })

    await bugs.flush()

    const allBugs = bugs.getAll()
    const bugDigest = allBugs
      .map((bug) => `${bug.id} [${bug.severity}] ${bug.phase} :: ${bug.endpoint}`)
      .join('\n')

    expect(closedLoop?.status, 'Closed-loop verification must be fully verified.').toBe('verified')
    expect(allBugs, bugDigest || 'No bugs expected').toEqual([])
  })
})
