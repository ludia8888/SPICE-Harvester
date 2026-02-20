import { readFile } from 'node:fs/promises'
import path from 'node:path'
import { expect, test } from '@playwright/test'
import type { APIRequestContext, Locator, Page, TestInfo } from '@playwright/test'
import { seedLocalStorage } from '../utils/mockBff'
import { FOUNDRY_QA_API_CONTRACT } from './foundryApiContract'
import type { ActionSideEffectEvidence, ClosedLoopVerificationResult } from './foundryQaTypes'
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

const buildLiveWeatherCsv = async () => {
  const url = 'https://api.open-meteo.com/v1/forecast?latitude=37.57&longitude=126.98&hourly=temperature_2m,precipitation&forecast_days=1&timezone=UTC'
  const response = await fetch(url)
  if (!response.ok) {
    throw new Error(`Open-Meteo fetch failed (${response.status})`)
  }
  const payload = (await response.json()) as {
    hourly?: {
      time?: string[]
      temperature_2m?: number[]
      precipitation?: number[]
    }
  }
  const time = payload.hourly?.time ?? []
  const temp = payload.hourly?.temperature_2m ?? []
  const rain = payload.hourly?.precipitation ?? []
  const rows = time.slice(0, 24).map((ts, index) => [ts, temp[index] ?? null, rain[index] ?? null])
  return Buffer.from(csvFromRows(['timestamp', 'temperature_2m', 'precipitation'], rows), 'utf-8')
}

const buildLiveFxCsv = async () => {
  const response = await fetch('https://api.frankfurter.app/latest?from=USD')
  if (!response.ok) {
    throw new Error(`Frankfurter fetch failed (${response.status})`)
  }
  const payload = (await response.json()) as {
    date?: string
    rates?: Record<string, number>
  }
  const date = payload.date ?? new Date().toISOString().slice(0, 10)
  const rates = payload.rates ?? {}
  const rows = Object.entries(rates)
    .slice(0, 20)
    .map(([currency, rate]) => [date, 'USD', currency, rate])
  return Buffer.from(csvFromRows(['date', 'base', 'quote', 'rate'], rows), 'utf-8')
}

const buildLiveEarthquakeCsv = async () => {
  const response = await fetch('https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson')
  if (!response.ok) {
    throw new Error(`USGS fetch failed (${response.status})`)
  }
  const payload = (await response.json()) as {
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
  }
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
  const headers = buildAuthHeaders(params.dbName)
  const idempotencyKey = `qa-upload-${params.datasetName}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`
  const response = await apiCall(
    params.request,
    '/api/v1/pipelines/datasets/csv-upload',
    {
      method: 'POST',
      headers: {
        ...headers,
        'Idempotency-Key': idempotencyKey,
      },
      query: {
        db_name: params.dbName,
        branch: 'main',
      },
      multipart: {
        dataset_name: params.datasetName,
        description: `Foundry QA ingest: ${params.datasetName}`,
        has_header: 'true',
        file: {
          name: params.fileName,
          mimeType: 'text/csv',
          buffer: params.fileBuffer,
        },
      },
    },
  )

  if (!response.ok) {
    params.bugs.add({
      severity: 'P0',
      phase: params.phase,
      repro_steps: [
        'Open Databases page',
        'Create database',
        `Upload CSV dataset (${params.datasetName})`,
      ],
      expected: 'CSV ingest succeeds and returns dataset_id',
      actual: `status=${response.status}; body=${response.text.slice(0, 400)}`,
      endpoint: 'POST /api/v1/pipelines/datasets/csv-upload',
      ui_path: `/db/${encodeURIComponent(params.dbName)}/overview`,
      evidence: {
        status: response.status,
        body: response.body,
        step: params.step,
        idempotency_key: idempotencyKey,
      },
      hypothesis: 'Dataset upload contract mismatch or storage/registry worker unavailable.',
    })
    return null
  }

  const data = extractApiData<Record<string, unknown>>(response.body)
  const dataset = (data?.dataset ?? null) as Record<string, unknown> | null
  const ingestRequestId =
    (typeof data?.ingest_request_id === 'string' && data.ingest_request_id) ||
    (typeof data?.ingestRequestId === 'string' && data.ingestRequestId) ||
    null

  const datasetId = typeof dataset?.dataset_id === 'string' ? dataset.dataset_id : null
  if (!datasetId) {
    params.bugs.add({
      severity: 'P1',
      phase: params.phase,
      repro_steps: [`Upload CSV dataset (${params.datasetName})`],
      expected: 'dataset_id exists in upload response',
      actual: `Missing dataset_id in response: ${JSON.stringify(response.body).slice(0, 400)}`,
      endpoint: 'POST /api/v1/pipelines/datasets/csv-upload',
      ui_path: `/db/${encodeURIComponent(params.dbName)}/overview`,
      evidence: { response: response.body, step: params.step },
      hypothesis: 'Upload succeeded partially but dataset registry payload is incomplete.',
    })
    return null
  }

  if (ingestRequestId) {
    const approve = await apiCall(params.request, `/api/v1/pipelines/datasets/ingest-requests/${encodeURIComponent(ingestRequestId)}/schema/approve`, {
      method: 'POST',
      headers,
      json: {},
    })
    if (!approve.ok) {
      params.bugs.add({
        severity: 'P2',
        phase: params.phase,
        repro_steps: [`Approve schema for ingest request (${ingestRequestId})`],
        expected: 'Schema approval succeeds',
        actual: `status=${approve.status}; body=${approve.text.slice(0, 300)}`,
        endpoint: 'POST /api/v1/pipelines/datasets/ingest-requests/{id}/schema/approve',
        ui_path: `/db/${encodeURIComponent(params.dbName)}/overview`,
        evidence: { response: approve.body, ingest_request_id: ingestRequestId },
        hypothesis: 'Ingest-request lifecycle is blocked by validation or ACL mismatch.',
      })
    }
  }

  return {
    datasetId,
    ingestRequestId,
    rawResponse: response.body,
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

test.describe.serial('Live Foundry QA', () => {
  test('foundry lifecycle via frontend-exposed api only', async ({ page, request, browserName }, testInfo) => {
    test.skip(browserName !== 'chromium', 'Foundry live QA is run in chromium only.')
    test.setTimeout(20 * 60 * 1000)

    const bugs = new QABugCollector()
    const dbName = makeDbName()
    const datasets = new Map<string, string>()
    let createdOrderInstanceId: string | null = null
    const uiSurfaceFailures: Array<{ status: number; method: string; url: string; pageUrl: string }> = []
    let collectUiSurfaceFailures = false

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
    await expect(page.getByRole('heading', { level: 1, name: 'Databases' })).toBeVisible()

    qaLog('opening database from list')
    await openDatabaseFromList(page, request, dbName)
    qaLog('database opened')
    await expect(page).toHaveURL(new RegExp(`/db/${dbName}/overview`))

    // Frontend surface smoke: navigate through mandatory screens like a user.
    const requiredScreens = [
      `/db/${encodeURIComponent(dbName)}/overview`,
      `/db/${encodeURIComponent(dbName)}/branches`,
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
          endpoint: 'local fixture + POST /api/v1/pipelines/datasets/csv-upload',
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
          endpoint: `fetch external + POST /api/v1/pipelines/datasets/csv-upload (${item.key})`,
          ui_path: `/db/${encodeURIComponent(dbName)}/overview`,
          evidence: { source: item.key },
          hypothesis: 'External source temporary failure or parser mismatch.',
        })
      }
    }
    qaLog(`live ingest done datasets=${datasets.size}`)

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
            if (content.length > 0) {
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
    const orderDatasetId = datasets.get('olist_orders')
    if (orderDatasetId) {
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
      }

      if (mappingSpecId) {
        const runObjectify = await apiCall(request, `/api/v1/objectify/datasets/${encodeURIComponent(orderDatasetId)}/run`, {
          method: 'POST',
          headers: {
            ...buildAuthHeaders(dbName),
            'Content-Type': 'application/json',
          },
          json: {
            mapping_spec_id: mappingSpecId,
            max_rows: 2000,
            allow_partial: true,
          },
        })

        if (!runObjectify.ok) {
          bugs.add({
            severity: 'P1',
            phase: 'Objectify',
            repro_steps: ['Run objectify job for orders dataset'],
            expected: 'Objectify run is queued successfully',
            actual: `status=${runObjectify.status}; body=${runObjectify.text.slice(0, 400)}`,
            endpoint: 'POST /api/v1/objectify/datasets/{datasetId}/run',
            ui_path: `/db/${encodeURIComponent(dbName)}/ontology`,
            evidence: { mapping_spec_id: mappingSpecId, response: runObjectify.body },
            hypothesis: 'Objectify queue or mapping contract mismatch blocked execution.',
          })
        }
      }
    }
    qaLog('objectify phase done')

    // Phase 5: Projection + multihop query
    const graphQuery = await apiCall(request, `/api/v1/graph-query/${encodeURIComponent(dbName)}`, {
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
                permission_policy: {
                  effect: 'ALLOW',
                  principals: ['role:Owner', 'role:Editor', 'role:DomainModeler'],
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
        const expectedActionStatus = actionTypeId === 'HoldOrder' ? 'ON_HOLD' : null

        if (executeApply.ok && actionTargetReady && expectedActionStatus) {
          for (let attempt = 0; attempt < 30; attempt += 1) {
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
                  pageSize: 200,
                },
              },
            )
            if (projectionCheck.ok) {
              const projectionPayload = projectionCheck.body as Record<string, unknown>
              const rows = Array.isArray(projectionPayload?.data)
                ? projectionPayload.data as Array<Record<string, unknown>>
                : []
              const target = rows.find((row) => String(row.order_id ?? row.primaryKey ?? '') === actionTargetOrderId) ?? null
              actionObservedStatus = target ? String(target.order_status ?? '') : null
              if (actionObservedStatus === expectedActionStatus) {
                actionProjectionVerified = true
                break
              }
            }
            await page.waitForTimeout(1000)
          }
        }

        if (executeApply.ok && auditLogId) {
          for (let attempt = 0; attempt < 20; attempt += 1) {
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
            await page.waitForTimeout(1000)
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

      const afterCount = afterObjects.length
      const created = afterObjects.find((item) => String(item.primaryKey ?? item.order_id ?? '') === syntheticOrderId)
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
        eqStrongRows.find((row) => String(row.__primaryKey ?? row.primaryKey ?? row.order_id ?? '') === syntheticOrderId) ?? null

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
      for (let attempt = 0; attempt < 12; attempt += 1) {
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

    // Phase 8: Lineage API checks
    const lineageMetrics = await apiCall(request, '/api/v1/lineage/metrics', {
      method: 'GET',
      headers: buildAuthHeaders(dbName),
      query: {
        db_name: dbName,
        window_minutes: 60,
      },
    })

    const lineageRuns = await apiCall(request, '/api/v1/lineage/runs', {
      method: 'GET',
      headers: buildAuthHeaders(dbName),
      query: {
        db_name: dbName,
        run_limit: 200,
        freshness_slo_minutes: 120,
      },
    })

    if (!lineageMetrics.ok) {
      bugs.add({
        severity: 'P2',
        phase: 'Lineage',
        repro_steps: ['Load lineage metrics'],
        expected: 'Lineage metrics endpoint responds successfully',
        actual: `status=${lineageMetrics.status}; body=${lineageMetrics.text.slice(0, 300)}`,
        endpoint: 'GET /api/v1/lineage/metrics',
        ui_path: `/db/${encodeURIComponent(dbName)}/lineage`,
        evidence: { response: lineageMetrics.body },
        hypothesis: 'Lineage store has no data yet or aggregation endpoint timed out.',
      })
    }

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
    qaLog('lineage phase done')

    await createScreenshot(page, testInfo, 'phase8_lineage')

    const summaryPayload = {
      dbName,
      apiContract: FOUNDRY_QA_API_CONTRACT,
      datasets: Object.fromEntries(datasets.entries()),
      pipelineId,
      createdOrderInstanceId,
      actionEvidence,
      closedLoop,
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
