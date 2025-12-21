import type { Page, Route } from '@playwright/test'

type JsonPayload = Record<string, unknown>

const jsonResponse = (payload: JsonPayload, status = 200) => ({
  status,
  contentType: 'application/json',
  body: JSON.stringify(payload),
})

const ok = (payload: JsonPayload = {}) => jsonResponse(payload, 200)

const accept = (payload: JsonPayload = {}) => jsonResponse(payload, 202)

const buildOntology = (id: string) => ({
  id,
  label: id,
  version: 1,
  properties: [
    { name: 'name', type: 'xsd:string', label: 'Name' },
    { name: 'category', type: 'xsd:string', label: 'Category' },
  ],
  relationships: [{ predicate: 'owned_by', target: 'Client', label: 'Owned by' }],
})

const handleBffRoute = async (route: Route) => {
  const request = route.request()
  const url = new URL(request.url())
  const pathname = url.pathname
  const method = request.method()

  if (pathname === '/api/v1/databases' && method === 'GET') {
    return route.fulfill(ok({ data: { databases: [{ name: 'demo' }] } }))
  }

  if (/^\/api\/v1\/databases\/[^/]+$/.test(pathname) && method === 'GET') {
    return route.fulfill(ok({ data: { name: pathname.split('/').pop() } }))
  }

  if (/^\/api\/v1\/databases\/[^/]+\/expected-seq$/.test(pathname)) {
    return route.fulfill(ok({ data: { expected_seq: 1 } }))
  }

  if (/^\/api\/v1\/databases\/[^/]+\/branches$/.test(pathname) && method === 'GET') {
    return route.fulfill(ok({ branches: [{ name: 'main' }, { name: 'feature/demo' }] }))
  }

  if (/^\/api\/v1\/databases\/[^/]+\/branches$/.test(pathname) && method === 'POST') {
    return route.fulfill(ok({ status: 'success' }))
  }

  if (/^\/api\/v1\/databases\/[^/]+\/branches\/.+$/.test(pathname) && method === 'DELETE') {
    return route.fulfill(ok({ status: 'success' }))
  }

  if (/^\/api\/v1\/databases\/[^/]+\/classes$/.test(pathname)) {
    return route.fulfill(
      ok({
        classes: [
          { id: 'Product', label: 'Product' },
          { id: 'Client', label: 'Client' },
        ],
      }),
    )
  }

  if (/^\/api\/v1\/database\/[^/]+\/ontology\/list$/.test(pathname)) {
    return route.fulfill(ok({ data: { ontologies: [buildOntology('Product'), buildOntology('Client')] } }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/ontology\/[^/]+\/schema$/.test(pathname)) {
    return route.fulfill(ok({ schema: { '@id': 'Product', properties: [] } }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/ontology\/validate$/.test(pathname)) {
    return route.fulfill(ok({ status: 'ok' }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/ontology\/[^/]+\/validate$/.test(pathname)) {
    return route.fulfill(ok({ status: 'ok' }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/ontology\/[^/]+$/.test(pathname) && method === 'GET') {
    const classId = pathname.split('/').pop() ?? 'Product'
    return route.fulfill(ok(buildOntology(classId)))
  }

  if (/^\/api\/v1\/database\/[^/]+\/ontology$/.test(pathname) && method === 'POST') {
    return route.fulfill(accept({ command_id: 'cmd-ontology-create', status: 'accepted' }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/ontology\/[^/]+$/.test(pathname) && method === 'PUT') {
    return route.fulfill(accept({ command_id: 'cmd-ontology-update', status: 'accepted' }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/ontology\/[^/]+$/.test(pathname) && method === 'DELETE') {
    return route.fulfill(accept({ command_id: 'cmd-ontology-delete', status: 'accepted' }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/mappings\/?$/.test(pathname)) {
    return route.fulfill(ok({ data: { items: [] }, summary: { total: 0 } }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/mappings\/export$/.test(pathname)) {
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      headers: { 'Content-Disposition': 'attachment; filename=demo_mappings.json' },
      body: JSON.stringify({}),
    })
  }

  if (/^\/api\/v1\/database\/[^/]+\/mappings\/(validate|import)$/.test(pathname)) {
    return route.fulfill(ok({ status: 'ok' }))
  }

  if (pathname === '/api/v1/data-connectors/google-sheets/preview') {
    return route.fulfill(
      ok({
        columns: ['col_a', 'col_b'],
        sample_rows: [['a', 'b']],
        total_rows: 1,
        total_columns: 2,
      }),
    )
  }

  if (pathname === '/api/v1/data-connectors/google-sheets/grid') {
    return route.fulfill(
      ok({
        tables: [{ table_id: 'T1', bbox: { top: 1, left: 1, bottom: 3, right: 3 } }],
      }),
    )
  }

  if (pathname === '/api/v1/data-connectors/google-sheets/register') {
    return route.fulfill(ok({ sheet_id: 'sheet-1', worksheet_name: 'Sheet1', database_name: 'demo', branch: 'main' }))
  }

  if (pathname === '/api/v1/data-connectors/google-sheets/registered') {
    return route.fulfill(ok({ data: { sheets: [{ sheet_id: 'sheet-1', worksheet_name: 'Sheet1', database_name: 'demo', branch: 'main' }] } }))
  }

  if (/^\/api\/v1\/data-connectors\/google-sheets\/[^/]+\/preview$/.test(pathname)) {
    return route.fulfill(ok({ columns: ['col_a'], sample_rows: [['value']] }))
  }

  if (/^\/api\/v1\/data-connectors\/google-sheets\/[^/]+$/.test(pathname) && method === 'DELETE') {
    return route.fulfill(ok({ status: 'ok' }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/suggest-mappings-from-google-sheets$/.test(pathname)) {
    return route.fulfill(ok({ mappings: [{ source_field: 'col_a', target_field: 'name' }] }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/import-from-google-sheets\/dry-run$/.test(pathname)) {
    return route.fulfill(ok({ stats: { rows: 1 }, errors: [] }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/import-from-google-sheets\/commit$/.test(pathname)) {
    return route.fulfill(ok({ write: { commands: [{ command_id: 'cmd-import-1' }] } }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/suggest-mappings-from-excel$/.test(pathname)) {
    return route.fulfill(ok({ mappings: [] }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/import-from-excel\/dry-run$/.test(pathname)) {
    return route.fulfill(ok({ stats: { rows: 1 }, errors: [] }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/import-from-excel\/commit$/.test(pathname)) {
    return route.fulfill(ok({ write: { commands: [{ command_id: 'cmd-import-excel-1' }] } }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/suggest-schema-from-google-sheets$/.test(pathname)) {
    return route.fulfill(ok({ suggested_schema: [buildOntology('Product')] }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/suggest-schema-from-excel$/.test(pathname)) {
    return route.fulfill(ok({ suggested_schema: [buildOntology('Product')] }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/suggest-schema-from-data$/.test(pathname)) {
    return route.fulfill(ok({ suggested_schema: [buildOntology('Product')] }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/class\/[^/]+\/instances$/.test(pathname)) {
    return route.fulfill(
      ok({
        instances: [
          {
            id: 'prod-1',
            version: 1,
            event_timestamp: '2025-01-01T00:00:00Z',
            display: { label: 'Prod 1' },
          },
        ],
      }),
    )
  }

  if (/^\/api\/v1\/database\/[^/]+\/class\/[^/]+\/instance\/[^/]+$/.test(pathname)) {
    return route.fulfill(
      ok({
        id: 'prod-1',
        version: 1,
        display: { label: 'Prod 1' },
        index_status: { event_sequence: 1 },
      }),
    )
  }

  if (/^\/api\/v1\/database\/[^/]+\/class\/[^/]+\/sample-values$/.test(pathname)) {
    return route.fulfill(ok({ sample: [] }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/instances\/[^/]+\/create$/.test(pathname)) {
    return route.fulfill(accept({ command_id: 'cmd-instance-create', status: 'accepted' }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/instances\/[^/]+\/bulk-create$/.test(pathname)) {
    return route.fulfill(accept({ command_id: 'cmd-instance-bulk', status: 'accepted' }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/instances\/[^/]+\/[^/]+\/update$/.test(pathname)) {
    return route.fulfill(accept({ command_id: 'cmd-instance-update', status: 'accepted' }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/instances\/[^/]+\/[^/]+\/delete$/.test(pathname)) {
    return route.fulfill(accept({ command_id: 'cmd-instance-delete', status: 'accepted' }))
  }

  if (/^\/api\/v1\/graph-query\/health$/.test(pathname)) {
    return route.fulfill(ok({ status: 'ok' }))
  }

  if (/^\/api\/v1\/graph-query\/[^/]+\/paths$/.test(pathname)) {
    return route.fulfill(ok({ paths: [] }))
  }

  if (/^\/api\/v1\/graph-query\/[^/]+$/.test(pathname) && method === 'POST') {
    return route.fulfill(
      ok({
        nodes: [
          {
            id: 'prod-1',
            type: 'Product',
            data_status: 'FULL',
            display: { label: 'Prod 1' },
          },
        ],
        edges: [],
      }),
    )
  }

  if (/^\/api\/v1\/ai\/translate\/query-plan\/[^/]+$/.test(pathname)) {
    return route.fulfill(
      ok({
        plan: { graph_query: { start_class: 'Product', hops: [], filters: {}, limit: 10 } },
        llm: { provider: 'mock', model: 'mock', cache_hit: true, latency_ms: 1 },
      }),
    )
  }

  if (/^\/api\/v1\/ai\/query\/[^/]+$/.test(pathname)) {
    return route.fulfill(ok({ answer: 'Mock answer', plan: {}, execution: {} }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/query\/builder$/.test(pathname)) {
    return route.fulfill(ok({ operators: { default: ['=', '!=', 'LIKE'] } }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/query$/.test(pathname)) {
    return route.fulfill(ok({ results: [{ name: 'Prod 1', price: 100 }], total: 1 }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/query\/raw$/.test(pathname)) {
    return route.fulfill(ok({ results: [] }))
  }

  if (/^\/api\/v1\/database\/[^/]+\/merge\/simulate$/.test(pathname)) {
    return route.fulfill(
      ok({
        data: {
          merge_preview: {
            conflicts: [
              {
                id: 'c1',
                path: { raw: 'Product.name', human_readable: 'Product.name' },
                sides: {
                  source: { value: 'Source', preview: 'Source' },
                  target: { value: 'Target', preview: 'Target' },
                },
                resolution: {
                  options: [
                    { id: 'use_source', label: 'Use source', value: 'Source', recommended: true },
                    { id: 'use_target', label: 'Use target', value: 'Target' },
                    { id: 'manual_merge', label: 'Manual value', value: null },
                  ],
                },
              },
            ],
          },
        },
      }),
    )
  }

  if (/^\/api\/v1\/database\/[^/]+\/merge\/resolve$/.test(pathname)) {
    return route.fulfill(accept({ command_id: 'cmd-merge-resolve', status: 'accepted' }))
  }

  if (/^\/api\/v1\/audit\/logs$/.test(pathname)) {
    return route.fulfill(
      ok({
        data: {
          items: [
            {
              event_id: 'evt-1',
              occurred_at: '2025-01-01T00:00:00Z',
              actor: 'tester',
              action: 'create',
              status: 'success',
              resource_type: 'Product',
              resource_id: 'prod-1',
              command_id: 'cmd-1',
            },
          ],
        },
      }),
    )
  }

  if (/^\/api\/v1\/audit\/chain-head$/.test(pathname)) {
    return route.fulfill(ok({ head: { event_id: 'evt-1' } }))
  }

  if (/^\/api\/v1\/lineage\/graph$/.test(pathname)) {
    return route.fulfill(
      ok({
        data: {
          graph: {
            nodes: [{ node_id: 'node-1', label: 'Node 1', node_type: 'event' }],
            edges: [{ from_node_id: 'node-1', to_node_id: 'node-1', edge_type: 'self' }],
            warnings: [],
          },
        },
      }),
    )
  }

  if (/^\/api\/v1\/lineage\/impact$/.test(pathname)) {
    return route.fulfill(ok({ data: { impacted: [] } }))
  }

  if (/^\/api\/v1\/lineage\/metrics$/.test(pathname)) {
    return route.fulfill(ok({ metrics: { total: 1 } }))
  }

  if (/^\/api\/v1\/tasks\/?$/.test(pathname)) {
    return route.fulfill(ok({ tasks: [{ task_id: 'task-1', status: 'running', task_type: 'demo' }] }))
  }

  if (/^\/api\/v1\/tasks\/[^/]+\/result$/.test(pathname)) {
    return route.fulfill(ok({ result: { status: 'ok' } }))
  }

  if (/^\/api\/v1\/tasks\/metrics\/summary$/.test(pathname)) {
    return route.fulfill(ok({ metrics: { total: 1 } }))
  }

  if (/^\/api\/v1\/tasks\/[^/]+$/.test(pathname) && method === 'GET') {
    return route.fulfill(ok({ task_id: 'task-1', status: 'running', task_type: 'demo' }))
  }

  if (/^\/api\/v1\/tasks\/[^/]+$/.test(pathname) && method === 'DELETE') {
    return route.fulfill(ok({ status: 'cancelled' }))
  }

  if (/^\/api\/v1\/admin\/replay-instance-state$/.test(pathname)) {
    return route.fulfill(ok({ task_id: 'task-replay-1' }))
  }

  if (/^\/api\/v1\/admin\/recompute-projection$/.test(pathname)) {
    return route.fulfill(ok({ task_id: 'task-recompute-1' }))
  }

  if (/^\/api\/v1\/admin\/cleanup-old-replays$/.test(pathname)) {
    return route.fulfill(ok({ status: 'ok' }))
  }

  if (/^\/api\/v1\/admin\/system-health$/.test(pathname)) {
    return route.fulfill(ok({ status: 'ok' }))
  }

  if (/^\/api\/v1\/summary$/.test(pathname)) {
    return route.fulfill(
      ok({
        data: {
          policy: { is_protected_branch: false, protected_branches: ['main'] },
          services: { redis: { status: 'ok' }, elasticsearch: { status: 'ok' } },
        },
      }),
    )
  }

  if (/^\/api\/v1\/commands\/[^/]+\/status$/.test(pathname)) {
    return route.fulfill(ok({ command_id: 'cmd-1', status: 'COMPLETED' }))
  }

  return route.fulfill(jsonResponse({ error: 'Unhandled mock route', path: pathname, method }, 404))
}

export const mockBffRoutes = async (page: Page) => {
  await page.route('**/api/v1/**', handleBffRoute)
}

export const seedLocalStorage = async (page: Page, overrides?: Record<string, string>) => {
  const storage = {
    'spice.language': 'en',
    'spice.branch': 'main',
    'spice.project': 'demo',
    'spice.theme': 'light',
    'spice.rememberToken': 'true',
    'spice.adminToken': 'test-token',
    'commandTracker.items': '[]',
    ...overrides,
  }

  await page.addInitScript((entries) => {
    Object.entries(entries).forEach(([key, value]) => {
      window.localStorage.setItem(key, value as string)
    })
  }, storage)
}
