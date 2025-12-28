import { test, expect, type APIRequestContext } from '@playwright/test'

const adminToken = process.env.ADMIN_TOKEN ?? 'test-token'
const bffBaseUrl = process.env.PLAYWRIGHT_BFF_BASE_URL ?? 'http://127.0.0.1:8002'

const authHeaders = {
  'X-Admin-Token': adminToken,
} as const

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const waitForCommand = async (request: APIRequestContext, commandId: string) => {
  for (let attempt = 0; attempt < 60; attempt += 1) {
    const statusResp = await request.get(`${bffBaseUrl}/api/v1/commands/${commandId}/status`, {
      headers: authHeaders,
    })
    if (!statusResp.ok()) {
      await sleep(500)
      continue
    }
    const payload = (await statusResp.json()) as { status?: string; data?: { status?: string } }
    const status = String(payload.status ?? payload.data?.status ?? '').toUpperCase()
    if (status === 'COMPLETED' || status === 'SUCCESS' || status === 'SUCCEEDED' || status === 'DONE') {
      return
    }
    if (status === 'FAILED' || status === 'ERROR') {
      throw new Error(`Command ${commandId} failed: ${JSON.stringify(payload)}`)
    }
    await sleep(500)
  }
  throw new Error(`Timed out waiting for command ${commandId}`)
}

test.describe('Pipeline Builder (large graph)', () => {
  test.describe.configure({ mode: 'serial' })

  test.setTimeout(180_000)

  test('can navigate a 200-node pipeline via search + focus mode', async ({ page, request }) => {
    const dbName = `e2e_pb_large_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 8)}`

    const createDbResp = await request.post(`${bffBaseUrl}/api/v1/databases`, {
      headers: authHeaders,
      data: { name: dbName, description: 'e2e pipeline builder large graph' },
    })
    expect(createDbResp.ok()).toBeTruthy()
    const createDbPayload = (await createDbResp.json()) as { data?: { command_id?: string } }
    const commandId = createDbPayload.data?.command_id
    expect(commandId).toBeTruthy()
    await waitForCommand(request, String(commandId))

    const nodes = Array.from({ length: 200 }, (_, index) => {
      const columns = 20
      const col = index % columns
      const row = Math.floor(index / columns)
      return {
        id: `node-${index}`,
        title: `node_${index}`,
        type: 'transform',
        icon: 'function',
        x: col * 300,
        y: row * 140,
        metadata: { operation: 'filter', expression: '1 = 1' },
      }
    })

    const edges = Array.from({ length: 199 }, (_, index) => ({
      id: `edge-${index}`,
      from: `node-${index}`,
      to: `node-${index + 1}`,
    }))

    const createPipelineResp = await request.post(`${bffBaseUrl}/api/v1/pipelines`, {
      headers: authHeaders,
      data: {
        db_name: dbName,
        name: 'large graph',
        location: 'e2e',
        description: 'large graph pipeline',
        pipeline_type: 'batch',
        branch: 'main',
        definition_json: {
          nodes,
          edges,
          parameters: [],
          outputs: [],
        },
      },
    })
    expect(createPipelineResp.ok()).toBeTruthy()

    await page.addInitScript(
      ({ project, token }) => {
        window.localStorage.setItem('spice.language', 'en')
        window.localStorage.setItem('spice.branch', 'main')
        window.localStorage.setItem('spice.project', project)
        window.localStorage.setItem('spice.theme', 'light')
        window.localStorage.setItem('spice.rememberToken', 'true')
        window.localStorage.setItem('spice.adminToken', token)
        window.localStorage.setItem('commandTracker.items', '[]')
      },
      { project: dbName, token: adminToken },
    )

    await page.goto(`/db/${encodeURIComponent(dbName)}/pipeline?branch=main&lang=en`)

    await expect(page.locator('.pipeline-node')).toHaveCount(200)

    await page.getByLabel('Search nodes').fill('node_199')
    await page.locator('.pipeline-sidebar-left').getByRole('button', { name: 'node_199' }).click()

    await page.getByRole('button', { name: 'Focus' }).click()
    await expect(page.locator('.pipeline-node')).toHaveCount(2)

    await page.getByRole('button', { name: 'Show all' }).click()
    await expect(page.locator('.pipeline-node')).toHaveCount(200)
  })
})
