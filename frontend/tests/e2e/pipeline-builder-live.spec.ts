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

test.describe('Pipeline Builder (live)', () => {
  test.describe.configure({ mode: 'serial' })

  test.setTimeout(180_000)

  test('graph + forms can define a pipeline (input → join → output) with immediate validation', async ({
    page,
    request,
  }) => {
    const dbName = `e2e_pb_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 8)}`

    const createDbResp = await request.post(`${bffBaseUrl}/api/v1/databases`, {
      headers: authHeaders,
      data: { name: dbName, description: 'e2e pipeline builder' },
    })
    expect(createDbResp.ok()).toBeTruthy()
    const createDbPayload = (await createDbResp.json()) as { data?: { command_id?: string } }
    const commandId = createDbPayload.data?.command_id
    expect(commandId).toBeTruthy()
    await waitForCommand(request, String(commandId))

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

    const newPipelineDialog = page.getByRole('dialog', { name: 'New pipeline' })
    await expect(newPipelineDialog).toBeVisible()
    await newPipelineDialog.getByLabel('Pipeline name').fill('e2e pipeline')
    await newPipelineDialog.getByLabel('Location').fill('e2e')
    await newPipelineDialog.getByLabel('Description').fill('pipeline builder live test')
    await newPipelineDialog.getByRole('button', { name: 'Create' }).click()
    await expect(newPipelineDialog).toBeHidden()

    const branchName = `e2e-${Date.now().toString(36)}`
    await page.locator('button.pipeline-pill').click()
    await page.getByRole('menuitem', { name: 'Create branch' }).click()
    const branchDialog = page.getByRole('dialog', { name: 'Create branch' })
    await expect(branchDialog).toBeVisible()
    await branchDialog.getByText('Branch name').click()
    await branchDialog.locator('input').first().fill(branchName)
    await branchDialog.getByRole('button', { name: 'Create' }).click()
    await expect(branchDialog).toBeHidden()
    await expect(page.getByText('Branch created.')).toBeVisible()
    await expect(page.locator('button.pipeline-pill')).toContainText(branchName)

    await page.locator('button.pipeline-pill').click()
    await page.getByRole('menuitem', { name: 'Archive branch' }).click()
    await expect(page.getByText('Branch archived.')).toBeVisible()
    await expect(page.locator('button.pipeline-pill')).toContainText('Archived')
    await expect(page.getByRole('button', { name: 'Save' })).toBeDisabled()
    await expect(page.getByRole('button', { name: 'Deploy', exact: true })).toBeDisabled()

    await page.locator('button.pipeline-pill').click()
    await page.getByRole('menuitem', { name: 'Restore branch' }).click()
    await expect(page.getByText('Branch restored.')).toBeVisible()
    await expect(page.locator('button.pipeline-pill')).not.toContainText('Archived')
    await expect(page.getByRole('button', { name: 'Save' })).toBeEnabled()

    const createManualDataset = async (name: string) => {
      await page.getByRole('button', { name: 'Add datasets' }).click()
      const datasetDialog = page.getByRole('dialog', { name: 'Add dataset' })
      await expect(datasetDialog).toBeVisible()
      await page.getByRole('tab', { name: 'Manual' }).click()
      const manualPanel = datasetDialog.getByLabel('Manual')
      await manualPanel.getByPlaceholder('Dataset name').fill(name)
      await manualPanel.getByPlaceholder('Columns (comma separated)').fill('id,name')
      await manualPanel.getByPlaceholder('Sample rows (optional, CSV lines)').fill('1,A\n2,B')
      await page.getByRole('button', { name: 'Create dataset' }).click()
      await expect(datasetDialog).toBeHidden()
      await expect(page.locator('.pipeline-node-title').filter({ hasText: name })).toBeVisible()
    }

    await createManualDataset('input_a')
    await createManualDataset('input_b')

    await page.locator('.pipeline-node-title').filter({ hasText: 'input_a' }).click()
    await page.locator('.node-action-bar .node-action-btn[aria-label="Join"]').click()
    const joinDialog = page.getByRole('dialog', { name: 'Create join' })
    await expect(joinDialog).toBeVisible()

    await joinDialog.getByLabel('Left dataset').selectOption({ label: 'input_a' })
    await joinDialog.getByLabel('Right dataset').selectOption({ label: 'input_b' })

    const createJoinButton = joinDialog.getByRole('button', { name: 'Create join' })
    await expect(createJoinButton).toBeDisabled()
    await expect(joinDialog.locator('#pipeline-join-left-key option[value="id"]')).toHaveCount(1)

    await joinDialog.getByLabel('Left key').selectOption('id')
    await joinDialog.getByLabel('Right key').selectOption('id')
    await expect(createJoinButton).toBeEnabled()
    await createJoinButton.click()

    await expect(joinDialog).toBeHidden()
    await expect(page.locator('.pipeline-node-title').filter({ hasText: 'Join' })).toBeVisible()

    await page.locator('.pipeline-node-title').filter({ hasText: 'Join' }).click()
    await page.locator('.node-action-bar .node-action-btn[aria-label="Filter"]').click()
    await expect(page.locator('.pipeline-node-title').filter({ hasText: 'Join · filter' })).toBeVisible()

    await page.locator('.pipeline-sidebar-left').getByRole('button', { name: 'Join · filter' }).click()
    await page.locator('.pipeline-sidebar-right').getByRole('button', { name: 'Edit node' }).click()
    const transformDialog = page.getByRole('dialog', { name: 'Edit transform' })
    await expect(transformDialog).toBeVisible()

    await transformDialog.getByLabel('Expression').fill('')
    await transformDialog.getByRole('button', { name: 'Save' }).click()
    await expect(page.getByText('Add an expression to continue.')).toBeVisible()

    await transformDialog.getByLabel('Expression').fill('id is not null')
    await transformDialog.getByRole('button', { name: 'Save' }).click()
    await expect(transformDialog).toBeHidden()
    await expect(page.getByText('id is not null')).toBeVisible()

    await page.locator('.pipeline-sidebar-left').getByRole('button', { name: 'Join · filter' }).click()
    await page.getByRole('button', { name: 'Add output' }).click()
    const outputDialog = page.getByRole('dialog', { name: 'Add output' })
    await expect(outputDialog).toBeVisible()
    await outputDialog.getByLabel('Output name').fill('Output 1')
    await outputDialog.getByLabel('Dataset name').fill('output_dataset')
    await outputDialog.getByRole('button', { name: 'Add output' }).click()
    await expect(outputDialog).toBeHidden()
    await expect(page.locator('.pipeline-node-title').filter({ hasText: 'Output 1' })).toBeVisible()

    await page.getByRole('button', { name: 'Save' }).click()
    await expect(page.getByText('Pipeline saved.')).toBeVisible()

    await page.getByTestId('pipeline-view-pseudocode').click()
    const pseudocodePanel = page.getByTestId('pipeline-pseudocode')
    await expect(pseudocodePanel).toBeVisible()
    await expect(pseudocodePanel).toContainText('Join')
    await expect(pseudocodePanel).toContainText('output_dataset')

    await page.getByTestId('pipeline-view-graph').click()
    await expect(page.locator('.pipeline-canvas')).toBeVisible()
  })

  test('csv upload input can be parsed into a dataset with schema-aware join suggestions', async ({
    page,
    request,
  }) => {
    const dbName = `e2e_pb_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 8)}`

    const createDbResp = await request.post(`${bffBaseUrl}/api/v1/databases`, {
      headers: authHeaders,
      data: { name: dbName, description: 'e2e pipeline builder csv upload' },
    })
    expect(createDbResp.ok()).toBeTruthy()
    const createDbPayload = (await createDbResp.json()) as { data?: { command_id?: string } }
    const commandId = createDbPayload.data?.command_id
    expect(commandId).toBeTruthy()
    await waitForCommand(request, String(commandId))

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

    const newPipelineDialog = page.getByRole('dialog', { name: 'New pipeline' })
    await expect(newPipelineDialog).toBeVisible()
    await newPipelineDialog.getByLabel('Pipeline name').fill('e2e pipeline csv')
    await newPipelineDialog.getByLabel('Location').fill('e2e')
    await newPipelineDialog.getByLabel('Description').fill('pipeline builder csv upload test')
    await newPipelineDialog.getByRole('button', { name: 'Create' }).click()
    await expect(newPipelineDialog).toBeHidden()

    const branchName = `e2e-${Date.now().toString(36)}`
    await page.locator('button.pipeline-pill').click()
    await page.getByRole('menuitem', { name: 'Create branch' }).click()
    const branchDialog = page.getByRole('dialog', { name: 'Create branch' })
    await expect(branchDialog).toBeVisible()
    await branchDialog.getByText('Branch name').click()
    await branchDialog.locator('input').first().fill(branchName)
    await branchDialog.getByRole('button', { name: 'Create' }).click()
    await expect(branchDialog).toBeHidden()
    await expect(page.getByText('Branch created.')).toBeVisible()
    await expect(page.locator('button.pipeline-pill')).toContainText(branchName)

    const previewPanel = page.locator('.pipeline-preview')
    await expect(previewPanel).toBeVisible()
    await previewPanel.locator('.preview-controls button').nth(1).click()
    await expect(page.getByRole('button', { name: 'Show preview' })).toBeVisible()

    const createManualDataset = async (name: string) => {
      await page.getByRole('button', { name: 'Add datasets' }).click()
      const datasetDialog = page.getByRole('dialog', { name: 'Add dataset' })
      await expect(datasetDialog).toBeVisible()
      await page.getByRole('tab', { name: 'Manual' }).click()
      const manualPanel = datasetDialog.getByLabel('Manual')
      await manualPanel.getByPlaceholder('Dataset name').fill(name)
      await manualPanel.getByPlaceholder('Columns (comma separated)').fill('id,name')
      await manualPanel.getByPlaceholder('Sample rows (optional, CSV lines)').fill('1,A\n2,B')
      await page.getByRole('button', { name: 'Create dataset' }).click()
      await expect(datasetDialog).toBeHidden()
      await expect(page.locator('.pipeline-node-title').filter({ hasText: name })).toBeVisible()
    }

    await page.getByRole('button', { name: 'Add datasets' }).click()
    const datasetDialog = page.getByRole('dialog', { name: 'Add dataset' })
    await expect(datasetDialog).toBeVisible()
    await page.getByRole('tab', { name: 'CSV upload' }).click()

    const csvInput = datasetDialog.locator('input[type="file"][accept=".csv"]')
    await csvInput.setInputFiles({
      name: 'input.csv',
      mimeType: 'text/csv',
      buffer: Buffer.from('id,name\n1,A\n2,B\n', 'utf-8'),
    })
    const csvPanel = datasetDialog.getByLabel('CSV upload')
    await csvPanel.getByPlaceholder('Dataset name').fill('csv_input')
    await datasetDialog.getByRole('button', { name: 'Upload and add to graph' }).click()
    await expect(datasetDialog).toBeHidden()
    await expect(page.locator('.pipeline-node-title').filter({ hasText: 'csv_input' })).toBeVisible()

    await createManualDataset('manual_input')

    await page.locator('.pipeline-node-title').filter({ hasText: 'csv_input' }).click()
    await page.locator('.node-action-bar .node-action-btn[aria-label="Join"]').click()
    const joinDialog = page.getByRole('dialog', { name: 'Create join' })
    await expect(joinDialog).toBeVisible()
    await joinDialog.getByLabel('Left dataset').selectOption({ label: 'csv_input' })
    await joinDialog.getByLabel('Right dataset').selectOption({ label: 'manual_input' })

    await expect(joinDialog.locator('#pipeline-join-left-key option[value="id"]')).toHaveCount(1)
    await joinDialog.getByLabel('Left key').selectOption('id')
    await joinDialog.getByLabel('Right key').selectOption('id')
    await joinDialog.getByRole('button', { name: 'Create join' }).click()
    await expect(joinDialog).toBeHidden()
    await expect(page.locator('.pipeline-node-title').filter({ hasText: 'Join' })).toBeVisible()

    await page.getByRole('button', { name: 'Add output' }).click()
    const outputDialog = page.getByRole('dialog', { name: 'Add output' })
    await expect(outputDialog).toBeVisible()
    await outputDialog.getByLabel('Output name').fill('Output 1')
    await outputDialog.getByLabel('Dataset name').fill('out_csv')
    await outputDialog.getByRole('button', { name: 'Add output' }).click()
    await expect(outputDialog).toBeHidden()
    await expect(page.locator('.pipeline-node-title').filter({ hasText: 'Output 1' })).toBeVisible()

    await page.locator('.pipeline-node-title').filter({ hasText: 'Join' }).click()
    await page.getByRole('button', { name: 'Show preview' }).click()
    await expect(page.locator('.pipeline-preview')).toBeVisible()

    const firstColumn = page.locator('[data-testid^="preview-column-"]').first()
    await expect(firstColumn).toBeVisible({ timeout: 60_000 })
    await firstColumn.click()
    await expect(page.getByTestId('preview-chart')).toBeVisible()
  })

  test('media upload input can be treated as unstructured dataset references', async ({ page, request }) => {
    const dbName = `e2e_pb_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 8)}`

    const createDbResp = await request.post(`${bffBaseUrl}/api/v1/databases`, {
      headers: authHeaders,
      data: { name: dbName, description: 'e2e pipeline builder media upload' },
    })
    expect(createDbResp.ok()).toBeTruthy()
    const createDbPayload = (await createDbResp.json()) as { data?: { command_id?: string } }
    const commandId = createDbPayload.data?.command_id
    expect(commandId).toBeTruthy()
    await waitForCommand(request, String(commandId))

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

    const newPipelineDialog = page.getByRole('dialog', { name: 'New pipeline' })
    await expect(newPipelineDialog).toBeVisible()
    await newPipelineDialog.getByLabel('Pipeline name').fill('e2e pipeline media')
    await newPipelineDialog.getByLabel('Location').fill('e2e')
    await newPipelineDialog.getByLabel('Description').fill('pipeline builder media upload test')
    await newPipelineDialog.getByRole('button', { name: 'Create' }).click()
    await expect(newPipelineDialog).toBeHidden()

    const branchName = `e2e-${Date.now().toString(36)}`
    await page.locator('button.pipeline-pill').click()
    await page.getByRole('menuitem', { name: 'Create branch' }).click()
    const branchDialog = page.getByRole('dialog', { name: 'Create branch' })
    await expect(branchDialog).toBeVisible()
    await branchDialog.getByText('Branch name').click()
    await branchDialog.locator('input').first().fill(branchName)
    await branchDialog.getByRole('button', { name: 'Create' }).click()
    await expect(branchDialog).toBeHidden()
    await expect(page.getByText('Branch created.')).toBeVisible()
    await expect(page.locator('button.pipeline-pill')).toContainText(branchName)

    await page.getByRole('button', { name: 'Add datasets' }).click()
    const datasetDialog = page.getByRole('dialog', { name: 'Add dataset' })
    await expect(datasetDialog).toBeVisible()
    await page.getByRole('tab', { name: 'Media upload' }).click()

    const mediaInput = datasetDialog.locator('input[type="file"][multiple]')
    await mediaInput.setInputFiles([
      {
        name: 'hello.txt',
        mimeType: 'text/plain',
        buffer: Buffer.from('hello world', 'utf-8'),
      },
      {
        name: 'image.png',
        mimeType: 'image/png',
        buffer: Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
      },
    ])

    const mediaPanel = datasetDialog.getByLabel('Media upload')
    await mediaPanel.getByPlaceholder('Dataset name').fill('media_input')
    await datasetDialog.getByRole('button', { name: 'Upload and add to graph' }).click()
    await expect(datasetDialog).toBeHidden()
    await expect(page.locator('.pipeline-node-title').filter({ hasText: 'media_input' })).toBeVisible()

    const previewPanel = page.locator('.pipeline-preview')
    await expect(previewPanel).toBeVisible()
    await expect(previewPanel.getByRole('cell', { name: 'hello.txt', exact: true })).toBeVisible()
    await expect(previewPanel.getByRole('cell', { name: 'image.png', exact: true })).toBeVisible()
  })
})
