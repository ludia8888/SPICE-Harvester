import { expect, test, type Page } from '@playwright/test'
import { enableAdminMode, mockBffRoutes, seedLocalStorage } from '../utils/mockBff'

const readTrackedCommands = async (page: Page) =>
  page.evaluate(() => {
    const raw = localStorage.getItem('commandTracker.items')
    if (!raw) {
      return [] as Array<{ id?: string }>
    }
    try {
      const parsed = JSON.parse(raw)
      return Array.isArray(parsed) ? parsed as Array<{ id?: string }> : []
    } catch {
      return [] as Array<{ id?: string }>
    }
  })

test.beforeEach(async ({ page }) => {
  await seedLocalStorage(page)
  await mockBffRoutes(page, { databases: ['demo', 'sandbox'], commandMode: 'async' })
  await page.goto('/?lang=en')
})

test('renders databases overview', async ({ page }) => {
  await expect(page.getByRole('heading', { name: 'Projects', level: 1 })).toBeVisible()
  await expect(page.locator('tbody tr', { hasText: 'demo' })).toBeVisible()
  await expect(page.locator('tbody tr', { hasText: 'sandbox' })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Create', exact: true })).toBeVisible()
})

test('opens a selected database', async ({ page }) => {
  const row = page.locator('tbody tr', { hasText: 'demo' }).first()
  await row.getByRole('button', { name: 'Open', exact: true }).click()
  await expect(page).toHaveURL(/\/db\/demo\/overview/)
  await expect(page.getByRole('heading', { name: 'demo', level: 1 })).toBeVisible()
})

test('creates a database and tracks command id', async ({ page }) => {
  await page.getByRole('button', { name: 'Create', exact: true }).click()
  const dialog = page.getByRole('dialog', { name: 'Create project' })
  await expect(dialog).toBeVisible()
  await dialog.getByPlaceholder('my_project').fill('alpha_project')
  await dialog.getByPlaceholder('Optional summary').fill('QA seed database')
  await dialog.getByRole('button', { name: 'Create', exact: true }).click()

  await expect.poll(async () => {
    const commands = await readTrackedCommands(page)
    return commands.some((item) => String(item?.id ?? '').startsWith('cmd-create-'))
  }).toBeTruthy()
})

test('deletes a database with confirmation', async ({ page }) => {
  await enableAdminMode(page)

  const row = page.locator('tbody tr', { hasText: 'demo' }).first()
  const deleteButton = row.getByRole('button', { name: 'Delete', exact: true })
  await expect(deleteButton).toBeEnabled()
  await deleteButton.click()

  const dialog = page.getByRole('dialog', { name: 'Delete project' })
  await expect(dialog).toBeVisible()
  await dialog.getByPlaceholder('Why are you deleting this project?').fill('cleanup')
  await dialog.getByPlaceholder('demo').fill('demo')
  await dialog.getByRole('button', { name: 'Delete', exact: true }).click()

  await expect.poll(async () => {
    const commands = await readTrackedCommands(page)
    return commands.some((item) => String(item?.id ?? '').startsWith('cmd-delete-'))
  }).toBeTruthy()
})
