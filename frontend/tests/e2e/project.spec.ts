import { expect, test } from '@playwright/test'
import { enableAdminMode, mockBffRoutes, seedLocalStorage } from '../utils/mockBff'

test.beforeEach(async ({ page }) => {
  await seedLocalStorage(page)
  await mockBffRoutes(page, { databases: ['demo', 'sandbox'], commandMode: 'async' })
  await page.goto('/?lang=en')
})

test('renders project overview', async ({ page }) => {
  await expect(page.getByRole('heading', { name: 'Project', level: 1 })).toBeVisible()

  const projectSelect = page.getByRole('combobox')
  await expect(projectSelect).toHaveValue('demo')
  await expect(projectSelect.locator('option')).toContainText(['demo', 'sandbox'])
})

test('opens a selected project', async ({ page }) => {
  const projectSelect = page.getByRole('combobox')
  await expect(projectSelect).toHaveValue('demo')

  await page.getByRole('button', { name: 'Open', exact: true }).click()
  await expect(page.getByText('Project opened: demo')).toBeVisible()
})

test('creates a project in admin mode', async ({ page }) => {
  await enableAdminMode(page)

  await page.getByPlaceholder('e.g. demo_project').fill('alpha_project')
  await page.getByPlaceholder('Short summary for teammates').fill('QA seed project')
  await page.getByRole('button', { name: 'Create project' }).click()

  await expect(page.getByText('Project creation accepted.')).toBeVisible()
  await expect(page.locator('.project-name')).toHaveText('alpha_project')
})

test('deletes a project with confirmation', async ({ page }) => {
  await enableAdminMode(page)

  const deleteButton = page.getByRole('button', { name: 'Delete project' })
  await expect(deleteButton).toBeEnabled()
  await deleteButton.click()

  const dialog = page.getByRole('dialog', { name: 'Delete project' })
  await expect(dialog).toBeVisible()
  await dialog.getByPlaceholder('Why are you doing this?').fill('cleanup')
  await dialog.getByPlaceholder('demo').fill('demo')
  await dialog.getByRole('button', { name: 'Delete' }).click()

  await expect(page.getByText('Delete request accepted.')).toBeVisible()
})
