import { expect, test } from '@playwright/test'
import { mockBffRoutes, seedLocalStorage } from '../utils/mockBff'

test.beforeEach(async ({ page }) => {
  await seedLocalStorage(page)
  await mockBffRoutes(page, { databases: ['demo'] })
  await page.goto('/?lang=en')
})

test('shows flow steps for branch, ontology, and mappings', async ({ page }) => {
  await expect(page.locator('.step-title', { hasText: 'Branch setup' })).toBeVisible()
  await expect(page.locator('.step-title', { hasText: 'Ontology' })).toBeVisible()
  await expect(page.locator('.step-title', { hasText: 'Mappings' })).toBeVisible()
})

test('renders sidebar rail actions', async ({ page }) => {
  const rail = page.locator('.sidebar-rail')
  await expect(rail.getByRole('button', { name: 'Databases', exact: true })).toBeVisible()
  await expect(rail.getByRole('button', { name: 'Tasks', exact: true })).toBeVisible()
  await expect(rail.getByRole('button', { name: 'Admin', exact: true })).toBeVisible()
})
