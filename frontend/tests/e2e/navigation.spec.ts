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
  await expect(page.getByRole('button', { name: 'Ontology', exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Mappings', exact: true })).toBeVisible()
})
