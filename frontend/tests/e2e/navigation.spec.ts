import { expect, test } from '@playwright/test'
import { mockBffRoutes, seedLocalStorage } from '../utils/mockBff'

test.beforeEach(async ({ page }) => {
  await seedLocalStorage(page)
  await mockBffRoutes(page, { databases: ['demo'] })
  await page.goto('/?lang=en')
})

test('shows shell and project list by default', async ({ page }) => {
  await expect(page.locator('.top-nav').getByRole('button', { name: 'Spice OS', exact: true })).toBeVisible()
  await expect(page.locator('.top-nav').getByText('No project selected')).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Projects', level: 1 })).toBeVisible()
})

test('renders left navigation actions', async ({ page }) => {
  const rail = page.locator('.lnb')
  await expect(rail.getByRole('button', { name: 'Projects', exact: true })).toBeVisible()
  await expect(rail.getByRole('button', { name: 'Job Monitor', exact: true })).toBeVisible()
  await expect(rail.getByRole('button', { name: 'Settings', exact: true })).toBeVisible()
})
