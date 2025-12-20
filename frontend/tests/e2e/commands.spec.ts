import { expect, test } from '@playwright/test'
import { mockBffRoutes, seedLocalStorage } from '../utils/mockBff'

test.beforeEach(async ({ page }) => {
  await seedLocalStorage(page)
  await mockBffRoutes(page, { databases: ['demo'] })
  await page.goto('/?lang=en')
})

test('tracks a command from the drawer', async ({ page }) => {
  const topNav = page.locator('.top-nav')
  await topNav.getByRole('button', { name: /^Commands/ }).click()

  const drawer = page.locator('.command-drawer')
  await expect(drawer).toBeVisible()

  await drawer.getByLabel('Add command id').fill('cmd-123')
  await drawer.getByRole('button', { name: 'Add', exact: true }).click()

  const row = drawer.getByRole('row', { name: /cmd-123/ })
  await expect(row).toBeVisible()
  await row.click()

  await expect(drawer.locator('.command-details').getByText('COMPLETED')).toBeVisible()
})
