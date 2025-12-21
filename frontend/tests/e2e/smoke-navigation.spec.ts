import { test, expect } from '@playwright/test'
import { mockBffRoutes, seedLocalStorage } from '../utils/mockBff'

test.beforeEach(async ({ page }) => {
  await mockBffRoutes(page)
  await seedLocalStorage(page)
})

test('databases -> overview navigation', async ({ page }) => {
  await page.goto('/?lang=en')
  await expect(page.getByRole('heading', { name: 'Databases' })).toBeVisible()
  await expect(page.getByText('demo')).toBeVisible()

  await page.getByRole('button', { name: 'Open' }).first().click()
  await expect(page).toHaveURL(/\/db\/demo\/overview/)
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible()
})

test('core routes load', async ({ page }) => {
  const routes = [
    ['/db/demo/branches?branch=main&lang=en', 'Branches'],
    ['/db/demo/ontology?branch=main&lang=en', 'Ontology'],
    ['/db/demo/mappings?branch=main&lang=en', 'Mappings'],
    ['/db/demo/data/sheets?branch=main&lang=en', 'Google Sheets'],
    ['/db/demo/data/import/sheets?branch=main&lang=en', 'Import (Google Sheets)'],
    ['/db/demo/data/import/excel?branch=main&lang=en', 'Import (Excel)'],
    ['/db/demo/data/schema-suggestion?branch=main&lang=en', 'Schema Suggestion'],
    ['/db/demo/instances?branch=main&lang=en', 'Instances'],
    ['/db/demo/explore/graph?branch=main&lang=en', 'Graph Explorer'],
    ['/db/demo/explore/query?branch=main&lang=en', 'Query Builder'],
    ['/db/demo/merge?branch=main&lang=en', 'Merge'],
    ['/db/demo/audit?branch=main&lang=en', 'Audit Logs'],
    ['/db/demo/lineage?branch=main&lang=en', 'Lineage'],
    ['/operations/tasks?lang=en', 'Tasks'],
    ['/operations/admin?lang=en', 'Admin Operations'],
  ] as const

  for (const [path, title] of routes) {
    await page.goto(path)
    await expect(page.getByRole('heading', { name: title })).toBeVisible()
  }
})
