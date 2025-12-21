import { test, expect } from '@playwright/test'
import { mockBffRoutes, seedLocalStorage } from '../utils/mockBff'

test.beforeEach(async ({ page }) => {
  await mockBffRoutes(page)
  await seedLocalStorage(page)
})

test('instances: list and open drawer', async ({ page }) => {
  await page.goto('/db/demo/instances?branch=main&lang=en')
  await expect(page.getByRole('heading', { name: 'Instances' })).toBeVisible()

  const classSelect = page.locator('select').first()
  await classSelect.selectOption('Product')

  await expect(page.getByText('prod-1')).toBeVisible()
  await page.getByRole('button', { name: 'Open' }).click()
  await expect(page.getByText('Branch ignored')).toBeVisible()
})

test('graph explorer: run query', async ({ page }) => {
  await page.goto('/db/demo/explore/graph?branch=main&lang=en')
  await expect(page.getByRole('heading', { name: 'Graph Explorer' })).toBeVisible()

  const startClass = page.locator('select').first()
  await startClass.selectOption('Product')

  await page.getByRole('button', { name: 'Run' }).click()
  await expect(page.locator('.graph-canvas')).toBeVisible()
  await expect(page.getByText('prod-1')).toBeVisible()
})

test('query builder: run query', async ({ page }) => {
  await page.goto('/db/demo/explore/query?branch=main&lang=en')
  await expect(page.getByRole('heading', { name: 'Query Builder' })).toBeVisible()

  const classSelect = page.locator('select').first()
  await classSelect.selectOption('Product')

  await page.getByRole('button', { name: 'Run Query' }).click()
  await expect(page.getByText('Prod 1')).toBeVisible()
})

test('merge: simulate and resolve', async ({ page }) => {
  await page.goto('/db/demo/merge?branch=main&lang=en')
  await expect(page.getByRole('heading', { name: 'Merge' })).toBeVisible()

  const selects = page.locator('select')
  await selects.nth(0).selectOption('feature/demo')
  await selects.nth(1).selectOption('main')

  await page.getByRole('button', { name: 'Simulate' }).click()
  await expect(page.getByText('Product.name')).toBeVisible()

  await page.getByRole('button', { name: 'Resolve merge' }).click()
  await expect(page.getByText('Resolution response will appear here.')).toBeVisible()
})

test('audit and lineage: load data', async ({ page }) => {
  await page.goto('/db/demo/audit?branch=main&lang=en')
  await page.getByRole('button', { name: 'Load logs' }).click()
  await expect(page.getByText('create')).toBeVisible()

  await page.goto('/db/demo/lineage?branch=main&lang=en')
  await page.getByLabel('Root (event id or node id)').fill('node-1')
  await page.getByRole('button', { name: 'Load graph' }).click()
  await expect(page.locator('.graph-canvas')).toBeVisible()
})
