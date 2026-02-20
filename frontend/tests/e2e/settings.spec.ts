import { expect, test } from '@playwright/test'
import { mockBffRoutes, openSettingsPopover, seedLocalStorage } from '../utils/mockBff'

test.beforeEach(async ({ page }) => {
  await seedLocalStorage(page)
  await mockBffRoutes(page, { databases: ['demo'] })
  await page.goto('/?lang=en')
})

test('updates branch context from settings', async ({ page }) => {
  const popover = await openSettingsPopover(page)
  const branchInput = popover.getByPlaceholder('e.g. main')

  await branchInput.fill('develop')
  await branchInput.blur()
  await page.keyboard.press('Escape')

  await expect(page.locator('.top-nav').getByText('develop').first()).toBeVisible()
  await expect(page).toHaveURL(/branch=develop/)

  const stored = await page.evaluate(() => localStorage.getItem('spice.branch'))
  expect(stored).toBe('develop')
})

test('switches language to Korean', async ({ page }) => {
  const popover = await openSettingsPopover(page)
  await popover.getByRole('combobox').selectOption('ko')
  await page.keyboard.press('Escape')

  await expect(page.locator('.step-title', { hasText: '프로젝트 설정' })).toBeVisible()
  await expect(page.getByRole('button', { name: '설정', exact: true }).first()).toBeVisible()
})

test('toggles dark mode', async ({ page }) => {
  const popover = await openSettingsPopover(page)
  await popover.getByText('Dark mode', { exact: true }).click()
  await page.keyboard.press('Escape')

  await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark')
  const isDark = await page.evaluate(() => document.documentElement.classList.contains('bp6-dark'))
  expect(isDark).toBe(true)
})
