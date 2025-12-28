import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: './tests',
  timeout: 60_000,
  expect: {
    timeout: 10_000,
  },
  fullyParallel: true,
  retries: 0,
  reporter: [['list'], ['html', { open: 'never' }]],
  outputDir: process.env.PLAYWRIGHT_OUTPUT_DIR ?? 'test-results',
  use: {
    baseURL: 'http://127.0.0.1:5173',
    trace: (process.env.PLAYWRIGHT_TRACE as 'on' | 'off' | 'retain-on-failure') ?? 'retain-on-failure',
    screenshot: (process.env.PLAYWRIGHT_SCREENSHOT as 'on' | 'off' | 'only-on-failure') ?? 'only-on-failure',
    video: (process.env.PLAYWRIGHT_VIDEO as 'on' | 'off' | 'retain-on-failure') ?? 'retain-on-failure',
  },
  webServer: {
    command: 'npm run dev -- --host 127.0.0.1 --port 5173',
    url: 'http://127.0.0.1:5173',
    reuseExistingServer: !process.env.CI,
  },
})
