import type { Page, Request } from '@playwright/test'

type CommandMode = 'async' | 'sync'

type MockBffOptions = {
  databases?: string[]
  expectedSeq?: number
  commandMode?: CommandMode
}

type SeedOptions = {
  adminToken?: string
  language?: 'en' | 'ko'
  branch?: string
  project?: string | null
  rememberToken?: boolean
  theme?: 'light' | 'dark'
}

const parseBody = (request: Request) => {
  const raw = request.postData()
  if (!raw) {
    return null
  }
  try {
    return JSON.parse(raw) as Record<string, unknown>
  } catch {
    return null
  }
}

export const seedLocalStorage = async (page: Page, options: SeedOptions = {}) => {
  const seed = {
    adminToken: options.adminToken ?? 'test-token',
    language: options.language ?? 'en',
    branch: options.branch ?? 'main',
    project: options.project ?? null,
    rememberToken: options.rememberToken ?? true,
    theme: options.theme ?? 'light',
  }

  await page.addInitScript((values) => {
    localStorage.setItem('spice.language', values.language)
    localStorage.setItem('spice.branch', values.branch)
    localStorage.setItem('spice.theme', values.theme)
    localStorage.setItem('spice.rememberToken', values.rememberToken ? 'true' : 'false')
    if (values.adminToken) {
      localStorage.setItem('spice.adminToken', values.adminToken)
    } else {
      localStorage.removeItem('spice.adminToken')
    }
    if (values.project) {
      localStorage.setItem('spice.project', values.project)
    } else {
      localStorage.removeItem('spice.project')
    }
    localStorage.setItem('commandTracker.items', '[]')
  }, seed)
}

export const mockBffRoutes = async (page: Page, options: MockBffOptions = {}) => {
  const databases = new Set(options.databases ?? ['demo'])
  const expectedSeq = options.expectedSeq ?? 1
  const commandMode = options.commandMode ?? 'async'
  let commandIndex = 1

  const nextCommandId = (prefix: string) => `${prefix}-${commandIndex++}`

  await page.route('**/api/v1/**', async (route) => {
    const request = route.request()
    const url = new URL(request.url())
    const path = url.pathname.replace(/\/+$/, '')
    const method = request.method().toUpperCase()

    if (path === '/api/v1/databases' && method === 'GET') {
      const payload = {
        data: {
          databases: Array.from(databases).map((name) => ({ name })),
        },
      }
      return route.fulfill({ status: 200, json: payload })
    }

    if (path === '/api/v1/databases' && method === 'POST') {
      const body = parseBody(request)
      const name = typeof body?.name === 'string' ? body.name : null
      if (name) {
        databases.add(name)
      }
      if (commandMode === 'sync') {
        return route.fulfill({ status: 200, json: { data: {} } })
      }
      return route.fulfill({
        status: 202,
        json: { data: { command_id: nextCommandId('cmd-create') } },
      })
    }

    const expectedMatch = path.match(/^\/api\/v1\/databases\/([^/]+)\/expected-seq$/)
    if (expectedMatch && method === 'GET') {
      return route.fulfill({ status: 200, json: { data: { expected_seq: expectedSeq } } })
    }

    const dbMatch = path.match(/^\/api\/v1\/databases\/([^/]+)$/)
    if (dbMatch && method === 'GET') {
      return route.fulfill({ status: 200, json: { data: { name: decodeURIComponent(dbMatch[1]) } } })
    }

    if (dbMatch && method === 'DELETE') {
      const dbName = decodeURIComponent(dbMatch[1])
      databases.delete(dbName)
      if (commandMode === 'sync') {
        return route.fulfill({ status: 200, json: { data: {} } })
      }
      return route.fulfill({
        status: 202,
        json: { data: { command_id: nextCommandId('cmd-delete') } },
      })
    }

    const commandMatch = path.match(/^\/api\/v1\/commands\/([^/]+)$/)
    if (commandMatch && method === 'GET') {
      const commandId = decodeURIComponent(commandMatch[1])
      return route.fulfill({
        status: 200,
        json: { command_id: commandId, status: 'COMPLETED', result: {} },
      })
    }

    return route.fulfill({
      status: 404,
      json: { detail: `No mock for ${method} ${path}` },
    })
  })
}

export const enableAdminMode = async (page: Page) => {
  await page.getByRole('button', { name: 'Settings', exact: true }).click()
  await page.locator('.settings-popover').waitFor({ state: 'visible' })
  const adminToggle = page.getByRole('checkbox', { name: 'Admin mode (dangerous actions)', exact: true })
  await adminToggle.check({ force: true })
  await page.keyboard.press('Escape')
}
