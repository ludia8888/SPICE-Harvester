import { LEGACY_ENDPOINT_BLOCKLIST_PATTERNS } from '../../src/api/legacyEndpointBlocklist'

type Method = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE'

const ALLOWED_PREFIXES = [
  '/api/v1/databases',
  '/api/v1/commands',
  '/api/v1/summary',
  '/api/v1/pipelines',
  '/api/v1/objectify',
  '/api/v1/graph-query',
  '/api/v1/admin/recompute-projection',
  '/api/v1/access-policies',
  '/api/v1/audit',
  '/api/v1/lineage',
  '/api/v2/datasets',
  '/api/v2/connectivity',
  '/api/v2/ontologies',
]

export const normalizeApiPath = (urlOrPath: string) => {
  const raw = String(urlOrPath ?? '').trim()
  if (!raw) {
    return ''
  }
  try {
    const parsed = raw.startsWith('http://') || raw.startsWith('https://')
      ? new URL(raw)
      : new URL(raw, 'http://localhost')
    return parsed.pathname
  } catch {
    return raw.split('?')[0] ?? ''
  }
}

export const isAllowedFoundryQaEndpoint = (method: string, urlOrPath: string) => {
  const normalizedMethod = String(method ?? '').trim().toUpperCase() as Method
  const path = normalizeApiPath(urlOrPath)
  if (!path.startsWith('/api/')) {
    return true
  }
  if (!normalizedMethod) {
    return false
  }
  if (LEGACY_ENDPOINT_BLOCKLIST_PATTERNS.some((pattern) => pattern.test(path))) {
    return false
  }
  return ALLOWED_PREFIXES.some((prefix) => path === prefix || path.startsWith(`${prefix}/`))
}

export const assertAllowedFoundryQaEndpoint = (method: string, urlOrPath: string) => {
  if (!isAllowedFoundryQaEndpoint(method, urlOrPath)) {
    const path = normalizeApiPath(urlOrPath)
    throw new Error(`Foundry QA API allowlist violation: ${String(method).toUpperCase()} ${path}`)
  }
}
