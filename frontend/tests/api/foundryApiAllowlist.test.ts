import { describe, expect, it } from 'vitest'
import { LEGACY_ENDPOINT_BLOCKLIST_PATTERNS } from '../../src/api/legacyEndpointBlocklist'
import { isAllowedFoundryQaEndpoint } from '../e2e/foundryApiAllowlist'

describe('foundry API allowlist', () => {
  it('disallows removed branch management endpoints', () => {
    expect(isAllowedFoundryQaEndpoint('GET', '/api/v1/databases/core/branches')).toBe(false)
    expect(isAllowedFoundryQaEndpoint('POST', '/api/v1/databases/core/branches')).toBe(false)
    expect(isAllowedFoundryQaEndpoint('DELETE', '/api/v1/databases/core/branches/feature-a')).toBe(false)
  })

  it('allows active QA endpoints', () => {
    expect(isAllowedFoundryQaEndpoint('GET', '/api/v1/summary?db=core&branch=main')).toBe(true)
    expect(isAllowedFoundryQaEndpoint('GET', '/api/v1/admin/system-health')).toBe(true)
    expect(isAllowedFoundryQaEndpoint('POST', '/api/v2/datasets')).toBe(true)
    expect(isAllowedFoundryQaEndpoint('GET', '/api/v2/ontologies/core/actionTypes')).toBe(true)
  })

  it('uses centralized legacy blocklist patterns', () => {
    expect(LEGACY_ENDPOINT_BLOCKLIST_PATTERNS.length).toBeGreaterThanOrEqual(10)

    const blockedSamples = [
      '/api/v1/databases/core/merge/simulate',
      '/api/v1/databases/core/suggest-mappings-from-google-sheets',
      '/api/v1/databases/core/import-from-google-sheets/dry-run',
      '/api/v1/databases/core/suggest-schema-from-google-sheets',
      '/api/v1/databases/core/suggest-mappings-from-excel',
      '/api/v1/databases/core/import-from-excel/dry-run',
      '/api/v1/databases/core/suggest-schema-from-data',
      '/api/v1/databases/core/branches/main',
      '/api/v1/pipelines/datasets/media-upload',
      '/api/v1/pipelines/datasets/ingest-requests/ing-1/schema/approve',
      '/api/v1/agent/pipeline-runs',
      '/api/v1/pipeline-plans/plan-1/preview',
      '/api/v1/pipelines/udfs',
    ]

    for (const sample of blockedSamples) {
      expect(LEGACY_ENDPOINT_BLOCKLIST_PATTERNS.some((pattern) => pattern.test(sample))).toBe(true)
      expect(isAllowedFoundryQaEndpoint('POST', sample)).toBe(false)
    }
  })
})
