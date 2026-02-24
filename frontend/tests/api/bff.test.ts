import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const makeJsonResponse = (
  payload: unknown,
  options?: {
    ok?: boolean
    status?: number
    statusText?: string
    headers?: Record<string, string>
  },
) => ({
  ok: options?.ok ?? true,
  status: options?.status ?? 200,
  statusText: options?.statusText ?? 'OK',
  headers: new Headers(options?.headers),
  json: () => Promise.resolve(payload),
})

const defaultContext = {
  language: 'ko' as const,
  adminToken: 'admin-token',
  adminActor: 'qa-bot',
}

const loadModule = async (env?: Record<string, string | undefined>) => {
  vi.resetModules()
  if (env) {
    Object.entries(env).forEach(([key, value]) => {
      if (value === undefined) {
        vi.unstubEnv(key)
      } else {
        vi.stubEnv(key, value)
      }
    })
  }
  return await import('../../src/api/bff')
}

describe('bff api helpers', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn())
  })

  afterEach(() => {
    vi.unstubAllEnvs()
    vi.unstubAllGlobals()
    vi.resetAllMocks()
  })

  it('listDatabasesCtx uses RequestContext headers and lang query', async () => {
    const { listDatabasesCtx } = await loadModule({
      VITE_API_BASE_URL: 'http://example.com/api/v1',
    })

    const fetchMock = global.fetch as ReturnType<typeof vi.fn>
    fetchMock.mockResolvedValue(
      makeJsonResponse({
        data: {
          databases: [{ name: 'core' }],
        },
      }),
    )

    const result = await listDatabasesCtx(defaultContext)
    expect(result).toEqual([{ name: 'core', created_at: null }])

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit]
    expect(url).toBe('http://example.com/api/v1/databases?lang=ko')
    const headers = init.headers as Headers
    expect(headers.get('Accept-Language')).toBe('ko')
    expect(headers.get('X-Admin-Token')).toBe('admin-token')
    expect(headers.get('X-Admin-Actor')).toBe('qa-bot')
  })

  it('createDatasetV2 hits v2 endpoint and sends project headers', async () => {
    const { createDatasetV2 } = await loadModule({
      VITE_API_BASE_URL: 'http://example.com/api/v1',
    })

    const fetchMock = global.fetch as ReturnType<typeof vi.fn>
    fetchMock.mockResolvedValue(
      makeJsonResponse({
        rid: 'ri.dataset.orders',
        name: 'orders',
      }),
    )

    await createDatasetV2(defaultContext, { dbName: 'core', name: 'orders' })

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit]
    expect(url).toBe('http://example.com/api/v2/datasets?lang=ko')
    const headers = init.headers as Headers
    expect(headers.get('X-DB-Name')).toBe('core')
    expect(headers.get('X-Project')).toBe('core')
    expect(headers.get('Content-Type')).toBe('application/json')
    expect(JSON.parse(String(init.body))).toEqual({
      name: 'orders',
      parentFolderRid: 'ri.foundry.main.folder.core',
    })
  })

  it('listConnectionsV2 keeps preview mode and clamps pageSize', async () => {
    const { listConnectionsV2 } = await loadModule({
      VITE_API_BASE_URL: 'http://example.com/api/v1',
    })

    const fetchMock = global.fetch as ReturnType<typeof vi.fn>
    fetchMock.mockResolvedValue(makeJsonResponse({ data: [] }))

    await listConnectionsV2(defaultContext, { dbName: 'core', pageSize: 2000 })

    const [url] = fetchMock.mock.calls[0] as [string]
    const parsed = new URL(url)
    expect(parsed.origin).toBe('http://example.com')
    expect(parsed.pathname).toBe('/api/v2/connectivity/connections')
    expect(parsed.searchParams.get('preview')).toBe('true')
    expect(parsed.searchParams.get('pageSize')).toBe('500')
    expect(parsed.searchParams.get('lang')).toBe('ko')
  })

  it('listActionTypesV2 normalizes nested actionTypes payload', async () => {
    const { listActionTypesV2 } = await loadModule({
      VITE_API_BASE_URL: 'http://example.com/api/v1',
    })

    const fetchMock = global.fetch as ReturnType<typeof vi.fn>
    fetchMock.mockResolvedValue(
      makeJsonResponse({
        data: {
          actionTypes: [{ apiName: 'HoldOrder', displayName: 'Hold Order' }],
          nextPageToken: 'token-1',
        },
      }),
    )

    const result = await listActionTypesV2(defaultContext, 'core')
    expect(result).toEqual({
      data: [{ apiName: 'HoldOrder', displayName: 'Hold Order' }],
      nextPageToken: 'token-1',
    })
  })

  it('applyActionV2 normalizes audit and writeback evidence fields', async () => {
    const { applyActionV2 } = await loadModule({
      VITE_API_BASE_URL: 'http://example.com/api/v1',
    })

    const fetchMock = global.fetch as ReturnType<typeof vi.fn>
    fetchMock.mockResolvedValue(
      makeJsonResponse({
        data: {
          command_id: 'cmd-123',
          sideEffectDelivery: { provider: 'webhook', status: 'delivered' },
        },
      }),
    )

    const response = await applyActionV2(
      defaultContext,
      'core',
      'HoldOrder',
      { parameters: { orderId: 'o-1' } },
      { branch: 'main' },
    )

    expect(response.auditLogId).toBe('cmd-123')
    expect(response.action_log_id).toBe('cmd-123')
    expect(response.writebackStatus).toBe('confirmed')
    expect(response.sideEffectDelivery).toEqual({ provider: 'webhook', status: 'delivered' })
  })

  it('access policy helpers use v1 contract and parse envelope', async () => {
    const { upsertAccessPolicyV1, listAccessPoliciesV1 } = await loadModule({
      VITE_API_BASE_URL: 'http://example.com/api/v1',
    })

    const fetchMock = global.fetch as ReturnType<typeof vi.fn>
    fetchMock
      .mockResolvedValueOnce(
        makeJsonResponse({
          status: 'success',
          data: {
            access_policy: {
              policy_id: 'policy-1',
              db_name: 'core',
              subject_type: 'role',
              subject_id: 'ops',
            },
          },
        }),
      )
      .mockResolvedValueOnce(
        makeJsonResponse({
          status: 'success',
          data: {
            access_policies: [{ policy_id: 'policy-1', status: 'active' }],
          },
        }),
      )

    const upserted = await upsertAccessPolicyV1(defaultContext, {
      db_name: 'core',
      scope: 'project',
      subject_type: 'role',
      subject_id: 'ops',
      policy: { allow: ['action.apply'] },
    })
    expect(upserted.policy_id).toBe('policy-1')

    const listed = await listAccessPoliciesV1(defaultContext, {
      db_name: 'core',
      subject_type: 'role',
      subject_id: 'ops',
    })
    expect(listed).toEqual([{ policy_id: 'policy-1', status: 'active' }])

    const firstCallUrl = new URL((fetchMock.mock.calls[0] as [string])[0])
    expect(firstCallUrl.pathname).toBe('/api/v1/access-policies')
    expect(firstCallUrl.searchParams.get('lang')).toBe('ko')
    const secondCallUrl = new URL((fetchMock.mock.calls[1] as [string])[0])
    expect(secondCallUrl.searchParams.get('db_name')).toBe('core')
    expect(secondCallUrl.searchParams.get('subject_type')).toBe('role')
    expect(secondCallUrl.searchParams.get('subject_id')).toBe('ops')
  })

  it('throws HttpError with retryAfter and opens settings on 503', async () => {
    const { listDatabasesCtx, HttpError } = await loadModule({
      VITE_API_BASE_URL: 'http://example.com/api/v1',
    })
    const { useAppStore } = await import('../../src/store/useAppStore')

    const originalSetSettingsOpen = useAppStore.getState().setSettingsOpen
    const setSettingsOpenSpy = vi.fn()
    ;(useAppStore as unknown as { setState: (next: Partial<unknown>) => void }).setState({
      setSettingsOpen: setSettingsOpenSpy,
    })

    const fetchMock = global.fetch as ReturnType<typeof vi.fn>
    fetchMock.mockResolvedValue(
      makeJsonResponse(
        { detail: 'service unavailable' },
        {
          ok: false,
          status: 503,
          statusText: 'Service Unavailable',
          headers: { 'Retry-After': '120' },
        },
      ),
    )

    try {
      await listDatabasesCtx(defaultContext)
      throw new Error('expected error')
    } catch (error) {
      expect(error).toBeInstanceOf(HttpError)
      const httpError = error as InstanceType<typeof HttpError>
      expect(httpError.status).toBe(503)
      expect(httpError.retryAfter).toBe(120)
    } finally {
      ;(useAppStore as unknown as { setState: (next: Partial<unknown>) => void }).setState({
        setSettingsOpen: originalSetSettingsOpen,
      })
    }

    expect(setSettingsOpenSpy).toHaveBeenCalledWith(true)
  })
})
