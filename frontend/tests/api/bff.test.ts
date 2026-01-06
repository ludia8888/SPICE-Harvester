import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const makeResponse = (payload: unknown, ok = true) => ({
  ok,
  statusText: 'Bad Request',
  json: () => Promise.resolve(payload),
})

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

  it('builds urls from the base and attaches auth headers', async () => {
    const { listDatabases } = await loadModule({
      VITE_API_BASE_URL: 'http://example.com/api/v1',
      VITE_BFF_TOKEN: 'token',
      VITE_USER_ID: 'user-1',
      VITE_USER_NAME: 'User One',
    })

    const fetchMock = global.fetch as ReturnType<typeof vi.fn>
    fetchMock.mockResolvedValue(makeResponse({ status: 'success', data: { databases: [] } }))

    await listDatabases()

    expect(fetchMock).toHaveBeenCalledWith(
      'http://example.com/api/v1/databases',
      expect.objectContaining({
        headers: expect.any(Headers),
      }),
    )

    const headers = fetchMock.mock.calls[0][1]?.headers as Headers
    expect(headers.get('Authorization')).toBe('Bearer token')
    expect(headers.get('X-User-ID')).toBe('user-1')
    expect(headers.get('X-User-Name')).toBe('User One')
  })

  it('throws when requests fail', async () => {
    const { deleteDatabase } = await loadModule()
    const fetchMock = global.fetch as ReturnType<typeof vi.fn>
    fetchMock.mockResolvedValue(makeResponse({ message: 'nope' }, false))

    await expect(deleteDatabase('core')).rejects.toThrow('nope')
  })

  it('uploads datasets and injects idempotency keys', async () => {
    vi.stubGlobal('crypto', { randomUUID: () => 'uuid-123' })
    const { uploadDataset } = await loadModule()
    const fetchMock = global.fetch as ReturnType<typeof vi.fn>
    fetchMock.mockResolvedValue(
      makeResponse({
        status: 'success',
        data: {
          dataset: { dataset_id: 'ds-1', name: 'orders' },
          ingest_request_id: 'ingest-1',
          schema_status: 'PENDING',
        },
      }),
    )

    const file = new File(['id,name\n1,A'], 'orders.csv', { type: 'text/csv' })
    const result = await uploadDataset({ dbName: 'core', file, mode: 'structured' })

    expect(result.dataset.dataset_id).toBe('ds-1')
    expect(result.schema_status).toBe('PENDING')
    const headers = fetchMock.mock.calls[0][1]?.headers as Headers
    expect(headers.get('Idempotency-Key')).toBe('uuid-123')
  })

  it('supports media uploads', async () => {
    const { uploadDataset } = await loadModule()
    const fetchMock = global.fetch as ReturnType<typeof vi.fn>
    fetchMock.mockResolvedValue(
      makeResponse({
        status: 'success',
        data: { dataset: { dataset_id: 'media-1', name: 'files' } },
      }),
    )

    const file = new File(['blob'], 'image.png', { type: 'image/png' })
    await uploadDataset({ dbName: 'core', file, mode: 'media' })

    expect(fetchMock.mock.calls[0][0]).toContain('/api/v1/pipelines/datasets/media-upload')
  })

  it('approves dataset schema with project headers', async () => {
    const { approveDatasetSchema } = await loadModule()
    const fetchMock = global.fetch as ReturnType<typeof vi.fn>
    fetchMock.mockResolvedValue(
      makeResponse({
        status: 'success',
        data: {
          dataset: { dataset_id: 'ds-1', name: 'orders' },
          ingest_request: { ingest_request_id: 'ingest-1', schema_status: 'APPROVED' },
        },
      }),
    )

    await approveDatasetSchema({ ingestRequestId: 'ingest-1', dbName: 'core' })

    expect(fetchMock.mock.calls[0][0]).toContain('/api/v1/pipelines/datasets/ingest-requests/ingest-1/schema/approve')
    const headers = fetchMock.mock.calls[0][1]?.headers as Headers
    expect(headers.get('X-DB-Name')).toBe('core')
    expect(headers.get('X-Project')).toBe('core')
  })

  it('calls dataset and pipeline endpoints with project headers', async () => {
    const {
      listDatasets,
      listPipelines,
      listPipelineArtifacts,
      getPipelineReadiness,
      getPipeline,
      updatePipeline,
      submitPipelineProposal,
      deployPipeline,
      createPipeline,
      createDatabase,
    } = await loadModule()

    const fetchMock = global.fetch as ReturnType<typeof vi.fn>
    fetchMock
      .mockResolvedValueOnce(makeResponse({ status: 'success', data: { datasets: [] } }))
      .mockResolvedValueOnce(makeResponse({ status: 'success', data: { pipelines: [] } }))
      .mockResolvedValueOnce(makeResponse({ status: 'success', data: { artifacts: [] } }))
      .mockResolvedValueOnce(makeResponse({ status: 'success', data: { status: 'READY', inputs: [] } }))
      .mockResolvedValueOnce(makeResponse({ status: 'success', data: { pipeline: { pipeline_id: 'p-1' } } }))
      .mockResolvedValueOnce(makeResponse({ status: 'success', data: { pipeline: { pipeline_id: 'p-1' } } }))
      .mockResolvedValueOnce(makeResponse({ status: 'success', data: { proposal: { id: 'prop-1' } } }))
      .mockResolvedValueOnce(makeResponse({ status: 'success', data: { status: 'ok' } }))
      .mockResolvedValueOnce(makeResponse({ status: 'success', data: { pipeline: { pipeline_id: 'p-2' } } }))
      .mockResolvedValueOnce(makeResponse({ status: 'success', data: { name: 'core' } }))

    await listDatasets('core')
    await listPipelines('core')
    await listPipelineArtifacts('pipe-1', { mode: 'build', limit: 10, dbName: 'core' })
    await getPipelineReadiness('pipe-1', { branch: 'main', dbName: 'core' })
    await getPipeline('pipe-1', { branch: 'main', previewNodeId: 'node-1', dbName: 'core' })
    await updatePipeline('pipe-1', { definition_json: { nodes: [] }, dbName: 'core' })
    await submitPipelineProposal('pipe-1', { title: 'Proposal', dbName: 'core' })
    await deployPipeline('pipe-1', { promoteBuild: true, dbName: 'core' })
    await createPipeline({ dbName: 'core', name: 'pipeline', pipelineType: 'batch' })
    await createDatabase('core')

    const headers = fetchMock.mock.calls[0][1]?.headers as Headers
    expect(headers.get('X-DB-Name')).toBe('core')
    expect(headers.get('X-Project')).toBe('core')
  })

  it('rejects unsupported structured uploads', async () => {
    const { uploadDataset } = await loadModule()
    const file = new File(['data'], 'notes.txt', { type: 'text/plain' })
    await expect(uploadDataset({ dbName: 'core', file, mode: 'structured' })).rejects.toThrow(
      'Only .csv or .xlsx files are supported for structured uploads.',
    )
  })
})
