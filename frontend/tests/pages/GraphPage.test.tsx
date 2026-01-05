import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, screen, waitFor } from '@testing-library/react'
import { GraphPage } from '../../src/pages/GraphPage'
import { renderWithClient, resetAppStore } from '../testUtils'
import { useAppStore } from '../../src/state/store'

const apiMocks = {
  listDatabases: vi.fn(),
  listDatasets: vi.fn(),
  listPipelines: vi.fn(),
  getPipeline: vi.fn(),
  listPipelineArtifacts: vi.fn(),
  getPipelineReadiness: vi.fn(),
  submitPipelineProposal: vi.fn(),
  updatePipeline: vi.fn(),
  deployPipeline: vi.fn(),
}

vi.mock('../../src/api/bff', () => ({
  listDatabases: () => apiMocks.listDatabases(),
  listDatasets: (dbName: string) => apiMocks.listDatasets(dbName),
  listPipelines: (dbName: string) => apiMocks.listPipelines(dbName),
  getPipeline: (pipelineId: string, params?: { dbName?: string }) => apiMocks.getPipeline(pipelineId, params),
  listPipelineArtifacts: (pipelineId: string, params?: { mode?: string; limit?: number; dbName?: string }) =>
    apiMocks.listPipelineArtifacts(pipelineId, params),
  getPipelineReadiness: (pipelineId: string, params?: { dbName?: string }) =>
    apiMocks.getPipelineReadiness(pipelineId, params),
  submitPipelineProposal: (pipelineId: string, payload: Record<string, unknown>) =>
    apiMocks.submitPipelineProposal(pipelineId, payload),
  updatePipeline: (pipelineId: string, payload: Record<string, unknown>) =>
    apiMocks.updatePipeline(pipelineId, payload),
  deployPipeline: (pipelineId: string, payload: Record<string, unknown>) =>
    apiMocks.deployPipeline(pipelineId, payload),
}))

let latestProps: Record<string, unknown> | null = null
let latestInstance: { zoomIn: () => void; zoomOut: () => void; fitView: () => void } | null = null

vi.mock('reactflow', async () => {
  const React = await import('react')
  return {
    __esModule: true,
    __getLatestProps: () => latestProps,
    __getLatestInstance: () => latestInstance,
    default: ({ onInit, ...props }: { onInit?: (instance: unknown) => void }) => {
      latestProps = props
      React.useEffect(() => {
        latestInstance = {
          zoomIn: vi.fn(),
          zoomOut: vi.fn(),
          fitView: vi.fn(),
        }
        onInit?.(latestInstance)
      }, [onInit])
      return <div data-testid="reactflow" />
    },
    addEdge: (connection: { source?: string; target?: string }, edges: Array<Record<string, unknown>>) => {
      if (!connection.source || !connection.target) {
        return edges
      }
      return [
        ...edges,
        {
          id: `${connection.source}-${connection.target}`,
          source: connection.source,
          target: connection.target,
        },
      ]
    },
    useNodesState: (initial: Array<Record<string, unknown>>) => {
      const [nodes, setNodes] = React.useState(initial)
      return [nodes, setNodes, vi.fn()]
    },
    useEdgesState: (initial: Array<Record<string, unknown>>) => {
      const [edges, setEdges] = React.useState(initial)
      return [edges, setEdges, vi.fn()]
    },
  }
})

describe('GraphPage', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    resetAppStore()
    useAppStore.setState({
      pipelineContext: { folderId: 'core-db', folderName: 'Core' },
    })
    apiMocks.listDatabases.mockResolvedValue([{ name: 'core-db', display_name: 'Core' }])
    apiMocks.listDatasets.mockResolvedValue([
      {
        dataset_id: 'ds-1',
        db_name: 'core-db',
        name: 'Orders',
        source_type: 'csv',
        branch: 'main',
        schema_json: { columns: [{ name: 'order_id', type: 'int' }] },
        sample_json: { columns: ['order_id', 'status'], rows: [{ order_id: 1, status: 'new' }] },
        row_count: 3,
      },
    ])
    apiMocks.listPipelines.mockResolvedValue([
      {
        pipeline_id: 'pipe-1',
        db_name: 'core-db',
        name: 'Orders Pipeline',
        pipeline_type: 'batch',
        branch: 'main',
        updated_at: '2024-01-10T10:00:00Z',
      },
    ])
    apiMocks.getPipeline.mockResolvedValue({
      pipeline_id: 'pipe-1',
      db_name: 'core-db',
      name: 'Orders Pipeline',
      pipeline_type: 'batch',
      branch: 'main',
      definition_json: {
        nodes: [
          { id: 'n1', type: 'input', metadata: { dataset_name: 'Orders' } },
          { id: 'n2', type: 'output', metadata: { output_dataset_name: 'Orders Output' } },
        ],
        edges: [{ from: 'n1', to: 'n2' }],
      },
    })
    apiMocks.listPipelineArtifacts.mockResolvedValue([
      {
        artifact_id: 'artifact-1',
        pipeline_id: 'pipe-1',
        job_id: 'job-1',
        mode: 'build',
        status: 'SUCCESS',
        created_at: '2024-01-11T10:00:00Z',
      },
    ])
    apiMocks.getPipelineReadiness.mockResolvedValue({
      status: 'READY',
      inputs: [],
    })
    apiMocks.submitPipelineProposal.mockResolvedValue({ ok: true })
    apiMocks.deployPipeline.mockResolvedValue({ ok: true })
    apiMocks.updatePipeline.mockResolvedValue({ pipeline: { pipeline_id: 'pipe-1' } })
    vi.spyOn(window, 'alert').mockImplementation(() => undefined)
    vi.spyOn(window, 'open').mockImplementation(() => null)
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('renders pipeline data and preview columns', async () => {
    renderWithClient(<GraphPage />)

    expect(await screen.findByText('Orders Pipeline')).toBeInTheDocument()
    expect(screen.getByTestId('reactflow')).toBeInTheDocument()
    expect(screen.getAllByText('order_id').length).toBeGreaterThan(0)
  })

  it('runs readiness checks and proposals', async () => {
    renderWithClient(<GraphPage />)

    await screen.findByText('Orders Pipeline')
    const settingsButton = screen.getByRole('button', { name: 'Settings' })
    fireEvent.click(settingsButton)

    await waitFor(() => {
      expect(apiMocks.getPipelineReadiness).toHaveBeenCalled()
      expect(window.alert).toHaveBeenCalled()
    })

    const proposeButton = screen.getByRole('button', { name: 'Propose' })
    fireEvent.click(proposeButton)
    await waitFor(() => expect(apiMocks.submitPipelineProposal).toHaveBeenCalled())

    const helpButton = screen.getByRole('button', { name: 'Help' })
    fireEvent.click(helpButton)
    expect(window.open).toHaveBeenCalled()

    const deployButton = screen.getByRole('button', { name: 'Deploy' })
    fireEvent.click(deployButton)
    await waitFor(() => expect(apiMocks.deployPipeline).toHaveBeenCalled())
  })

  it('filters preview columns and toggles panels', async () => {
    renderWithClient(<GraphPage />)

    const searchInput = await screen.findByPlaceholderText(/Search .* columns/i)
    fireEvent.change(searchInput, { target: { value: 'missing' } })
    expect(screen.getByText('No matching columns.')).toBeInTheDocument()

    fireEvent.change(searchInput, { target: { value: 'status' } })
    expect(screen.queryByText('No matching columns.')).not.toBeInTheDocument()

    const bottomPanel = document.querySelector('.pipeline-bottom-panel') as HTMLElement | null
    expect(bottomPanel?.className).toContain('is-open')

    const panelToggle = screen.getByRole('button', { name: 'Close preview' })
    fireEvent.click(panelToggle)
    expect(bottomPanel?.className).not.toContain('is-open')
  })

  it('shows empty dataset outputs and output tabs when no datasets exist', async () => {
    apiMocks.listDatasets.mockResolvedValueOnce([])
    apiMocks.listPipelines.mockResolvedValueOnce([])

    renderWithClient(<GraphPage />)

    expect(await screen.findByText('No datasets available yet.')).toBeInTheDocument()
    expect(screen.getByText('No datasets available')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Object types' }))
    expect(screen.getByText('No outputs configured yet.')).toBeInTheDocument()
  })

  it('reorders preview columns and formats values', async () => {
    apiMocks.listDatasets.mockResolvedValueOnce([
      {
        dataset_id: 'ds-1',
        db_name: 'core-db',
        name: 'Orders',
        source_type: 'csv',
        branch: 'main',
        schema_json: { columns: [{ name: 'order_id', type: 'int' }, { name: 'meta', type: 'json' }] },
        sample_json: {
          columns: ['order_id', 'meta', 'active'],
          rows: [{ order_id: 1, meta: { a: 1 }, active: true }],
        },
        row_count: 1,
      },
    ])

    renderWithClient(<GraphPage />)

    expect(await screen.findByText('Orders Pipeline')).toBeInTheDocument()
    expect(screen.getByText('true')).toBeInTheDocument()
    expect(screen.getByText('{"a":1}')).toBeInTheDocument()
    expect(screen.getAllByText('Integer').length).toBeGreaterThan(0)

    const orderButton = screen.getByRole('button', { name: 'order_id' })
    const activeButton = screen.getByRole('button', { name: 'active' })
    const dataTransfer = {
      data: {} as Record<string, string>,
      setData(type: string, value: string) {
        this.data[type] = value
      },
      getData(type: string) {
        return this.data[type]
      },
      effectAllowed: '',
      dropEffect: '',
    }

    fireEvent.dragStart(orderButton, { dataTransfer })
    fireEvent.dragEnter(activeButton)
    fireEvent.dragOver(activeButton, { dataTransfer })
    fireEvent.drop(activeButton, { dataTransfer })
    fireEvent.dragEnd(orderButton)

    const columnLabels = Array.from(document.querySelectorAll('.pipeline-preview-column-left span'))
      .map((node) => node.textContent?.trim())
      .filter((label): label is string => Boolean(label))
    expect(columnLabels).toEqual(['meta', 'active', 'order_id'])
  })

  it('saves and proposes pipeline changes after connections', async () => {
    renderWithClient(<GraphPage />)

    await waitFor(() => expect(apiMocks.getPipeline).toHaveBeenCalled())

    const reactFlowModule = await import('reactflow')
    const props = reactFlowModule.__getLatestProps() as {
      onConnect?: (payload: { source: string; target: string }) => void
    }
    await act(async () => {
      props.onConnect?.({ source: 'n1', target: 'n2' })
    })

    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Save' })).not.toBeDisabled()
    })

    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(apiMocks.updatePipeline).toHaveBeenCalled())

    await act(async () => {
      props.onConnect?.({ source: 'n2', target: 'n3' })
    })
    fireEvent.click(screen.getByRole('button', { name: 'Propose' }))
    await waitFor(() => expect(apiMocks.submitPipelineProposal).toHaveBeenCalled())
  })

  it('switches folders and alerts when readiness fails', async () => {
    apiMocks.listDatabases.mockResolvedValueOnce([
      { name: 'core-db', display_name: 'Core' },
      { name: 'secondary', display_name: 'Secondary' },
    ])
    apiMocks.getPipelineReadiness.mockRejectedValueOnce(new Error('boom'))

    renderWithClient(<GraphPage />)

    const fileButton = screen.getByRole('button', { name: 'File' })
    fireEvent.click(fileButton)
    fireEvent.click(await screen.findByText('Secondary'))

    expect(useAppStore.getState().pipelineContext?.folderId).toBe('secondary')

    const settingsButton = screen.getByRole('button', { name: 'Settings' })
    await waitFor(() => expect(settingsButton).not.toBeDisabled())
    fireEvent.click(settingsButton)
    await waitFor(() => expect(window.alert).toHaveBeenCalledWith('Failed to load pipeline readiness.'))
  })
})
