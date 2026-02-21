import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, screen, waitFor } from '@testing-library/react'
import { OntologyPage } from '../../src/pages/OntologyPage'
import { renderWithClient, resetAppStore } from '../testUtils'
import { useAppStore } from '../../src/store/useAppStore'

const apiMocks = vi.hoisted(() => ({
  createObjectTypeV2: vi.fn(),
  getObjectTypeFullMetadataV2: vi.fn(),
  getOntology: vi.fn(),
  getSummary: vi.fn(),
  listOntology: vi.fn(),
  updateObjectTypeV2: vi.fn(),
}))

const registerCommandMock = vi.hoisted(() => vi.fn())
const showAppToastMock = vi.hoisted(() => vi.fn())
const toastApiErrorMock = vi.hoisted(() => vi.fn())

vi.mock('../../src/api/bff', () => ({
  createObjectTypeV2: apiMocks.createObjectTypeV2,
  getObjectTypeFullMetadataV2: apiMocks.getObjectTypeFullMetadataV2,
  getOntology: apiMocks.getOntology,
  getSummary: apiMocks.getSummary,
  listOntology: apiMocks.listOntology,
  updateObjectTypeV2: apiMocks.updateObjectTypeV2,
}))

vi.mock('../../src/api/useRequestContext', () => ({
  useRequestContext: () => ({
    language: 'en',
    adminToken: 'admin-token',
    adminActor: 'qa-bot',
  }),
}))

vi.mock('../../src/commands/useCommandRegistration', () => ({
  useCommandRegistration: () => registerCommandMock,
}))

vi.mock('../../src/app/AppToaster', () => ({
  showAppToast: showAppToastMock,
}))

vi.mock('../../src/errors/toastApiError', () => ({
  toastApiError: toastApiErrorMock,
}))

describe('OntologyPage', () => {
  beforeEach(() => {
    resetAppStore()
    useAppStore.setState({
      context: { project: 'core', branch: 'main', language: 'en' },
      adminToken: 'admin-token',
      adminMode: true,
    })
    apiMocks.listOntology.mockReset()
    apiMocks.getSummary.mockReset()
    apiMocks.getOntology.mockReset()
    apiMocks.getObjectTypeFullMetadataV2.mockReset()
    apiMocks.createObjectTypeV2.mockReset()
    apiMocks.updateObjectTypeV2.mockReset()

    apiMocks.listOntology.mockResolvedValue({
      ontologies: [{ id: 'Order', label: 'Order', description: 'Order object' }],
    })
    apiMocks.getSummary.mockResolvedValue({
      data: {
        policy: {
          is_protected_branch: false,
        },
      },
    })
    apiMocks.getOntology.mockResolvedValue({
      id: 'Order',
      label: 'Order',
      properties: [{ name: 'order_id', type: 'xsd:string', required: true, primaryKey: true }],
      metadata: {},
    })
    apiMocks.getObjectTypeFullMetadataV2.mockResolvedValue({
      properties: {
        order_id: {
          dataType: { type: 'STRING' },
        },
      },
    })
    apiMocks.createObjectTypeV2.mockResolvedValue({ command_id: 'cmd-create-1' })
    apiMocks.updateObjectTypeV2.mockResolvedValue({ command_id: 'cmd-update-1' })
    registerCommandMock.mockReset()
    showAppToastMock.mockReset()
    toastApiErrorMock.mockReset()
  })

  it('loads ontology class list and class detail via bff contract', async () => {
    renderWithClient(<OntologyPage dbName="core" />)

    const classButton = await screen.findByRole('button', { name: 'Order' })
    fireEvent.click(classButton)

    await waitFor(() => {
      expect(apiMocks.getOntology).toHaveBeenCalledWith(expect.any(Object), 'core', 'Order', 'main')
    })
    expect(screen.getByDisplayValue('Order')).toBeInTheDocument()
  })

  it('applies selected object type update through v2 contract', async () => {
    renderWithClient(<OntologyPage dbName="core" />)

    const classButton = await screen.findByRole('button', { name: 'Order' })
    fireEvent.click(classButton)

    await waitFor(() => {
      expect(screen.getByDisplayValue('Order')).toBeInTheDocument()
      expect(screen.getByDisplayValue(/"id": "Order"/)).toBeInTheDocument()
    })

    const applyButton = screen.getByRole('button', { name: 'Apply' })
    fireEvent.click(applyButton)

    await waitFor(() => {
      expect(apiMocks.updateObjectTypeV2).toHaveBeenCalled()
    })
    expect(apiMocks.updateObjectTypeV2).toHaveBeenCalledWith(
      expect.any(Object),
      'core',
      'Order',
      expect.objectContaining({
        status: 'ACTIVE',
        primaryKey: 'order_id',
      }),
      { branch: 'main' },
    )
    expect(registerCommandMock).toHaveBeenCalledWith(
      expect.objectContaining({
        commandId: 'cmd-update-1',
        kind: 'UPDATE_ONTOLOGY',
        targetClassId: 'Order',
      }),
    )
  })

  it('blocks apply early when backing metadata is present without datasetVersionId', async () => {
    renderWithClient(<OntologyPage dbName="core" />)

    const classButton = await screen.findByRole('button', { name: 'Order' })
    fireEvent.click(classButton)

    const payload = {
      id: 'Order',
      label: 'Order',
      properties: [{ name: 'order_id', type: 'xsd:string', required: true, primaryKey: true }],
      metadata: {},
      backingDatasetId: 'dataset-orders',
    }

    await waitFor(() => {
      expect(screen.getByDisplayValue(/"id": "Order"/)).toBeInTheDocument()
    })

    const editor = screen
      .getAllByRole('textbox')
      .find((element) => element.tagName === 'TEXTAREA') as HTMLTextAreaElement | undefined
    expect(editor).toBeDefined()
    fireEvent.change(editor!, { target: { value: JSON.stringify(payload, null, 2) } })

    const applyButton = screen.getByRole('button', { name: 'Apply' })
    fireEvent.click(applyButton)

    await waitFor(() => {
      expect(toastApiErrorMock).toHaveBeenCalled()
    })
    expect(apiMocks.updateObjectTypeV2).not.toHaveBeenCalled()
    expect(toastApiErrorMock.mock.calls[0]?.[0]).toEqual(
      expect.objectContaining({
        message: 'datasetVersionId is required when mapping/backing source metadata is provided.',
      }),
    )
  })
})
