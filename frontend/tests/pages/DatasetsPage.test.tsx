import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { DatasetsPage } from '../../src/pages/DatasetsPage'
import { renderWithClient, resetAppStore } from '../testUtils'
import { useAppStore } from '../../src/state/store'

const apiMocks = {
  listDatabases: vi.fn(),
  listDatasets: vi.fn(),
  createDatabase: vi.fn(),
  deleteDatabase: vi.fn(),
  uploadDataset: vi.fn(),
}

vi.mock('../../src/api/bff', () => ({
  listDatabases: () => apiMocks.listDatabases(),
  listDatasets: (dbName: string) => apiMocks.listDatasets(dbName),
  createDatabase: (name: string, description?: string) => apiMocks.createDatabase(name, description),
  deleteDatabase: (name: string) => apiMocks.deleteDatabase(name),
  uploadDataset: (params: { dbName: string; file: File; mode: string }) => apiMocks.uploadDataset(params),
}))

describe('DatasetsPage', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    resetAppStore()
    apiMocks.listDatabases.mockResolvedValue([
      {
        name: 'core-db',
        display_name: 'Core Data',
        dataset_count: 2,
        owner_id: 'system',
        owner_name: 'System',
      },
    ])
    apiMocks.listDatasets.mockResolvedValue([
      {
        dataset_id: 'ds-1',
        db_name: 'core-db',
        name: 'orders',
        source_type: 'csv',
        branch: 'main',
        row_count: 120,
        created_at: '2024-01-01T10:00:00Z',
        updated_at: '2024-01-02T10:00:00Z',
      },
    ])
    apiMocks.createDatabase.mockResolvedValue({ name: 'New Project' })
  })

  it('renders projects and filters by search', async () => {
    renderWithClient(<DatasetsPage />)

    expect(await screen.findByText('Core Data')).toBeInTheDocument()

    const searchInput = screen.getByPlaceholderText('Search...')
    fireEvent.change(searchInput, { target: { value: 'missing' } })
    expect(await screen.findByText('No matching projects')).toBeInTheDocument()
  })

  it('navigates from project list to dataset detail view', async () => {
    renderWithClient(<DatasetsPage />)

    const projectRow = await screen.findByText('Core Data')
    fireEvent.click(projectRow)

    expect(await screen.findByText('Files')).toBeInTheDocument()
    expect(await screen.findByText('orders')).toBeInTheDocument()

    const fileRow = screen.getByText('orders')
    fireEvent.click(fileRow)

    expect(await screen.findByText('File: orders')).toBeInTheDocument()
    expect(screen.getByText('Source: csv')).toBeInTheDocument()
  })

  it('shows empty project state when no datasets exist', async () => {
    apiMocks.listDatasets.mockResolvedValueOnce([])

    renderWithClient(<DatasetsPage />)

    const projectRow = await screen.findByText('Core Data')
    fireEvent.click(projectRow)

    expect(await screen.findByText('This project is empty')).toBeInTheDocument()
  })

  it('supports keyboard selection for projects and files', async () => {
    renderWithClient(<DatasetsPage />)

    const projectLabel = await screen.findByText('Core Data')
    const projectRow = projectLabel.closest('.files-table-row')
    expect(projectRow).not.toBeNull()
    if (!projectRow) {
      return
    }

    fireEvent.keyDown(projectRow, { key: 'Enter' })
    expect(await screen.findByText('Files')).toBeInTheDocument()

    const fileLabel = await screen.findByText('orders')
    const fileRow = fileLabel.closest('.files-table-row')
    expect(fileRow).not.toBeNull()
    if (!fileRow) {
      return
    }

    fireEvent.keyDown(fileRow, { key: ' ' })
    expect(await screen.findByText('File: orders')).toBeInTheDocument()
  })

  it('toggles favorites and creates a project', async () => {
    renderWithClient(<DatasetsPage />)

    const favoriteButton = await screen.findByLabelText('Add to favorites')
    fireEvent.click(favoriteButton)
    expect(favoriteButton).toHaveAttribute('aria-pressed', 'true')

    const newProjectButton = screen.getByRole('button', { name: 'New project' })
    fireEvent.click(newProjectButton)

    const nameInput = await screen.findByLabelText('Project name')
    fireEvent.change(nameInput, { target: { value: 'New Project' } })

    const createButton = screen.getByRole('button', { name: 'Create' })
    fireEvent.click(createButton)

    await waitFor(() => {
      expect(apiMocks.createDatabase).toHaveBeenCalledWith('New Project', undefined)
    })
  })

  it('opens the create menu and routes to pipeline builder', async () => {
    renderWithClient(<DatasetsPage />)

    const projectRow = await screen.findByText('Core Data')
    fireEvent.click(projectRow)

    const newButton = await screen.findByRole('button', { name: 'New' })
    fireEvent.click(newButton)

    const searchInput = screen.getByPlaceholderText('Search for apps...')
    fireEvent.change(searchInput, { target: { value: 'zzz' } })
    expect(await screen.findByText('No matches')).toBeInTheDocument()
    fireEvent.change(searchInput, { target: { value: '' } })

    const pipelineOption = await screen.findByText('Pipeline Builder')
    fireEvent.click(pipelineOption)

    expect(useAppStore.getState().activeNav).toBe('pipeline')
  })

  it('uploads files and shows completion state', async () => {
    apiMocks.uploadDataset.mockResolvedValue({ dataset_id: 'ds-2', name: 'report' })
    const user = userEvent.setup()

    renderWithClient(<DatasetsPage />)

    const projectRow = await screen.findByText('Core Data')
    fireEvent.click(projectRow)

    const newButton = await screen.findByRole('button', { name: 'New' })
    fireEvent.click(newButton)

    const uploadOption = await screen.findByText('Upload files...')
    fireEvent.click(uploadOption)

    const fileInput = document.querySelector('input[type="file"]') as HTMLInputElement | null
    expect(fileInput).not.toBeNull()
    if (!fileInput) {
      return
    }
    const file = new File(['id,name\n1,A'], 'report.csv', { type: 'text/csv' })
    await user.upload(fileInput, file)

    const uploadButton = screen.getByRole('button', { name: 'Upload' })
    fireEvent.click(uploadButton)

    expect(await screen.findByText(/Upload finished/i)).toBeInTheDocument()
    expect(apiMocks.uploadDataset).toHaveBeenCalled()
  })

  it('filters projects by ownership and favorites', async () => {
    apiMocks.listDatabases.mockResolvedValueOnce([
      {
        name: 'core-db',
        display_name: 'Core Data',
        dataset_count: 2,
        owner_id: 'system',
        owner_name: 'System',
      },
      {
        name: 'ext-db',
        display_name: 'External Data',
        dataset_count: 4,
        owner_id: 'external',
        owner_name: 'Ext',
      },
    ])

    renderWithClient(<DatasetsPage />)

    expect(await screen.findByText('Core Data')).toBeInTheDocument()
    expect(screen.queryByText('External Data')).not.toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'All projects' }))
    expect(await screen.findByText('External Data')).toBeInTheDocument()
    const toggleButton = document.querySelector('.files-explore-link') as HTMLButtonElement | null
    expect(toggleButton?.textContent).toContain('Your projects')

    fireEvent.click(screen.getByRole('button', { name: 'Favorites' }))
    expect(await screen.findByText('No favorites yet')).toBeInTheDocument()

    if (toggleButton) {
      fireEvent.click(toggleButton)
    }
    const projectTab = screen
      .getAllByRole('button', { name: 'Your projects' })
      .find((button) => button.className.includes('files-explore-tab'))
    if (projectTab) {
      fireEvent.click(projectTab)
    }
    const favoriteButton = await screen.findByLabelText('Add to favorites')
    fireEvent.click(favoriteButton)
    fireEvent.click(screen.getByRole('button', { name: 'Favorites' }))
    expect(await screen.findByText('Core Data')).toBeInTheDocument()
  })

  it('shows dataset error state when loading fails', async () => {
    apiMocks.listDatasets.mockRejectedValueOnce(new Error('dataset fetch failed'))

    renderWithClient(<DatasetsPage />)

    const projectRow = await screen.findByText('Core Data')
    fireEvent.click(projectRow)

    expect(await screen.findByText('dataset fetch failed')).toBeInTheDocument()
  })

  it('shows upload progress state while files are uploading', async () => {
    let resolveUpload: (value: { dataset_id: string; name: string }) => void
    const uploadPromise = new Promise<{ dataset_id: string; name: string }>((resolve) => {
      resolveUpload = resolve
    })
    apiMocks.uploadDataset.mockReturnValue(uploadPromise)
    const user = userEvent.setup()

    renderWithClient(<DatasetsPage />)

    const projectRow = await screen.findByText('Core Data')
    fireEvent.click(projectRow)

    const newButton = await screen.findByRole('button', { name: 'New' })
    fireEvent.click(newButton)

    const uploadOption = await screen.findByText('Upload files...')
    fireEvent.click(uploadOption)

    const fileInput = document.querySelector('input[type="file"]') as HTMLInputElement | null
    expect(fileInput).not.toBeNull()
    if (!fileInput) {
      return
    }
    await user.upload(fileInput, new File(['id,name\n1,A'], 'progress.csv', { type: 'text/csv' }))

    fireEvent.click(screen.getByRole('button', { name: 'Upload' }))
    expect(await screen.findByText(/Uploading files/i)).toBeInTheDocument()
    expect(await screen.findByText('progress.csv')).toBeInTheDocument()
    expect(document.querySelector('.upload-status-dot')).not.toBeNull()

    resolveUpload!({ dataset_id: 'ds-3', name: 'progress' })
    expect(await screen.findByText(/Upload finished/i)).toBeInTheDocument()
  })

  it('shows upload failures and allows removing queued files', async () => {
    apiMocks.uploadDataset.mockRejectedValue(new Error('boom'))
    const user = userEvent.setup()

    renderWithClient(<DatasetsPage />)

    const projectRow = await screen.findByText('Core Data')
    fireEvent.click(projectRow)

    const newButton = await screen.findByRole('button', { name: 'New' })
    fireEvent.click(newButton)

    const uploadOption = await screen.findByText('Upload files...')
    fireEvent.click(uploadOption)

    const clickSpy = vi.spyOn(HTMLInputElement.prototype, 'click')
    const chooseButton = await screen.findByRole('button', { name: /choose from your computer/i })
    fireEvent.click(chooseButton)
    expect(clickSpy).toHaveBeenCalled()
    clickSpy.mockRestore()

    const fileInput = document.querySelector('input[type="file"]') as HTMLInputElement | null
    expect(fileInput).not.toBeNull()
    if (!fileInput) {
      return
    }

    const file = new File(['id,name\n1,A'], 'report.csv', { type: 'text/csv' })
    await user.upload(fileInput, file)
    expect(await screen.findByText('report.csv')).toBeInTheDocument()

    const removeButton = screen.getByLabelText('Remove report.csv')
    fireEvent.click(removeButton)
    expect(screen.queryByText('report.csv')).not.toBeInTheDocument()

    await user.upload(fileInput, file)
    const uploadButton = screen.getByRole('button', { name: 'Upload' })
    fireEvent.click(uploadButton)

    expect(await screen.findByText(/Upload finished/i)).toBeInTheDocument()
    expect(screen.getByText('No resources were created')).toBeInTheDocument()
    expect(screen.getByText('1 file failed to upload.')).toBeInTheDocument()
  })

  it('shows upload errors when project context is missing and handles create/delete errors', async () => {
    apiMocks.createDatabase.mockRejectedValueOnce(new Error('create failed'))
    apiMocks.deleteDatabase.mockResolvedValueOnce(undefined)
    const user = userEvent.setup()

    renderWithClient(<DatasetsPage />)

    const newProjectButton = await screen.findByRole('button', { name: 'New project' })
    fireEvent.click(newProjectButton)

    const nameInput = await screen.findByLabelText('Project name')
    fireEvent.change(nameInput, { target: { value: 'Bad Project' } })
    fireEvent.click(screen.getByRole('button', { name: 'Create' }))

    expect(await screen.findByText('create failed')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))

    const projectRow = await screen.findByText('Core Data')
    fireEvent.click(projectRow)

    const openUploadButton = await screen.findByRole('button', { name: 'New' })
    fireEvent.click(openUploadButton)
    const uploadOption = await screen.findByText('Upload files...')
    fireEvent.click(uploadOption)

    const fileInput = document.querySelector('input[type="file"]') as HTMLInputElement | null
    expect(fileInput).not.toBeNull()
    if (!fileInput) {
      return
    }
    const file = new File(['id,name\n1,A'], 'root.csv', { type: 'text/csv' })
    await user.upload(fileInput, file)

    fireEvent.click(screen.getByText('Root'))
    fireEvent.click(screen.getByRole('button', { name: 'Upload' }))
    expect(await screen.findByText('Select a project before uploading.')).toBeInTheDocument()

    const uploadDialog = screen.getByRole('dialog', { name: /upload/i })
    fireEvent.click(within(uploadDialog).getByLabelText('Close'))

    const projectRowAgain = await screen.findByText('Core Data')
    fireEvent.click(projectRowAgain)
    await screen.findByText('Files')
    const actionsButton = await screen.findByRole('button', { name: 'Actions' })
    fireEvent.click(actionsButton)
    fireEvent.click(screen.getByText('Delete project'))
    fireEvent.click(await screen.findByRole('button', { name: 'Delete' }))

    await waitFor(() => {
      expect(apiMocks.deleteDatabase).toHaveBeenCalledWith('core-db')
    })
  })

  it('refreshes data from the actions menu', async () => {
    renderWithClient(<DatasetsPage />)

    const projectRow = await screen.findByText('Core Data')
    fireEvent.click(projectRow)

    const actionsButton = await screen.findByRole('button', { name: 'Actions' })
    fireEvent.click(actionsButton)

    const refreshItem = await screen.findByRole('menuitem', { name: 'Refresh' })
    fireEvent.click(refreshItem)

    await waitFor(() => {
      expect(apiMocks.listDatabases).toHaveBeenCalledTimes(2)
      expect(apiMocks.listDatasets).toHaveBeenCalledTimes(2)
    })
  })
})
