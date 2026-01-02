import { useEffect, useMemo, useRef, useState } from 'react'
import type { ChangeEvent, DragEvent } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Button,
  Card,
  Dialog,
  FormGroup,
  H3,
  Icon,
  InputGroup,
  Menu,
  MenuDivider,
  MenuItem,
  Popover,
  Radio,
  RadioGroup,
  Spinner,
  Text,
} from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'
import { useAppStore } from '../state/store'
import {
  createDatabase,
  deleteDatabase,
  listDatabases,
  listDatasets,
  uploadDataset,
  type DatabaseRecord,
  type DatasetRecord,
  type UploadMode,
} from '../api/bff'

type DatasetFile = {
  id: string
  name: string
  datasetName: string
  source: string
  updatedAt: string
  updatedLabel: string
  sizeBytes: number
}

type FolderRecord = {
  id: string
  name: string
  description?: string
  updatedAt?: string
  datasetCount?: number
  ownerId?: string
  ownerName?: string
  role?: string
  shared?: boolean
  sharedWith?: string[]
}

type UploadFile = {
  id: string
  file: File
  status: UploadStatus
  datasetName?: string
  error?: string
}

type UploadStatus = 'queued' | 'active' | 'complete' | 'error'

type SortKey = 'name' | 'updated' | 'size'
type ViewMode = 'grid' | 'list'
type ProjectTab = 'projects' | 'recents' | 'favorites'

type CreateOption = {
  id: 'folder' | 'weblink' | 'upload' | 'pipeline' | 'aip-agent' | 'aip-logic'
  label: string
  description: string
  icon: IconName
  category: CreateCategory
  disabled?: boolean
}

const createCategories = [
  'All',
  'Analytics & Operations',
  'Application development',
  'Data integration',
  'Models',
  'Ontology',
  'Security & governance',
  'Support',
] as const

type CreateCategory = (typeof createCategories)[number]

const normalizeDatabaseRecord = (record: DatabaseRecord | string): FolderRecord | null => {
  if (typeof record === 'string') {
    return { id: record, name: record }
  }

  const name = record.name || record.db_name || record.id
  if (!name) {
    return null
  }

  const datasetCount =
    record.dataset_count ??
    record.datasetCount ??
    (Array.isArray(record.datasets) ? record.datasets.length : undefined)

  return {
    id: name,
    name: record.display_name || record.label || name,
    description: record.description,
    updatedAt: record.updated_at || record.created_at,
    datasetCount,
    ownerId: record.owner_id,
    ownerName: record.owner_name,
    role: record.role,
    shared: record.shared,
    sharedWith: record.shared_with ?? record.sharedWith,
  }
}

const uploadOptions: Array<{ value: UploadMode; title: string; description: string }> = [
  {
    value: 'structured',
    title: 'Upload as individual structured datasets (recommended)',
    description:
      'Datasets are the most basic representation of tabular data. They can be used and transformed by many applications.',
  },
  {
    value: 'media',
    title: 'Upload to a new media set',
    description:
      'Media sets enable media-specific capabilities for audio, imagery, video, and documents.',
  },
  {
    value: 'unstructured',
    title: 'Upload to a new unstructured dataset',
    description:
      'Unstructured datasets can store arbitrary files for processing and analysis.',
  },
  {
    value: 'raw',
    title: 'Upload as individual raw files',
    description: 'Raw files cannot be used in data pipelines, analyses, or models.',
  },
]

const createOptions: CreateOption[] = [
  {
    id: 'folder',
    label: 'Project (Root only)',
    description: '',
    icon: 'folder-new',
    category: 'All',
  },
  {
    id: 'weblink',
    label: 'Web link',
    description: 'Save a link to an external website.',
    icon: 'link',
    category: 'Data integration',
    disabled: true,
  },
  {
    id: 'upload',
    label: 'Upload files...',
    description: 'Upload files directly from your computer.',
    icon: 'upload',
    category: 'Data integration',
  },
  {
    id: 'pipeline',
    label: 'Pipeline Builder',
    description: 'Build and automate data pipeline workflows.',
    icon: 'flow-branch',
    category: 'Data integration',
  },
  {
    id: 'aip-agent',
    label: 'AIP Agent',
    description: 'Build no-code interactive assistants with enterprise tools.',
    icon: 'user',
    category: 'Application development',
    disabled: true,
  },
  {
    id: 'aip-logic',
    label: 'AIP Logic',
    description: 'Build composable no-code functions for transformations.',
    icon: 'flow-branch',
    category: 'Application development',
    disabled: true,
  },
]

const formatFileSize = (bytes: number) => {
  if (bytes < 1024) {
    return `${bytes} B`
  }

  const kiloBytes = bytes / 1024
  if (kiloBytes < 1024) {
    return `${kiloBytes.toFixed(2)} KB`
  }

  const megaBytes = kiloBytes / 1024
  if (megaBytes < 1024) {
    return `${megaBytes.toFixed(2)} MB`
  }

  const gigaBytes = megaBytes / 1024
  return `${gigaBytes.toFixed(2)} GB`
}

const formatRelativeTime = (isoDate?: string) => {
  if (!isoDate) {
    return ''
  }
  const date = new Date(isoDate)
  if (Number.isNaN(date.getTime())) {
    return isoDate
  }
  const diffMs = Date.now() - date.getTime()
  const diffMinutes = Math.floor(diffMs / 60000)
  if (diffMinutes < 1) {
    return 'just now'
  }
  if (diffMinutes < 60) {
    return `${diffMinutes} minute${diffMinutes === 1 ? '' : 's'} ago`
  }
  const diffHours = Math.floor(diffMinutes / 60)
  if (diffHours < 24) {
    return `${diffHours} hour${diffHours === 1 ? '' : 's'} ago`
  }
  const diffDays = Math.floor(diffHours / 24)
  if (diffDays < 7) {
    return `${diffDays} day${diffDays === 1 ? '' : 's'} ago`
  }
  return date.toLocaleDateString()
}

const formatShortDate = (isoDate?: string) => {
  if (!isoDate) {
    return '-'
  }
  const date = new Date(isoDate)
  if (Number.isNaN(date.getTime())) {
    return isoDate
  }
  return date.toLocaleDateString()
}

const formatRowCount = (value?: number) => {
  if (value === undefined || value === null) {
    return '-'
  }
  return value.toLocaleString()
}

const getResourceName = (fileName: string) => fileName.replace(/\.[^/.]+$/, '')

const parseTimestamp = (value?: string) => {
  if (!value) {
    return 0
  }
  const parsed = Date.parse(value)
  return Number.isNaN(parsed) ? 0 : parsed
}

const mapDatasetToFile = (dataset: DatasetRecord): DatasetFile => ({
  id: dataset.dataset_id,
  name: dataset.name,
  datasetName: dataset.name,
  source: dataset.source_type,
  updatedAt: dataset.updated_at || dataset.created_at || '',
  updatedLabel: formatRelativeTime(dataset.updated_at || dataset.created_at),
  sizeBytes: dataset.row_count ?? 0,
})

type BreadcrumbSegment = {
  label: string
  icon: IconName
  onClick?: () => void
}

export const DatasetsPage = () => {
  const queryClient = useQueryClient()
  const databasesQuery = useQuery({ queryKey: ['databases'], queryFn: listDatabases })
  const setActiveNav = useAppStore((state) => state.setActiveNav)
  const setPipelineContext = useAppStore((state) => state.setPipelineContext)
  const currentUserId = (import.meta.env.VITE_USER_ID as string | undefined) ?? 'system'
  const [activeFolderId, setActiveFolderId] = useState<string | null>(null)
  const [activeFileId, setActiveFileId] = useState<string | null>(null)
  const [isCreateFolderOpen, setCreateFolderOpen] = useState(false)
  const [newFolderName, setNewFolderName] = useState('')
  const [createFolderError, setCreateFolderError] = useState<string | null>(null)
  const [isCreatingFolder, setCreatingFolder] = useState(false)
  const [isDeleteProjectOpen, setDeleteProjectOpen] = useState(false)
  const [isDeletingProject, setDeletingProject] = useState(false)
  const [deleteProjectError, setDeleteProjectError] = useState<string | null>(null)
  const [isCreateMenuOpen, setCreateMenuOpen] = useState(false)
  const [createMenuCategory, setCreateMenuCategory] = useState<CreateCategory>('All')
  const [createMenuSearch, setCreateMenuSearch] = useState('')
  const [isUploadOpen, setUploadOpen] = useState(false)
  const [uploadMode, setUploadMode] = useState<UploadMode>(uploadOptions[0].value)
  const [isUploading, setUploading] = useState(false)
  const [isUploadComplete, setUploadComplete] = useState(false)
  const [uploadError, setUploadError] = useState<string | null>(null)
  const [sortKey, setSortKey] = useState<SortKey>('name')
  const [viewMode, setViewMode] = useState<ViewMode>('grid')
  const [activeProjectTab, setActiveProjectTab] = useState<ProjectTab>('projects')
  const [favoriteFolderIds, setFavoriteFolderIds] = useState<string[]>([])
  const [showAllProjects, setShowAllProjects] = useState(false)
  const [filesSearchQuery, setFilesSearchQuery] = useState('')
  const [uploadFiles, setUploadFiles] = useState<UploadFile[]>([])
  const [isDragActive, setDragActive] = useState(false)
  const fileInputRef = useRef<HTMLInputElement | null>(null)

  const datasetsQuery = useQuery({
    queryKey: ['datasets', activeFolderId],
    queryFn: () => listDatasets(activeFolderId ?? ''),
    enabled: Boolean(activeFolderId),
  })

  const folders = useMemo(
    () => (databasesQuery.data ?? []).map(normalizeDatabaseRecord).filter(Boolean) as FolderRecord[],
    [databasesQuery.data],
  )

  const files = useMemo(() => (datasetsQuery.data ?? []).map(mapDatasetToFile), [datasetsQuery.data])

  const databaseErrorMessage = databasesQuery.error instanceof Error ? databasesQuery.error.message : null
  const datasetErrorMessage = datasetsQuery.error instanceof Error ? datasetsQuery.error.message : null

  useEffect(() => {
    if (!activeFolderId) {
      setActiveFileId(null)
      return
    }
    if (files.length > 0 && activeFileId) {
      const exists = files.some((file) => file.id === activeFileId)
      if (!exists) {
        setActiveFileId(null)
      }
    }
  }, [activeFileId, activeFolderId, files])

  useEffect(() => {
    if (activeFolderId && folders.length > 0) {
      const exists = folders.some((folder) => folder.id === activeFolderId)
      if (!exists) {
        setActiveFolderId(null)
        setActiveFileId(null)
      }
    }
  }, [activeFolderId, folders])

  const activeFolder = folders.find((folder) => folder.id === activeFolderId) ?? null
  const activeFile = files.find((file) => file.id === activeFileId) ?? null
  const canDeleteProject =
    !!activeFolder && (activeFolder.role?.toLowerCase() === 'owner' || activeFolder.ownerId === currentUserId)
  const isRootView = !activeFolder
  const isFilesView = !!activeFolder && !activeFile
  const isDatasetView = !!activeFile
  const isCreateFolderDisabled = newFolderName.trim().length === 0
  const totalUploads = uploadFiles.length
  const completedCount = uploadFiles.filter((file) => file.status === 'complete' || file.status === 'error').length
  const successfulUploads = uploadFiles.filter((file) => file.status === 'complete')
  const failedUploads = uploadFiles.filter((file) => file.status === 'error')
  const uploadStepLabel = totalUploads === 0 ? '0/0' : `${completedCount}/${totalUploads}`
  const uploadProgressPercent = totalUploads === 0 ? 0 : (completedCount / totalUploads) * 100
  const uploadProgressText = `Uploading files... (${uploadStepLabel})`
  const resourceLabel = successfulUploads.length === 1 ? 'resource' : 'resources'
  const uploadCompleteMessage = successfulUploads.length
    ? `Successfully created ${successfulUploads.length} ${resourceLabel}`
    : 'No resources were created'
  const uploadFailureMessage = failedUploads.length
    ? `${failedUploads.length} file${failedUploads.length === 1 ? '' : 's'} failed to upload.`
    : null
  const uploadDialogTitle = isUploading
    ? 'Uploading...'
    : isUploadComplete
      ? 'Upload finished'
      : 'Upload files'
  const uploadDialogIcon = isUploading ? 'upload' : isUploadComplete ? 'tick-circle' : 'upload'
  const normalizedSearchQuery = filesSearchQuery.trim().toLowerCase()
  const emptyProjectsMessage = normalizedSearchQuery
    ? 'No matching projects'
    : activeProjectTab === 'favorites' && folders.length > 0
      ? 'No favorites yet'
      : 'No projects yet'

  const filteredCreateOptions = createOptions.filter((option) => {
    if (!isRootView && option.id === 'folder') {
      return false
    }
    const matchesCategory = createMenuCategory === 'All' || option.category === createMenuCategory
    const matchesSearch = option.label.toLowerCase().includes(createMenuSearch.toLowerCase())
    return matchesCategory && matchesSearch
  })

  const getFolderSize = (folder: FolderRecord) => folder.datasetCount ?? 0

  const getFolderUpdatedAt = (folder: FolderRecord) => folder.updatedAt ?? ''

  const sortedFolders = [...folders].sort((left, right) => {
    if (sortKey === 'size') {
      return getFolderSize(right) - getFolderSize(left)
    }
    if (sortKey === 'updated') {
      return parseTimestamp(getFolderUpdatedAt(right)) - parseTimestamp(getFolderUpdatedAt(left))
    }
    return left.name.localeCompare(right.name)
  })

  const visibleFolders = sortedFolders.filter((folder) => {
    if (normalizedSearchQuery) {
      const haystack = [
        folder.name,
        folder.description,
        folder.ownerName,
        folder.ownerId,
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
      if (!haystack.includes(normalizedSearchQuery)) {
        return false
      }
    }
    if (activeProjectTab === 'favorites') {
      return favoriteFolderIds.includes(folder.id)
    }
    if (activeProjectTab === 'projects') {
      if (showAllProjects) {
        return true
      }
      if (!folder.ownerId) {
        return true
      }
      return folder.ownerId === currentUserId
    }
    return true
  })

  const sortedFiles = [...files].sort((left, right) => {
    if (sortKey === 'size') {
      return right.sizeBytes - left.sizeBytes
    }
    if (sortKey === 'updated') {
      return parseTimestamp(right.updatedAt) - parseTimestamp(left.updatedAt)
    }
    return left.name.localeCompare(right.name)
  })

  const handleToggleFavoriteFolder = (folderId: string) => {
    setFavoriteFolderIds((current) => {
      const isFavorite = current.includes(folderId)
      return isFavorite ? current.filter((id) => id !== folderId) : [...current, folderId]
    })
  }

  const getUploadStatus = (uploadFile: UploadFile) => uploadFile.status

  const addUploadFiles = (incomingFiles: FileList | File[]) => {
    const normalized = Array.from(incomingFiles)
    if (normalized.length === 0) {
      return
    }

    setUploadFiles((current) => {
      const existingIds = new Set(current.map((file) => file.id))
      const additions = normalized
        .map((file) => ({
          id: `${file.name}-${file.size}-${file.lastModified}`,
          file,
          status: 'queued' as const,
        }))
        .filter((entry) => !existingIds.has(entry.id))
      return [...current, ...additions]
    })
  }

  const handleFileInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    if (isUploading) {
      return
    }
    if (event.currentTarget.files) {
      addUploadFiles(event.currentTarget.files)
    }
    event.currentTarget.value = ''
  }

  const handleDrop = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault()
    if (isUploading) {
      setDragActive(false)
      return
    }
    setDragActive(false)
    addUploadFiles(event.dataTransfer.files)
  }

  const handleDragOver = (event: DragEvent<HTMLDivElement>) => {
    if (isUploading) {
      return
    }
    event.preventDefault()
    setDragActive(true)
  }

  const handleDragLeave = () => {
    setDragActive(false)
  }

  const handleOpenFilePicker = () => {
    if (isUploading) {
      return
    }
    fileInputRef.current?.click()
  }

  const handleRemoveUploadFile = (id: string) => {
    if (isUploading) {
      return
    }
    setUploadFiles((current) => current.filter((file) => file.id !== id))
  }

  const handleCloseUploadDialog = () => {
    setUploadOpen(false)
    setDragActive(false)
    setUploading(false)
    setUploadComplete(false)
    setUploadError(null)
    setUploadFiles([])
  }

  const handleOpenUploadDialog = () => {
    setUploadOpen(true)
    setUploading(false)
    setUploadComplete(false)
    setUploadError(null)
    setUploadFiles([])
  }

  const handleOpenCreateMenu = () => {
    setCreateMenuSearch('')
    setCreateMenuCategory('All')
    setCreateMenuOpen(true)
  }

  const handleCloseCreateMenu = () => {
    setCreateMenuOpen(false)
  }

  const handleOpenCreateFolder = () => {
    setNewFolderName('')
    setCreateFolderOpen(true)
  }

  const handleCloseCreateFolder = () => {
    setCreateFolderOpen(false)
    setNewFolderName('')
    setCreateFolderError(null)
  }

  const handleOpenDeleteProject = () => {
    setDeleteProjectError(null)
    setDeleteProjectOpen(true)
  }

  const handleCloseDeleteProject = () => {
    if (isDeletingProject) {
      return
    }
    setDeleteProjectOpen(false)
    setDeleteProjectError(null)
  }

  const handleConfirmDeleteProject = () => {
    if (!activeFolder) {
      return
    }

    const runDelete = async () => {
      try {
        setDeletingProject(true)
        setDeleteProjectError(null)
        await deleteDatabase(activeFolder.id)
        setFavoriteFolderIds((current) => current.filter((id) => id !== activeFolder.id))
        setActiveFolderId(null)
        setActiveFileId(null)
        await queryClient.invalidateQueries({ queryKey: ['databases'] })
        handleCloseDeleteProject()
      } catch (error) {
        setDeleteProjectError(error instanceof Error ? error.message : 'Failed to delete project')
      } finally {
        setDeletingProject(false)
      }
    }

    void runDelete()
  }

  const handleCreateFolder = () => {
    const trimmedName = newFolderName.trim()
    if (!trimmedName) {
      return
    }

    const create = async () => {
      try {
        setCreatingFolder(true)
        setCreateFolderError(null)
        await createDatabase(trimmedName)
        await queryClient.invalidateQueries({ queryKey: ['databases'] })
        handleCloseCreateFolder()
        setCreateFolderError(null)
      } catch (error) {
        setCreateFolderError(error instanceof Error ? error.message : 'Failed to create project')
      } finally {
        setCreatingFolder(false)
      }
    }

    void create()
  }

  const handleSelectCreateOption = (optionId: CreateOption['id']) => {
    if (optionId === 'folder') {
      setCreateMenuOpen(false)
      handleOpenCreateFolder()
      return
    }
    if (optionId === 'upload') {
      setCreateMenuOpen(false)
      handleOpenUploadDialog()
      return
    }
    if (optionId === 'pipeline') {
      if (activeFolder) {
        setPipelineContext({
          folderId: activeFolder.id,
          folderName: activeFolder.name,
        })
      } else {
        setPipelineContext(null)
      }
      setCreateMenuOpen(false)
      setActiveNav('pipeline')
      return
    }
  }

  const handleStartUpload = () => {
    if (uploadFiles.length === 0) {
      return
    }
    if (!activeFolder) {
      setUploadError('Select a project before uploading.')
      return
    }

    const runUpload = async () => {
      setDragActive(false)
      setUploadError(null)
      setUploading(true)

      const results = await Promise.all(
        uploadFiles.map(async (uploadFile) => {
          setUploadFiles((current) =>
            current.map((item) =>
              item.id === uploadFile.id ? { ...item, status: 'active' } : item,
            ),
          )
          try {
            const dataset = await uploadDataset({
              dbName: activeFolder.id,
              file: uploadFile.file,
              mode: uploadMode,
            })
            setUploadFiles((current) =>
              current.map((item) =>
                item.id === uploadFile.id
                  ? { ...item, status: 'complete', datasetName: dataset.name }
                  : item,
              ),
            )
            return dataset
          } catch (error) {
            setUploadFiles((current) =>
              current.map((item) =>
                item.id === uploadFile.id
                  ? {
                      ...item,
                      status: 'error',
                      error: error instanceof Error ? error.message : 'Upload failed',
                    }
                  : item,
              ),
            )
            return null
          }
        }),
      )

      setUploading(false)
      setUploadComplete(true)
      if (results.some((dataset) => dataset)) {
        await queryClient.invalidateQueries({ queryKey: ['datasets', activeFolder.id] })
      }
    }

    void runUpload()
  }

  const actionsMenu = (
    <Menu>
      <MenuItem icon="sort" text="Sort by">
        <MenuItem
          icon={sortKey === 'name' ? 'small-tick' : 'blank'}
          text="Name"
          onClick={() => setSortKey('name')}
        />
        <MenuItem
          icon={sortKey === 'updated' ? 'small-tick' : 'blank'}
          text="Updated"
          onClick={() => setSortKey('updated')}
        />
        <MenuItem
          icon={sortKey === 'size' ? 'small-tick' : 'blank'}
          text="Size"
          onClick={() => setSortKey('size')}
        />
      </MenuItem>
      <MenuItem
        icon="refresh"
        text="Refresh"
        onClick={() => {
          void databasesQuery.refetch()
          if (activeFolderId) {
            void datasetsQuery.refetch()
          }
        }}
        disabled={databasesQuery.isFetching || datasetsQuery.isFetching}
      />
      <MenuItem icon="layout" text="View">
        <MenuItem
          icon={viewMode === 'grid' ? 'small-tick' : 'blank'}
          text="Grid"
          onClick={() => setViewMode('grid')}
        />
        <MenuItem
          icon={viewMode === 'list' ? 'small-tick' : 'blank'}
          text="List"
          onClick={() => setViewMode('list')}
        />
      </MenuItem>
      {activeFolder ? (
        <>
          <MenuDivider />
          <MenuItem
            icon="trash"
            text="Delete project"
            intent="danger"
            onClick={handleOpenDeleteProject}
            disabled={!canDeleteProject}
          />
        </>
      ) : null}
    </Menu>
  )

  const renderActionsPopover = () => (
    <Popover content={actionsMenu} placement="bottom-end">
      <Button minimal rightIcon="caret-down" text="Actions" />
    </Popover>
  )

  const breadcrumbSegments: BreadcrumbSegment[] = [
    {
      label: 'Root',
      icon: 'root-folder' as IconName,
      onClick: () => {
        setActiveFolderId(null)
        setActiveFileId(null)
      },
    },
    ...(activeFolder
      ? [
          {
            label: activeFolder.name,
            icon: 'folder-close' as IconName,
            onClick: () => setActiveFileId(null),
          },
        ]
      : []),
    ...(activeFile
      ? [
          {
            label: activeFile.name,
            icon: 'document' as IconName,
          },
        ]
      : []),
  ]

  return (
    <div className="page files-page">
      <div className="page-topbar">
        {breadcrumbSegments.map((segment, index) => {
          const isLast = index === breadcrumbSegments.length - 1
          const content = (
            <>
              <Icon icon={segment.icon} className="breadcrumb-icon" />
              <span className="breadcrumb-label">{segment.label}</span>
            </>
          )

          return (
            <span key={segment.label} className="breadcrumb-item">
              {segment.onClick && !isLast ? (
                <button type="button" className="breadcrumb-link" onClick={segment.onClick}>
                  {content}
                </button>
              ) : (
                <span className="breadcrumb-current">{content}</span>
              )}
              {!isLast ? <Icon icon="chevron-right" className="breadcrumb-separator" /> : null}
            </span>
          )
        })}
      </div>
      {isRootView ? (
        <section className="files-explore">
          <div className="files-explore-header">
            <Text className="files-explore-title">Explore all projects</Text>
            <div className="files-explore-search">
              <InputGroup
                leftIcon="search"
                placeholder="Search..."
                value={filesSearchQuery}
                onChange={(event) => setFilesSearchQuery(event.target.value)}
              />
            </div>
          </div>
          <div className="files-explore-tabs">
            <div className="files-explore-tab-group">
              <button
                type="button"
                className={`files-explore-tab ${activeProjectTab === 'projects' ? 'is-active' : ''}`}
                onClick={() => setActiveProjectTab('projects')}
              >
                Your projects
              </button>
              <button
                type="button"
                className={`files-explore-tab ${activeProjectTab === 'recents' ? 'is-active' : ''}`}
                onClick={() => setActiveProjectTab('recents')}
              >
                Recents
              </button>
              <button
                type="button"
                className={`files-explore-tab ${activeProjectTab === 'favorites' ? 'is-active' : ''}`}
                onClick={() => setActiveProjectTab('favorites')}
              >
                Favorites
              </button>
            </div>
            <div className="files-explore-actions">
              <button
                type="button"
                className="files-explore-link"
                onClick={() => setShowAllProjects((current) => !current)}
              >
                {showAllProjects ? 'Your projects' : 'All projects'}
              </button>
              <Button icon="add" intent="success" text="New project" small onClick={handleOpenCreateFolder} />
            </div>
          </div>
        </section>
      ) : null}
      {isDatasetView ? <H3>{activeFile?.datasetName ?? 'Dataset'}</H3> : null}
      {databasesQuery.isLoading ? <Spinner size={24} /> : null}
      {isRootView ? (
        <section className="files-panel files-projects">
          <div className="files-panel-body">
            {databaseErrorMessage ? (
              <div className="files-empty">
                <Icon icon="error" size={32} className="files-empty-icon" />
                <Text className="files-empty-title">{databaseErrorMessage}</Text>
              </div>
            ) : visibleFolders.length === 0 ? (
              <div className="files-empty">
                <Icon icon="folder-close" size={40} className="files-empty-icon" />
                <Text className="files-empty-title">{emptyProjectsMessage}</Text>
              </div>
            ) : (
              <>
                <div className="files-table-frame">
                  <div className="files-table-header">
                    <span className="files-table-col name">Projects</span>
                    <span className="files-table-col portfolio">Portfolio</span>
                    <span className="files-table-col role">Role</span>
                    <span className="files-table-col owner">Created by</span>
                    <span className="files-table-col updated">Last viewed</span>
                  </div>
                  <div className="files-table-scroll">
                    <div className="files-table">
                      {visibleFolders.map((folder, index) => {
                        const isFavorite = favoriteFolderIds.includes(folder.id)
                        return (
                          <div
                            key={folder.id}
                            role="button"
                            tabIndex={0}
                            className="files-table-row"
                            onClick={() => {
                              setActiveFolderId(folder.id)
                              setActiveFileId(null)
                            }}
                            onKeyDown={(event) => {
                              if (event.key === 'Enter' || event.key === ' ') {
                                event.preventDefault()
                                setActiveFolderId(folder.id)
                                setActiveFileId(null)
                              }
                            }}
                          >
                            <span className="files-table-col name">
                              <Icon icon="folder-close" size={14} className="files-table-icon" />
                              <span className="files-table-name-stack">
                                <span className="files-table-title-row">
                                  <span className="files-table-title">{folder.name}</span>
                                  <button
                                    type="button"
                                    className={`files-table-favorite-button ${isFavorite ? 'is-active' : ''}`}
                                    aria-pressed={isFavorite}
                                    aria-label={isFavorite ? 'Remove from favorites' : 'Add to favorites'}
                                    onClick={(event) => {
                                      event.stopPropagation()
                                      handleToggleFavoriteFolder(folder.id)
                                    }}
                                  >
                                    <Icon icon={isFavorite ? 'star' : 'star-empty'} size={12} />
                                  </button>
                                </span>
                                <span className="files-table-meta">
                                  {folder.description || `${folder.datasetCount ?? 0} files`}
                                </span>
                              </span>
                            </span>
                            <span className="files-table-col portfolio">Default</span>
                            <span className="files-table-col role">
                              {folder.role || (folder.ownerId === currentUserId ? 'Owner' : 'Viewer')}
                            </span>
                            <span className="files-table-col owner">
                              {folder.ownerName || folder.ownerId || 'Unknown'}
                            </span>
                            <span className="files-table-col updated">{index === 0 ? 'Today' : 'Last week'}</span>
                          </div>
                        )
                      })}
                    </div>
                  </div>
                </div>
              </>
            )}
          </div>
        </section>
      ) : isFilesView ? (
        <section className="files-panel">
          <header className="files-panel-header">
            <Text className="files-panel-title">Files</Text>
            <div className="files-panel-actions">
              {renderActionsPopover()}
              <Button icon="add" intent="success" rightIcon="caret-down" text="New" onClick={handleOpenCreateMenu} />
            </div>
          </header>
          <div className="files-panel-body">
            {datasetsQuery.isFetching ? (
              <div className="files-empty">
                <Spinner size={24} />
              </div>
            ) : datasetErrorMessage ? (
              <div className="files-empty">
                <Icon icon="error" size={32} className="files-empty-icon" />
                <Text className="files-empty-title">{datasetErrorMessage}</Text>
              </div>
            ) : files.length === 0 ? (
              <div className="files-empty">
                <Icon icon="folder-open" size={40} className="files-empty-icon" />
                <Text className="files-empty-title">This project is empty</Text>
              </div>
            ) : (
              <>
                <div className="files-table-frame">
                  <div className="files-table-header">
                    <span className="files-table-col name">Files</span>
                    <span className="files-table-col portfolio">Source</span>
                    <span className="files-table-col role">Rows</span>
                    <span className="files-table-col owner">Updated</span>
                    <span className="files-table-col updated">Last viewed</span>
                  </div>
                  <div className="files-table-scroll">
                    <div className="files-table">
                      {sortedFiles.map((file, index) => (
                        <div
                          key={file.id}
                          role="button"
                          tabIndex={0}
                          className="files-table-row"
                          onClick={() => setActiveFileId(file.id)}
                          onKeyDown={(event) => {
                            if (event.key === 'Enter' || event.key === ' ') {
                              event.preventDefault()
                              setActiveFileId(file.id)
                            }
                          }}
                        >
                          <span className="files-table-col name">
                            <Icon icon="document" size={14} className="files-table-icon" />
                            <span className="files-table-name-stack">
                              <span className="files-table-title-row">
                                <span className="files-table-title">{file.name}</span>
                                <Icon icon="star-empty" size={12} className="files-table-favorite-icon" />
                              </span>
                              <span className="files-table-meta">Source: {file.source}</span>
                            </span>
                          </span>
                          <span className="files-table-col portfolio">{file.source || 'manual'}</span>
                          <span className="files-table-col role">{formatRowCount(file.sizeBytes)}</span>
                          <span className="files-table-col owner">{formatShortDate(file.updatedAt)}</span>
                          <span className="files-table-col updated">
                            {file.updatedLabel || (index === 0 ? 'Today' : 'Last week')}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </>
            )}
          </div>
        </section>
      ) : activeFile ? (
        <div className="grid">
          <Card className="card">
            <Text className="card-title">{activeFile.datasetName}</Text>
            <Text className="card-meta">File: {activeFile.name}</Text>
            <Text className="card-meta">Source: {activeFile.source}</Text>
            <Text className="card-meta">Updated: {activeFile.updatedLabel || activeFile.updatedAt}</Text>
          </Card>
        </div>
      ) : null}
      <Dialog
        isOpen={isUploadOpen}
        onClose={handleCloseUploadDialog}
        title={uploadDialogTitle}
        icon={uploadDialogIcon}
        className="upload-dialog bp5-dark"
      >
        <div className="upload-dialog-body">
          {isUploading ? (
            <div className="upload-progress-view">
              {uploadFiles.length > 0 ? (
                <div className="upload-file-list is-progress">
                  {uploadFiles.map((uploadFile) => {
                    const status = getUploadStatus(uploadFile)
                    return (
                      <div key={uploadFile.id} className="upload-file-row">
                        <div className="upload-file-meta">
                          <span className={`upload-status ${status}`}>
                            {status === 'complete' ? (
                              <Icon icon="tick" size={12} />
                            ) : status === 'error' ? (
                              <Icon icon="error" size={12} />
                            ) : status === 'active' ? (
                              <span className="upload-status-dot" />
                            ) : null}
                          </span>
                          <Icon icon="document" className="upload-file-icon" />
                          <Text className="upload-file-name">{uploadFile.file.name}</Text>
                        </div>
                        <div className="upload-file-actions">
                          <Text className="upload-file-size">{formatFileSize(uploadFile.file.size)}</Text>
                        </div>
                      </div>
                    )
                  })}
                </div>
              ) : null}
              <div className="upload-progress">
                <div className="upload-progress-bar">
                  <span className="upload-progress-fill" style={{ width: `${uploadProgressPercent}%` }} />
                </div>
                <Text className="upload-progress-text">{uploadProgressText}</Text>
              </div>
            </div>
          ) : isUploadComplete ? (
            <div className="upload-complete-view">
              <Text className="upload-complete-message">{uploadCompleteMessage}</Text>
              {uploadFailureMessage ? (
                <Text className="upload-complete-warning">{uploadFailureMessage}</Text>
              ) : null}
              <div className="upload-complete-list">
                {uploadFiles
                  .filter((uploadFile) => uploadFile.status === 'complete')
                  .map((uploadFile) => (
                    <div key={uploadFile.id} className="upload-complete-row">
                      <Icon icon="document" className="upload-complete-icon" />
                      <Text className="upload-complete-name">
                        {uploadFile.datasetName || getResourceName(uploadFile.file.name)}
                      </Text>
                    </div>
                  ))}
              </div>
              <div className="upload-dialog-footer">
                <Button intent="primary" text="Done" onClick={handleCloseUploadDialog} />
              </div>
            </div>
          ) : (
            <>
              <div
                className={`upload-dropzone ${isDragActive ? 'is-dragging' : ''}`}
                onClick={handleOpenFilePicker}
                onDragOver={handleDragOver}
                onDragLeave={handleDragLeave}
                onDrop={handleDrop}
              >
                <Icon icon="cloud-upload" size={40} className="upload-dropzone-icon" />
                <Text className="upload-dropzone-text">
                  Drop files here or{' '}
                  <button
                    type="button"
                    className="upload-dropzone-link"
                    onClick={(event) => {
                      event.stopPropagation()
                      handleOpenFilePicker()
                    }}
                  >
                    choose from your computer
                  </button>
                </Text>
                <input
                  ref={fileInputRef}
                  type="file"
                  multiple
                  className="upload-file-input"
                  onChange={handleFileInputChange}
                />
              </div>
              {uploadFiles.length > 0 ? (
                <div className="upload-file-list">
                  {uploadFiles.map((uploadFile) => (
                    <div key={uploadFile.id} className="upload-file-row">
                      <div className="upload-file-meta">
                        <Icon icon="document" className="upload-file-icon" />
                        <Text className="upload-file-name">{uploadFile.file.name}</Text>
                      </div>
                      <div className="upload-file-actions">
                        <Text className="upload-file-size">{formatFileSize(uploadFile.file.size)}</Text>
                        <Button
                          minimal
                          icon="small-cross"
                          aria-label={`Remove ${uploadFile.file.name}`}
                          onClick={() => handleRemoveUploadFile(uploadFile.id)}
                        />
                      </div>
                    </div>
                  ))}
                </div>
              ) : null}
              {uploadError ? (
                <Text className="upload-error">{uploadError}</Text>
              ) : null}
              <RadioGroup
                className="upload-options"
                selectedValue={uploadMode}
                onChange={(event) => setUploadMode(event.currentTarget.value as UploadMode)}
              >
                {uploadOptions.map((option) => (
                  <Radio
                    key={option.value}
                    value={option.value}
                    labelElement={
                      <div className="upload-option">
                        <Text className="upload-option-title">{option.title}</Text>
                        <Text className="upload-option-description">{option.description}</Text>
                      </div>
                    }
                  />
                ))}
              </RadioGroup>
              <div className="upload-dialog-footer">
                <Button intent="primary" text="Upload" disabled={uploadFiles.length === 0} onClick={handleStartUpload} />
              </div>
            </>
          )}
        </div>
      </Dialog>
      <Dialog
        isOpen={isCreateMenuOpen}
        onClose={handleCloseCreateMenu}
        title="Create"
        icon="add"
        className="create-dialog bp5-dark"
      >
        <div className="create-dialog-body">
          <div className="create-dialog-search">
            <InputGroup
              leftIcon="search"
              placeholder="Search for apps..."
              value={createMenuSearch}
              onChange={(event) => setCreateMenuSearch(event.currentTarget.value)}
            />
          </div>
          <div className="create-dialog-content">
            <aside className="create-dialog-sidebar">
              {createCategories.map((category) => (
                <button
                  key={category}
                  type="button"
                  className={`create-dialog-category ${createMenuCategory === category ? 'is-active' : ''}`}
                  onClick={() => setCreateMenuCategory(category)}
                >
                  {category}
                </button>
              ))}
            </aside>
            <div className="create-dialog-list">
              {filteredCreateOptions.map((option) => (
                <button
                  key={option.id}
                  type="button"
                  className="create-dialog-item"
                  onClick={() => handleSelectCreateOption(option.id)}
                  disabled={option.disabled}
                >
                  <Icon icon={option.icon} className="create-dialog-item-icon" />
                  <div className="create-dialog-item-text">
                    <Text className="create-dialog-item-title">{option.label}</Text>
                    {option.description ? (
                      <Text className="create-dialog-item-description">{option.description}</Text>
                    ) : null}
                  </div>
                </button>
              ))}
              {filteredCreateOptions.length === 0 ? (
                <Text className="create-dialog-empty">No matches</Text>
              ) : null}
            </div>
          </div>
        </div>
      </Dialog>
      <Dialog
        isOpen={isCreateFolderOpen}
        onClose={handleCloseCreateFolder}
        title="New project"
        icon="folder-new"
        className="folder-dialog bp5-dark"
      >
          <div className="folder-dialog-body">
            <FormGroup label="Project name" labelFor="folder-name-input">
              <InputGroup
                id="folder-name-input"
                placeholder="Enter project name"
                value={newFolderName}
                autoFocus
                onChange={(event) => setNewFolderName(event.currentTarget.value)}
              />
            </FormGroup>
            {createFolderError ? <Text className="upload-error">{createFolderError}</Text> : null}
            <div className="folder-dialog-footer">
              <Button minimal text="Cancel" onClick={handleCloseCreateFolder} />
              <Button
                intent="success"
                text={isCreatingFolder ? 'Creating...' : 'Create'}
                onClick={handleCreateFolder}
                disabled={isCreateFolderDisabled || isCreatingFolder}
              />
            </div>
          </div>
      </Dialog>
      <Dialog
        isOpen={isDeleteProjectOpen}
        onClose={handleCloseDeleteProject}
        title="Delete project"
        icon="trash"
        className="folder-dialog bp5-dark"
      >
        <div className="folder-dialog-body">
          <Text>
            Delete <strong>{activeFolder?.name ?? 'this project'}</strong>? This will remove the project and its
            datasets.
          </Text>
          {deleteProjectError ? <Text className="upload-error">{deleteProjectError}</Text> : null}
          <div className="folder-dialog-footer">
            <Button minimal text="Cancel" onClick={handleCloseDeleteProject} disabled={isDeletingProject} />
            <Button
              intent="danger"
              text={isDeletingProject ? 'Deleting...' : 'Delete'}
              onClick={handleConfirmDeleteProject}
              disabled={isDeletingProject || !canDeleteProject}
            />
          </div>
        </div>
      </Dialog>
    </div>
  )
}
