import { useEffect, useMemo, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Button,
  Card,
  Dialog,
  FormGroup,
  Icon,
  InputGroup,
  Menu,
  MenuDivider,
  MenuItem,
  Popover,
  Spinner,
  Text,
} from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'
import { UploadFilesDialog } from '../components/UploadFilesDialog'
import { useAppStore } from '../state/store'
import {
  createDatabase,
  deleteDatabase,
  getDatasetRawFile,
  listDatabases,
  listDatasets,
  type DatabaseRecord,
  type DatasetRecord,
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

  const description = record.description
  const nameLooksSlug = /^[a-z][a-z0-9_-]*$/.test(name)
  const descriptionLooksLikeTitle = Boolean(description && /[A-Z\s]/.test(description))
  const shouldUseDescriptionAsName = !record.display_name && !record.label && descriptionLooksLikeTitle && nameLooksSlug

  const displayName = record.display_name || record.label || (shouldUseDescriptionAsName ? description : name)
  const datasetCount =
    record.dataset_count ??
    record.datasetCount ??
    (Array.isArray(record.datasets) ? record.datasets.length : undefined)

  return {
    id: name,
    name: displayName || name,
    description: shouldUseDescriptionAsName ? undefined : description,
    updatedAt: record.updated_at || record.created_at,
    datasetCount,
    ownerId: record.owner_id,
    ownerName: record.owner_name,
    role: record.role,
    shared: record.shared,
    sharedWith: record.shared_with ?? record.sharedWith,
  }
}

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
  },
  {
    id: 'aip-logic',
    label: 'AIP Logic',
    description: 'Build composable no-code functions for transformations.',
    icon: 'flow-branch',
    category: 'Application development',
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

const parseTimestamp = (value?: string) => {
  if (!value) {
    return 0
  }
  const parsed = Date.parse(value)
  return Number.isNaN(parsed) ? 0 : parsed
}

const RECENTS_STORAGE_PREFIX = 'spice.recents.projects'
const MAX_RECENTS = 20

const normalizeRecentsPayload = (payload: unknown): Record<string, string> => {
  if (!payload || typeof payload !== 'object') {
    return {}
  }
  if (Array.isArray(payload)) {
    return payload.reduce<Record<string, string>>((acc, item) => {
      if (!item || typeof item !== 'object') {
        return acc
      }
      const entry = item as { id?: unknown; lastViewed?: unknown }
      const id = typeof entry.id === 'string' ? entry.id.trim() : ''
      const lastViewed = typeof entry.lastViewed === 'string' ? entry.lastViewed : ''
      if (id && lastViewed) {
        acc[id] = lastViewed
      }
      return acc
    }, {})
  }
  return Object.entries(payload).reduce<Record<string, string>>((acc, [id, ts]) => {
    if (typeof ts === 'string' && ts) {
      acc[id] = ts
    }
    return acc
  }, {})
}

const loadRecents = (storageKey: string): Record<string, string> => {
  if (typeof window === 'undefined') {
    return {}
  }
  try {
    const raw = window.localStorage.getItem(storageKey)
    if (!raw) {
      return {}
    }
    return normalizeRecentsPayload(JSON.parse(raw))
  } catch {
    return {}
  }
}

const saveRecents = (storageKey: string, recents: Record<string, string>) => {
  if (typeof window === 'undefined') {
    return
  }
  try {
    window.localStorage.setItem(storageKey, JSON.stringify(recents))
  } catch {
    // Ignore storage errors (quota/private mode).
  }
}

const trimRecents = (recents: Record<string, string>, limit: number) => {
  const entries = Object.entries(recents).filter(([, ts]) => Boolean(ts))
  entries.sort((left, right) => parseTimestamp(right[1]) - parseTimestamp(left[1]))
  return Object.fromEntries(entries.slice(0, limit))
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

const buildProjectId = (value: string) => {
  let id = value.trim().toLowerCase()
  if (!id) {
    return ''
  }
  id = id.replace(/[^a-z0-9_-]+/g, '_')
  id = id.replace(/[_-]{2,}/g, '_')
  id = id.replace(/^[_-]+/, '').replace(/[_-]+$/, '')
  if (!id) {
    return ''
  }
  if (!/^[a-z]/.test(id)) {
    id = `db_${id}`
  }
  if (id.length < 3) {
    id = `${id}_db`
  }
  if (id.length > 50) {
    id = id.slice(0, 50)
  }
  id = id.replace(/[_-]{2,}/g, '_')
  id = id.replace(/[_-]+$/, '')
  if (id.length < 3) {
    id = id.padEnd(3, 'a')
  }
  return id
}

const isValidProjectId = (value: string) => {
  if (!value || value.length < 3 || value.length > 50) {
    return false
  }
  if (!/^[a-z][a-z0-9_-]*$/.test(value)) {
    return false
  }
  if (/[_-]{2,}/.test(value)) {
    return false
  }
  if (/[_-]$/.test(value)) {
    return false
  }
  return true
}

export const DatasetsPage = () => {
  const queryClient = useQueryClient()
  const databasesQuery = useQuery({ queryKey: ['databases'], queryFn: listDatabases })
  const setActiveNav = useAppStore((state) => state.setActiveNav)
  const setPipelineContext = useAppStore((state) => state.setPipelineContext)
  const setAiAgentOpen = useAppStore((state) => state.setAiAgentOpen)
  const currentUserId = (import.meta.env.VITE_USER_ID as string | undefined) ?? 'system'
  const recentsStorageKey = `${RECENTS_STORAGE_PREFIX}.${currentUserId || 'system'}`
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
  const [sortKey, setSortKey] = useState<SortKey>('name')
  const [viewMode, setViewMode] = useState<ViewMode>('grid')
  const [activeProjectTab, setActiveProjectTab] = useState<ProjectTab>('projects')
  const [favoriteFolderIds, setFavoriteFolderIds] = useState<string[]>([])
  const [recentProjects, setRecentProjects] = useState<Record<string, string>>(() =>
    loadRecents(recentsStorageKey),
  )
  const [showAllProjects, setShowAllProjects] = useState(false)
  const [filesSearchQuery, setFilesSearchQuery] = useState('')
  const trimmedFolderName = newFolderName.trim()
  const normalizedProjectId = buildProjectId(trimmedFolderName)
  const projectIdIsValid = Boolean(trimmedFolderName) && isValidProjectId(normalizedProjectId)
  const projectNameIssue = trimmedFolderName && !projectIdIsValid
    ? 'Project IDs must start with a lowercase letter and use only letters, numbers, underscores, or hyphens (3-50 chars).'
    : null
  const projectNameHelper =
    trimmedFolderName && normalizedProjectId
      ? `Project ID will be "${normalizedProjectId}".`
      : 'Use a short name (lowercase letters, numbers, underscores, hyphens).'

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
  const activeDataset = useMemo(
    () => (datasetsQuery.data ?? []).find((dataset) => dataset.dataset_id === activeFileId) ?? null,
    [datasetsQuery.data, activeFileId],
  )

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

  useEffect(() => {
    setRecentProjects(loadRecents(recentsStorageKey))
  }, [recentsStorageKey])

  useEffect(() => {
    saveRecents(recentsStorageKey, recentProjects)
  }, [recentProjects, recentsStorageKey])

  useEffect(() => {
    if (!activeFolderId || folders.length === 0) {
      return
    }
    const exists = folders.some((folder) => folder.id === activeFolderId)
    if (!exists) {
      return
    }
    setRecentProjects((current) =>
      trimRecents(
        {
          ...current,
          [activeFolderId]: new Date().toISOString(),
        },
        MAX_RECENTS,
      ),
    )
  }, [activeFolderId, folders])

  useEffect(() => {
    if (folders.length === 0) {
      return
    }
    const validIds = new Set(folders.map((folder) => folder.id))
    setRecentProjects((current) => {
      const next = Object.fromEntries(Object.entries(current).filter(([id]) => validIds.has(id)))
      return Object.keys(next).length === Object.keys(current).length ? current : next
    })
  }, [folders])

  const activeFolder = folders.find((folder) => folder.id === activeFolderId) ?? null
  const activeFile = files.find((file) => file.id === activeFileId) ?? null
  const rawFileQuery = useQuery({
    queryKey: ['dataset-raw-file', activeFolderId, activeFileId],
    queryFn: () =>
      getDatasetRawFile({
        dbName: activeFolder?.id ?? '',
        datasetId: activeFileId ?? '',
        fileName:
          activeDataset && String(activeDataset.source_type || '').toLowerCase() === 'media'
            ? activeDataset.source_ref || undefined
            : undefined,
      }),
    enabled: Boolean(activeFolder && activeFileId),
  })
  const rawFile = rawFileQuery.data ?? null
  const rawFileError = rawFileQuery.error instanceof Error ? rawFileQuery.error.message : null
  const rawFileSizeLabel =
    rawFile && rawFile.size_bytes !== undefined ? formatFileSize(rawFile.size_bytes) : '-'
  const rawFileEncodingLabel = rawFile
    ? rawFile.encoding === 'base64'
      ? 'base64 (binary)'
      : 'utf-8'
    : '-'
  const rawFileContentType = rawFile?.content_type || '-'
  const rawFileFilename = rawFile?.filename || activeFile?.name || '-'
  const rawFileUri = rawFile?.s3_uri || '-'
  const canDeleteProject =
    !!activeFolder && (activeFolder.role?.toLowerCase() === 'owner' || activeFolder.ownerId === currentUserId)
  const isRootView = !activeFolder
  const isFilesView = !!activeFolder && !activeFile
  const isDatasetView = !!activeFile
  const isCreateFolderDisabled = !projectIdIsValid
  const normalizedSearchQuery = filesSearchQuery.trim().toLowerCase()
  const emptyProjectsMessage = normalizedSearchQuery
    ? 'No matching projects'
    : activeProjectTab === 'recents' && folders.length > 0
      ? 'No recent projects yet'
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

  const getFolderUpdatedAt = (folder: FolderRecord) => recentProjects[folder.id] || folder.updatedAt || ''

  const getFolderLastViewedLabel = (folder: FolderRecord) => {
    const lastViewed = getFolderUpdatedAt(folder)
    return lastViewed ? formatRelativeTime(lastViewed) : '-'
  }

  const sortedFolders = [...folders].sort((left, right) => {
    if (activeProjectTab === 'recents') {
      const leftViewed = parseTimestamp(getFolderUpdatedAt(left))
      const rightViewed = parseTimestamp(getFolderUpdatedAt(right))
      if (leftViewed !== rightViewed) {
        return rightViewed - leftViewed
      }
    }
    if (sortKey === 'size') {
      return getFolderSize(right) - getFolderSize(left)
    }
    if (sortKey === 'updated') {
      return parseTimestamp(getFolderUpdatedAt(right)) - parseTimestamp(getFolderUpdatedAt(left))
    }
    return left.name.localeCompare(right.name)
  })

  const hasProjectAccess = (folder: FolderRecord) => {
    if (!folder.ownerId || folder.ownerId === currentUserId) {
      return true
    }
    if (folder.role) {
      return true
    }
    if (folder.shared) {
      return true
    }
    if (folder.sharedWith?.includes(currentUserId)) {
      return true
    }
    return false
  }

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
      return hasProjectAccess(folder)
    }
    if (activeProjectTab === 'recents') {
      return Boolean(recentProjects[folder.id])
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

  const handleCloseUploadDialog = () => {
    setUploadOpen(false)
  }

  const handleOpenUploadDialog = () => {
    setUploadOpen(true)
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
    setCreateFolderError(null)
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
        setRecentProjects((current) => {
          const { [activeFolder.id]: _removed, ...rest } = current
          return rest
        })
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
    const projectId = buildProjectId(trimmedName)
    if (!isValidProjectId(projectId)) {
      setCreateFolderError(
        'Project IDs must start with a lowercase letter and use only letters, numbers, underscores, or hyphens (3-50 chars).',
      )
      return
    }
    const description = projectId === trimmedName ? undefined : trimmedName

    const create = async () => {
      try {
        setCreatingFolder(true)
        setCreateFolderError(null)
        await createDatabase(projectId, description)
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
    if (optionId === 'aip-agent') {
      setCreateMenuOpen(false)
      setAiAgentOpen(true)
      return
    }
    if (optionId === 'aip-logic') {
      setCreateMenuOpen(false)
      setActiveNav('workshop')
      return
    }
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
                      {visibleFolders.map((folder) => {
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
                            <span className="files-table-col updated">{getFolderLastViewedLabel(folder)}</span>
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
      ) : (
        activeFile && (
          <section className="files-detail">
            <div className="files-detail-grid">
              <Card className="card files-detail-card files-detail-summary">
                <Text className="files-detail-title">{activeFile.datasetName}</Text>
                <div className="files-detail-meta">
                  <span className="files-detail-meta-label">File</span>
                  <span className="files-detail-meta-value">{activeFile.name}</span>
                  <span className="files-detail-meta-label">Source</span>
                  <span className="files-detail-meta-value">{activeFile.source}</span>
                  <span className="files-detail-meta-label">Rows</span>
                  <span className="files-detail-meta-value">{formatRowCount(activeDataset?.row_count)}</span>
                  <span className="files-detail-meta-label">Size</span>
                  <span className="files-detail-meta-value">{rawFileSizeLabel}</span>
                  <span className="files-detail-meta-label">Updated</span>
                  <span className="files-detail-meta-value">
                    {activeFile.updatedLabel || activeFile.updatedAt || '-'}
                  </span>
                </div>
              </Card>
              <Card className="card files-detail-card files-detail-raw">
                <div className="files-detail-raw-header">
                  <div>
                    <Text className="files-detail-title">Raw file content</Text>
                    <Text className="files-detail-subtitle">{rawFileFilename}</Text>
                  </div>
                </div>
                <div className="files-detail-meta files-detail-meta-raw">
                  <span className="files-detail-meta-label">Content type</span>
                  <span className="files-detail-meta-value">{rawFileContentType}</span>
                  <span className="files-detail-meta-label">Encoding</span>
                  <span className="files-detail-meta-value">{rawFileEncodingLabel}</span>
                  <span className="files-detail-meta-label">Storage</span>
                  <span className="files-detail-meta-value">{rawFileUri}</span>
                </div>
                <div className="files-detail-raw-body">
                  {rawFileQuery.isLoading ? (
                    <div className="files-detail-loading">
                      <Spinner size={24} />
                    </div>
                  ) : rawFileError ? (
                    <Text className="card-meta">{rawFileError}</Text>
                  ) : rawFile ? (
                    <pre className="raw-file-content">{rawFile.content}</pre>
                  ) : (
                    <Text className="card-meta">No raw file content available.</Text>
                  )}
                </div>
              </Card>
            </div>
          </section>
        )
      )}
      <UploadFilesDialog
        isOpen={isUploadOpen}
        onClose={handleCloseUploadDialog}
        activeFolderId={activeFolderId}
      />
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
                onChange={(event) => {
                  setNewFolderName(event.currentTarget.value)
                  setCreateFolderError(null)
                }}
              />
            </FormGroup>
            <Text className="card-meta">{projectNameHelper}</Text>
            {(projectNameIssue || createFolderError) ? (
              <Text className="upload-error">{projectNameIssue || createFolderError}</Text>
            ) : null}
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
