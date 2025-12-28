import { useEffect, useRef, useState } from 'react'
import type { ChangeEvent, DragEvent } from 'react'
import { useQuery } from '@tanstack/react-query'
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
  Tag,
  Text,
} from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'
import { useAppStore } from '../state/store'

type DatasetFile = {
  id: string
  name: string
  datasetName: string
  source: string
  updatedAt: string
  sizeBytes: number
}

type FolderRecord = {
  id: string
  name: string
  files: DatasetFile[]
}

type UploadFile = {
  id: string
  file: File
}

type SortKey = 'name' | 'updated' | 'size'
type ViewMode = 'grid' | 'list'

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

const fetchFolderTree = async (): Promise<FolderRecord[]> => {
  await new Promise((resolve) => setTimeout(resolve, 400))
  return [
    {
      id: 'bigdata-doctor',
      name: 'Bigdata Doctor',
      files: [
        {
          id: 'bdd-1',
          name: 'transactions.csv',
          datasetName: 'transactions',
          source: 'google_sheets',
          updatedAt: '2 hours ago',
          sizeBytes: 116570,
        },
        {
          id: 'bdd-2',
          name: 'customer_profiles.csv',
          datasetName: 'customer_profiles',
          source: 'csv_upload',
          updatedAt: 'just now',
          sizeBytes: 5710,
        },
      ],
    },
    {
      id: 'marketing-atlas',
      name: 'Marketing Atlas',
      files: [
        {
          id: 'mk-1',
          name: 'campaigns.xlsx',
          datasetName: 'campaigns',
          source: 'xlsx_import',
          updatedAt: 'yesterday',
          sizeBytes: 95700,
        },
        {
          id: 'mk-2',
          name: 'ads_performance.csv',
          datasetName: 'ads_performance',
          source: 'csv_upload',
          updatedAt: '3 days ago',
          sizeBytes: 39560,
        },
      ],
    },
  ]
}

const uploadOptions = [
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
    label: 'Folder',
    description: 'Create a new folder to organize files.',
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

const getResourceName = (fileName: string) => fileName.replace(/\.[^/.]+$/, '')

type BreadcrumbSegment = {
  label: string
  icon: IconName
  onClick?: () => void
}

export const DatasetsPage = () => {
  const { data, isLoading, isFetching, refetch } = useQuery({ queryKey: ['folders'], queryFn: fetchFolderTree })
  const setActiveNav = useAppStore((state) => state.setActiveNav)
  const [foldersState, setFoldersState] = useState<FolderRecord[] | null>(null)
  const [activeFolderId, setActiveFolderId] = useState<string | null>(null)
  const [activeFileId, setActiveFileId] = useState<string | null>(null)
  const [isCreateFolderOpen, setCreateFolderOpen] = useState(false)
  const [newFolderName, setNewFolderName] = useState('')
  const [isCreateMenuOpen, setCreateMenuOpen] = useState(false)
  const [createMenuCategory, setCreateMenuCategory] = useState<CreateCategory>('All')
  const [createMenuSearch, setCreateMenuSearch] = useState('')
  const [isUploadOpen, setUploadOpen] = useState(false)
  const [uploadMode, setUploadMode] = useState(uploadOptions[0].value)
  const [isUploading, setUploading] = useState(false)
  const [uploadProgress, setUploadProgress] = useState(0)
  const [sortKey, setSortKey] = useState<SortKey>('name')
  const [viewMode, setViewMode] = useState<ViewMode>('grid')
  const [uploadFiles, setUploadFiles] = useState<UploadFile[]>([])
  const [isDragActive, setDragActive] = useState(false)
  const fileInputRef = useRef<HTMLInputElement | null>(null)

  useEffect(() => {
    if (data && foldersState === null) {
      setFoldersState(data)
    }
  }, [data, foldersState])

  useEffect(() => {
    if (!isUploading) {
      return
    }
    if (uploadFiles.length === 0) {
      setUploading(false)
      return
    }

    const stepsPerFile = 4
    const totalSteps = uploadFiles.length * stepsPerFile
    let currentStep = 0
    setUploadProgress(0)

    const intervalId = window.setInterval(() => {
      currentStep += 1
      const progress = Math.min(currentStep / totalSteps, 1)
      setUploadProgress(progress)
      if (progress >= 1) {
        window.clearInterval(intervalId)
      }
    }, 400)

    return () => window.clearInterval(intervalId)
  }, [isUploading, uploadFiles.length])

  const folders = foldersState ?? data ?? []
  const activeFolder = folders.find((folder) => folder.id === activeFolderId) ?? null
  const files = activeFolder?.files ?? []
  const activeFile = files.find((file) => file.id === activeFileId) ?? null
  const isRootView = !activeFolder
  const isFilesView = !!activeFolder && !activeFile
  const isDatasetView = !!activeFile
  const isCreateFolderDisabled = newFolderName.trim().length === 0
  const totalUploads = uploadFiles.length
  const completedCount = totalUploads === 0 ? 0 : Math.floor(uploadProgress * totalUploads)
  const isUploadComplete = uploadProgress >= 1 && totalUploads > 0
  const activeUploadIndex = isUploading && !isUploadComplete ? completedCount : null
  const uploadStepLabel = totalUploads === 0
    ? '0/0'
    : `${Math.min(completedCount + 1, totalUploads)}/${totalUploads}`
  const uploadProgressPercent = Math.min(uploadProgress * 100, 100)
  const uploadProgressText = `Uploading files... (${uploadStepLabel})`
  const resourceLabel = totalUploads === 1 ? 'resource' : 'resources'
  const uploadCompleteMessage = `Successfully created ${totalUploads} ${resourceLabel}`
  const uploadDialogTitle = isUploading ? (isUploadComplete ? 'Upload finished' : 'Uploading...') : 'Upload files'
  const uploadDialogIcon = isUploading ? (isUploadComplete ? 'tick-circle' : 'upload') : 'upload'

  const filteredCreateOptions = createOptions.filter((option) => {
    const matchesCategory = createMenuCategory === 'All' || option.category === createMenuCategory
    const matchesSearch = option.label.toLowerCase().includes(createMenuSearch.toLowerCase())
    return matchesCategory && matchesSearch
  })

  const getFolderSize = (folder: FolderRecord) =>
    folder.files.reduce((total, file) => total + file.sizeBytes, 0)

  const getFolderUpdatedAt = (folder: FolderRecord) => folder.files[0]?.updatedAt ?? ''

  const sortedFolders = [...folders].sort((left, right) => {
    if (sortKey === 'size') {
      return getFolderSize(right) - getFolderSize(left)
    }
    if (sortKey === 'updated') {
      return getFolderUpdatedAt(right).localeCompare(getFolderUpdatedAt(left))
    }
    return left.name.localeCompare(right.name)
  })

  const sortedFiles = [...files].sort((left, right) => {
    if (sortKey === 'size') {
      return right.sizeBytes - left.sizeBytes
    }
    if (sortKey === 'updated') {
      return right.updatedAt.localeCompare(left.updatedAt)
    }
    return left.name.localeCompare(right.name)
  })

  const getUploadStatus = (index: number) => {
    if (!isUploading) {
      return 'idle'
    }
    if (isUploadComplete || index < completedCount) {
      return 'complete'
    }
    if (activeUploadIndex === index) {
      return 'active'
    }
    return 'queued'
  }

  const addUploadFiles = (incomingFiles: FileList | File[]) => {
    const normalized = Array.from(incomingFiles)
    if (normalized.length === 0) {
      return
    }

    setUploadFiles((current) => {
      const existingIds = new Set(current.map((file) => file.id))
      const additions = normalized
        .map((file) => ({ id: `${file.name}-${file.size}-${file.lastModified}`, file }))
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
    setUploadProgress(0)
  }

  const handleOpenUploadDialog = () => {
    setUploadOpen(true)
    setUploading(false)
    setUploadProgress(0)
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
  }

  const handleCreateFolder = () => {
    const trimmedName = newFolderName.trim()
    if (!trimmedName) {
      return
    }

    setFoldersState((current) => {
      const base = current ?? data ?? []
      return [
        ...base,
        {
          id: `folder-${Date.now()}`,
          name: trimmedName,
          files: [],
        },
      ]
    })
    handleCloseCreateFolder()
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
      setCreateMenuOpen(false)
      setActiveNav('pipeline')
    }
  }

  const handleStartUpload = () => {
    if (uploadFiles.length === 0) {
      return
    }
    setDragActive(false)
    setUploading(true)
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
      <MenuItem icon="refresh" text="Refresh" onClick={() => void refetch()} disabled={isFetching} />
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
      icon: 'root-folder',
      onClick: () => {
        setActiveFolderId(null)
        setActiveFileId(null)
      },
    },
    ...(activeFolder
      ? [
          {
            label: activeFolder.name,
            icon: 'folder-close',
            onClick: () => setActiveFileId(null),
          },
        ]
      : []),
    ...(activeFile
      ? [
          {
            label: activeFile.name,
            icon: 'document',
          },
        ]
      : []),
  ]

  return (
    <div className="page">
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
      {isDatasetView ? <H3>{activeFile?.datasetName ?? 'Dataset'}</H3> : null}
      {isLoading ? <Spinner size={24} /> : null}
      {isRootView ? (
        <section className="files-panel">
          <header className="files-panel-header">
            <Text className="files-panel-title">Folders</Text>
            <div className="files-panel-actions">
              {renderActionsPopover()}
              <Button icon="add" intent="success" text="New Folder" onClick={handleOpenCreateFolder} />
            </div>
          </header>
          <div className="files-panel-body">
            {folders.length === 0 ? (
              <div className="files-empty">
                <Icon icon="folder-close" size={40} className="files-empty-icon" />
                <Text className="files-empty-title">No folders yet</Text>
              </div>
            ) : (
              <div className={`grid files-grid ${viewMode === 'list' ? 'is-list' : ''}`}>
                {sortedFolders.map((folder) => (
                  <Card
                    key={folder.id}
                    interactive
                    className="card select-card"
                    onClick={() => {
                      setActiveFolderId(folder.id)
                      setActiveFileId(null)
                    }}
                  >
                    <Text className="card-title">{folder.name}</Text>
                    <Text className="card-meta">{folder.files.length} files</Text>
                  </Card>
                ))}
              </div>
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
            {files.length === 0 ? (
              <div className="files-empty">
                <Icon icon="folder-open" size={40} className="files-empty-icon" />
                <Text className="files-empty-title">This project is empty</Text>
              </div>
            ) : (
              <div className={`grid files-grid ${viewMode === 'list' ? 'is-list' : ''}`}>
                {sortedFiles.map((file) => (
                  <Card
                    key={file.id}
                    interactive
                    className="card select-card"
                    onClick={() => setActiveFileId(file.id)}
                  >
                    <Text className="card-title">{file.name}</Text>
                    <Text className="card-meta">Source: {file.source}</Text>
                    <Text className="card-meta">Updated: {file.updatedAt}</Text>
                    <Tag minimal intent="primary">{file.datasetName}</Tag>
                  </Card>
                ))}
              </div>
            )}
          </div>
        </section>
      ) : (
        <div className="grid">
          <Card className="card">
            <Text className="card-title">{activeFile.datasetName}</Text>
            <Text className="card-meta">File: {activeFile.name}</Text>
            <Text className="card-meta">Source: {activeFile.source}</Text>
            <Text className="card-meta">Updated: {activeFile.updatedAt}</Text>
            <Tag minimal intent="primary">{activeFile.datasetName}</Tag>
          </Card>
        </div>
      )}
      <Dialog
        isOpen={isUploadOpen}
        onClose={handleCloseUploadDialog}
        title={uploadDialogTitle}
        icon={uploadDialogIcon}
        className="upload-dialog bp5-dark"
      >
        <div className="upload-dialog-body">
          {isUploading ? (
            isUploadComplete ? (
              <div className="upload-complete-view">
                <Text className="upload-complete-message">{uploadCompleteMessage}</Text>
                <div className="upload-complete-list">
                  {uploadFiles.map((uploadFile) => (
                    <div key={uploadFile.id} className="upload-complete-row">
                      <Icon icon="document" className="upload-complete-icon" />
                      <Text className="upload-complete-name">{getResourceName(uploadFile.file.name)}</Text>
                    </div>
                  ))}
                </div>
                <div className="upload-dialog-footer">
                  <Button intent="primary" text="Done" onClick={handleCloseUploadDialog} />
                </div>
              </div>
            ) : (
              <div className="upload-progress-view">
                {uploadFiles.length > 0 ? (
                  <div className="upload-file-list is-progress">
                    {uploadFiles.map((uploadFile, index) => {
                      const status = getUploadStatus(index)
                      return (
                        <div key={uploadFile.id} className="upload-file-row">
                          <div className="upload-file-meta">
                            <span className={`upload-status ${status}`}>
                              {status === 'complete' ? (
                                <Icon icon="tick" size={12} />
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
            )
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
              <RadioGroup
                className="upload-options"
                selectedValue={uploadMode}
                onChange={(event) => setUploadMode(event.currentTarget.value)}
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
                    <Text className="create-dialog-item-description">{option.description}</Text>
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
        title="New folder"
        icon="folder-new"
        className="folder-dialog bp5-dark"
      >
        <div className="folder-dialog-body">
          <FormGroup label="Folder name" labelFor="folder-name-input">
            <InputGroup
              id="folder-name-input"
              placeholder="Enter folder name"
              value={newFolderName}
              autoFocus
              onChange={(event) => setNewFolderName(event.currentTarget.value)}
            />
          </FormGroup>
          <div className="folder-dialog-footer">
            <Button minimal text="Cancel" onClick={handleCloseCreateFolder} />
            <Button intent="success" text="Create" onClick={handleCreateFolder} disabled={isCreateFolderDisabled} />
          </div>
        </div>
      </Dialog>
    </div>
  )
}
