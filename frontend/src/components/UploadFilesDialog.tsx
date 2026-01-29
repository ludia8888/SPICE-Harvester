import { useEffect, useRef, useState } from 'react'
import type { ChangeEvent, DragEvent } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Button, Dialog, Icon, Radio, RadioGroup, Text } from '@blueprintjs/core'
import {
  approveDatasetSchema,
  uploadDataset,
  type DatasetRecord,
  type DatasetUploadResult,
  type UploadMode,
} from '../api/bff'

type UploadStatus = 'queued' | 'active' | 'complete' | 'error'

type UploadFile = {
  id: string
  file: File
  status: UploadStatus
  datasetName?: string
  error?: string
  ingestRequestId?: string
  schemaStatus?: string
  schemaSuggestion?: Record<string, unknown>
  approvalStatus?: 'idle' | 'pending' | 'error'
  approvalError?: string
}

type UploadFilesDialogProps = {
  isOpen: boolean
  onClose: () => void
  activeFolderId: string | null
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

const getSchemaStatusLabel = (status?: string) => {
  if (!status) {
    return null
  }
  const normalized = status.toUpperCase()
  if (normalized === 'APPROVED') {
    return 'Schema approved'
  }
  if (normalized === 'PENDING') {
    return 'Schema pending approval'
  }
  if (normalized === 'REJECTED') {
    return 'Schema rejected'
  }
  return `Schema ${normalized.toLowerCase()}`
}

const getSchemaStatusClass = (status?: string) => {
  if (!status) {
    return ''
  }
  return `is-${status.toLowerCase()}`
}

const upsertDatasetCache = (
  queryClient: ReturnType<typeof useQueryClient>,
  dbName: string,
  dataset: DatasetRecord,
) => {
  const normalized = {
    ...dataset,
    db_name: dataset.db_name || dbName,
    source_type: dataset.source_type || 'upload',
    branch: dataset.branch || 'main',
  }
  queryClient.setQueryData(['datasets', dbName], (current) => {
    const list = Array.isArray(current) ? (current as DatasetRecord[]) : []
    const index = list.findIndex((item) => item.dataset_id === normalized.dataset_id)
    if (index >= 0) {
      const next = [...list]
      next[index] = { ...next[index], ...normalized }
      return next
    }
    return [...list, normalized]
  })
}

export const UploadFilesDialog = ({ isOpen, onClose, activeFolderId }: UploadFilesDialogProps) => {
  const queryClient = useQueryClient()
  const [uploadMode, setUploadMode] = useState<UploadMode>(uploadOptions[0].value)
  const [isUploading, setUploading] = useState(false)
  const [isUploadComplete, setUploadComplete] = useState(false)
  const [uploadError, setUploadError] = useState<string | null>(null)
  const [uploadFiles, setUploadFiles] = useState<UploadFile[]>([])
  const [isDragActive, setDragActive] = useState(false)
  const fileInputRef = useRef<HTMLInputElement | null>(null)

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

  useEffect(() => {
    if (!isOpen) {
      return
    }
    setDragActive(false)
    setUploading(false)
    setUploadComplete(false)
    setUploadError(null)
    setUploadFiles([])
  }, [isOpen])

  const applyUploadResult = (uploadFile: UploadFile, result: DatasetUploadResult) => {
    return {
      ...uploadFile,
      status: 'complete' as const,
      datasetName: result.dataset.name,
      ingestRequestId: result.ingest_request_id,
      schemaStatus: result.schema_status,
      schemaSuggestion: result.schema_suggestion,
      approvalStatus: 'idle' as const,
      approvalError: undefined,
    }
  }

  const handleApproveSchema = async (uploadFile: UploadFile) => {
    if (!activeFolderId || !uploadFile.ingestRequestId) {
      return
    }
    setUploadFiles((current) =>
      current.map((item) =>
        item.id === uploadFile.id
          ? {
              ...item,
              approvalStatus: 'pending',
              approvalError: undefined,
            }
          : item,
      ),
    )
    try {
      const response = await approveDatasetSchema({
        ingestRequestId: uploadFile.ingestRequestId,
        dbName: activeFolderId,
        schemaJson:
          uploadFile.schemaSuggestion && typeof uploadFile.schemaSuggestion === 'object'
            ? uploadFile.schemaSuggestion
            : undefined,
      })
      const updatedStatus = response.ingest_request?.schema_status ?? 'APPROVED'
      setUploadFiles((current) =>
        current.map((item) =>
          item.id === uploadFile.id
            ? {
                ...item,
                schemaStatus: updatedStatus,
                approvalStatus: 'idle',
              }
            : item,
        ),
      )
      await queryClient.invalidateQueries({ queryKey: ['datasets', activeFolderId] })
    } catch (error) {
      setUploadFiles((current) =>
        current.map((item) =>
          item.id === uploadFile.id
            ? {
                ...item,
                approvalStatus: 'error',
                approvalError: error instanceof Error ? error.message : 'Schema approval failed',
              }
            : item,
        ),
      )
    }
  }

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
          approvalStatus: 'idle' as const,
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
    setDragActive(false)
    setUploading(false)
    setUploadComplete(false)
    setUploadError(null)
    setUploadFiles([])
    onClose()
  }

  const handleStartUpload = () => {
    if (uploadFiles.length === 0) {
      return
    }
    if (!activeFolderId) {
      setUploadError('Select a project before uploading.')
      return
    }

    const activeDbName = activeFolderId
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
            const result = await uploadDataset({
              dbName: activeDbName,
              file: uploadFile.file,
              mode: uploadMode,
            })
            upsertDatasetCache(queryClient, activeDbName, result.dataset)
            setUploadFiles((current) =>
              current.map((item) =>
                item.id === uploadFile.id ? applyUploadResult(item, result) : item,
              ),
            )
            return result.dataset
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
    }

    void runUpload()
  }

  return (
    <Dialog
      isOpen={isOpen}
      onClose={handleCloseUploadDialog}
      title={uploadDialogTitle}
      icon={uploadDialogIcon}
      className="upload-dialog bp6-dark"
    >
      <div className="upload-dialog-body">
        {isUploading ? (
          <div className="upload-progress-view">
            <div className="upload-file-list is-progress">
              {uploadFiles.map((uploadFile) => {
                const status = uploadFile.status
                return (
                  <div key={uploadFile.id} className="upload-file-row">
                    <div className="upload-file-meta">
                      <span className={`upload-status ${status}`}>
                        {status === 'complete' ? (
                          <Icon icon="tick" size={12} />
                        ) : status === 'error' ? (
                          <Icon icon="error" size={12} />
                        ) : (
                          <span className="upload-status-dot" />
                        )}
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
                    <div className="upload-complete-actions">
                      {getSchemaStatusLabel(uploadFile.schemaStatus) ? (
                        <span
                          className={`upload-schema-status ${getSchemaStatusClass(uploadFile.schemaStatus)}`}
                        >
                          {getSchemaStatusLabel(uploadFile.schemaStatus)}
                        </span>
                      ) : null}
                      {uploadFile.schemaStatus?.toUpperCase() === 'PENDING' &&
                      uploadFile.ingestRequestId ? (
                        <Button
                          minimal
                          small
                          icon="endorsed"
                          text={
                            uploadFile.approvalStatus === 'pending'
                              ? 'Approving...'
                              : 'Approve schema'
                          }
                          onClick={() => {
                            void handleApproveSchema(uploadFile)
                          }}
                          disabled={uploadFile.approvalStatus === 'pending'}
                        />
                      ) : null}
                    </div>
                    {uploadFile.approvalError ? (
                      <Text className="upload-approval-error">{uploadFile.approvalError}</Text>
                    ) : null}
                  </div>
                ))}
            </div>
            {failedUploads.length > 0 ? (
              <div className="upload-complete-list is-errors">
                {failedUploads.map((uploadFile) => (
                  <div key={uploadFile.id} className="upload-complete-row">
                    <Icon icon="error" className="upload-complete-icon" />
                    <Text className="upload-complete-name">{uploadFile.file.name}</Text>
                    {uploadFile.error ? (
                      <Text className="upload-approval-error">{uploadFile.error}</Text>
                    ) : null}
                  </div>
                ))}
              </div>
            ) : null}
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
            {uploadError ? <Text className="upload-error">{uploadError}</Text> : null}
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
  )
}
