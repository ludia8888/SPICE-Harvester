import { useState, useRef, useEffect } from 'react'
import {
  Button,
  Callout,
  Dialog,
  DialogBody,
  DialogFooter,
  FormGroup,
  Icon,
  InputGroup,
  Intent,
  Spinner,
  Tag,
} from '@blueprintjs/core'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import type { RequestContext, PipelineRecord } from '../../api/bff'
import { listPipelines, createPipeline, deletePipeline, updatePipeline } from '../../api/bff'

type Props = {
  isOpen: boolean
  onClose: () => void
  ctx: RequestContext
  dbName: string
  branch: string
  currentPipelineId?: string
  onSelect: (pipelineId: string) => void
}

export const PipelineManagerDialog = ({
  isOpen,
  onClose,
  ctx,
  dbName,
  branch,
  currentPipelineId,
  onSelect,
}: Props) => {
  const queryClient = useQueryClient()
  const [showCreate, setShowCreate] = useState(false)
  const [newName, setNewName] = useState('')
  const [newDesc, setNewDesc] = useState('')

  /* Inline rename state */
  const [editingId, setEditingId] = useState<string | null>(null)
  const [editName, setEditName] = useState('')
  const editRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    if (editingId && editRef.current) {
      editRef.current.focus()
      editRef.current.select()
    }
  }, [editingId])

  const listQ = useQuery({
    queryKey: ['pipelines', dbName, branch],
    queryFn: () => listPipelines(ctx, { db_name: dbName, branch }),
    enabled: isOpen,
  })

  const createMut = useMutation({
    mutationFn: () =>
      createPipeline(ctx, {
        db_name: dbName,
        name: newName,
        description: newDesc || undefined,
        branch,
        location: dbName,
        pipeline_type: 'transform',
      }),
    onSuccess: (created) => {
      queryClient.invalidateQueries({ queryKey: ['pipelines', dbName, branch] })
      setShowCreate(false)
      setNewName('')
      setNewDesc('')
      if (created?.pipeline_id) {
        onSelect(created.pipeline_id)
      }
    },
  })

  const deleteMut = useMutation({
    mutationFn: (id: string) => deletePipeline(ctx, id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['pipelines', dbName, branch] }),
  })

  const renameMut = useMutation({
    mutationFn: ({ id, name }: { id: string; name: string }) =>
      updatePipeline(ctx, id, { name }),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: ['pipelines', dbName, branch] })
      queryClient.invalidateQueries({ queryKey: ['pipelines', 'detail', variables.id] })
      setEditingId(null)
      setEditName('')
    },
  })

  const pipelines: PipelineRecord[] = Array.isArray(listQ.data) ? listQ.data : []

  const startRename = (pl: PipelineRecord, e?: React.MouseEvent) => {
    e?.stopPropagation()
    setEditingId(pl.pipeline_id)
    setEditName(pl.name)
  }

  const commitRename = () => {
    if (editingId && editName.trim() && editName.trim() !== getPipelineName(editingId)) {
      renameMut.mutate({ id: editingId, name: editName.trim() })
    } else {
      setEditingId(null)
      setEditName('')
    }
  }

  const cancelRename = () => {
    setEditingId(null)
    setEditName('')
  }

  const getPipelineName = (id: string) =>
    pipelines.find((p) => p.pipeline_id === id)?.name ?? ''

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title="Pipelines"
      icon="data-lineage"
      style={{ width: 540 }}
    >
      <DialogBody>
        {/* Create new pipeline form */}
        {showCreate ? (
          <div style={{ marginBottom: 16 }}>
            <FormGroup label="Pipeline Name" labelFor="new-pl-name">
              <InputGroup
                id="new-pl-name"
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                placeholder="e.g. orders_etl"
                autoFocus
              />
            </FormGroup>
            <FormGroup label="Description (optional)" labelFor="new-pl-desc">
              <InputGroup
                id="new-pl-desc"
                value={newDesc}
                onChange={(e) => setNewDesc(e.target.value)}
                placeholder="What does this pipeline do?"
              />
            </FormGroup>
            {createMut.error && (
              <Callout intent={Intent.DANGER} compact style={{ marginBottom: 8 }}>
                Failed to create pipeline.
              </Callout>
            )}
            <div style={{ display: 'flex', gap: 8 }}>
              <Button
                intent={Intent.PRIMARY}
                icon="plus"
                loading={createMut.isPending}
                disabled={!newName.trim()}
                onClick={() => createMut.mutate()}
              >
                Create
              </Button>
              <Button
                minimal
                onClick={() => {
                  setShowCreate(false)
                  setNewName('')
                  setNewDesc('')
                }}
              >
                Cancel
              </Button>
            </div>
          </div>
        ) : (
          <div style={{ marginBottom: 12 }}>
            <Button icon="plus" small onClick={() => setShowCreate(true)}>
              New Pipeline
            </Button>
          </div>
        )}

        {/* Pipeline list */}
        {listQ.isLoading && (
          <div style={{ textAlign: 'center', padding: 20 }}>
            <Spinner size={20} />
          </div>
        )}
        {listQ.error && (
          <Callout intent={Intent.DANGER} compact>Failed to load pipelines.</Callout>
        )}
        {!listQ.isLoading && pipelines.length === 0 && (
          <div style={{ padding: '12px 0', color: 'var(--foundry-text-muted)', fontSize: 13 }}>
            No pipelines yet.
          </div>
        )}
        {pipelines.length > 0 && (
          <div className="pipeline-manager-list">
            {pipelines.map((pl) => {
              const isCurrent = pl.pipeline_id === currentPipelineId
              const isEditing = editingId === pl.pipeline_id
              return (
                <div
                  key={pl.pipeline_id}
                  className={`pipeline-manager-item${isCurrent ? ' is-current' : ''}`}
                  onClick={() => {
                    if (!isCurrent && !isEditing) onSelect(pl.pipeline_id)
                  }}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 10,
                    padding: '8px 12px',
                    borderRadius: 6,
                    cursor: isEditing ? 'default' : isCurrent ? 'default' : 'pointer',
                    background: isCurrent ? 'var(--foundry-bg-active, rgba(45,114,210,0.08))' : 'transparent',
                    transition: 'background 0.15s',
                  }}
                  onMouseEnter={(e) => {
                    if (!isCurrent && !isEditing) (e.currentTarget as HTMLElement).style.background = 'var(--foundry-bg-hover, rgba(0,0,0,0.04))'
                  }}
                  onMouseLeave={(e) => {
                    (e.currentTarget as HTMLElement).style.background = isCurrent
                      ? 'var(--foundry-bg-active, rgba(45,114,210,0.08))'
                      : 'transparent'
                  }}
                >
                  <Icon icon="data-lineage" size={14} style={{ opacity: 0.6 }} />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    {isEditing ? (
                      <div
                        style={{ display: 'flex', alignItems: 'center', gap: 6 }}
                        onClick={(e) => e.stopPropagation()}
                      >
                        <input
                          ref={editRef}
                          className="plm-rename-input"
                          value={editName}
                          onChange={(e) => setEditName(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') commitRename()
                            if (e.key === 'Escape') cancelRename()
                          }}
                          onBlur={commitRename}
                        />
                        {renameMut.isPending && <Spinner size={12} />}
                      </div>
                    ) : (
                      <div
                        style={{
                          fontWeight: 500,
                          fontSize: 13,
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          whiteSpace: 'nowrap',
                        }}
                        onDoubleClick={(e) => startRename(pl, e)}
                      >
                        {pl.name}
                      </div>
                    )}
                    {pl.description && !isEditing && (
                      <div style={{ fontSize: 11, color: 'var(--foundry-text-muted)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {pl.description}
                      </div>
                    )}
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    {pl.status && (
                      <Tag
                        minimal
                        intent={
                          pl.status === 'deployed'
                            ? Intent.SUCCESS
                            : pl.status === 'failed'
                              ? Intent.DANGER
                              : Intent.NONE
                        }
                        style={{ fontSize: 10, textTransform: 'capitalize' }}
                      >
                        {pl.status}
                      </Tag>
                    )}
                    {isCurrent && (
                      <Tag minimal intent={Intent.PRIMARY} style={{ fontSize: 10 }}>
                        Current
                      </Tag>
                    )}
                    {!isEditing && (
                      <Button
                        small
                        minimal
                        icon="edit"
                        title="Rename pipeline"
                        onClick={(e) => startRename(pl, e)}
                      />
                    )}
                    <Button
                      small
                      minimal
                      icon="trash"
                      intent={Intent.DANGER}
                      loading={deleteMut.isPending}
                      onClick={(e) => {
                        e.stopPropagation()
                        if (confirm(`Delete pipeline "${pl.name}"?`)) {
                          deleteMut.mutate(pl.pipeline_id)
                        }
                      }}
                    />
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </DialogBody>
      <DialogFooter
        actions={
          <Button onClick={onClose}>Close</Button>
        }
      />
    </Dialog>
  )
}
