import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  Dialog,
  DialogBody,
  DialogFooter,
  FormGroup,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
  Text,
} from '@blueprintjs/core'
import {
  createDatabaseCtx,
  deleteDatabaseCtx,
  getDatabaseExpectedSeq,
  listDatabasesCtx,
  type DatabaseEntry,
} from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { DangerConfirmDialog } from '../components/DangerConfirmDialog'
import { PageHeader } from '../components/layout/PageHeader'
import { showAppToast } from '../app/AppToaster'
import { useCommandRegistration } from '../commands/useCommandRegistration'
import { extractCommandId } from '../commands/extractCommandId'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { navigate } from '../state/pathname'
import { useAppStore } from '../store/useAppStore'

type DeleteTarget = {
  dbName: string
}

export const DatabasesPage = () => {
  const queryClient = useQueryClient()
  const requestContext = useRequestContext()
  const registerCommand = useCommandRegistration()
  const language = useAppStore((state) => state.context.language)
  const adminMode = useAppStore((state) => state.adminMode)
  const devMode = useAppStore((state) => state.devMode)
  const adminToken = useAppStore((state) => state.adminToken)

  const [createOpen, setCreateOpen] = useState(false)
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [deleteTarget, setDeleteTarget] = useState<DeleteTarget | null>(null)
  const [sortOrder, setSortOrder] = useState<'default' | 'name-asc' | 'name-desc' | 'newest' | 'oldest'>('default')

  const listQuery = useQuery({
    queryKey: qk.databases(requestContext.language),
    queryFn: () => listDatabasesCtx(requestContext),
  })

  const createMutation = useMutation({
    mutationFn: () =>
      createDatabaseCtx(requestContext, {
        name: name.trim(),
        description: description.trim() || undefined,
      }),
    onSuccess: (result) => {
      const commandId = result.commandId
      if (commandId) {
        registerCommand({ commandId, kind: 'CREATE_DATABASE', targetDbName: name.trim() })
        void showAppToast({
          intent: Intent.SUCCESS,
          message: `Create accepted: ${name.trim()}`,
        })
      } else {
        void queryClient.invalidateQueries({ queryKey: qk.databases(requestContext.language) })
        void showAppToast({
          intent: Intent.SUCCESS,
          message: `Created: ${name.trim()}`,
        })
      }
      setName('')
      setDescription('')
      setCreateOpen(false)
    },
    onError: (error) => toastApiError(error, language),
  })

  const deleteMutation = useMutation({
    mutationFn: async ({ dbName, reason }: { dbName: string; reason: string }) => {
      const attempt = async () => {
        const expectedSeq = await getDatabaseExpectedSeq(requestContext, dbName)
        return deleteDatabaseCtx(requestContext, dbName, expectedSeq, {
          'X-Change-Reason': reason,
        })
      }
      try {
        return await attempt()
      } catch (err) {
        if (err instanceof Error && 'status' in err && (err as { status: number }).status === 409) {
          return await attempt()
        }
        throw err
      }
    },
    onSuccess: (payload, vars) => {
      const commandId = payload.commandId ?? extractCommandId(payload as unknown)
      if (commandId) {
        registerCommand({ commandId, kind: 'DELETE_DATABASE', targetDbName: vars.dbName })
        void showAppToast({
          intent: Intent.WARNING,
          message: `Delete accepted: ${vars.dbName}`,
        })
      } else {
        void queryClient.invalidateQueries({ queryKey: qk.databases(requestContext.language) })
        void showAppToast({
          intent: Intent.WARNING,
          message: `Deleted: ${vars.dbName}`,
        })
      }
    },
    onError: (error) => toastApiError(error, language),
    onSettled: () => setDeleteTarget(null),
  })

  const databases = useMemo(() => {
    const rawData = listQuery.data ?? []
    const normalized: DatabaseEntry[] = rawData.map((entry) =>
      typeof entry === 'string' ? { name: entry, created_at: null } : entry,
    )
    if (sortOrder === 'name-asc') return [...normalized].sort((a, b) => a.name.localeCompare(b.name))
    if (sortOrder === 'name-desc') return [...normalized].sort((a, b) => b.name.localeCompare(a.name))
    if (sortOrder === 'newest') return [...normalized].sort((a, b) => (b.created_at ?? '').localeCompare(a.created_at ?? ''))
    if (sortOrder === 'oldest') return [...normalized].sort((a, b) => (a.created_at ?? '').localeCompare(b.created_at ?? ''))
    return normalized
  }, [listQuery.data, sortOrder])

  const handleDelete = (dbName: string) => {
    if (devMode) {
      deleteMutation.mutate({ dbName, reason: 'Quick delete' })
    } else {
      setDeleteTarget({ dbName })
    }
  }

  return (
    <div>
      <PageHeader
        title="Projects"
        subtitle="Create, open, and manage projects."
        actions={
          <>
            <Button
              icon="plus"
              intent={Intent.PRIMARY}
              onClick={() => setCreateOpen(true)}
            >
              Create
            </Button>
            <Button
              icon="refresh"
              onClick={() => void queryClient.invalidateQueries({ queryKey: qk.databases(requestContext.language) })}
              loading={listQuery.isFetching}
              style={{ marginLeft: 8 }}
            >
              Refresh
            </Button>
          </>
        }
      />

      <div className="card-stack">
        <Card>
          <div className="card-title" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Text>Projects</Text>
            {listQuery.isFetching ? <Text className="muted small">Loading...</Text> : null}
            <div style={{ marginLeft: 'auto' }}>
              <HTMLSelect
                minimal
                value={sortOrder}
                onChange={(e) => setSortOrder(e.currentTarget.value as typeof sortOrder)}
                options={[
                  { value: 'default', label: 'Default' },
                  { value: 'name-asc', label: 'Name A → Z' },
                  { value: 'name-desc', label: 'Name Z → A' },
                  { value: 'newest', label: 'Newest first' },
                  { value: 'oldest', label: 'Oldest first' },
                ]}
                iconName="double-caret-vertical"
              />
            </div>
          </div>
          {listQuery.error ? (
            <Callout intent={Intent.DANGER}>Failed to load projects.</Callout>
          ) : null}
          {databases.length === 0 ? (
            <Text className="muted">No projects yet.</Text>
          ) : (
            <HTMLTable striped interactive className="command-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Created</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {databases.map((db) => (
                  <tr key={db.name}>
                    <td>{db.name}</td>
                    <td className="muted small">
                      {db.created_at ? new Date(db.created_at).toLocaleDateString() : '—'}
                    </td>
                    <td>
                      <Button small icon="arrow-right" onClick={() => navigate(`/db/${encodeURIComponent(db.name)}/overview`)}>
                        Open
                      </Button>
                      <Button
                        small
                        icon="trash"
                        intent={Intent.DANGER}
                        disabled={!adminMode || !adminToken}
                        style={{ marginLeft: 8 }}
                        loading={deleteMutation.isPending}
                        onClick={() => handleDelete(db.name)}
                      >
                        Delete
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </HTMLTable>
          )}
          {!adminMode ? (
            <Text className="muted small" style={{ marginTop: 8 }}>
              Enable Admin mode to delete projects.
            </Text>
          ) : null}
        </Card>
      </div>

      <Dialog
        isOpen={createOpen}
        onClose={() => setCreateOpen(false)}
        title="Create project"
      >
        <DialogBody>
          <FormGroup label="Name" helperText="lowercase + numbers + '_' or '-'">
            <InputGroup
              value={name}
              onChange={(event) => setName(event.currentTarget.value)}
              placeholder="my_project"
              autoFocus
            />
          </FormGroup>
          <FormGroup label="Description">
            <InputGroup
              value={description}
              onChange={(event) => setDescription(event.currentTarget.value)}
              placeholder="Optional summary"
            />
          </FormGroup>
          <Callout intent={Intent.PRIMARY} icon="info-sign">
            202 responses are tracked in the Command Tracker.
          </Callout>
        </DialogBody>
        <DialogFooter
          actions={
            <>
              <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
              <Button
                intent={Intent.PRIMARY}
                onClick={() => createMutation.mutate()}
                disabled={!name.trim() || createMutation.isPending}
                loading={createMutation.isPending}
              >
                Create
              </Button>
            </>
          }
        />
      </Dialog>

      <DangerConfirmDialog
        isOpen={Boolean(deleteTarget)}
        title="Delete project"
        description="This action permanently deletes the project and its data."
        confirmLabel="Delete"
        cancelLabel="Cancel"
        confirmTextToType={deleteTarget?.dbName ?? ''}
        reasonLabel="Change reason"
        reasonPlaceholder="Why are you deleting this project?"
        typedLabel="Type project name to confirm"
        typedPlaceholder={deleteTarget?.dbName ?? ''}
        onCancel={() => setDeleteTarget(null)}
        onConfirm={({ reason }) => {
          if (deleteTarget) {
            deleteMutation.mutate({ dbName: deleteTarget.dbName, reason })
          }
        }}
        loading={deleteMutation.isPending}
        footerHint="Required for auditability."
      />
    </div>
  )
}
