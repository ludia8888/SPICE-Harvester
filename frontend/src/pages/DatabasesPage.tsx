import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  FormGroup,
  HTMLTable,
  InputGroup,
  Intent,
  Text,
} from '@blueprintjs/core'
import { createDatabase, deleteDatabase, getDatabaseExpectedSeq, listDatabases } from '../api/bff'
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
  const adminToken = useAppStore((state) => state.adminToken)

  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [deleteTarget, setDeleteTarget] = useState<DeleteTarget | null>(null)

  const listQuery = useQuery({
    queryKey: qk.databases(requestContext.language),
    queryFn: () => listDatabases(requestContext),
  })

  const createMutation = useMutation({
    mutationFn: () => createDatabase(requestContext, { name: name.trim(), description: description.trim() || undefined }),
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
    },
    onError: (error) => toastApiError(error, language),
  })

  const deleteMutation = useMutation({
    mutationFn: async ({ dbName, reason }: { dbName: string; reason: string }) => {
      const expectedSeq = await getDatabaseExpectedSeq(requestContext, dbName)
      return deleteDatabase(requestContext, dbName, expectedSeq, {
        'X-Change-Reason': reason,
      })
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

  const databases = useMemo(() => listQuery.data ?? [], [listQuery.data])

  return (
    <div>
      <PageHeader
        title="Databases"
        subtitle="Create, open, and manage projects backed by the BFF."
        actions={
          <Button
            icon="refresh"
            onClick={() => void queryClient.invalidateQueries({ queryKey: qk.databases(requestContext.language) })}
            loading={listQuery.isFetching}
          >
            Refresh
          </Button>
        }
      />

      <div className="card-stack">
        <Card>
          <div className="card-title">
            <Text>Create database</Text>
          </div>
          <div className="form-row">
            <FormGroup label="Name" helperText="lowercase + numbers + '_' or '-'">
              <InputGroup value={name} onChange={(event) => setName(event.currentTarget.value)} />
            </FormGroup>
            <FormGroup label="Description">
              <InputGroup
                value={description}
                onChange={(event) => setDescription(event.currentTarget.value)}
                placeholder="Optional summary"
              />
            </FormGroup>
            <Button
              intent={Intent.PRIMARY}
              onClick={() => createMutation.mutate()}
              disabled={!name.trim() || createMutation.isPending}
              loading={createMutation.isPending}
            >
              Create
            </Button>
          </div>
          <Callout intent={Intent.PRIMARY} style={{ marginTop: 12 }}>
            202 responses are tracked in the Command Tracker.
          </Callout>
        </Card>

        <Card>
          <div className="card-title">
            <Text>Databases</Text>
            {listQuery.isFetching ? <Text className="muted small">Loading...</Text> : null}
          </div>
          {listQuery.error ? (
            <Callout intent={Intent.DANGER}>Failed to load databases.</Callout>
          ) : null}
          {databases.length === 0 ? (
            <Text className="muted">No databases yet.</Text>
          ) : (
            <HTMLTable striped interactive className="command-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {databases.map((dbName) => (
                  <tr key={dbName}>
                    <td>{dbName}</td>
                    <td>
                      <Button small icon="arrow-right" onClick={() => navigate(`/db/${encodeURIComponent(dbName)}/overview`)}>
                        Open
                      </Button>
                      <Button
                        small
                        icon="trash"
                        intent={Intent.DANGER}
                        disabled={!adminMode || !adminToken}
                        style={{ marginLeft: 8 }}
                        onClick={() => setDeleteTarget({ dbName })}
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
              Enable Admin mode to delete databases.
            </Text>
          ) : null}
        </Card>
      </div>

      <DangerConfirmDialog
        isOpen={Boolean(deleteTarget)}
        title="Delete database"
        description="This action permanently deletes the database and its data."
        confirmLabel="Delete"
        cancelLabel="Cancel"
        confirmTextToType={deleteTarget?.dbName ?? ''}
        reasonLabel="Change reason"
        reasonPlaceholder="Why are you deleting this database?"
        typedLabel="Type database name to confirm"
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
