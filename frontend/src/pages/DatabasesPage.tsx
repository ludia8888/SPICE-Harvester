import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Intent,
  Icon,
} from '@blueprintjs/core'
import { createDatabase, deleteDatabase, getDatabaseExpectedSeq, listDatabases } from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { DangerConfirmDialog } from '../components/DangerConfirmDialog'
import { ProjectFileIcon } from '../components/ProjectFileIcon'
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


  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [deleteTarget, setDeleteTarget] = useState<DeleteTarget | null>(null)

  const listQuery = useQuery({
    queryKey: qk.databases(requestContext.language),
    queryFn: () => listDatabases(requestContext),
  })

  const createMutation = useMutation({
    mutationFn: (vars?: { name: string; description?: string }) => {
      const dbName = vars?.name || name;
      const dbDesc = vars?.description || description;
      return createDatabase(requestContext, { name: dbName.trim(), description: dbDesc })
    },
    onSuccess: (result, vars) => {
      const dbName = vars?.name || name;
      const commandId = result.commandId
      if (commandId) {
        registerCommand({ commandId, kind: 'CREATE_DATABASE', targetDbName: dbName.trim() })
        void showAppToast({
          intent: Intent.SUCCESS,
          message: `Create accepted: ${dbName.trim()}`,
        })
      } else {
        void queryClient.invalidateQueries({ queryKey: qk.databases(requestContext.language) })
        void showAppToast({
          intent: Intent.SUCCESS,
          message: `Created: ${dbName.trim()}`,
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
    <div className="project-explorer-container">
      <PageHeader
        title="Projects"
        subtitle="Manage your projects."
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

      {deleteMutation.isPending && (
        <Callout intent={Intent.WARNING} style={{ marginBottom: 20 }}>
          Deleting project...
        </Callout>
      )}

      <div className="project-grid">
        {/* New Project "Folder" */}
        <div
          className="project-file-icon new-project-item"
          onClick={() => {
            setName('')
            setDescription('')
            // Open dialog or just focus input if we were inline, but let's use a prompt for now or existing state
            // Re-using existing state logic but perhaps we need a dialog.
            // For now, let's keep it simple: clicking this shows the creation form in a dialog?
            // Actually, the previous UI had a form. Let's make this item trigger a dialog.
            // Since I haven't created a CreateDialog, I'll use a local state to show a specific creation area or just a Prompt.
            // Better: Reuse the creation logic but put it in a nicer container.
            const newName = prompt("Enter new project name (lowercase, numbers, _, -):");
            if (newName) {
              setName(newName);
              // use timeout to allow state update to propagate if needed, or just call mutate directly if we don't rely on 'name' state for the mutation *call* but the mutation closure uses it.
              // Actually createMutation uses 'name' state. So we need to set it and then trigger.
              // But setState is async. 
              // Let's refactor createMutation to accept args.
              createMutation.mutate({ name: newName });
            }
          }}
        >
          <div className="icon-container">
            <Icon icon="add" size={48} className="folder-icon" />
          </div>
          <div className="filename-container">
            <span className="filename">New Project</span>
          </div>
        </div>

        {databases.map((dbName) => (
          <ProjectFileIcon
            key={dbName}
            name={dbName}
            onClick={() => navigate(`/db/${encodeURIComponent(dbName)}/overview`)}
            onDelete={adminMode ? () => setDeleteTarget({ dbName }) : undefined}
            isAdmin={adminMode}
          />
        ))}
      </div>

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
