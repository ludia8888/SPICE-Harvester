import { useCallback } from 'react'
import { useAppStore, type CommandKind } from '../store/useAppStore'

type RegisterOptions = {
  commandId?: string | null
  kind?: CommandKind
  title?: string
  targetDbName?: string | null
  targetClassId?: string | null
  targetInstanceId?: string | null
}

export const useCommandRegistration = () => {
  const context = useAppStore((state) => state.context)
  const trackCommand = useAppStore((state) => state.trackCommand)

  return useCallback(
    ({ commandId, kind = 'UNKNOWN', title, targetDbName, targetClassId, targetInstanceId }: RegisterOptions) => {
      if (!commandId) {
        return
      }

      trackCommand({
        id: commandId,
        kind,
        title,
        target: {
          dbName: targetDbName ?? context.project ?? '',
          classId: targetClassId ?? undefined,
          instanceId: targetInstanceId ?? undefined,
        },
        context: { project: context.project, branch: context.branch },
        submittedAt: new Date().toISOString(),
        writePhase: 'SUBMITTED',
        indexPhase: 'UNKNOWN',
      })
    },
    [context.branch, context.project, trackCommand],
  )
}
