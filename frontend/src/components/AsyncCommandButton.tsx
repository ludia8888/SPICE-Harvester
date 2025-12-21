import { useState, type ReactNode } from 'react'
import { Button, Intent } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'
import { showAppToast } from '../app/AppToaster'
import { toastApiError } from '../errors/toastApiError'
import { useAppStore, type CommandKind } from '../store/useAppStore'
import { asArray, asRecord, getString } from '../utils/typed'

const extractCommandIds = (payload: unknown): string[] => {
  const ids = new Set<string>()
  const visit = (value: unknown) => {
    if (!value) {
      return
    }
    if (Array.isArray(value)) {
      value.forEach(visit)
      return
    }
    if (typeof value !== 'object') {
      return
    }
    const obj = asRecord(value)
    const direct =
      getString(obj.commandId) ??
      getString(obj.command_id) ??
      getString(asRecord(obj.data).command_id)
    if (direct) {
      ids.add(direct)
    }
    const write = asRecord(obj.write ?? asRecord(obj.data).write)
    const commands = asArray<unknown>(write.commands ?? obj.commands)
    commands.forEach((command) => {
      const commandRecord = asRecord(command)
      const commandId = getString(commandRecord.command_id)
      if (commandId) {
        ids.add(commandId)
        return
      }
      const nested = asRecord(commandRecord.command)
      const nestedId = getString(nested.command_id) ?? getString(nested.id)
      if (nestedId) {
        ids.add(nestedId)
      }
    })
  }
  visit(payload)
  return Array.from(ids)
}

export const AsyncCommandButton = ({
  onSubmit,
  commandKind,
  commandTitle,
  commandSource,
  dbName,
  branch,
  successMessage,
  onSuccess,
  onError,
  intent,
  icon,
  disabled,
  children,
}: {
  onSubmit: () => Promise<unknown>
  commandKind: CommandKind
  commandTitle?: string
  commandSource?: string
  dbName?: string
  branch?: string
  successMessage?: string
  onSuccess?: (payload: unknown, commandIds: string[]) => void
  onError?: (error: unknown) => void
  intent?: Intent
  icon?: IconName
  disabled?: boolean
  children: ReactNode
}) => {
  const [pending, setPending] = useState(false)
  const context = useAppStore((state) => state.context)
  const trackCommands = useAppStore((state) => state.trackCommands)

  const handleClick = async () => {
    if (pending) {
      return
    }
    setPending(true)
    try {
      const payload = await onSubmit()
      const commandIds = extractCommandIds(payload)
      if (commandIds.length) {
        trackCommands(
          commandIds.map((id) => ({
            id,
            kind: commandKind,
            title: commandTitle,
            source: commandSource,
            targetDbName: dbName ?? context.project ?? undefined,
            context: {
              project: dbName ?? context.project ?? undefined,
              branch: branch ?? context.branch,
            },
          })),
        )
      }
      if (successMessage) {
        void showAppToast({ intent: Intent.SUCCESS, message: successMessage })
      }
      onSuccess?.(payload, commandIds)
    } catch (error) {
      onError?.(error)
      toastApiError(error, context.language)
    } finally {
      setPending(false)
    }
  }

  return (
    <Button intent={intent} icon={icon} onClick={handleClick} disabled={disabled || pending} loading={pending}>
      {children}
    </Button>
  )
}
