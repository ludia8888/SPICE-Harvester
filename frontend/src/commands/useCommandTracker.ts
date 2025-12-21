import { useEffect, useMemo } from 'react'
import { useQueries, useQueryClient } from '@tanstack/react-query'
import { getCommandStatus, HttpError, listDatabases } from '../api/bff'
import { getInvalidationKeys } from './commandInvalidationMap'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'

const TERMINAL_STATUSES = new Set(['COMPLETED', 'FAILED', 'CANCELLED'])

export const useCommandTracker = () => {
  const queryClient = useQueryClient()

  const context = useAppStore((state) => state.context)
  const adminToken = useAppStore((state) => state.adminToken)
  const commands = useAppStore((state) => state.commands)
  const patchCommand = useAppStore((state) => state.patchCommand)

  const trackedCommands = useMemo(
    () => Object.values(commands).sort((a, b) => b.submittedAt.localeCompare(a.submittedAt)),
    [commands],
  )

  const pollingCommands = useMemo(
    () => trackedCommands.filter((cmd) => cmd.writePhase === 'SUBMITTED' && !cmd.expired),
    [trackedCommands],
  )

  const requestContext = useMemo(
    () => ({ language: context.language, adminToken }),
    [adminToken, context.language],
  )

  const statusQueries = useQueries({
    queries: pollingCommands.map((command) => ({
      queryKey: qk.commandStatus(command.id, context.language),
      queryFn: () => getCommandStatus(requestContext, command.id),
      enabled: Boolean(adminToken),
      refetchInterval: 1000,
      retry: false,
    })),
  })

  useEffect(() => {
    statusQueries.forEach((query, index) => {
      const command = pollingCommands[index]
      if (!command) {
        return
      }
      if (query.error instanceof HttpError && query.error.status === 404) {
        if (!command.expired) {
          patchCommand(command.id, { expired: true, status: 'EXPIRED' })
        }
        return
      }

      const status = query.data?.status
      if (!status) {
        return
      }

      patchCommand(command.id, { status })

      if (!TERMINAL_STATUSES.has(status)) {
        return
      }

      if (status === 'COMPLETED') {
        const needsVisibilityCheck =
          (command.kind === 'CREATE_DATABASE' || command.kind === 'DELETE_DATABASE') &&
          Boolean(command.target.dbName)
        patchCommand(command.id, {
          writePhase: 'WRITE_DONE',
          indexPhase: needsVisibilityCheck ? 'INDEXING_PENDING' : 'VISIBLE_IN_SEARCH',
        })
      } else if (status === 'CANCELLED') {
        patchCommand(command.id, { writePhase: 'CANCELLED' })
      } else {
        patchCommand(command.id, { writePhase: 'FAILED', error: query.data?.error ?? null })
      }

      const keys = getInvalidationKeys(command, context.language)
      keys.forEach((key) => void queryClient.invalidateQueries({ queryKey: key, exact: false }))
    })
  }, [context.language, patchCommand, pollingCommands, queryClient, statusQueries])

  const visibilityCandidates = useMemo(
    () =>
      trackedCommands.filter((cmd) => {
        if (cmd.writePhase !== 'WRITE_DONE' || cmd.indexPhase === 'VISIBLE_IN_SEARCH' || cmd.expired) {
          return false
        }
        const isDbCommand = cmd.kind === 'CREATE_DATABASE' || cmd.kind === 'DELETE_DATABASE'
        return isDbCommand && Boolean(cmd.target.dbName)
      }),
    [trackedCommands],
  )

  const visibilityQueries = useQueries({
    queries: visibilityCandidates.map((command) => ({
      queryKey: ['visibility', command.id, context.language],
      queryFn: async () => {
        const databases = await queryClient.fetchQuery({
          queryKey: qk.databases(context.language),
          queryFn: () => listDatabases(requestContext),
        })
        const present = databases.includes(command.target.dbName)
        return command.kind === 'CREATE_DATABASE' ? present : !present
      },
      enabled: Boolean(adminToken),
      refetchInterval: 1500,
      retry: false,
    })),
  })

  useEffect(() => {
    visibilityQueries.forEach((query, index) => {
      const command = visibilityCandidates[index]
      if (!command) {
        return
      }
      if (query.data === true) {
        patchCommand(command.id, { indexPhase: 'VISIBLE_IN_SEARCH' })
      }
    })
  }, [patchCommand, visibilityCandidates, visibilityQueries])
}
