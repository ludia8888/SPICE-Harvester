import { useEffect, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useCommandTracker } from '../commands/useCommandTracker'
import { startUrlSync, useAppStore } from '../store/useAppStore'

export const AppBootstrap = () => {
  const queryClient = useQueryClient()
  const adminToken = useAppStore((state) => state.adminToken)
  const context = useAppStore((state) => state.context)

  useCommandTracker()

  useEffect(() => startUrlSync(), [])

  useEffect(() => {
    if (typeof document === 'undefined') {
      return
    }
    document.documentElement.lang = context.language
  }, [context.language])

  const previousContextRef = useRef(context)
  useEffect(() => {
    const previous = previousContextRef.current
    if (
      previous.project === context.project &&
      previous.branch === context.branch &&
      previous.language === context.language
    ) {
      return
    }

    previousContextRef.current = context
    void queryClient.cancelQueries()
  }, [context, queryClient])

  const previousTokenRef = useRef(adminToken)
  useEffect(() => {
    if (previousTokenRef.current === adminToken) {
      return
    }

    previousTokenRef.current = adminToken
    void queryClient.cancelQueries()
    queryClient.clear()
  }, [adminToken, queryClient])

  return null
}

