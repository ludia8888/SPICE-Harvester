import { useMemo } from 'react'
import { useAppStore } from '../store/useAppStore'

export const useRequestContext = () => {
  const language = useAppStore((state) => state.context.language)
  const adminToken = useAppStore((state) => state.adminToken)

  return useMemo(() => ({ language, adminToken }), [language, adminToken])
}
