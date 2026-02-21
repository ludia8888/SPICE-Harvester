import type { ReactElement, ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render } from '@testing-library/react'
import { useAppStore } from '../src/store/useAppStore'

export const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: Infinity, staleTime: Infinity },
      mutations: { retry: false },
    },
  })

export const renderWithClient = (ui: ReactElement, options?: { client?: QueryClient; wrapper?: ReactNode }) => {
  const queryClient = options?.client ?? createTestQueryClient()
  const wrapper = (
    <QueryClientProvider client={queryClient}>
      {options?.wrapper ?? null}
      {ui}
    </QueryClientProvider>
  )
  return { queryClient, ...render(wrapper) }
}

export const resetAppStore = () => {
  useAppStore.setState({
    context: { project: null, branch: 'main', language: 'ko' },
    theme: 'light',
    adminToken: '',
    rememberToken: false,
    adminMode: false,
    settingsOpen: false,
    inspector: null,
    commands: {},
  })
}
