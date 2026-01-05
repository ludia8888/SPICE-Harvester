import type { ReactElement, ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render } from '@testing-library/react'
import { useAppStore } from '../src/state/store'

export const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false },
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
    activeNav: 'home',
    pipelineContext: null,
  })
}
