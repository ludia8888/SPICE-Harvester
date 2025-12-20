import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import '@blueprintjs/icons/lib/css/blueprint-icons.css'
import '@blueprintjs/core/lib/css/blueprint.css'
import './index.css'
import App from './App.tsx'
import { AppBootstrap } from './app/AppBootstrap.tsx'
import { AppErrorBoundary } from './app/AppErrorBoundary.tsx'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
    },
  },
})

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <AppErrorBoundary>
        <AppBootstrap />
        <App />
      </AppErrorBoundary>
    </QueryClientProvider>
  </StrictMode>,
)
