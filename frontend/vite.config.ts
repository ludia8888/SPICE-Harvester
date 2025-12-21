import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const apiProxyTarget = env.VITE_API_PROXY_TARGET || env.BFF_BASE_URL || 'http://localhost:8002'

  const proxy = {
    '/api': {
      target: apiProxyTarget,
      changeOrigin: true,
      secure: false,
      ws: true,
    },
  } as const

  return {
    plugins: [react()],
    server: {
      proxy,
    },
    preview: {
      proxy,
    },
    build: {
      chunkSizeWarningLimit: 900,
      rollupOptions: {
        output: {
          manualChunks: (id) => {
            if (!id.includes('node_modules')) {
              return undefined
            }
            if (id.includes('react-cytoscapejs') || id.includes('cytoscape')) {
              return 'cytoscape'
            }
            if (id.includes('@blueprintjs/icons')) {
              return 'blueprint-icons'
            }
            if (id.includes('@blueprintjs')) {
              return 'blueprint-core'
            }
            if (id.includes('@tanstack')) {
              return 'tanstack'
            }
            if (id.includes('react-router')) {
              return 'react-router'
            }
            if (id.includes('react')) {
              return 'react-vendor'
            }
            return 'vendor'
          },
        },
      },
    },
  }
})
