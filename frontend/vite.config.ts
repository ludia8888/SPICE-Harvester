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
  }
})
