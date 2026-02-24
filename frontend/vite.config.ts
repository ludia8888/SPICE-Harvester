import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react-swc'

export default defineConfig({
  esbuild: { jsx: 'automatic' },
  plugins: [react()],
  server: {
    port: 5173,
    allowedHosts: true,
    proxy: {
      '/api/v1': {
        target: process.env.VITE_PROXY_TARGET ?? 'http://localhost:8002',
        changeOrigin: true,
        ws: true,
      },
      '/api/v2': {
        target: process.env.VITE_PROXY_TARGET ?? 'http://localhost:8002',
        changeOrigin: true,
        ws: true,
      },
    },
  },
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: './tests/setupTests.ts',
    include: ['tests/**/*.test.{ts,tsx}'],
    exclude: ['tests/e2e/**'],
    css: false,
    coverage: {
      provider: 'istanbul',
      reporter: ['text', 'lcov'],
      lines: 90,
      functions: 90,
      statements: 90,
      branches: 90,
      include: ['src/**/*.{ts,tsx}'],
      exclude: [
        'src/vite-env.d.ts',
        '**/*.d.ts',
        '**/*.test.*',
        '**/__tests__/**',
        'tests/**',
      ],
    },
  },
})
