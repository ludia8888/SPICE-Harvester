const rawBase = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? '/api/v1'

export const API_BASE_URL = rawBase.replace(/\/+$/, '') || '/api/v1'
