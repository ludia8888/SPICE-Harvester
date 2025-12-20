import { HttpError } from '../api/bff'

export type ErrorKind = 'AUTH' | 'NOT_FOUND' | 'OCC_CONFLICT' | 'VALIDATION' | 'TEMPORARY' | 'UNKNOWN'

export type ClassifiedError = {
  kind: ErrorKind
  status?: number
  detail?: unknown
}

export const classifyError = (error: unknown): ClassifiedError => {
  if (error instanceof HttpError) {
    const status = error.status
    if (status === 401 || status === 403) {
      return { kind: 'AUTH', status, detail: error.detail }
    }
    if (status === 404) {
      return { kind: 'NOT_FOUND', status, detail: error.detail }
    }
    if (status === 409) {
      return { kind: 'OCC_CONFLICT', status, detail: error.detail }
    }
    if (status === 400 || status === 422) {
      return { kind: 'VALIDATION', status, detail: error.detail }
    }
    if (status === 502 || status === 503 || status === 504) {
      return { kind: 'TEMPORARY', status, detail: error.detail }
    }
    return { kind: 'UNKNOWN', status, detail: error.detail }
  }

  return { kind: 'UNKNOWN' }
}
