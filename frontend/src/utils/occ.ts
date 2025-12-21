import type { HttpError } from '../api/bff'

type OccDetail = {
  detail?: { expected_seq?: number; actual_seq?: number }
  expected_seq?: number
  actual_seq?: number
}

export const extractOccActualSeq = (error: unknown): number | null => {
  if (!error || typeof error !== 'object') {
    return null
  }
  const maybe = error as HttpError
  const detail = (maybe.detail || {}) as OccDetail
  const actual = detail.detail?.actual_seq ?? detail.actual_seq
  return typeof actual === 'number' && Number.isFinite(actual) ? actual : null
}
