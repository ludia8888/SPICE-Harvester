export type UnknownRecord = Record<string, unknown>

export const isRecord = (value: unknown): value is UnknownRecord =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

export const asRecord = (value: unknown): UnknownRecord => (isRecord(value) ? value : {})

export const asArray = <T = unknown>(value: unknown): T[] =>
  Array.isArray(value) ? (value as T[]) : []

export const getString = (value: unknown): string | undefined =>
  typeof value === 'string' ? value : undefined

export const getNumber = (value: unknown): number | undefined =>
  typeof value === 'number' && Number.isFinite(value) ? value : undefined

export const getBoolean = (value: unknown): boolean | undefined =>
  typeof value === 'boolean' ? value : undefined
