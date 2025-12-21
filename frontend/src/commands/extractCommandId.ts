type AnyRecord = Record<string, unknown>

const readString = (value: unknown) =>
  typeof value === 'string' && value.trim() ? value.trim() : null

export const extractCommandId = (payload: unknown): string | null => {
  if (!payload || typeof payload !== 'object') {
    return null
  }

  const data = payload as AnyRecord
  const direct =
    readString(data.command_id) ||
    readString(data.commandId) ||
    readString((data.data as AnyRecord | undefined)?.command_id) ||
    readString((data.data as AnyRecord | undefined)?.commandId)

  return direct ?? null
}
