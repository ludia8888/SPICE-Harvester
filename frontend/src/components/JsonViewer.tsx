import { Text } from '@blueprintjs/core'

const serialize = (value: unknown) => {
  if (value === null || value === undefined) {
    return ''
  }
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return String(value)
  }
}

export const JsonViewer = ({ value, empty }: { value: unknown; empty?: string }) => {
  const text = serialize(value)
  if (!text) {
    return <Text className="muted">{empty ?? 'No data available.'}</Text>
  }
  return <pre className="json-viewer">{text}</pre>
}
