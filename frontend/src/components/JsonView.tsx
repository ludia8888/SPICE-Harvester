import type { ReactNode } from 'react'

export const JsonView = ({ value, fallback }: { value: unknown; fallback?: ReactNode }) => {
  if (value === undefined || value === null) {
    return <div className="muted small">{fallback ?? 'No data.'}</div>
  }
  return <pre className="json-view">{JSON.stringify(value, null, 2)}</pre>
}
