import { useState, useCallback, useMemo } from 'react'
import { Button, Text, Intent } from '@blueprintjs/core'

/* ── tree node renderer ── */
const JsonNode = ({ data, depth, defaultOpen }: { data: unknown; depth: number; defaultOpen: boolean }) => {
  const [open, setOpen] = useState(defaultOpen)
  const indent = depth * 16

  if (data === null) return <span className="json-tree-null">null</span>
  if (data === undefined) return <span className="json-tree-null">undefined</span>
  if (typeof data === 'boolean') return <span className="json-tree-bool">{String(data)}</span>
  if (typeof data === 'number') return <span className="json-tree-number">{data}</span>
  if (typeof data === 'string') return <span className="json-tree-string">&quot;{data}&quot;</span>

  if (Array.isArray(data)) {
    if (data.length === 0) return <span>{'[]'}</span>
    return (
      <span>
        <span className="json-tree-toggle" onClick={() => setOpen(!open)}>
          {open ? '\u25BC' : '\u25B6'}
        </span>
        {!open ? (
          <span style={{ opacity: 0.6 }}>[{data.length} items]</span>
        ) : (
          <>
            {'[\n'}
            {data.map((item, i) => (
              <span key={i} style={{ paddingLeft: indent + 16 }}>
                {'  '}
                <JsonNode data={item} depth={depth + 1} defaultOpen={depth < 1} />
                {i < data.length - 1 ? ',' : ''}
                {'\n'}
              </span>
            ))}
            <span style={{ paddingLeft: indent }}>{']'}</span>
          </>
        )}
      </span>
    )
  }

  if (typeof data === 'object') {
    const entries = Object.entries(data as Record<string, unknown>)
    if (entries.length === 0) return <span>{'{}'}</span>
    return (
      <span>
        <span className="json-tree-toggle" onClick={() => setOpen(!open)}>
          {open ? '\u25BC' : '\u25B6'}
        </span>
        {!open ? (
          <span style={{ opacity: 0.6 }}>{`{${entries.length} keys}`}</span>
        ) : (
          <>
            {'{\n'}
            {entries.map(([key, val], i) => (
              <span key={key} style={{ paddingLeft: indent + 16 }}>
                {'  '}
                <span className="json-tree-key">&quot;{key}&quot;</span>
                {': '}
                <JsonNode data={val} depth={depth + 1} defaultOpen={depth < 1} />
                {i < entries.length - 1 ? ',' : ''}
                {'\n'}
              </span>
            ))}
            <span style={{ paddingLeft: indent }}>{'}'}</span>
          </>
        )}
      </span>
    )
  }

  return <span>{String(data)}</span>
}

/* ── main component ── */
export const JsonViewer = ({ value, empty }: { value: unknown; empty?: string }) => {
  const [copied, setCopied] = useState(false)

  const text = useMemo(() => {
    if (value === null || value === undefined) return ''
    try {
      return JSON.stringify(value, null, 2)
    } catch {
      return String(value)
    }
  }, [value])

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    })
  }, [text])

  if (!text) {
    return <Text className="muted">{empty ?? 'No data available.'}</Text>
  }

  return (
    <div>
      <div className="json-viewer-toolbar">
        <Button
          minimal
          small
          icon={copied ? 'tick' : 'clipboard'}
          intent={copied ? Intent.SUCCESS : Intent.NONE}
          onClick={handleCopy}
        >
          {copied ? 'Copied' : 'Copy'}
        </Button>
      </div>
      <pre className="json-viewer json-tree" style={{ whiteSpace: 'pre-wrap' }}>
        <JsonNode data={value} depth={0} defaultOpen />
      </pre>
    </div>
  )
}
