import { useState, useCallback, useRef } from 'react'
import { Icon, InputGroup, Spinner, Tag } from '@blueprintjs/core'

type ColumnInfo = { name: string; type?: string }
type PreviewData = {
  columns?: Array<string | { name: string; type?: string; data_type?: string }>
  rows?: Array<Record<string, unknown>>
  data?: Array<Record<string, unknown>>
}

type Props = {
  isOpen: boolean
  onToggle: () => void
  loading?: boolean
  error?: string | null
  data?: PreviewData | null
  selectedNodeLabel?: string
}

function resolveColumns(data: PreviewData): ColumnInfo[] {
  if (!data.columns) return []
  return data.columns.map((c) => {
    if (typeof c === 'string') return { name: c }
    return { name: c.name, type: c.type ?? c.data_type }
  })
}

function resolveRows(data: PreviewData): Array<Record<string, unknown>> {
  return data.rows ?? data.data ?? []
}

function typeIcon(type?: string): string {
  if (!type) return 'th'
  const t = type.toLowerCase()
  if (t.includes('int') || t.includes('float') || t.includes('double') || t.includes('decimal') || t.includes('number')) return 'numerical'
  if (t.includes('date') || t.includes('time') || t.includes('timestamp')) return 'calendar'
  if (t.includes('bool')) return 'segmented-control'
  return 'font'
}

const MIN_PANEL_HEIGHT = 160
const MAX_PANEL_HEIGHT = 600
const DEFAULT_PANEL_HEIGHT = 280

export const PipelinePreviewPanel = ({
  isOpen,
  onToggle,
  loading,
  error,
  data,
  selectedNodeLabel,
}: Props) => {
  const [columnSearch, setColumnSearch] = useState('')
  const [panelHeight, setPanelHeight] = useState(DEFAULT_PANEL_HEIGHT)
  const dragRef = useRef<{ startY: number; startH: number } | null>(null)

  const columns = data ? resolveColumns(data) : []
  const rows = data ? resolveRows(data).slice(0, 50) : []

  const filteredColumns = columnSearch
    ? columns.filter((c) => c.name.toLowerCase().includes(columnSearch.toLowerCase()))
    : columns

  /* ── Resize drag handlers ─── */
  const onDragStart = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault()
      dragRef.current = { startY: e.clientY, startH: panelHeight }

      const onMove = (ev: MouseEvent) => {
        if (!dragRef.current) return
        const delta = dragRef.current.startY - ev.clientY
        const newH = Math.min(MAX_PANEL_HEIGHT, Math.max(MIN_PANEL_HEIGHT, dragRef.current.startH + delta))
        setPanelHeight(newH)
      }

      const onUp = () => {
        dragRef.current = null
        document.removeEventListener('mousemove', onMove)
        document.removeEventListener('mouseup', onUp)
      }

      document.addEventListener('mousemove', onMove)
      document.addEventListener('mouseup', onUp)
    },
    [panelHeight],
  )

  return (
    <div
      className={`pipeline-bottom-panel${isOpen ? ' is-open' : ''}`}
      style={isOpen ? { height: panelHeight } : undefined}
    >
      {/* Drag handle for resizing */}
      {isOpen && (
        <div className="pipeline-bottom-panel-drag" onMouseDown={onDragStart} />
      )}

      {/* Header */}
      <div className="pipeline-bottom-panel-header" onClick={onToggle} style={{ cursor: 'pointer' }}>
        <div className="pipeline-bottom-panel-title">
          <Icon icon="panel-table" size={14} />
          <span>Data preview</span>
          {selectedNodeLabel && <Tag minimal round>{selectedNodeLabel}</Tag>}
          {loading && <Spinner size={14} />}
        </div>
        <button className="pipeline-bottom-panel-collapse" onClick={(e) => { e.stopPropagation(); onToggle() }}>
          <Icon icon={isOpen ? 'chevron-down' : 'chevron-up'} size={14} />
        </button>
      </div>

      {/* Body */}
      {isOpen && (
        <div className="pipeline-bottom-panel-body">
          {error && (
            <div className="pipeline-preview-notice is-warn">
              <div className="pipeline-preview-notice-header">
                <Icon icon="warning-sign" size={14} />
                <span className="pipeline-preview-notice-title">Preview failed</span>
              </div>
              <div className="pipeline-preview-notice-message">{error}</div>
            </div>
          )}

          {!error && loading && columns.length === 0 && (
            <div className="pipeline-preview-empty" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <Spinner size={16} />
              <span>Running preview… This may take a few moments.</span>
            </div>
          )}

          {!error && !loading && columns.length === 0 && (
            <div className="pipeline-preview-empty">
              <span>Select a node on the canvas to preview its data.</span>
            </div>
          )}

          {columns.length > 0 && (
            <div className="pipeline-preview">
              {/* Left sidebar: column list */}
              <div className="pipeline-preview-sidebar">
                <div className="pipeline-preview-selector">
                  <div className="pipeline-preview-selector-left">
                    <Icon icon="th" size={12} />
                    <span>{selectedNodeLabel ?? 'Preview'}</span>
                  </div>
                </div>
                {/* Column search */}
                <div className="pipeline-preview-column-search">
                  <InputGroup
                    small
                    leftIcon="search"
                    placeholder="Search columns..."
                    value={columnSearch}
                    onChange={(e) => setColumnSearch(e.target.value)}
                  />
                </div>
                <div className="pipeline-preview-columns">
                  {filteredColumns.map((col) => (
                    <div key={col.name} className="pipeline-preview-column">
                      <div className="pipeline-preview-column-left">
                        <Icon icon={typeIcon(col.type) as 'font'} size={12} className="pipeline-preview-column-icon" />
                        <span>{col.name}</span>
                      </div>
                      {col.type && <Tag minimal style={{ fontSize: 10 }}>{col.type}</Tag>}
                    </div>
                  ))}
                  {columnSearch && filteredColumns.length === 0 && (
                    <div style={{ padding: '8px 12px', fontSize: 11, color: 'var(--foundry-text-muted)' }}>
                      No columns match &quot;{columnSearch}&quot;
                    </div>
                  )}
                </div>
              </div>

              {/* Right: data table */}
              <div className="pipeline-preview-table">
                <div className="pipeline-preview-table-scroll">
                  {/* Header row */}
                  <div className="pipeline-preview-table-header">
                    <div className="pipeline-preview-table-cell is-index is-header">#</div>
                    {columns.map((col) => (
                      <div key={col.name} className="pipeline-preview-table-cell is-header">
                        <span className="pipeline-preview-header-main">{col.name}</span>
                        {col.type && <span className="pipeline-preview-header-type">{col.type}</span>}
                      </div>
                    ))}
                  </div>

                  {/* Data rows */}
                  {rows.map((row, ri) => (
                    <div key={ri} className="pipeline-preview-table-row">
                      <div className="pipeline-preview-table-cell is-index">{ri + 1}</div>
                      {columns.map((col) => (
                        <div key={col.name} className="pipeline-preview-table-cell">
                          {row[col.name] != null ? String(row[col.name]) : ''}
                        </div>
                      ))}
                    </div>
                  ))}
                </div>
                {rows.length >= 50 && (
                  <div style={{ padding: '8px 12px', fontSize: 11, color: 'var(--foundry-text-muted)' }}>
                    Showing first 50 rows
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
