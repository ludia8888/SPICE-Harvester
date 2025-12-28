import { Button, HTMLSelect, HTMLTable, Icon } from '@blueprintjs/core'
import { useMemo, useState } from 'react'
import type { PreviewColumn, PreviewRow } from './pipelineTypes'

const typeIcons: Record<string, string> = {
    String: '"',
    Date: '📅',
    Integer: '01',
    Boolean: 'T/F',
}

type PreviewCopy = {
    title: string
    dataPreview: string
    noNodes: string
    searchPlaceholder: (count: number) => string
    formatType: (type: string) => string
    validationTitle?: string
    rowCountLabel?: string
    sampleRowCountLabel?: string
}

type PreviewProps = {
    nodes: Array<{ id: string; title: string }>
    selectedNodeId?: string | null
    columns: PreviewColumn[]
    rows: PreviewRow[]
    rowCount?: number | null
    sampleRowCount?: number | null
    columnStats?: Record<string, unknown> | null
    copy: PreviewCopy
    onSelectNode: (nodeId: string) => void
    onToggleFullscreen: () => void
    onCollapse: () => void
    fullscreen?: boolean
}

type ChartBucket = {
    label: string
    count: number
}

export const PipelinePreview = ({
    nodes,
    selectedNodeId,
    columns,
    rows,
    rowCount,
    sampleRowCount,
    columnStats,
    copy,
    onSelectNode,
    onToggleFullscreen,
    onCollapse,
    fullscreen = false,
}: PreviewProps) => {
    const [query, setQuery] = useState('')
    const [selectedColumn, setSelectedColumn] = useState<string | null>(null)
    const filteredColumns = useMemo(() => {
        const normalized = query.trim().toLowerCase()
        if (!normalized) return columns
        return columns.filter((column) => column.key.toLowerCase().includes(normalized))
    }, [columns, query])

    const nodeOptions = nodes.length ? nodes : [{ id: 'none', title: copy.noNodes }]
    const selectedValue = selectedNodeId ?? nodeOptions[0]?.id ?? ''

    const resolveTypeKey = (type: string) => {
        const normalized = type.toLowerCase()
        if (normalized.includes('date') || normalized.includes('time')) return 'Date'
        if (normalized.includes('bool')) return 'Boolean'
        if (normalized.includes('int') || normalized.includes('float') || normalized.includes('double') || normalized.includes('decimal') || normalized.includes('number')) {
            return 'Integer'
        }
        return 'String'
    }

    const statsSummaryForColumn = (columnKey: string) => {
        if (!columnStats || typeof columnStats !== 'object') return ''
        const columnsPayload = (columnStats as { columns?: unknown }).columns
        if (!columnsPayload || typeof columnsPayload !== 'object') return ''
        const stat = (columnsPayload as Record<string, unknown>)[columnKey]
        if (!stat || typeof stat !== 'object') return ''
        const payload = stat as Record<string, unknown>
        const nulls = typeof payload.null_count === 'number' ? payload.null_count : null
        const empties = typeof payload.empty_count === 'number' ? payload.empty_count : null
        const distinct = typeof payload.distinct_count === 'number' ? payload.distinct_count : null
        const parts: string[] = []
        if (nulls !== null) parts.push(`null=${nulls}`)
        if (empties !== null) parts.push(`empty=${empties}`)
        if (distinct !== null) parts.push(`distinct=${distinct}`)
        const topValues = Array.isArray(payload.top_values) ? payload.top_values : []
        if (topValues.length) {
            const rendered = topValues
                .slice(0, 5)
                .map((item) => {
                    if (!item || typeof item !== 'object') return null
                    const row = item as Record<string, unknown>
                    const value = typeof row.value === 'string' ? row.value : String(row.value ?? '')
                    const count = typeof row.count === 'number' ? row.count : Number(row.count ?? 0)
                    if (!value) return null
                    return `${value}(${count})`
                })
                .filter(Boolean)
                .join(', ')
            if (rendered) parts.push(`top=${rendered}`)
        }
        return parts.join(' · ')
    }

    const chartBucketsForColumn = (columnKey: string): ChartBucket[] => {
        if (!columnStats || typeof columnStats !== 'object') return []
        const columnsPayload = (columnStats as { columns?: unknown }).columns
        if (!columnsPayload || typeof columnsPayload !== 'object') return []
        const stat = (columnsPayload as Record<string, unknown>)[columnKey]
        if (!stat || typeof stat !== 'object') return []
        const payload = stat as Record<string, unknown>

        const numeric = payload.numeric
        if (numeric && typeof numeric === 'object') {
            const histogram = (numeric as Record<string, unknown>).histogram
            if (Array.isArray(histogram)) {
                return histogram
                    .map((bucket) => {
                        if (!bucket || typeof bucket !== 'object') return null
                        const item = bucket as Record<string, unknown>
                        const start = typeof item.start === 'number' ? item.start : Number(item.start ?? NaN)
                        const end = typeof item.end === 'number' ? item.end : Number(item.end ?? NaN)
                        const count = typeof item.count === 'number' ? item.count : Number(item.count ?? 0)
                        if (!Number.isFinite(start) || !Number.isFinite(end) || !Number.isFinite(count)) return null
                        const label = `${start.toFixed(2)}–${end.toFixed(2)}`
                        return { label, count }
                    })
                    .filter(Boolean) as ChartBucket[]
            }
        }

        const topValues = Array.isArray(payload.top_values) ? payload.top_values : []
        return topValues
            .map((item) => {
                if (!item || typeof item !== 'object') return null
                const row = item as Record<string, unknown>
                const value = typeof row.value === 'string' ? row.value : String(row.value ?? '')
                const count = typeof row.count === 'number' ? row.count : Number(row.count ?? 0)
                if (!Number.isFinite(count) || !value) return null
                return { label: value, count }
            })
            .filter(Boolean) as ChartBucket[]
    }

    const chartBuckets = selectedColumn ? chartBucketsForColumn(selectedColumn) : []
    const chartMax = chartBuckets.length ? Math.max(...chartBuckets.map((bucket) => bucket.count)) : 0

    return (
        <div className={`pipeline-preview ${fullscreen ? 'is-fullscreen' : ''}`}>
            <div className="preview-header">
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <Icon icon="search" size={14} />
                    <span className="preview-title">{copy.title}</span>
                </div>
                <div className="preview-controls">
                    <Button icon={fullscreen ? 'minimize' : 'fullscreen'} small minimal onClick={onToggleFullscreen} />
                    <Button icon="chevron-down" small minimal onClick={onCollapse} />
                </div>
            </div>
            <div className="preview-toolbar">
                <div className="preview-toolbar-left">
                    <span className="preview-toolbar-label">{copy.dataPreview}</span>
                    {typeof rowCount === 'number' ? (
                        <span className="preview-toolbar-meta">
                            {(copy.rowCountLabel ?? 'Rows')}: {rowCount}
                            {typeof sampleRowCount === 'number'
                                ? ` · ${(copy.sampleRowCountLabel ?? 'Sample')}: ${sampleRowCount}`
                                : null}
                        </span>
                    ) : null}
                    <div className="preview-node-select">
                        <Icon icon="layout-auto" size={12} />
                        <HTMLSelect
                            minimal
                            value={selectedValue}
                            onChange={(event) => onSelectNode(event.currentTarget.value)}
                            disabled={!nodes.length}
                        >
                            {nodeOptions.map((node) => (
                                <option key={node.id} value={node.id}>
                                    {node.title}
                                </option>
                            ))}
                        </HTMLSelect>
                    </div>
                </div>
            </div>
            <div className="preview-content">
                <div className="preview-schema">
                    <div className="schema-search">
                        <Icon icon="search" size={12} />
                        <input
                            type="text"
                            placeholder={copy.searchPlaceholder(columns.length)}
                            value={query}
                            onChange={(event) => setQuery(event.currentTarget.value)}
                        />
                    </div>
                    <div className="schema-list">
                        {filteredColumns.map((column) => (
                            <div key={column.key} className="schema-item">
                                <span className="schema-icon">{typeIcons[resolveTypeKey(column.type)] ?? '"'}</span>
                                <button
                                    type="button"
                                    className={`schema-name schema-name-btn ${selectedColumn === column.key ? 'is-selected' : ''}`}
                                    onClick={() => setSelectedColumn(column.key)}
                                    title={statsSummaryForColumn(column.key)}
                                    data-testid={`preview-column-${column.key}`}
                                >
                                    {column.key}
                                </button>
                                <span className="schema-type">{copy.formatType(column.type)}</span>
                            </div>
                        ))}
                    </div>
                    {selectedColumn && chartBuckets.length ? (
                        <div className="schema-chart" data-testid="preview-chart">
                            <div className="schema-chart-title">{selectedColumn}</div>
                            <div className="schema-chart-bars">
                                {chartBuckets.slice(0, 12).map((bucket) => (
                                    <div key={bucket.label} className="schema-chart-row">
                                        <div className="schema-chart-label" title={bucket.label}>
                                            {bucket.label}
                                        </div>
                                        <div className="schema-chart-bar-track">
                                            <div
                                                className="schema-chart-bar"
                                                style={{
                                                    width: `${chartMax ? Math.round((bucket.count / chartMax) * 100) : 0}%`,
                                                }}
                                            />
                                        </div>
                                        <div className="schema-chart-count">{bucket.count}</div>
                                    </div>
                                ))}
                            </div>
                        </div>
                    ) : null}
                </div>
                <div className="preview-grid">
                    <HTMLTable compact striped style={{ width: '100%', fontSize: '12px' }}>
                        <thead>
                            <tr>
                                <th>#</th>
                                {columns.map((column) => (
                                    <th key={column.key}>
                                        <div className="grid-header">
                                            <span>{column.key}</span>
                                            <span className="grid-type">{copy.formatType(column.type)}</span>
                                        </div>
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {rows.map((row, index) => (
                                <tr key={`${index}-${row.id ?? ''}`}>
                                    <td>{index + 1}</td>
                                    {columns.map((column) => (
                                        <td key={column.key}>{row[column.key] ?? ''}</td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </HTMLTable>
                </div>
            </div>
        </div>
    )
}
