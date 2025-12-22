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
}

type PreviewProps = {
    nodes: Array<{ id: string; title: string }>
    selectedNodeId?: string | null
    columns: PreviewColumn[]
    rows: PreviewRow[]
    copy: PreviewCopy
    onSelectNode: (nodeId: string) => void
    onZoomIn: () => void
    onZoomOut: () => void
    onToggleFullscreen: () => void
    onCollapse: () => void
    fullscreen?: boolean
}

export const PipelinePreview = ({
    nodes,
    selectedNodeId,
    columns,
    rows,
    copy,
    onSelectNode,
    onZoomIn,
    onZoomOut,
    onToggleFullscreen,
    onCollapse,
    fullscreen = false,
}: PreviewProps) => {
    const [query, setQuery] = useState('')
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

    return (
        <div className={`pipeline-preview ${fullscreen ? 'is-fullscreen' : ''}`}>
            <div className="preview-header">
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <Icon icon="search" size={14} />
                    <span className="preview-title">{copy.title}</span>
                </div>
                <div className="preview-controls">
                    <Button icon={fullscreen ? 'minimize' : 'fullscreen'} small minimal onClick={onToggleFullscreen} />
                    <Button icon="zoom-in" small minimal onClick={onZoomIn} />
                    <Button icon="zoom-out" small minimal onClick={onZoomOut} />
                    <Button icon="chevron-down" small minimal onClick={onCollapse} />
                </div>
            </div>
            <div className="preview-toolbar">
                <div className="preview-toolbar-left">
                    <span className="preview-toolbar-label">{copy.dataPreview}</span>
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
                                <span className="schema-name">{column.key}</span>
                                <span className="schema-type">{copy.formatType(column.type)}</span>
                            </div>
                        ))}
                    </div>
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
