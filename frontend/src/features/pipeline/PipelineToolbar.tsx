import { Icon } from '@blueprintjs/core'
import type { PipelineTool } from './pipelineTypes'

type PipelineViewMode = 'graph' | 'pseudocode'

type ToolbarCopy = {
    tools: string
    select: string
    remove: string
    layout: string
    focus: string
    showAll: string
    addDatasets: string
    parameters: string
    transform: string
    edit: string
    graphView: string
    pseudocodeView: string
}

type ToolbarProps = {
    activeTool: PipelineTool
    view: PipelineViewMode
    copy: ToolbarCopy
    onToolChange: (tool: PipelineTool) => void
    onViewChange: (view: PipelineViewMode) => void
    onLayout: () => void
    onFocus: () => void
    onShowAll: () => void
    canFocus: boolean
    focusActive: boolean
    onRemove: () => void
    onAddDatasets: () => void
    onParameters: () => void
    onTransform: () => void
    onAddGroupBy: () => void
    onAddAggregate: () => void
    onAddPivot: () => void
    onAddWindow: () => void
    onEdit: () => void
}

export const PipelineToolbar = ({
    activeTool,
    view,
    copy,
    onToolChange,
    onViewChange,
    onLayout,
    onFocus,
    onShowAll,
    canFocus,
    focusActive,
    onRemove,
    onAddDatasets,
    onParameters,
    onTransform,
    onAddGroupBy,
    onAddAggregate,
    onAddPivot,
    onAddWindow,
    onEdit,
}: ToolbarProps) => {
    return (
        <div className="pipeline-toolbar">
            <div className="toolbar-group">
                <button className={`toolbar-btn ${activeTool === 'tools' ? 'active' : ''}`} type="button" onClick={() => onToolChange('tools')}>
                    <Icon icon="locate" size={14} /> {copy.tools}
                </button>
                <button className={`toolbar-btn ${activeTool === 'select' ? 'active' : ''}`} type="button" onClick={() => onToolChange('select')}>
                    <Icon icon="select" size={14} /> {copy.select}
                </button>
                <button className={`toolbar-btn ${activeTool === 'remove' ? 'active' : ''}`} type="button" onClick={onRemove}>
                    <Icon icon="trash" size={14} /> {copy.remove}
                </button>
                <button className="toolbar-btn" type="button" onClick={onLayout}>
                    <Icon icon="layout-auto" size={14} /> {copy.layout}
                </button>
                <button className="toolbar-btn" type="button" onClick={onFocus} disabled={!canFocus}>
                    <Icon icon="eye-open" size={14} /> {copy.focus}
                </button>
                <button className="toolbar-btn" type="button" onClick={onShowAll} disabled={!focusActive}>
                    <Icon icon="refresh" size={14} /> {copy.showAll}
                </button>
            </div>

            <div className="toolbar-group">
                <button className="toolbar-btn primary" type="button" onClick={onAddDatasets}>
                    <Icon icon="add" size={14} /> {copy.addDatasets}
                </button>
                <button className="toolbar-btn" type="button" onClick={onParameters}>
                    <Icon icon="variable" size={14} /> {copy.parameters}
                </button>
            </div>

            <div className="toolbar-group">
                <button className="toolbar-btn accent" type="button" onClick={onTransform}>
                    <Icon icon="function" size={14} /> {copy.transform}
                </button>
                <button className="toolbar-btn secondary" type="button" onClick={onAddGroupBy}>
                    <Icon icon="grouped-bar-chart" size={14} /> GroupBy
                </button>
                <button className="toolbar-btn secondary" type="button" onClick={onAddAggregate}>
                    <Icon icon="chart" size={14} /> Aggregate
                </button>
                <button className="toolbar-btn secondary" type="button" onClick={onAddPivot}>
                    <Icon icon="pivot" size={14} /> Pivot
                </button>
                <button className="toolbar-btn secondary" type="button" onClick={onAddWindow}>
                    <Icon icon="timeline-line-chart" size={14} /> Window
                </button>
                <button className="toolbar-btn" type="button" onClick={onEdit}>
                    <Icon icon="edit" size={14} /> {copy.edit}
                </button>
            </div>

            <div className="toolbar-group">
                <button
                    className={`toolbar-btn ${view === 'graph' ? 'active' : ''}`}
                    data-testid="pipeline-view-graph"
                    type="button"
                    onClick={() => onViewChange('graph')}
                >
                    <Icon icon="diagram-tree" size={14} /> {copy.graphView}
                </button>
                <button
                    className={`toolbar-btn ${view === 'pseudocode' ? 'active' : ''}`}
                    data-testid="pipeline-view-pseudocode"
                    type="button"
                    onClick={() => onViewChange('pseudocode')}
                >
                    <Icon icon="code" size={14} /> {copy.pseudocodeView}
                </button>
            </div>
            <div className="toolbar-group toolbar-spacer" />
        </div>
    )
}
